use chrono::Timelike;
use gtfs_realtime::trip_update::StopTimeEvent;
use gtfs_realtime::trip_update::StopTimeUpdate;
use gtfs_realtime::FeedHeader;
use gtfs_realtime::FeedMessage;
use gtfs_realtime::{Alert, EntitySelector, FeedEntity, Position, TranslatedString, TripUpdate, VehiclePosition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::time::UNIX_EPOCH;

#[derive(Clone, Debug)]
pub struct GtfsViaRailResults {
    pub trip_updates: FeedMessage,
    pub vehicle_positions: FeedMessage,
    pub alerts: FeedMessage,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LocalizedText {
    pub en: String,
    pub fr: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ViaAlert {
    pub header: LocalizedText,
    pub description: LocalizedText,
    pub url: LocalizedText,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ViaTrainObject {
    departed: bool,
    arrived: bool,
    from: String,
    to: String,
    instance: String,
    speed: Option<i16>, //kmh
    lat: Option<f32>,
    lng: Option<f32>,
    direction: Option<f32>,
    times: Vec<ViaTrainRtStop>,
    poll: Option<String>,
    #[serde(rename = "pollMin")]
    poll_min: Option<i32>,
    #[serde(rename = "pollRadius")]
    poll_radius: Option<i32>,
    #[serde(default)]
    alerts: Vec<ViaAlert>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ViaTrainRtStop {
    station: String,
    code: String,
    estimated: String,
    scheduled: String,
    eta: String,
    arrival: Option<EstimatedAndScheduled>,
    departure: Option<EstimatedAndScheduled>,
    #[serde(default)]
    cancelled: bool,
    diff: Option<String>,
    #[serde(rename = "diffMin")]
    diff_min: Option<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EstimatedAndScheduled {
    estimated: Option<String>,
    scheduled: Option<String>,
}

struct TripCandidate<'a> {
    trip_id: &'a str,
    route_id: &'a str,
    trip: &'a gtfs_structures::Trip,
}

/// Find the best matching trip by comparing the realtime scheduled time against GTFS stop_times.
/// Returns (trip_id, route_id, start_date, time_difference_in_seconds).
fn find_best_trip_match<'a>(
    candidates: &[TripCandidate<'a>],
    rt_first_scheduled_time: &chrono::DateTime<chrono::FixedOffset>,
    print_debug: bool,
    trip_short_name: &str,
) -> Option<(&'a str, &'a str, String, i64)> {
    let rt_local = rt_first_scheduled_time.with_timezone(&chrono_tz::America::Toronto);
    let rt_date = rt_local.date_naive();
    let rt_seconds_from_midnight = rt_local.time().num_seconds_from_midnight();

    let mut best_match: Option<(&str, &str, String, i64)> = None;

    for candidate in candidates {
        let first_departure = candidate.trip.stop_times.first().and_then(|st| st.departure_time);

        if let Some(gtfs_departure_seconds) = first_departure {
            let diff = (rt_seconds_from_midnight as i64 - gtfs_departure_seconds as i64).abs();

            if print_debug {
                println!(
                    "  [{}] Candidate trip_id={}, gtfs_departure={}s, rt_departure={}s, diff={}s",
                    trip_short_name,
                    candidate.trip_id,
                    gtfs_departure_seconds,
                    rt_seconds_from_midnight,
                    diff
                );
            }

            let is_better = match &best_match {
                Some((_, _, _, best_diff)) => diff < *best_diff,
                None => true,
            };

            if is_better {
                let start_date = format!("{}", rt_date.format("%Y%m%d"));
                best_match = Some((candidate.trip_id, candidate.route_id, start_date, diff));
            }
        }
    }

    best_match
}

pub async fn get_via_rail_gtfs_rt(
    gtfs: &gtfs_structures::Gtfs,
) -> Result<GtfsViaRailResults, Box<dyn Error + Sync + Send>> {
    get_via_rail_gtfs_rt_internal(gtfs, false).await
}

pub async fn get_via_rail_gtfs_rt_debug(
    gtfs: &gtfs_structures::Gtfs,
) -> Result<GtfsViaRailResults, Box<dyn Error + Sync + Send>> {
    get_via_rail_gtfs_rt_internal(gtfs, true).await
}

fn convert_localized_text_to_translated_string(text: &LocalizedText) -> TranslatedString {
    TranslatedString {
        translation: vec![
            gtfs_realtime::translated_string::Translation {
                text: text.en.clone(),
                language: Some("en".to_string()),
            },
            gtfs_realtime::translated_string::Translation {
                text: text.fr.clone(),
                language: Some("fr".to_string()),
            },
        ],
    }
}

async fn get_via_rail_gtfs_rt_internal(
    gtfs: &gtfs_structures::Gtfs,
    print_debug: bool,
) -> Result<GtfsViaRailResults, Box<dyn Error + Sync + Send>> {
    // Build trip_short_name -> Vec<TripCandidate> lookup (1-to-many)
    let mut trip_name_hashmap: HashMap<String, Vec<TripCandidate>> = HashMap::new();
    for trip in gtfs.trips.values() {
        if let Some(short_name) = &trip.trip_short_name {
            trip_name_hashmap
                .entry(short_name.clone())
                .or_default()
                .push(TripCandidate {
                    trip_id: &trip.id,
                    route_id: &trip.route_id,
                    trip,
                });
        }
    }

    // Build stop_code -> stop_id lookup
    let stops_code_to_id: HashMap<&str, &str> = gtfs
        .stops
        .values()
        .filter_map(|stop| {
            stop.code.as_ref().map(|code| (code.as_str(), stop.id.as_str()))
        })
        .collect();

    let client = reqwest::Client::new();

    let mut headers = reqwest::header::HeaderMap::new();

    headers.insert(reqwest::header::USER_AGENT, "Catenary".parse().unwrap());
    let request = client
        .get("https://tsimobile.viarail.ca/data/allData.json")
        .headers(headers)
        .send()
        .await?;

    let text_via = request.text().await?;

    if print_debug {
        println!("{}", text_via);
    }

    let via_traintable = serde_json::from_str::<HashMap<String, ViaTrainObject>>(&text_via)?;

    let mut trip_updates: Vec<FeedEntity> = Vec::new();
    let mut vehicle_positions: Vec<FeedEntity> = Vec::new();
    let mut alerts: Vec<FeedEntity> = Vec::new();

    for (id, obj) in via_traintable.into_iter() {
        // Find the first valid scheduled time from departure
        let first_scheduled = obj.times.iter()
            .find_map(|stop| {
                stop.departure.as_ref()
                    .and_then(|dep| dep.scheduled.as_ref())
                    .filter(|s| !s.contains("mdash") && !s.is_empty())
                    .or_else(|| {
                        Some(&stop.scheduled).filter(|s| !s.contains("mdash") && !s.is_empty())
                    })
            });

        let first_time = first_scheduled.and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s).ok()
        });

        let first_time = match first_time {
            Some(t) => t,
            None => continue,
        };

        let trip_short_name: &str = id.split(' ').collect::<Vec<&str>>()[0];

        let candidates = match trip_name_hashmap.get(trip_short_name) {
            Some(c) => c,
            None => continue,
        };

        let (trip_id, route_id, start_date_gtfs_rt, diff) =
            match find_best_trip_match(candidates, &first_time, print_debug, trip_short_name) {
                Some(m) => m,
                None => continue,
            };

        if print_debug {
            println!(
                "Trip {}: Selected trip_id={}, start_date={}, schedule_diff={}s",
                trip_short_name, trip_id, start_date_gtfs_rt, diff
            );
        }

        let stop_time_updates: Vec<StopTimeUpdate> = obj
            .times
            .iter()
            .map(|via_stop_time| StopTimeUpdate {
                stop_sequence: None,
                stop_id: stops_code_to_id
                    .get(via_stop_time.code.as_str())
                    .map(|s| s.to_string()),
                arrival: via_stop_time
                    .arrival
                    .as_ref()
                    .and_then(|arrival| {
                        arrival.estimated.as_ref().and_then(|estimate_time| {
                            chrono::DateTime::parse_from_rfc3339(estimate_time.as_str())
                                .ok()
                                .map(|dt| StopTimeEvent {
                                    time: Some(dt.timestamp()),
                                    uncertainty: None,
                                    delay: None,
                                    scheduled_time: None,
                                })
                        })
                    }),
                departure: via_stop_time
                    .departure
                    .as_ref()
                    .and_then(|departure| {
                        departure.estimated.as_ref().and_then(|estimate_time| {
                            chrono::DateTime::parse_from_rfc3339(estimate_time.as_str())
                                .ok()
                                .map(|dt| StopTimeEvent {
                                    time: Some(dt.timestamp()),
                                    uncertainty: None,
                                    delay: None,
                                    scheduled_time: None,
                                })
                        })
                    }),
                departure_occupancy_status: None,
                schedule_relationship: if via_stop_time.cancelled {
                    Some(gtfs_realtime::trip_update::stop_time_update::ScheduleRelationship::Skipped as i32)
                } else {
                    None
                },
                stop_time_properties: None,
            })
            .collect();

        let trip_descriptor = gtfs_realtime::TripDescriptor {
            direction_id: None,
            trip_id: Some(trip_id.to_string()),
            route_id: Some(route_id.to_string()),
            start_date: Some(start_date_gtfs_rt.clone()),
            start_time: None,
            schedule_relationship: None,
            modified_trip: None,
        };

        // Trip update entity
        trip_updates.push(FeedEntity {
            id: format!("{}-tu", id),
            is_deleted: None,
            trip_update: Some(TripUpdate {
                trip: trip_descriptor.clone(),
                vehicle: None,
                stop_time_update: stop_time_updates,
                timestamp: None,
                delay: None,
                trip_properties: None,
            }),
            vehicle: None,
            alert: None,
            shape: None,
            stop: None,
            trip_modifications: None,
        });

        // Vehicle position entity
        if let Some(ref poll) = obj.poll {
            if let Ok(poll_dt) = chrono::DateTime::parse_from_rfc3339(poll.as_str()) {
                vehicle_positions.push(FeedEntity {
                    id: format!("{}-vp", id),
                    is_deleted: None,
                    trip_update: None,
                    vehicle: Some(VehiclePosition {
                        trip: Some(trip_descriptor.clone()),
                        vehicle: None,
                        position: match (obj.lat, obj.lng) {
                            (Some(lat), Some(lng)) => Some(Position {
                                latitude: lat,
                                longitude: lng,
                                bearing: obj.direction,
                                odometer: None,
                                speed: obj.speed.map(|kmh| kmh as f32 / 3.6),
                            }),
                            _ => None,
                        },
                        current_stop_sequence: None,
                        congestion_level: None,
                        occupancy_percentage: None,
                        occupancy_status: None,
                        multi_carriage_details: vec![],
                        stop_id: None,
                        current_status: None,
                        timestamp: Some(poll_dt.timestamp() as u64),
                    }),
                    alert: None,
                    shape: None,
                    stop: None,
                    trip_modifications: None,
                });
            }
        }

        // Generate alerts from ViaAlert data
        for (alert_idx, via_alert) in obj.alerts.iter().enumerate() {
            let informed_entity = EntitySelector {
                agency_id: None,
                route_id: Some(route_id.to_string()),
                trip: Some(trip_descriptor.clone()),
                route_type: None,
                stop_id: None,
                direction_id: None,
            };

            alerts.push(FeedEntity {
                id: format!("{}-alert-{}", id, alert_idx),
                is_deleted: None,
                trip_update: None,
                vehicle: None,
                alert: Some(Alert {
                    active_period: vec![],
                    informed_entity: vec![informed_entity],
                    cause: None,
                    effect: None,
                    url: Some(convert_localized_text_to_translated_string(&via_alert.url)),
                    header_text: Some(convert_localized_text_to_translated_string(&via_alert.header)),
                    description_text: Some(convert_localized_text_to_translated_string(&via_alert.description)),
                    tts_header_text: None,
                    tts_description_text: None,
                    severity_level: None,
                    image: None,
                    image_alternative_text: None,
                    cause_detail: None,
                    effect_detail: None,
                }),
                shape: None,
                stop: None,
                trip_modifications: None,
            });
        }
    }

    let header = FeedHeader {
        gtfs_realtime_version: "2.0".to_string(),
        incrementality: None,
        timestamp: Some(
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        ),
        feed_version: None,
    };

    Ok(GtfsViaRailResults {
        trip_updates: FeedMessage {
            entity: trip_updates,
            header: header.clone(),
        },
        vehicle_positions: FeedMessage {
            entity: vehicle_positions,
            header: header.clone(),
        },
        alerts: FeedMessage {
            entity: alerts,
            header,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let gtfs = gtfs_structures::Gtfs::from_url_async(
            "https://www.viarail.ca/sites/all/files/gtfs/viarail.zip"
        )
        .await
        .expect("Failed to load VIA Rail GTFS");

        let via_rail_result = get_via_rail_gtfs_rt_debug(&gtfs).await;
        println!("{:#?}", via_rail_result);
    }

    #[test]
    fn test_cancellation_parsing() {
        let json_data = r#"
        {
          "15": {
            "alerts": [],
            "departed": true,
            "arrived": false,
            "from": "HALIFAX",
            "to": "MONTRÉAL",
            "instance": "2026-01-23",
            "times": [
              {
                "station": "Halifax",
                "code": "HLFX",
                "estimated": "&mdash;",
                "scheduled": "2026-01-23T15:30:00Z",
                "eta": "&mdash;",
                "departure": {
                  "estimated": null,
                  "scheduled": "2026-01-23T15:30:00Z"
                },
                "cancelled": true
              }
            ]
          },
          "6": {
            "times": [
              {
                "station": "Cassiar Cannery",
                "code": "CASS",
                "estimated": "&mdash;",
                "scheduled": "2026-01-23T16:20:00Z",
                "eta": "&mdash;",
                "arrival": null,
                "departure": null
              }
            ],
            "departed": false,
            "arrived": false,
            "from": "PRINCE RUPERT",
            "to": "PRINCE GEORGE",
            "instance": "2026-01-23",
            "alerts": []
          }
        }
        "#;

        let via_traintable: HashMap<String, ViaTrainObject> =
            serde_json::from_str(json_data).expect("Failed to parse JSON");

        let train_15 = via_traintable.get("15").expect("Train 15 not found");
        let stop_15 = &train_15.times[0];
        assert_eq!(stop_15.code, "HLFX");
        assert!(stop_15.cancelled, "Train 15 stop should be cancelled");

        let train_6 = via_traintable.get("6").expect("Train 6 not found");
        let stop_6 = &train_6.times[0];
        assert_eq!(stop_6.code, "CASS");
        assert!(!stop_6.cancelled, "Train 6 stop should NOT be cancelled");
    }
}
