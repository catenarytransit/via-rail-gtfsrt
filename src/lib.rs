use gtfs_realtime::trip_update::StopTimeEvent;
use gtfs_realtime::trip_update::StopTimeUpdate;
use gtfs_realtime::FeedHeader;
use gtfs_realtime::FeedMessage;
use gtfs_realtime::{FeedEntity, Position, TripUpdate, VehiclePosition};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::time::UNIX_EPOCH;

#[derive(Debug, serde::Deserialize)]
struct TripSmall {
    pub route_id: String,
    pub trip_id: String,
    pub trip_short_name: String,
    pub direction_id: u8,
}

#[derive(Debug, serde::Deserialize)]
struct StopsSmall {
    pub stop_id: String,
    pub stop_code: String,
}

lazy_static! {
    static ref TRIP_NAME_HASHMAP: HashMap<String, TripSmall> = {
        let my_str = include_str!("../trips.csv");

        let mut rdr = csv::Reader::from_reader(my_str.as_bytes());

        let mut m = HashMap::new();

        for result in rdr.deserialize::<TripSmall>() {
            // The iterator yields Result<StringRecord, Error>, so we check the
            // error here.
            if let Ok(result) = result {
                m.insert(result.trip_short_name.clone(), result);
            }
        }
        m
    };

    static ref STOPS_NAME_TO_ID_HASHMAP: HashMap<String, String> = {
        let my_str = include_str!("../stops.csv");

        let mut rdr = csv::Reader::from_reader(my_str.as_bytes());

        let mut m = HashMap::new();

        for result in rdr.deserialize::<StopsSmall>() {
            // The iterator yields Result<StringRecord, Error>, so we check the
            // error here.
            if let Ok(result) = result {
                m.insert(result.stop_code, result.stop_id);
            }
        }
        m
    };
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
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EstimatedAndScheduled {
    estimated: Option<String>,
    scheduled: String,
}

pub async fn get_via_rail_gtfs_rt() -> Result<FeedMessage, Box<dyn Error + Sync + Send>> {
    let client = reqwest::Client::new();

    let mut headers = reqwest::header::HeaderMap::new();

    headers.insert(reqwest::header::USER_AGENT, "Catenary".parse().unwrap());
    let request = client
        .get("https://tsimobile.viarail.ca/data/allData.json")
        .headers(headers)
        .send()
        .await?;

    let text_via = request.text().await?;

    println!("{}", text_via);

    let via_traintable = serde_json::from_str::<HashMap<String, ViaTrainObject>>(&text_via)?;

    let mapped_objects = via_traintable
        .into_iter()
        .filter_map(|(id, obj)| {
            let first_time = chrono::DateTime::parse_from_rfc3339(obj.times[0].scheduled.as_str());

            match first_time {
                Ok(first_time) => {
                    let torono_time_of_start =
                        first_time.with_timezone(&chrono_tz::America::Toronto);

                    let start_date_gtfs_rt = format!("{}", torono_time_of_start.format("%Y%m%d"));

                    let trip_short_name: &str = id.split(' ').collect::<Vec<&str>>()[0];

                    let trip_data = TRIP_NAME_HASHMAP.get(trip_short_name);

                    match trip_data {
                        Some(trip_data) => {
                            let stop_sequence = obj
                                .times
                                .iter()
                                .map(|via_stop_time| StopTimeUpdate {
                                    stop_sequence: None,
                                    stop_id: STOPS_NAME_TO_ID_HASHMAP
                                        .get(&via_stop_time.code)
                                        .cloned(),
                                    arrival: via_stop_time
                                        .arrival
                                        .as_ref()
                                        .map(|arrival| {
                                            arrival.estimated.as_ref().map(|estimate_time| {
                                                StopTimeEvent {
                                                    time: Some(
                                                        chrono::DateTime::parse_from_rfc3339(
                                                            estimate_time.as_str(),
                                                        )
                                                        .unwrap()
                                                        .timestamp(),
                                                    ),
                                                    uncertainty: None,
                                                    delay: None,
                                                    scheduled_time: None,
                                                }
                                            })
                                        })
                                        .flatten(),
                                    departure: via_stop_time
                                        .departure
                                        .as_ref()
                                        .map(|departure| {
                                            departure.estimated.as_ref().map(|estimate_time| {
                                                StopTimeEvent {
                                                    time: Some(
                                                        chrono::DateTime::parse_from_rfc3339(
                                                            estimate_time.as_str(),
                                                        )
                                                        .unwrap()
                                                        .timestamp(),
                                                    ),
                                                    uncertainty: None,
                                                    delay: None,
                                                    scheduled_time: None,
                                                }
                                            })
                                        })
                                        .flatten(),
                                    departure_occupancy_status: None,
                                    schedule_relationship: None,
                                    stop_time_properties: None,
                                })
                                .collect::<Vec<StopTimeUpdate>>();

                            let trip_descriptor = gtfs_realtime::TripDescriptor {
                                direction_id: None,
                                trip_id: Some(trip_data.trip_id.clone()),
                                route_id: Some(trip_data.route_id.clone()),
                                start_date: Some(start_date_gtfs_rt),
                                start_time: None,
                                schedule_relationship: None,
                                modified_trip: None,
                            };

                            //println!("{}: {:#?}", id, obj);

                            Some(FeedEntity {
                                id,
                                is_deleted: None,
                                trip_update: Some(TripUpdate {
                                    trip: trip_descriptor.clone(),
                                    vehicle: None,
                                    stop_time_update: stop_sequence,
                                    timestamp: None,
                                    delay: None,
                                    trip_properties: None,
                                }),
                                vehicle: match obj.poll {
                                    Some(poll) => Some(VehiclePosition {
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
                                        timestamp: Some(
                                            chrono::DateTime::parse_from_rfc3339(poll.as_str())
                                                .unwrap()
                                                .timestamp()
                                                as u64,
                                        ),
                                    }),
                                    None => None,
                                },
                                alert: None,
                                shape: None,
                                stop: None,
                                trip_modifications: None,
                            })
                        }
                        None => None,
                    }
                }
                Err(_) => None,
            }
        })
        .collect::<Vec<FeedEntity>>();

    Ok(FeedMessage {
        entity: mapped_objects,
        header: FeedHeader {
            gtfs_realtime_version: "2.0".to_string(),
            incrementality: None,
            timestamp: Some(
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
            ),
            feed_version: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let via_rail_result = get_via_rail_gtfs_rt().await;
    }
}
