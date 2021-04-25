use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use log::{debug, error, info, trace};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    address: String,
    username: String,
    polling_interval: u64,
}

#[derive(Debug, Clone)]
enum Type {
    Lights = 0,
    Groups = 1,
    Sensors = 2,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Command {
    #[serde(skip_deserializing)]
    on: bool,
    #[serde(skip_serializing)]
    state: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LightState {
    #[serde(skip_serializing)]
    on: bool,
    #[serde(skip_deserializing)]
    state: Option<String>,
    reachable: bool,
    #[serde(skip_serializing_if = "Option::is_none", rename(deserialize = "bri"))]
    brightness: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", rename(deserialize = "ct"))]
    color_temp: Option<i64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GroupState {
    #[serde(skip_serializing)]
    all_on: bool,
    #[serde(skip_serializing)]
    any_on: bool,
    #[serde(skip_deserializing)]
    state: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SensorState {
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename(deserialize = "lastupdated")
    )]
    last_seen: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    battery: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lightlevel: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dark: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    daylight: Option<bool>,
}

#[derive(Debug, Clone)]
struct IdType {
    id: String,
    t: Type,
}

#[derive(Debug)]
struct Mapping(HashMap<String, IdType>);

impl Mapping {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn add(&mut self, r#type: &Type, state: &Value) {
        if !state.is_object() {
            error!("state {:?} doesn't have the expected structure", r#type);
            return;
        }

        for (id, val) in state.as_object().unwrap() {
            if let Some(Some(name)) = val.get("name").map(|v| v.as_str()) {
                self.0.insert(
                    name.to_owned(),
                    IdType {
                        id: id.to_owned(),
                        t: r#type.clone(),
                    },
                );
            } else {
                error!("name is missing {} {}", id, val);
            }
        }
    }

    fn get(&self, name: &str) -> Option<&IdType> {
        self.0.get(name)
    }
}

fn hue_state_stream(config: &Config, r#type: &str) -> impl Stream<Item = serde_json::Value> {
    let config = config.clone();
    let r#type = r#type.to_owned();
    stream! {
        let mut last = None;
        loop {
            let data = match reqwest::get(format!(
                "http://{}/api/{}/{}",
                config.address, config.username, r#type
            ))
            .await
            {
                Ok(result) => match result.text().await {
                    Ok(text) => Some(text),
                    Err(err) => {
                        error!(
                            "error while retrieving the text for {} error: {}",
                            r#type, err
                        );
                        None
                    }
                },
                Err(err) => {
                    error!("error while requesting {} error: {}", r#type, err);
                    None
                }
            };
            if data.is_some() && last != data {
                last = data;
                match serde_json::from_str(&last.clone().unwrap()) {
                    Ok(json) => {
                        debug!("polling let to new {} data", r#type);
                        trace!("new {} data {:#?}", r#type, json);
                        yield json
                    },
                    Err(err) => error!("couldn't parse requested {} error: {}", r#type, err),
                }
            }
            tokio::time::sleep(Duration::from_millis(config.polling_interval)).await;
        }
    }
}

async fn set_hue_state(config: &Config, r#type: &Type, id: &str, state: &str) {
    let (type_string, cmd) = match r#type {
        Type::Lights => ("lights", "state"),
        Type::Groups => ("groups", "action"),
        _ => {
            error!("cannot set state for {}", id);
            return;
        }
    };

    let url = format!(
        "http://{}/api/{}/{}/{}/{}",
        config.address, config.username, type_string, id, cmd
    );

    debug!("url: {}", url);

    let client = reqwest::Client::new();
    match client.put(url).body(state.to_owned()).send().await {
        Ok(_) => {}
        Err(err) => {
            error!(
                "couldn't set hue state {} {} {} err: {}",
                cmd, id, state, err
            );
        }
    };
}

async fn publish(
    old_values: &mut HashMap<String, String>,
    tx: &Sender<Message>,
    topic: &str,
    value: &str,
) -> bool {
    let updated = match old_values.insert(topic.to_owned(), value.to_owned()) {
        Some(old) => old != value,
        None => true,
    };

    if updated {
        if let Err(err) = tx
            .send(Message {
                topic: topic.to_owned(),
                data: value.to_owned(),
            })
            .await
        {
            debug!("receiver dropped {}", err);
            false
        } else {
            true
        }
    } else {
        true
    }
}

pub async fn main(config: &Config, tx: Sender<Message>, mut rx: Receiver<Message>) {
    let lights = hue_state_stream(&config, "lights");
    let groups = hue_state_stream(&config, "groups");
    let sensors = hue_state_stream(&config, "sensors");
    pin_mut!(lights);
    pin_mut!(groups);
    pin_mut!(sensors);

    let mut old_values = HashMap::new();

    let mut mapping = Mapping::new();

    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                trace!("received {:?}", msg);
                if let Some(name) = msg.topic.split("/").nth(1) {
                    match serde_json::from_str::<Command>(&msg.data) {
                        Ok(mut obj) => {
                            match obj.state.as_str() {
                                "ON" => { obj.on = true;},
                                "OFF" => { obj.on = false;},
                                _ => {error!("command contained an invalid state {:#?}", msg); break;},
                            }
                            if let Some(IdType{t, id}) = mapping.get(name) {
                                set_hue_state(&config, t, id, &serde_json::to_string(&obj).unwrap()).await;
                            } else {
                                error!("unknown name {}", name);
                            }
                        },
                        Err(err) => {
                            error!("couldn't parse command {:#?} err: {}", msg, err);
                        }
                    }
                } else {
                    error!("received invalid command {:#?}", msg);
                }
            },
            Some(state) = lights.next() => {
                mapping.add(&Type::Lights, &state);

                for (id, val) in state.as_object().unwrap() {
                    if let Some(light_state) = val.get("state") {
                        match serde_json::from_value::<LightState>(light_state.to_owned()) {
                            Ok(mut obj) => {
                                obj.state = Some((if obj.on { "ON" } else { "OFF"}).to_owned());
                                if let Some(name) = val.get("name") {
                                    if let Some(name) = name.as_str() {
                                        if !publish(&mut old_values, &tx, name, &serde_json::to_string(&obj).unwrap()).await {
                                            return;
                                        }
                                    }
                            } else {
                                error!("name is missing {} {}", id, val);
                            }
                            },
                            Err(err) => {
                                error!("couldn't parse light_state {} {} err: {}", id, val, err);
                            }
                        }
                    } else {
                        error!("state is missing {} {}", id, val);
                    }
                }
            },
            Some(state) = groups.next() => {
                mapping.add(&Type::Groups, &state);

                for (id, val) in state.as_object().unwrap() {
                    if let Some(group_state) = val.get("state") {
                        match serde_json::from_value::<GroupState>(group_state.to_owned()) {
                            Ok(mut obj) => {
                                obj.state = (if obj.any_on { "PARTIAL" } else { "OFF"}).to_owned();
                                if obj.all_on {
                                    obj.state = "ON".to_owned();
                                }
                                if let Some(name) = val.get("name") {
                                    if let Some(name) = name.as_str() {
                                        if !publish(&mut old_values, &tx, name, &serde_json::to_string(&obj).unwrap()).await {
                                            return;
                                        }
                                    }
                                } else {
                                    error!("name is missing {} {}", id, val);
                                }
                            },
                            Err(err) => {
                                error!("couldn't parse group_state {} {} err: {}", id, val, err);
                            }
                        }
                    } else {
                        error!("state is missing {} {}", id, val);
                    }
                }
            },
            Some(state) = sensors.next() => {
                mapping.add(&Type::Sensors, &state);

                for (id, val) in state.as_object().unwrap() {
                    if let Some(type_obj) = val.get("type") {
                        match type_obj.as_str().unwrap_or_else(|| "") {
                            "ZHATemperature" | "ZLLTemperature" | "ZLLPresence" | "ZLLLightLevel" => {
                                if let Some(group_state) = val.get("state") {
                                    match serde_json::from_value::<SensorState>(group_state.to_owned()) {
                                        Ok(mut obj) => {
                                            obj.temperature = obj.temperature.map(|v| v / 100.0);
                                            if let Some(name) = val.get("name") {
                                                if let Some(name) = name.as_str() {
                                                    if !publish(&mut old_values, &tx, name, &serde_json::to_string(&obj).unwrap()).await {
                                                        return;
                                                    }
                                                }
                                            } else {
                                                error!("name is missing {} {}", id, val);
                                            }
                                        },
                                        Err(err) => {
                                            error!("couldn't parse group_state {} {} err: {}", id, val, err);
                                        }
                                    }
                                } else {
                                    error!("state is missing {} {}", id, val);
                                }
                            }
                            _ => (),
                        }
                    }
                }
            },
            else => break,
        }
    }

    debug!("Exit hue::main");
}
