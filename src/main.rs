use anyhow::{Context, Result};
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::{pin_mut, stream::StreamExt};
use log::{debug, error, info, trace};
use paho_mqtt as mqtt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HueConfig {
    address: String,
    username: String,
    polling_interval: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MqttConfig {
    url: String,
    prefix: String,
    retain: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    hue: HueConfig,
    mqtt: MqttConfig,
}

#[derive(Debug, Clone)]
enum Type {
    Lights = 0,
    Groups = 1,
    Sensors = 2,
}

type Mapping = HashMap<String, (String, Type)>;

fn add_mapping(mapping: &mut Mapping, id: &str, name: &str, t: Type) {
    mapping.insert(name.to_owned(), (id.to_owned(), t));
}

fn hue_state_stream(
    config: &HueConfig,
    client: reqwest::Client,
    r#type: &str,
    convert: fn(&Value) -> Option<String>,
) -> impl Stream<Item = (String, String, String)> {
    let config = config.clone();
    let r#type = r#type.to_owned();
    stream! {
        let mut last = None;
        loop {
            let data = match client.get(format!(
                "http://{}/api/{}/{}",
                config.address, config.username, r#type
            ))
            .send().await
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
                match serde_json::from_str::<Value>(&last.clone().unwrap()) {
                    Ok(json) => {
                        debug!("polling let to new {} data", r#type);
                        trace!("new {} data {:#?}", r#type, json);
                        for (id, val) in json.as_object().unwrap() {
                            if let Some(name) = val["name"].as_str() {
                                if let Some(v) = convert(val) {
                                    yield (id.to_owned(), name.to_owned(), v)
                                }
                            } else {
                                error!("id {} val {} does not contain a name", id, val);
                            }
                        }
                    },
                    Err(err) => error!("couldn't parse requested {} error: {}", r#type, err),
                }
            }
            tokio::time::sleep(Duration::from_millis(config.polling_interval)).await;
        }
    }
}

async fn set_hue_state(
    config: &HueConfig,
    client: reqwest::Client,
    r#type: &Type,
    id: &str,
    state: &str,
) {
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
    client: &mut mqtt::AsyncClient,
    old_values: &mut HashMap<String, String>,
    topic: &str,
    payload: &str,
) {
    let updated = match old_values.insert(topic.to_owned(), payload.to_owned()) {
        Some(old) => old != payload,
        None => true,
    };

    if updated {
        debug!("send message topic: {:?} payload: {}", topic, payload);
        if let Err(err) = client
            .publish(
                mqtt::MessageBuilder::new()
                    .topic(topic)
                    .payload(payload)
                    .qos(1)
                    .retained(true)
                    .finalize(),
            )
            .await
        {
            error!("couldn't send message {}", err);
        }
    }
}

fn convert_light(input: &Value) -> Option<String> {
    let mut out = serde_json::json!({
        "state": (if input["state"]["on"].as_bool() == Some(true) { "ON" } else { "OFF"}).to_owned(),
    });

    if let Some(v) = input["state"]["bri"].as_i64() {
        out["brightness"] = json!(v);
    }
    if let Some(v) = input["state"]["ct"].as_i64() {
        out["color_temp"] = json!(v);
    }
    if let Some(v) = input["state"]["reachable"].as_bool() {
        out["reachable"] = json!(v);
    }
    Some(out.to_string())
}

fn convert_group(input: &Value) -> Option<String> {
    Some(serde_json::json!({
        "state": if input["state"]["any_on"].as_bool() == Some(true) { if input["state"]["any_on"].as_bool() == Some(true) { "ON" } else { "PARTIAL"} } else { "OFF"},
    }).to_string())
}

fn convert_sensor(input: &Value) -> Option<String> {
    if let Some(type_obj) = input["type"].as_str() {
        match type_obj {
            "ZHATemperature" | "ZLLTemperature" | "ZLLPresence" | "ZLLLightLevel" => {}
            _ => return None,
        }
    }

    let mut out = json!({});

    if let Some(v) = input["state"]["lastupdated"].as_str() {
        let last_seen = if v.ends_with('Z') {
            v.to_owned()
        } else {
            v.to_owned() + "Z"
        };
        out["last_seen"] = json!(last_seen);
    }
    if let Some(v) = input["config"]["battery"].as_i64() {
        out["battery"] = json!(v);
    }
    if let Some(v) = input["state"]["presence"].as_bool() {
        out["presence"] = json!(v);
    }
    if let Some(v) = input["state"]["lightlevel"].as_i64() {
        out["lightlevel"] = json!(v);
    }
    if let Some(v) = input["state"]["dark"].as_bool() {
        out["dark"] = json!(v);
    }
    if let Some(v) = input["state"]["daylight"].as_bool() {
        out["daylight"] = json!(v);
    }
    if let Some(v) = input["state"]["temperature"].as_f64() {
        out["temperature"] = json!(v / 100.0);
    }
    Some(out.to_string())
}

async fn handle_incoming_message(
    msg: &mqtt::Message,
    mapping: &Mapping,
    config: &HueConfig,
    client: reqwest::Client,
) {
    match msg.topic().split("/").nth(1) {
        Some(name) => match serde_json::from_str::<serde_json::Value>(&msg.payload_str()) {
            Ok(val) => {
                let mut out = json!({});
                if let Some(v) = val["state"].as_str() {
                    match v {
                        "ON" => out["on"] = json!(true),
                        "OFF" => out["on"] = json!(false),
                        _ => error!("command contained an invalid state {:#?}", msg),
                    };
                }
                if let Some((id, t)) = mapping.get(name) {
                    set_hue_state(&config, client.clone(), t, id, &out.to_string()).await;
                } else {
                    error!("unknown name {}", name);
                }
            }
            Err(err) => error!("received invalid msg {:#?} err {}", msg, err),
        },
        None => error!("received invalid command {:#?}", msg),
    }
}

async fn on_connected(cli: &mut mqtt::AsyncClient, config: &MqttConfig) {
    let _ = cli
        .publish(mqtt::Message::new_retained(
            config.prefix.to_owned() + "/connected",
            "true",
            mqtt::QOS_1,
        ))
        .await;
    if let Err(err) = cli
        .subscribe(config.prefix.to_owned() + "/+/set", mqtt::QOS_1)
        .await
    {
        error!("couldn't subscribe {}", err);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let config: Config = serde_json::from_str(
        &std::fs::read_to_string("config.json").with_context(|| "Couldn't read config.json")?,
    )
    .with_context(|| "Couldn't parse config file")?;
    info!("hue2mqtt\nconfig: {:#?}", config);

    let client = reqwest::Client::new();

    let lights = hue_state_stream(&config.hue, client.clone(), "lights", convert_light);
    let groups = hue_state_stream(&config.hue, client.clone(), "groups", convert_group);
    let sensors = hue_state_stream(&config.hue, client.clone(), "sensors", convert_sensor);
    pin_mut!(lights);
    pin_mut!(groups);
    pin_mut!(sensors);

    let mut old_values = HashMap::new();
    let mut mapping = Mapping::new();

    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(&config.mqtt.url)
        .client_id(config.mqtt.prefix.to_owned())
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .finalize();

    let mut cli = mqtt::AsyncClient::new(create_opts)?;

    let mut strm = cli.get_stream(25);

    let lwt = mqtt::Message::new_retained(
        config.mqtt.prefix.to_owned() + "/connected",
        "false",
        mqtt::QOS_1,
    );

    debug!("Connecting to the MQTT server...");
    if let Err(err) = cli
        .connect(
            mqtt::ConnectOptionsBuilder::new()
                .mqtt_version(mqtt::MQTT_VERSION_5)
                .keep_alive_interval(std::time::Duration::from_secs(30))
                .will_message(lwt.clone())
                .finalize(),
        )
        .await
    {
        error!("couldn't connect {}", err);
    } else {
        on_connected(&mut cli, &config.mqtt).await;
    }

    loop {
        tokio::select! {
            Some(incoming) = strm.next() => {
                if let Some(msg) = incoming {
                    handle_incoming_message(&msg, &mapping, &config.hue, client.clone()).await;
                } else {
                    error!("Lost connection. Attempting reconnect.");
                    while let Err(err) = cli.reconnect().await {
                        error!("Error reconnecting: {}", err);
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                    on_connected(&mut cli, &config.mqtt).await;
                }
            },
            Some((id, name, val)) = lights.next() => {
                add_mapping(&mut mapping, &id, &name, Type::Lights);
                publish(&mut cli, &mut old_values, &(config.mqtt.prefix.to_owned() + "/" + &name), &val).await;
            },
            Some((id, name, val)) = groups.next() => {
                add_mapping(&mut mapping, &id, &name, Type::Groups);
                publish(&mut cli, &mut old_values, &(config.mqtt.prefix.to_owned() + "/" + &name), &val).await;
            },
            Some((id, name, val)) = sensors.next() => {
                add_mapping(&mut mapping, &id, &name, Type::Sensors);
                publish(&mut cli, &mut old_values, &(config.mqtt.prefix.to_owned() + "/" + &name), &val).await;
            },
            Ok(_) = tokio::signal::ctrl_c() => break,
            else => break,
        }
    }

    // we have to publish the lwt if we shutdown cleanly
    if let Err(err) = cli.publish(lwt).await {
        error!("Couldn't publish last message {}", err);
    }

    info!("bye");

    Ok(())
}
