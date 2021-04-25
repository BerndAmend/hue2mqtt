use anyhow::{Context, Result};
use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::channel;

mod hue;
mod mqtt;

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    hue: hue::Config,
    mqtt: mqtt::Config,
}

#[derive(Debug)]
pub struct Message {
    topic: String,
    data: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let config: Config = serde_json::from_str(
        &std::fs::read_to_string("config.json").with_context(|| "Couldn't read config.json")?,
    )
    .with_context(|| "Couldn't parse config file")?;
    info!("hue2mqtt\nconfig: {:#?}", config);

    let (hue2mqtt_tx, hue2mqtt_rx) = channel(100); // hue -> mqtt
    let (mqtt2hue_tx, mqtt2hue_rx) = channel(100); // mqtt -> hue

    {
        let hue_config = config.hue;
        tokio::spawn(async move {
            hue::main(&hue_config, hue2mqtt_tx, mqtt2hue_rx).await;
        });
    }

    {
        let mqtt_config = config.mqtt;
        tokio::spawn(async move {
            mqtt::main(&mqtt_config, mqtt2hue_tx, hue2mqtt_rx).await;
        });
    }

    tokio::signal::ctrl_c().await?;

    Ok(())
}
