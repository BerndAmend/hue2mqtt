use log::{debug, error, info, warn};
use paho_mqtt as mqtt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use crate::Message;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    url: String,
    prefix: String,
    retain: bool,
}

pub async fn main(config: &Config, tx: Sender<Message>, mut rx: Receiver<Message>) {
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(&config.url)
        .client_id(config.prefix.to_owned())
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .finalize();

    let mut cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        error!("Error creating the client: {:?}", e);
        std::process::exit(1);
    });

    let mut strm = cli.get_stream(25);

    let lwt = mqtt::Message::new(
        config.prefix.to_owned() + "/connected",
        "false",
        mqtt::QOS_1,
    );

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .keep_alive_interval(std::time::Duration::from_secs(30))
        .will_message(lwt.clone())
        .finalize();

    debug!("Connecting to the MQTT server...");
    if let Err(err) = cli.connect(conn_opts).await {
        error!("couldn't connect {}", err);
    } else {
        if let Err(err) = cli
            .subscribe(config.prefix.to_owned() + "/+/set", mqtt::QOS_1)
            .await
        {
            error!("couldn't subscribe {}", err);
        }
    }

    loop {
        tokio::select! {
            // handle outgoing messages
            Some(outgoing) = rx.recv() => {
                debug!("send message {:?}", outgoing);
                if let Err(err) = cli.publish(
                    mqtt::MessageBuilder::new()
                        .topic(config.prefix.to_owned() + "/" + &outgoing.topic)
                        .payload(outgoing.data)
                        .qos(1)
                        .retained(true)
                        .finalize(),
                ).await {
                    error!("couldn't send message {}", err);
                }
            },
            // handle incoming messages
            Some(incoming) = strm.next() => {
                if let Some(msg) = incoming {
                    debug!("received message {:?}", msg);
                    if let Err(err) = tx.send(Message {topic: msg.topic().to_owned(), data: msg.payload_str().to_string()}).await {
                        debug!("receiver dropped {}", err);
                        break;
                    }
                } else {
                    error!("Lost connection. Attempting reconnect.");
                    while let Err(err) = cli.reconnect().await {
                        error!("Error reconnecting: {}", err);
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                    if let Err(err) = cli.subscribe(config.prefix.to_owned() + "/+/set", mqtt::QOS_1).await {
                        error!("couldn't subscribe {}", err);
                    }
                }
            },
            else => break,
        }
    }

    // we have to publish the lwt if we shutdown cleanly
    if let Err(err) = cli.publish(lwt).await {
        error!("Couldn't publish last message {}", err);
    }
    debug!("Exit mqtt::main");
}
