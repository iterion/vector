use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::{Event, Value},
    sinks::util::{tcp::TcpSinkConfig, udp::UdpSinkConfig},
};

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: add back when serde-rs/serde#1358 is addressed
// #[serde(deny_unknown_fields)]
pub struct GelfSinkConfig {
    #[serde(flatten)]
    pub mode: Mode,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Mode {
    Tcp(TcpSinkConfig),
    Udp(GelfUdpConfig),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GelfUdpConfig {
    #[serde(flatten)]
    pub udp: UdpSinkConfig,
}

inventory::submit! {
    SinkDescription::new::<GelfSinkConfig>("statsd")
}

fn default_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12201)
}

impl GenerateConfig for GelfSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(&Self {
            mode: Mode::Udp(GelfUdpConfig {
                // batch: Default::default(),
                udp: UdpSinkConfig::from_address(default_address().to_string()),
            }),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "statsd")]
impl SinkConfig for GelfSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        match &self.mode {
            Mode::Tcp(config) => {
                let encode_event = move |event| encode_event(event).map(Into::into);
                config.build(cx, encode_event)
            }
            Mode::Udp(config) => {
                let encode_event = move |event| encode_event(event).map(Into::into);
                config.udp.build(cx, encode_event)
            }
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn sink_type(&self) -> &'static str {
        "gelf"
    }
}

fn encode_event(mut event: Event) -> Option<Vec<u8>> {
    let log = event.as_mut_log();

    let message = log.remove(crate::config::log_schema().message_key());
    let full_message = log.remove("full_message");
    let host = log
        .remove(crate::config::log_schema().host_key())
        .unwrap_or(Value::Bytes("vector".into()));
    let timestamp = log.remove(crate::config::log_schema().timestamp_key());
    let level = log.remove("level");

    let mut map = serde_json::map::Map::new();
    map.insert("version".to_string(), json!("1.1"));
    map.insert("host".to_string(), json!(host));
    map.insert("short_message".to_string(), json!(message));
    if let Some(Value::Integer(level)) = level {
        map.insert("level".to_string(), json!(level));
    }
    if let Some(Value::Bytes(bytes)) = full_message {
        map.insert("full_message".to_string(), json!(bytes));
    }

    if let Some(Value::Timestamp(ts)) = timestamp {
        map.insert(
            "timestamp".to_string(),
            json!(ts.timestamp_millis() as f64 / 1000.0),
        );
    }

    for (key, value) in log.all_fields() {
        let key = "_".to_owned() + &key;
        map.insert(key, json!(value));
    }

    // TODO emit a warning on fail
    match serde_json::to_vec(&map) {
        Ok(json) => Some(json),
        Err(_) => None,
    }
}

#[cfg(test)]
mod test {

    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use futures::{channel::mpsc, StreamExt, TryStreamExt};
    use futures_util::SinkExt;
    use tokio::net::UdpSocket;
    use tokio_util::{codec::BytesCodec, udp::UdpFramed};

    use super::*;
    use crate::{event::LogEvent, test_util::*};

    #[test]
    fn generate_gelf_config() {
        crate::test_util::test_generate_config::<GelfSinkConfig>();
    }

    #[tokio::test]
    async fn test_send_to_gelf() {
        trace_init();

        let addr = next_addr();
        // let mut batch = BatchConfig::default();
        // batch.max_bytes = Some(512);

        let config = GelfSinkConfig {
            mode: Mode::Udp(GelfUdpConfig {
                // batch,
                udp: UdpSinkConfig::from_address(addr.to_string()),
            }),
        };

        let context = SinkContext::new_test();
        let (sink, _healthcheck) = config.build(context).await.unwrap();

        let dt = Utc.ymd(2022, 1, 10).and_hms(8, 30, 9);
        let mut log1 = LogEvent::from("log1");
        log1.insert_flat("timestamp", dt);
        let mut log2 = LogEvent::from("log2");
        log2.insert_flat("timestamp", dt);
        let events = vec![Event::Log(log1), Event::Log(log2)];
        let (mut tx, rx) = mpsc::channel(0);

        let socket = UdpSocket::bind(addr).await.unwrap();
        tokio::spawn(async move {
            let mut stream = UdpFramed::new(socket, BytesCodec::new())
                .map_err(|error| error!(message = "Error reading line.", %error))
                .map_ok(|(bytes, _addr)| bytes.freeze());

            while let Some(Ok(item)) = stream.next().await {
                tx.send(item).await.unwrap();
            }
        });

        sink.run_events(events).await.unwrap();

        let messages = collect_n(rx, 1).await;
        assert_eq!(
            messages[0],
            Bytes::from("{\"version\":\"1.1\",\"host\":\"vector\",\"short_message\":\"log1\",\"timestamp\":1641803409.0}")
        );
    }
}
