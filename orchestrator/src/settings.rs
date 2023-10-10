use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::env;
use std::net::IpAddr;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Server {
    pub listener_port: u16,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct NodeManager {
    pub listener_port: u16,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Cli {
    pub listener_port: u16,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub listener_address: IpAddr,
    pub server: Server,
    pub node_manager: NodeManager,
    pub cli: Cli,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
        let settings = Config::builder()
            // default config
            // .add_source(File::with_name("orchestrator/config/default"))
            .add_source(File::with_name("config/orchestrator/default"))
            // .add_source(
            //     File::with_name(&format!("orchestrator/config/{}", run_mode)).required(false),
            // )
            .add_source(
                File::with_name(&format!("config/orchestrator/{}", run_mode)).required(false),
            )
            .build()?;
        settings.try_deserialize()
    }
}
