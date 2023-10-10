use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::env;
use std::net::IpAddr;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct OrchestratorManager {
    pub listener_port: u16,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub listener_address: IpAddr,
    pub orchestrator_manager: OrchestratorManager,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
        let settings = Config::builder()
            // default config
            // .add_source(File::with_name("node/config/default"))
            .add_source(File::with_name("config/node/default"))
            // .add_source(File::with_name(&format!("node/config/{}", run_mode)).required(false))
            .add_source(File::with_name(&format!("config/node/{}", run_mode)).required(false))
            .build()?;
        settings.try_deserialize()
    }
}
