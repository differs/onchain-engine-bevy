use std::time::Duration;

use bevy_app::{App, NoopPluginGroup, ScheduleRunnerPlugin};
use bevy_log::LogPlugin;
use shanhai_client::{ChainConfig, RabbitChainPlugin};

fn main() {
    let mut config = ChainConfig::default();
    if let Ok(url) = std::env::var("SH_NODE_RPC") {
        config.rpc_url = url;
    }
    if let Ok(token) = std::env::var("SH_RPC_TOKEN") {
        config.token = Some(token);
    }
    if let Ok(object_id) = std::env::var("SH_SYNC_OBJECT") {
        config.object_id = Some(object_id);
    }
    if let Ok(ticks) = std::env::var("SH_SYNC_TICKS").map(|v| v.parse::<u32>().unwrap_or(60)) {
        config.sync_every_ticks = ticks;
    }

    App::new()
        .add_plugins((
            NoopPluginGroup,
            LogPlugin::default(),
            ScheduleRunnerPlugin::run_loop(Duration::from_millis(100)),
        ))
        .add_plugins(RabbitChainPlugin::with_config(config))
        .run();
}
