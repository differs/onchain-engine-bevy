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
    if let Ok(chain_id) = std::env::var("SH_CHAIN_ID").map(|v| v.parse::<u64>().unwrap_or(10088)) {
        config.chain_id = chain_id;
    }
    if let Ok(ticks) = std::env::var("SH_SYNC_TICKS").map(|v| v.parse::<u32>().unwrap_or(60)) {
        config.sync_every_ticks = ticks;
    }

    // SH_CHAIN_FLOW=1 时：chain_flow 系统在后台 worker 跑完整链上流程，
    // 完成后广播 ChainFlowResult 并发送 AppExit，App 自动退出（供 client_e2e.sh 断言）。
    // 否则：骨架循环运行（确定性 tick + 对象同步）。
    // 统一用 run_loop：网络 IO 在 worker 线程，主循环 100ms 一帧只做非阻塞 poll。
    let runner = ScheduleRunnerPlugin::run_loop(Duration::from_millis(100));

    App::new()
        .add_plugins((
            NoopPluginGroup,
            LogPlugin::default(),
            runner,
        ))
        .add_plugins(RabbitChainPlugin::with_config(config))
        .run();
}
