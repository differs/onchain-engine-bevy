//! 山海 RabbitChain 原生客户端骨架（bevy fork）。
//!
//! 本 crate 只依赖 bevy 的 app/ecs/log 子 crate（不依赖渲染），编译快、可脱离图形
//! 环境运行确定性逻辑；渲染层（bevy_sprite 等）后续按需加入。
//!
//! 确定性契约：所有随机性来自 `shanhai-core` 的 `Prng`，seed 由调用方显式给定
//! （生产环境由链上数据派生），保证服务端 / 链上执行器 / 客户端三方结果一致。
//! 网络层：`rabbit_client::RpcClient` 直连节点 JSON-RPC（COMPUTE_JSON_SPEC.md），
//! 本模块的 `sync_objects` 系统演示从链上拉取对象状态（如 Config 规则对象）。

use bevy_app::{App, Update};
use bevy_ecs::prelude::{Local, Res, ResMut, Resource};
use bevy_log::info;
use rabbit_client::RpcClient;
use shanhai_core::battle::{Element, Team, Unit, resolve};

/// 链连接配置（缺省指向本地 profile 节点，端口见 NETWORKS.md）。
#[derive(Resource, Clone)]
pub struct ChainConfig {
    pub rpc_url: String,
    pub token: Option<String>,
    /// 待同步的链上对象 id（None = 不拉取，如 `rabbit_getObject` 的参数）
    pub object_id: Option<String>,
    /// 每 N 个 tick 拉取一次链上对象
    pub sync_every_ticks: u32,
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8545".to_string(),
            token: None,
            object_id: None,
            sync_every_ticks: 60,
        }
    }
}

/// 确定性结算模拟器：记录最近一次本地结算（与链上/服务端同一套规则）。
#[derive(Resource, Default)]
pub struct Simulator {
    pub step: u64,
    pub last_seed: u64,
    pub last_winner: u8,
    pub last_rounds: u32,
}

/// 连接句柄：RpcClient + 本地 tokio runtime（bevy 系统内做阻塞式 RPC 调用）。
#[derive(Resource)]
pub struct ChainHandle {
    pub client: RpcClient,
    pub rt: tokio::runtime::Runtime,
}

pub struct RabbitChainPlugin {
    pub config: ChainConfig,
}

impl Default for RabbitChainPlugin {
    fn default() -> Self {
        Self {
            config: ChainConfig::default(),
        }
    }
}

impl RabbitChainPlugin {
    pub fn with_config(config: ChainConfig) -> Self {
        Self { config }
    }
}

impl bevy_app::Plugin for RabbitChainPlugin {
    fn build(&self, app: &mut App) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let handle = ChainHandle {
            client: RpcClient::new(&self.config.rpc_url, self.config.token.clone()),
            rt,
        };
        app.insert_resource(self.config.clone())
            .insert_resource(Simulator::default())
            .insert_resource(handle)
            .add_systems(Update, (deterministic_tick, sync_objects));
    }
}

fn demo_unit(id: u32, class: u8, element: Element) -> Unit {
    Unit {
        id,
        class,
        element,
        atk: 100,
        def: 30,
        hp: 1000,
        max_hp: 1000,
        spd: 10,
        crit_permille: 50,
    }
}

fn deterministic_tick(mut sim: ResMut<Simulator>, chain: Res<ChainConfig>) {
    sim.step += 1;
    let seed = sim.step.wrapping_mul(2_654_435_761);
    let a = Team {
        units: vec![demo_unit(1, 0, Element::Metal), demo_unit(2, 2, Element::Wood)],
    };
    let b = Team { units: vec![demo_unit(3, 0, Element::Fire)] };
    let r = resolve(&a, &b, seed);
    sim.last_seed = seed;
    sim.last_winner = r.winner;
    sim.last_rounds = r.total_rounds;
    info!(
        "shanhai_client: step={} seed={} winner={} rounds={} rpc={}",
        sim.step, seed, r.winner, r.total_rounds, chain.rpc_url
    );
}

/// 链上对象同步：周期拉取 `rabbit_getObject`，本地持有真实链上状态（防客户端伪造资产）。
fn sync_objects(
    mut tick: Local<u32>,
    chain: Res<ChainConfig>,
    handle: Res<ChainHandle>,
) {
    let Some(object_id) = &chain.object_id else {
        return;
    };
    *tick += 1;
    if *tick % chain.sync_every_ticks.max(1) != 0 {
        return;
    }
    match handle.rt.block_on(handle.client.get_object(object_id)) {
        Ok(v) => info!("[chain-sync] object={object_id} => {v}"),
        Err(e) => info!("[chain-sync] object={object_id} error={e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_is_deterministic() {
        let a = Team { units: vec![demo_unit(1, 0, Element::Metal)] };
        let b = Team { units: vec![demo_unit(2, 1, Element::Fire)] };
        let r1 = resolve(&a, &b, 42);
        let r2 = resolve(&a, &b, 42);
        assert_eq!(
            serde_json::to_string(&r1).unwrap(),
            serde_json::to_string(&r2).unwrap()
        );
        assert!(r1.winner == 0 || r1.winner == 1 || r1.winner == 2);
    }

    #[test]
    fn config_defaults_skip_sync() {
        let cfg = ChainConfig::default();
        assert!(cfg.object_id.is_none());
        assert_eq!(cfg.sync_every_ticks, 60);
    }
}
