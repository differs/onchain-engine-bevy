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
use rabbitcore::crypto::Hash;
use rabbitcore::game::{ActionInput, ActionKind};
use shanhai_core::battle::{Element, Team, Unit, resolve};

pub mod chain;

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
            .add_systems(Update, (deterministic_tick, sync_objects, chain_flow));
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

/// 客户端全链玩法流程（#4）：玩家本地钱包签名 ActionStart → ActionSettle → SellDrop。
/// 由 `SH_CHAIN_FLOW=1` 环境变量触发（无节点时跳过，骨架保持可运行）。
fn chain_flow(
    mut done: Local<bool>,
    chain: Res<ChainConfig>,
    handle: Res<ChainHandle>,
) {
    if *done || std::env::var("SH_CHAIN_FLOW").ok().as_deref() != Some("1") {
        return;
    }
    *done = true;
    info!("[chain-flow] starting full on-chain gameplay loop");
    match handle.rt.block_on(run_chain_flow(&chain.rpc_url, chain.token.clone())) {
        Ok(summary) => info!("[chain-flow] PASS: {summary}"),
        Err(e) => info!("[chain-flow] FAIL: {e}"),
    }
}

/// 全链玩法一把梭（客户端签名）：玩家打怪 → 结算 → 卖掉落。
/// 需要 devnet 已就绪 + testkit 水龙头（`--enable-time-travel`）。
pub async fn run_chain_flow(
    rpc_url: &str,
    token: Option<String>,
) -> Result<String, String> {
    let client = RpcClient::new(rpc_url, token.clone());
    let seed_bytes = hex::decode("11".repeat(32)).map_err(|e| format!("seed: {e}"))?;
    let player = chain::PlayerWallet::derive(&seed_bytes, 9001);
    // A：玩家原生余额自举（gas 真实扣除）
    let _ = client
        .fund_account(&format!("0x{}", hex::encode(player.address().as_bytes())), 10_000_000)
        .await
        .map_err(|e| format!("fund native: {e}"))?;
    // 国库 SHC 池：掉落变现资金（testkit 水龙头注入）
    let _ = client
        .fund_token(
            &format!("0x{}", hex::encode(rabbitcore::governance::treasury_address().as_bytes())),
            10_000,
            1,
        )
        .await
        .map_err(|e| format!("fund treasury: {e}"))?;
    let base_fee = fetch_base_fee(&client).await;
    let nonce = chrono::Utc::now().timestamp_millis() as u64;
    let session_id = format!("client-{}", nonce);
    let created_at = chrono::Utc::now().timestamp().saturating_sub(120).max(1) as u64;

    // 0) 链上怪物表（权威规则）：读不到则铸造默认 v1（Mint 免 gas，玩家签名）。
    //    battle 的 gate/executor 校验 session.rules_version == 链上配置版本。
    if let Err(_) = client
        .get_object(&format!(
            "0x{}",
            hex::encode(rabbitcore::game::monster_config_object_id().0.as_bytes())
        ))
        .await
    {
        info!("[chain-flow] monster config missing, minting default v1");
        let mint_cfg = chain::build_mint_monster_config_tx(&player, nonce, 10088);
        let _ = submit_and_wait(&mint_cfg, rpc_url, token.clone()).await?;
    }

    // 1) ActionStart：Mint session（玩家 owner）
    let action_type = ActionKind::Battle;
    let inputs = ActionInput::Battle {
        monster_id: "goblin".into(),
        team: vec![rabbitcore::game::TeamUnit { atk: 60, def: 20, hp: 500 }],
    };
    let session = rabbitcore::game::ActionSession {
        session_id: session_id.clone(),
        action_type: action_type.clone(),
        inputs: inputs.clone(),
        rules_version: 1,
        creator: format!("0x{}", hex::encode(player.address().as_bytes())),
        created_at_unix: created_at,
        settled: false,
        result: None,
    };
    let start = chain::build_action_start_tx(
        &player,
        &session_id,
        action_type.clone(),
        inputs.clone(),
        created_at,
        1,
        nonce,
        10088,
    );
    let _start_res = submit_and_wait(&start, rpc_url, token.clone()).await?;

    // 2) 随机揭晓：最新真实链块哈希（先承诺后揭晓，防挑 seed）
    let latest = client.latest_block().await.map_err(|e| format!("latest: {e}"))?;
    let block_hash = latest
        .get("hash")
        .and_then(|x| x.as_str())
        .ok_or_else(|| "latest block missing hash".to_string())?
        .to_string();
    let block_hash_bytes = hex::decode(block_hash.trim_start_matches("0x"))
        .map_err(|e| format!("block hash hex: {e}"))?;
    let seed = rabbitcore::game::derive_action_seed(
        &Hash::from_bytes(block_hash_bytes.as_slice().try_into().map_err(|_| "bad hash len")?),
        &session_id,
    );

    // 3) 本地确定性执行（真实战斗引擎，与节点同一函数）
    let outcome = chain::execute_locally(&session, seed).map_err(|e| format!("execute: {e}"))?;
    let drops = outcome.drops.clone();

    // 4) ActionSettle：Invoke 消费 session v1 → v2 + 掉落对象（玩家签名）
    let settle = chain::build_action_settle_tx(
        &player,
        &session,
        seed,
        &block_hash,
        outcome.result,
        drops,
        player.address(),
        nonce + 1,
        10088,
        base_fee,
    );
    let settle_res = submit_and_wait(&settle, rpc_url, token.clone()).await?;
    let settle_refs = settle_res
        .get("output_refs")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    // 5) SellDrop：变现掉落（goblin 必掉 goblin_coin，价 3 SHC）
    let mut sold = String::new();
    if let Some(drop) = outcome.drops.first() {
        let sell = chain::build_sell_drop_tx(
            &player,
            &session_id,
            &drop.item_id,
            nonce + 2,
            10088,
            base_fee,
        );
        let sell_res = submit_and_wait(&sell, rpc_url, token.clone()).await?;
        let status = sell_res.get("status").and_then(|x| x.as_str()).unwrap_or("?");
        sold = format!("{}x{}:{}", drop.item_id, drop.count, status);
    }

    Ok(format!(
        "start=OK settle_outputs={} sell=[{sold}] session={session_id} seed={seed} player=0x{}",
        settle_refs.len(),
        hex::encode(player.address().as_bytes())
    ))
}

/// 从 `rabbit_gasPrice` 读取 base fee（失败回退 devnet 初始值 1）。
async fn fetch_base_fee(client: &RpcClient) -> u64 {
    match client.gas_price().await {
        Ok(v) => v
            .get("base_fee")
            .and_then(|x| x.as_str())
            .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .or_else(|| v.get("base_fee_shc").and_then(|x| x.as_u64()))
            .unwrap_or(1),
        Err(_) => 1,
    }
}

/// BlockTime 提交 + 轮询：提交后轮询 `rabbit_getComputeTxResult` 直到真结果。
async fn submit_and_wait(
    tx: &rabbitcore::compute::ComputeTx,
    rpc_url: &str,
    token: Option<String>,
) -> Result<serde_json::Value, String> {
    let client = RpcClient::new(rpc_url, token);
    let value = rabbitcore::compute::spec_json::compute_tx_to_spec_json(tx);
    let queued = client.submit_compute_tx(&value).await.map_err(|e| e.to_string())?;
    if queued.get("queued").and_then(|x| x.as_bool()) != Some(true) {
        return Ok(queued);
    }
    let tx_id = format!("0x{}", hex::encode(tx.tx_id.0.as_bytes()));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if std::time::Instant::now() > deadline {
            return Err(format!("timed out waiting for block-time result of {tx_id}"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        match client.get_compute_tx_result(&tx_id).await {
            Ok(v) => {
                if v.get("queued").and_then(|x| x.as_bool()) != Some(true) {
                    return Ok(v);
                }
            }
            Err(e) => return Err(e.to_string()),
        }
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
