//! 山海 RabbitChain 原生客户端骨架（bevy fork）。
//!
//! 本 crate 只依赖 bevy 的 app/ecs/log 子 crate（不依赖渲染），编译快、可脱离图形
//! 环境运行确定性逻辑；渲染层（bevy_sprite 等）后续按需加入。
//!
//! 确定性契约：所有随机性来自 `shanhai-core` 的 `Prng`，seed 由调用方显式给定
//! （生产环境由链上数据派生），保证服务端 / 链上执行器 / 客户端三方结果一致。
//! 网络层：`rabbit_client::RpcClient` 直连节点 JSON-RPC（COMPUTE_JSON_SPEC.md）。
//!
//! IO 模型（2026-08 重构）：**网络 IO 与 ECS 帧解耦**。`spawn_worker` 在独立线程
//! 运行 tokio runtime，`ChainHandle` 只是请求通道；系统内**绝不** `block_on`，
//! 而是持有 `PendingRpc` / `ChainFlowTask` 每帧非阻塞 `poll()`，结果通过
//! bevy `Message`（`ObjectSynced` / `ChainFlowResult`）广播回 ECS。

use bevy_app::{App, AppExit, Update};
use bevy_ecs::message::{Message, MessageWriter};
use bevy_ecs::prelude::{Local, Res, ResMut, Resource};
use bevy_log::info;
use rabbit_client::{RpcClient, RpcError};
use rabbitcore::compute::ObjectId;
use rabbitcore::crypto::Hash;
use rabbitcore::game::{ActionInput, ActionKind, EnhanceConfig, MonsterTableConfig};
use serde_json::{Value, json};
use shanhai_core::battle::{Element, Team, Unit, resolve};
use tokio::sync::{mpsc, oneshot};

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
    /// 链 profile chain id（NETWORKS.md：local=31337 / testnet=10087 / devnet=10088 / mainnet=10086）。
    /// 所有交易 `chain_id` 字段的来源；由 `SH_CHAIN_ID` 环境变量覆盖。
    pub chain_id: u64,
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8545".to_string(),
            token: None,
            object_id: None,
            sync_every_ticks: 60,
            chain_id: 10088,
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

// ---------------------------------------------------------------------------
// 异步 IO 基础设施：独立 worker 线程跑 tokio runtime，系统只做非阻塞轮询。
// ---------------------------------------------------------------------------

/// 发给 worker 的请求。
enum ChainRequest {
    /// 单次 RPC 调用（方法名 + 参数，应答走 oneshot）。
    Rpc {
        method: String,
        params: Value,
        reply: oneshot::Sender<Result<Value, RpcError>>,
    },
    /// 完整链上玩法闭环（`run_chain_flow`），完成后回复汇总摘要。
    RunFlow {
        rpc_url: String,
        token: Option<String>,
        chain_id: u64,
        reply: oneshot::Sender<Result<String, String>>,
    },
}

/// 进行中的单次 RPC：系统内每帧 `poll()`，绝不阻塞。
pub struct PendingRpc {
    rx: oneshot::Receiver<Result<Value, RpcError>>,
}

impl PendingRpc {
    /// 非阻塞取结果：`None` = 未就绪；`Some(Ok/Err)` = 已完成。
    pub fn poll(&mut self) -> Option<Result<Value, RpcError>> {
        match self.rx.try_recv() {
            Ok(res) => Some(res),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(RpcError::BadResponse(
                "chain worker closed".into(),
            ))),
        }
    }
}

/// 进行中的完整链上玩法闭环：同上，帧内非阻塞轮询。
pub struct ChainFlowTask {
    rx: oneshot::Receiver<Result<String, String>>,
}

impl ChainFlowTask {
    pub fn poll(&mut self) -> Option<Result<String, String>> {
        match self.rx.try_recv() {
            Ok(res) => Some(res),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                Some(Err("chain worker closed".into()))
            }
        }
    }
}

/// 连接句柄：只是通往 worker 的请求通道（线程安全），系统不持有任何阻塞原语。
#[derive(Resource, Clone)]
pub struct ChainHandle {
    tx: mpsc::UnboundedSender<ChainRequest>,
}

impl ChainHandle {
    /// 发起一次异步 RPC；`None` = worker 已不可用。
    pub fn call(&self, method: impl Into<String>, params: Value) -> Option<PendingRpc> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(ChainRequest::Rpc {
                method: method.into(),
                params,
                reply,
            })
            .ok()?;
        Some(PendingRpc { rx })
    }

    /// 后台启动完整链上玩法闭环（玩家钱包签名打怪→结算→卖掉落）。
    pub fn spawn_chain_flow(
        &self,
        rpc_url: String,
        token: Option<String>,
        chain_id: u64,
    ) -> Option<ChainFlowTask> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(ChainRequest::RunFlow {
                rpc_url,
                token,
                chain_id,
                reply,
            })
            .ok()?;
        Some(ChainFlowTask { rx })
    }
}

/// 在独立线程启动 tokio runtime，消费请求队列。App 销毁时 channel 关闭，
/// worker 主循环自然退出（在途任务随 runtime drop 丢弃，无泄漏）。
fn spawn_worker(rpc_url: String, token: Option<String>) -> mpsc::UnboundedSender<ChainRequest> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ChainRequest>();
    std::thread::Builder::new()
        .name("chain-rpc-worker".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_io()
                .enable_time()
                .build()
                .expect("chain rpc worker tokio runtime");
            rt.block_on(async move {
                let client = RpcClient::new(rpc_url, token);
                while let Some(req) = rx.recv().await {
                    match req {
                        ChainRequest::Rpc { method, params, reply } => {
                            let client = client.clone();
                            tokio::spawn(async move {
                                let _ = reply.send(client.call(&method, params).await);
                            });
                        }
                        ChainRequest::RunFlow {
                            rpc_url,
                            token,
                            chain_id,
                            reply,
                        } => {
                            tokio::spawn(async move {
                                let _ = reply.send(run_chain_flow(&rpc_url, token, chain_id).await);
                            });
                        }
                    }
                }
            });
        })
        .expect("spawn chain rpc worker thread");
    tx
}

/// 链上对象同步完成的广播（`sync_objects` 系统发出）。
#[derive(Message, Debug, Clone)]
pub struct ObjectSynced {
    pub object_id: String,
    pub value: Value,
}

/// 完整链上玩法闭环的终态广播（`chain_flow` 系统发出）。
#[derive(Message, Debug, Clone)]
pub struct ChainFlowResult(pub Result<String, String>);

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
        // 独立线程 + tokio runtime 处理所有网络 IO；系统侧只轮询结果。
        let tx = spawn_worker(self.config.rpc_url.clone(), self.config.token.clone());
        app.insert_resource(self.config.clone())
            .insert_resource(Simulator::default())
            .insert_resource(ChainHandle { tx })
            .add_message::<ObjectSynced>()
            .add_message::<ChainFlowResult>()
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

/// 链上对象同步：非阻塞轮询 `rabbit_getObject`（后台 worker 执行，本系统只 poll）。
/// 请求在途时每帧检查一次，完成后发出 `ObjectSynced` 消息（本地持有真实链上状态，
/// 防客户端伪造资产）。
fn sync_objects(
    mut tick: Local<u32>,
    mut pending: Local<Option<PendingRpc>>,
    chain: Res<ChainConfig>,
    handle: Res<ChainHandle>,
    mut synced: MessageWriter<ObjectSynced>,
) {
    let Some(object_id) = &chain.object_id else {
        return;
    };
    if pending.is_none() {
        *tick += 1;
        if *tick % chain.sync_every_ticks.max(1) != 0 {
            return;
        }
        match handle.call("rabbit_getObject", json!([object_id])) {
            Some(p) => *pending = Some(p),
            None => {
                info!("[chain-sync] worker unavailable, retry next window");
                return;
            }
        }
    }
    if let Some(p) = pending.as_mut() {
        if let Some(res) = p.poll() {
            let _ = pending.take();
            match res {
                Ok(v) => {
                    info!("[chain-sync] object={object_id} => {v}");
                    synced.write(ObjectSynced {
                        object_id: object_id.clone(),
                        value: v,
                    });
                }
                Err(e) => info!("[chain-sync] object={object_id} error={e}"),
            }
        }
    }
}

/// 客户端全链玩法流程（#4）：玩家本地钱包签名 ActionStart → ActionSettle → SellDrop。
/// 由 `SH_CHAIN_FLOW=1` 环境变量触发：流程在后台 worker 执行，本系统每帧轮询，
/// 完成后广播 `ChainFlowResult` 并退出 App（无节点时骨架保持可运行）。
fn chain_flow(
    mut task: Local<Option<ChainFlowTask>>,
    chain: Res<ChainConfig>,
    handle: Res<ChainHandle>,
    mut result_msg: MessageWriter<ChainFlowResult>,
    mut exit: MessageWriter<AppExit>,
) {
    if std::env::var("SH_CHAIN_FLOW").ok().as_deref() != Some("1") {
        return;
    }
    if task.is_none() {
        match handle.spawn_chain_flow(chain.rpc_url.clone(), chain.token.clone(), chain.chain_id) {
            Some(t) => {
                info!("[chain-flow] spawning background full on-chain gameplay loop");
                *task = Some(t);
            }
            None => {
                info!("[chain-flow] FAIL: worker unavailable");
                result_msg.write(ChainFlowResult(Err("worker unavailable".into())));
                exit.write(AppExit::Success);
                return;
            }
        }
    }
    if let Some(t) = task.as_mut() {
        if let Some(result) = t.poll() {
            match &result {
                Ok(summary) => info!("[chain-flow] PASS: {summary}"),
                Err(e) => info!("[chain-flow] FAIL: {e}"),
            }
            result_msg.write(ChainFlowResult(result));
            // SH_CHAIN_FLOW=1 是"跑完即退"模式：闭环终结后结束 App。
            exit.write(AppExit::Success);
        }
    }
}

/// 全链玩法一把梭（客户端签名）：玩家打怪 → 结算 → 卖掉落。
/// 需要 devnet 已就绪 + testkit 水龙头（`--enable-time-travel`）。
pub async fn run_chain_flow(
    rpc_url: &str,
    token: Option<String>,
    chain_id: u64,
) -> Result<String, String> {
    let client = RpcClient::new(rpc_url, token.clone());
    let seed_bytes = hex::decode("11".repeat(32)).map_err(|e| format!("seed: {e}"))?;
    let player = chain::PlayerWallet::derive(&seed_bytes, 9001);
    // A：玩家原生余额自举（gas 真实扣除）
    client
        .fund_account(&format!("0x{}", hex::encode(player.address().as_bytes())), 10_000_000)
        .await
        .map_err(|e| format!("fund native: {e}"))?;
    // 国库 SHC 池：掉落变现资金（testkit 水龙头注入）
    client
        .fund_token(
            &format!("0x{}", hex::encode(rabbitcore::governance::treasury_address().as_bytes())),
            10_000,
            1,
        )
        .await
        .map_err(|e| format!("fund treasury: {e}"))?;
    let base_fee = fetch_base_fee(&client).await?;
    let nonce = next_nonce();
    let session_id = format!("client-{nonce}");
    let created_at = chrono::Utc::now().timestamp().saturating_sub(120).max(1) as u64;

    // 0) 链上怪物表（权威规则）：`rabbit_getObject` 对缺失对象返回 null → mint 默认 v1
    //    （Mint 免 gas，玩家签名）；网络错误 → fail-loud（不静默 mint，避免掩盖故障）。
    let monsters_cfg = match client
        .get_object(&object_id_hex(rabbitcore::game::monster_config_object_id()))
        .await
    {
        Ok(v) if v.is_null() => {
            info!("[chain-flow] monster config missing, minting default v1");
            let mint_cfg = chain::build_mint_monster_config_tx(&player, nonce, chain_id);
            let _ = submit_and_wait(&mint_cfg, rpc_url, token.clone()).await?;
            MonsterTableConfig::default()
        }
        Ok(v) => parse_config_obj::<MonsterTableConfig>(&v, "monster config")?,
        Err(e) => return Err(format!("get monster config: {e}")),
    };
    // 强化配置：同样以链上对象为权威（缺失用默认 v1；网络错误 fail-loud）。
    let enhance_cfg = match client
        .get_object(&object_id_hex(rabbitcore::game::enhance_config_object_id()))
        .await
    {
        Ok(v) if v.is_null() => EnhanceConfig::default(),
        Ok(v) => parse_config_obj::<EnhanceConfig>(&v, "enhance config")?,
        Err(e) => return Err(format!("get enhance config: {e}")),
    };
    // rules_version 绑定链上配置版本：gate/executor 校验
    // `session.rules_version == 链上配置版本`，不一致直接拒绝。
    let rules_version = monsters_cfg.version;

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
        rules_version,
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
        rules_version,
        nonce,
        chain_id,
    );
    let _start_res = submit_and_wait(&start, rpc_url, token.clone()).await?;

    // 2) 随机揭晓：latest-2 稳定块哈希（先承诺后揭晓，防挑 seed）。
    //    不用 latest：快速矿工可在同一高度重挖覆盖，gate 按 hash 查块会找不到。
    let (block_hash, seed) = fetch_stable_random_block(&client, &session_id).await?;

    // 3) 本地确定性执行（真实战斗引擎 + 链上配置，与节点同一函数）
    let outcome =
        chain::execute_locally(&session, seed, &enhance_cfg, &monsters_cfg).map_err(|e| {
            format!("execute: {e}")
        })?;
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
        chain_id,
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
            chain_id,
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

/// 逻辑对象 id → `rabbit_getObject` 参数（0x 前缀 hex）。
fn object_id_hex(id: ObjectId) -> String {
    format!("0x{}", hex::encode(id.0.as_bytes()))
}

/// 解析 `rabbit_getObject` 返回：`state` = hex(serde_json bytes)（对象序列化格式）。
/// 解析失败 fail-loud（对象损坏不应静默回退默认配置，否则本地执行与链上重算不一致）。
fn parse_config_obj<T: serde::de::DeserializeOwned>(v: &Value, what: &str) -> Result<T, String> {
    let state = v
        .get("state")
        .and_then(|x| x.as_str())
        .ok_or_else(|| format!("{what}: missing state"))?;
    let bytes = hex::decode(state.trim_start_matches("0x"))
        .map_err(|e| format!("{what}: state hex decode: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("{what}: state parse: {e}"))
}

/// 取**稳定块**作为动作随机源（与服务端 `settle.rs::fetch_stable_random_block` 同模式）：
/// `latest-2` 已确认、不会被同高度重写，且时间戳仍 >= session.created_at
/// （创建回拨 120s），先承诺后揭晓语义不变。返回 (block_hash_hex, seed)。
async fn fetch_stable_random_block(
    client: &RpcClient,
    session_id: &str,
) -> Result<(String, u64), String> {
    let latest = client.latest_block().await.map_err(|e| format!("latest: {e}"))?;
    let latest_number = latest
        .get("number")
        .and_then(|x| x.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0);
    let stable_number = latest_number.saturating_sub(2).max(1);
    let stable = client
        .get_block_by_number(stable_number)
        .await
        .map_err(|e| format!("stable block {stable_number}: {e}"))?;
    if stable.is_null() {
        // 节点按高度查不到该块（高度不足或已从热存储淘汰）——fail-loud，不挑块。
        return Err(format!("stable block {stable_number} not found"));
    }
    let block_hash_hex = stable
        .get("hash")
        .and_then(|x| x.as_str())
        .ok_or_else(|| "stable block missing hash".to_string())?
        .to_string();
    let block_hash_bytes = hex::decode(block_hash_hex.trim_start_matches("0x"))
        .map_err(|e| format!("block hash hex: {e}"))?;
    let block_hash = Hash::from_bytes(
        block_hash_bytes
            .as_slice()
            .try_into()
            .map_err(|_| "bad block hash len".to_string())?,
    );
    let seed = rabbitcore::game::derive_action_seed(&block_hash, session_id);
    Ok((block_hash_hex, seed))
}

/// 并发安全 nonce：毫秒时间戳 << 16 | 自增序号。
/// 同一毫秒内最多 65536 个不重复 nonce，多会话并发不碰撞。
fn next_nonce() -> u64 {
    static NONCE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    (ms << 16) | (NONCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0xFFFF)
}

/// 从 `rabbit_gasPrice` 读取 base fee。失败 fail-loud：静默回退 1 会低估
/// `max_fee`，导致交易在区块执行时被拒（费用不足），报错误导。
async fn fetch_base_fee(client: &RpcClient) -> Result<u64, String> {
    let v = client.gas_price().await.map_err(|e| format!("gas price: {e}"))?;
    v.get("base_fee")
        .and_then(|x| x.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .or_else(|| v.get("base_fee_shc").and_then(|x| x.as_u64()))
        .ok_or_else(|| format!("gas price response missing base_fee: {v}"))
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
        assert_eq!(cfg.chain_id, 10088);
    }

    #[test]
    fn parse_config_obj_roundtrips_monster_table() {
        let cfg = MonsterTableConfig::default();
        let state = serde_json::to_vec(&cfg).unwrap();
        let obj = serde_json::json!({
            "object_id": "0xab",
            "state": format!("0x{}", hex::encode(&state)),
        });
        let parsed: MonsterTableConfig = parse_config_obj(&obj, "monster config").expect("parse");
        assert_eq!(parsed, cfg);
        // 缺失 state 或损坏 → fail-loud
        assert!(parse_config_obj::<MonsterTableConfig>(&json!({}), "monster config").is_err());
        assert!(parse_config_obj::<MonsterTableConfig>(
            &json!({"state": "0xzz"}),
            "monster config"
        )
        .is_err());
    }

    #[test]
    fn next_nonce_is_unique_across_calls() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let n = next_nonce();
            assert!(seen.insert(n), "nonce {n} 重复");
        }
    }

    #[test]
    fn pending_rpc_returns_none_while_in_flight() {
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut pending = PendingRpc { rx };
        // 发送者存活但未应答 → 未就绪（系统每帧 poll，不阻塞）
        assert!(pending.poll().is_none());
    }

    #[test]
    fn pending_rpc_surfaces_result_when_answered() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut pending = PendingRpc { rx };
        assert!(pending.poll().is_none());
        let _ = tx.send(Ok(serde_json::json!({"ok": true})));
        let result = pending.poll().expect("answered");
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["ok"], true);
    }

    #[test]
    fn pending_rpc_surfaces_closed_channel() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        drop(tx); // worker 死亡 = 通道关闭
        let mut pending = PendingRpc { rx };
        let result = pending.poll().expect("closed channel should yield a result");
        assert!(result.is_err());
    }

    #[test]
    fn chain_flow_task_polls_to_completion() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut task = ChainFlowTask { rx };
        assert!(task.poll().is_none());
        let _ = tx.send(Ok("start=OK settle_outputs=2".into()));
        let result = task.poll().expect("answered");
        assert_eq!(result.unwrap(), "start=OK settle_outputs=2");
    }
}
