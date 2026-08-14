//! 客户端直连链：玩家在客户端本地用真实战斗引擎跑 `ActionStart → ActionSettle → SellDrop`。
//!
//! 与 `shanhai-server` 同一套确定性规则（`rabbitcore::game::execute_action`），
//! 但交易由**玩家钱包**在客户端本地构建并签名（非托管：私钥不出客户端）。
//! 流程：
//!   1. `ActionStart`：Mint 铸造 session 对象（输入承诺绑定，玩家为 owner）
//!   2. 取最新真实链块 → `derive_action_seed`（先承诺后揭晓的随机源）
//!   3. 本地确定性执行（打怪/强化）→ 声称结果 + 掉落
//!   4. `ActionSettle`：Invoke 消费 session v1 → session v2（settled）+ 掉落对象
//!   5. `SellDrop`：Invoke 消耗掉落对象 v1 → 国库 SHC → 玩家
//!
//! 节点 `gate_game_tx` + `StateExecutor` 会重算校验每一步（防伪造），
//! 客户端本地结果必须与链上重算一致，否则被拒。

use rabbitcore::compute::{
    Command, ComputeTx, ObjectId, OutputId, OutputProposal, Ownership, Script, TxId, TxSignature,
    TxWitness, Version, GAME_DOMAIN,
};
use rabbitcore::crypto::{Address, Hash, keccak256};
use rabbitcore::game::{ActionDrop, ActionInput, ActionKind, GameOp};

/// 玩家钱包：从种子确定性派生 ed25519 密钥（与 shanhai-server `wallet.rs` 同算法，
/// 使链上对象 owner = 玩家地址，私钥只在客户端持有）。
pub struct PlayerWallet {
    signing_key: ed25519_dalek::SigningKey,
    address: Address,
}

impl PlayerWallet {
    /// keccak256(master_seed ‖ "player" ‖ player_id) → 32 字节私钥。
    pub fn derive(master_seed: &[u8], player_id: u64) -> Self {
        let mut data = Vec::with_capacity(master_seed.len() + 8 + 8);
        data.extend_from_slice(master_seed);
        data.extend_from_slice(b"player");
        data.extend_from_slice(&player_id.to_le_bytes());
        let hash = keccak256(&data);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&hash);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&bytes);
        let public_key = signing_key.verifying_key().to_bytes();
        let hash = keccak256(&public_key);
        let address = Address::from_slice(&hash[12..]).expect("player address");
        Self { signing_key, address }
    }

    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn signing_key(&self) -> &ed25519_dalek::SigningKey {
        &self.signing_key
    }
}

/// 输出 id = keccak(object_id ‖ version)（与节点/服务端一致）。
pub fn output_id_for(object_id: &ObjectId, version: u64) -> OutputId {
    let mut data = Vec::with_capacity(40);
    data.extend_from_slice(object_id.0.as_bytes());
    data.extend_from_slice(&version.to_be_bytes());
    OutputId(Hash::from_bytes(keccak256(&data)))
}

/// 玩家自有对象提案（owner = 玩家地址，花费需玩家签名）。
pub fn proposal_owned(
    owner: Address,
    object_id: ObjectId,
    predecessor: Option<OutputId>,
    version: u64,
    state: Vec<u8>,
) -> OutputProposal {
    OutputProposal {
        output_id: output_id_for(&object_id, version),
        object_id,
        domain_id: GAME_DOMAIN,
        kind: rabbitcore::compute::ObjectKind::State,
        owner: Ownership::Address(owner),
        predecessor,
        version: Version(version),
        state,
        state_root: None,
        resources: vec![],
        lock: Script::default(),
        logic: None,
        created_at: 0,
        ttl: None,
        rent_reserve: None,
        flags: 0,
        extensions: vec![],
    }
}

/// 玩家 ed25519 签名 + 赋规范 tx_id。
pub fn sign_tx_with(mut tx: ComputeTx, signer: &ed25519_dalek::SigningKey) -> ComputeTx {
    use ed25519_dalek::Signer as _;
    let signature = signer.sign(&tx.signing_preimage()).to_bytes();
    let public_key = signer.verifying_key().to_bytes();
    tx.witness.signatures = vec![TxSignature::ed25519(signature, public_key)];
    tx.with_expected_tx_id()
}

/// 填 EIP-1559 fee 字段：gas_limit = 节点估算值，max_fee = 预算（priority 0）。
/// 必须签名前设置（fee 字段参与签名 preimage）。
pub fn apply_fee_fields(tx: &mut ComputeTx, base_fee: u64) {
    let mut probe = tx.clone();
    probe.witness.signatures =
        vec![TxSignature::ed25519([0u8; 64], [0u8; 32]); 1];
    let gas_limit = rabbitcore::compute::estimate_tx_gas(&probe).max(21_000);
    tx.max_fee = gas_limit.saturating_mul(base_fee);
    tx.priority_fee = 0;
    tx.gas_limit = gas_limit;
}

/// ActionStart：Mint 铸造 session 对象（玩家为 creator + owner；Mint 免 gas）。
pub fn build_action_start_tx(
    player: &PlayerWallet,
    session_id: &str,
    action_type: ActionKind,
    inputs: ActionInput,
    created_at_unix: u64,
    rules_version: u64,
    nonce: u64,
    chain_id: u64,
) -> ComputeTx {
    let object_id = rabbitcore::game::action_session_object_id(session_id);
    let op = GameOp::ActionStart {
        session_id: session_id.to_string(),
        action_type: action_type.clone(),
        inputs: inputs.clone(),
        rules_version,
        creator: format!("0x{}", hex::encode(player.address().as_bytes())),
        created_at_unix,
    };
    let session = rabbitcore::game::ActionSession {
        session_id: session_id.to_string(),
        action_type,
        inputs,
        rules_version,
        creator: format!("0x{}", hex::encode(player.address().as_bytes())),
        created_at_unix,
        settled: false,
        result: None,
    };
    let state = serde_json::to_vec(&session).expect("action session serialization");
    let tx = ComputeTx {
        tx_id: TxId(Hash::zero()),
        domain_id: GAME_DOMAIN,
        command: Command::Mint,
        input_set: vec![],
        read_set: vec![],
        output_proposals: vec![proposal_owned(
            *player.address(),
            object_id,
            None,
            1,
            state,
        )],
        fee: 0,
        nonce: Some(nonce),
        metadata: vec![],
        payload: serde_json::to_vec(&op).expect("game op serialization"),
        deadline_unix_secs: None,
        chain_id: Some(chain_id),
        network_id: Some(chain_id as u32),
        witness: TxWitness { signatures: vec![], threshold: None },
        max_fee: 0,
        priority_fee: 0,
        gas_limit: 0,
    };
    sign_tx_with(tx, player.signing_key())
}

/// ActionSettle：Invoke 消费 session v1 → session v2（settled）+ 掉落对象。
pub fn build_action_settle_tx(
    player: &PlayerWallet,
    session: &rabbitcore::game::ActionSession,
    seed: u64,
    random_block_hash: &str,
    claimed: serde_json::Value,
    drops: Vec<ActionDrop>,
    drop_owner: &Address,
    nonce: u64,
    chain_id: u64,
    base_fee: u64,
) -> ComputeTx {
    let object_id = rabbitcore::game::action_session_object_id(&session.session_id);
    let session_input = output_id_for(&object_id, 1);
    let op = GameOp::ActionSettle {
        session_id: session.session_id.clone(),
        seed,
        random_block_hash: random_block_hash.to_string(),
        claimed: claimed.clone(),
        drops: drops.clone(),
    };
    let mut settled = session.clone();
    settled.settled = true;
    settled.result = Some(claimed);
    let mut output_proposals = vec![proposal_owned(
        *player.address(),
        object_id,
        Some(session_input),
        2,
        serde_json::to_vec(&settled).expect("settled session serialization"),
    )];
    for d in &drops {
        let drop_state = serde_json::json!({
            "kind": "action_drop",
            "session_id": session.session_id,
            "item_id": d.item_id,
            "count": d.count,
            "price_shc": d.price_shc,
        })
        .to_string()
        .into_bytes();
        output_proposals.push(proposal_owned(
            *drop_owner,
            rabbitcore::game::action_drop_object_id(&session.session_id, &d.item_id),
            None,
            1,
            drop_state,
        ));
    }
    let mut tx = ComputeTx {
        tx_id: TxId(Hash::zero()),
        domain_id: GAME_DOMAIN,
        command: Command::Invoke,
        input_set: vec![session_input],
        read_set: vec![],
        output_proposals,
        fee: 0,
        nonce: Some(nonce),
        metadata: vec![],
        payload: serde_json::to_vec(&op).expect("game op serialization"),
        deadline_unix_secs: None,
        chain_id: Some(chain_id),
        network_id: Some(chain_id as u32),
        witness: TxWitness { signatures: vec![], threshold: None },
        max_fee: 0,
        priority_fee: 0,
        gas_limit: 0,
    };
    apply_fee_fields(&mut tx, base_fee);
    sign_tx_with(tx, player.signing_key())
}

/// SellDrop：Invoke 消耗掉落对象 v1，国库 SHC → 玩家（玩家签名）。
pub fn build_sell_drop_tx(
    player: &PlayerWallet,
    session_id: &str,
    item_id: &str,
    nonce: u64,
    chain_id: u64,
    base_fee: u64,
) -> ComputeTx {
    let object_id = rabbitcore::game::action_drop_object_id(session_id, item_id);
    let drop_input = output_id_for(&object_id, 1);
    let op = GameOp::SellDrop {
        session_id: session_id.to_string(),
        item_id: item_id.to_string(),
    };
    // 收据输出 v2（无输出 Invoke 过不了 basic_sanity_check）
    let receipt_state = serde_json::json!({
        "kind": "action_drop_redeemed",
        "session_id": session_id,
        "item_id": item_id,
    })
    .to_string()
    .into_bytes();
    let mut tx = ComputeTx {
        tx_id: TxId(Hash::zero()),
        domain_id: GAME_DOMAIN,
        command: Command::Invoke,
        input_set: vec![drop_input],
        read_set: vec![],
        output_proposals: vec![proposal_owned(
            *player.address(),
            object_id,
            Some(drop_input),
            2,
            receipt_state,
        )],
        fee: 0,
        nonce: Some(nonce),
        metadata: vec![],
        payload: serde_json::to_vec(&op).expect("game op serialization"),
        deadline_unix_secs: None,
        chain_id: Some(chain_id),
        network_id: Some(chain_id as u32),
        witness: TxWitness { signatures: vec![], threshold: None },
        max_fee: 0,
        priority_fee: 0,
        gas_limit: 0,
    };
    apply_fee_fields(&mut tx, base_fee);
    sign_tx_with(tx, player.signing_key())
}

/// 本地确定性执行（与节点 gate/executor 同一函数 → 同一输出）。
/// 规则配置必须与链上对象一致（治理 `UpdateConfig` 可更新怪物表/强化表；
/// `session.rules_version` 绑定链上配置版本，不一致会被 gate 拒绝）。
/// 调用方从链上拉取配置（缺失时用 `Default` = v1 常量表，与旧行为逐位一致）。
pub fn execute_locally(
    session: &rabbitcore::game::ActionSession,
    seed: u64,
    enhance_cfg: &rabbitcore::game::EnhanceConfig,
    monsters: &rabbitcore::game::MonsterTableConfig,
) -> Result<rabbitcore::game::ActionOutcome, rabbitcore::game::GameError> {
    rabbitcore::game::execute_action(
        &session.action_type,
        &session.inputs,
        seed,
        enhance_cfg,
        monsters,
    )
}

/// 铸造链上怪物表配置对象 v1（默认表；玩家签名。Mint 免 gas，且非提案对象
/// 不校验签名者，任何账户都能铸造——服务端用权威签名，客户端用玩家签名）。
pub fn build_mint_monster_config_tx(
    player: &PlayerWallet,
    nonce: u64,
    chain_id: u64,
) -> ComputeTx {
    let state = serde_json::to_vec(&shanhai_core::monster::MonsterTableConfig::default())
        .expect("monster config serialization");
    let tx = ComputeTx {
        tx_id: TxId(Hash::zero()),
        domain_id: GAME_DOMAIN,
        command: Command::Mint,
        input_set: vec![],
        read_set: vec![],
        output_proposals: vec![proposal_owned(
            *player.address(),
            rabbitcore::game::monster_config_object_id(),
            None,
            1,
            state,
        )],
        fee: 0,
        nonce: Some(nonce),
        metadata: vec![],
        payload: vec![],
        deadline_unix_secs: None,
        chain_id: Some(chain_id),
        network_id: Some(chain_id as u32),
        witness: TxWitness { signatures: vec![], threshold: None },
        max_fee: 0,
        priority_fee: 0,
        gas_limit: 0,
    };
    sign_tx_with(tx, player.signing_key())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> Vec<u8> {
        hex::decode("11".repeat(32)).unwrap()
    }

    fn demo_wallet() -> PlayerWallet {
        PlayerWallet::derive(&seed(), 9001)
    }

    fn demo_session(wallet: &PlayerWallet) -> (rabbitcore::game::ActionSession, ActionInput) {
        let inputs = ActionInput::Battle {
            monster_id: "goblin".into(),
            team: vec![rabbitcore::game::TeamUnit { atk: 60, def: 20, hp: 500 }],
        };
        let session = rabbitcore::game::ActionSession {
            session_id: "client-demo-1".into(),
            action_type: ActionKind::Battle,
            inputs: inputs.clone(),
            rules_version: 1,
            creator: format!("0x{}", hex::encode(wallet.address().as_bytes())),
            created_at_unix: 1_700_000_000,
            settled: false,
            result: None,
        };
        (session, inputs)
    }

    #[test]
    fn start_tx_is_player_owned_mint() {
        let wallet = demo_wallet();
        let (session, inputs) = demo_session(&wallet);
        let tx = build_action_start_tx(
            &wallet,
            &session.session_id,
            ActionKind::Battle,
            inputs,
            session.created_at_unix,
            session.rules_version,
            1,
            10088,
        );
        assert_eq!(tx.command, Command::Mint);
        assert_eq!(tx.output_proposals.len(), 1);
        let p = &tx.output_proposals[0];
        assert_eq!(p.owner, Ownership::Address(*wallet.address()));
        // 能解析 payload 且 verify 通过
        let op = GameOp::parse(&tx.payload).expect("parse");
        assert!(rabbitcore::game::verify(&op).is_ok());
    }

    #[test]
    fn settle_tx_consumes_session_v1_and_produces_drops() {
        let wallet = demo_wallet();
        let (session, inputs) = demo_session(&wallet);
        let seed = 42u64;
        let outcome = execute_locally(
            &session,
            seed,
            &rabbitcore::game::EnhanceConfig::default(),
            &rabbitcore::game::MonsterTableConfig::default(),
        )
        .expect("local execute");
        let tx = build_action_settle_tx(
            &wallet,
            &session,
            seed,
            "0xabab",
            outcome.result.clone(),
            outcome.drops.clone(),
            wallet.address(),
            2,
            10088,
            1,
        );
        assert_eq!(tx.command, Command::Invoke);
        assert_eq!(tx.input_set.len(), 1);
        assert_eq!(tx.input_set[0], output_id_for(&rabbitcore::game::action_session_object_id(&session.session_id), 1));
        // 输出 = session v2 + 每掉落一个对象
        assert!(tx.output_proposals.len() >= 1);
        assert_eq!(tx.output_proposals[0].version, Version(2));
        // 打 goblin 必胜，必掉 goblin_coin（price 3）
        let drop_obj = rabbitcore::game::action_drop_object_id("client-demo-1", "goblin_coin");
        assert!(tx
            .output_proposals
            .iter()
            .any(|p| p.object_id == drop_obj && p.version == Version(1)));
        let op = GameOp::parse(&tx.payload).expect("parse");
        assert!(rabbitcore::game::verify(&op).is_ok());
    }

    #[test]
    fn sell_drop_tx_is_invoke_with_fee_and_receipt() {
        let wallet = demo_wallet();
        let tx = build_sell_drop_tx(&wallet, "client-demo-1", "goblin_coin", 3, 10088, 1);
        assert_eq!(tx.command, Command::Invoke);
        assert_eq!(tx.input_set.len(), 1);
        assert_eq!(tx.gas_limit, tx.max_fee); // base_fee=1
        assert!(tx.max_fee > 0);
        let op = GameOp::parse(&tx.payload).expect("parse");
        assert!(rabbitcore::game::verify(&op).is_ok());
    }

    #[test]
    fn player_wallet_derivation_is_deterministic() {
        let a = PlayerWallet::derive(&seed(), 7);
        let b = PlayerWallet::derive(&seed(), 7);
        let c = PlayerWallet::derive(&seed(), 8);
        assert_eq!(a.address(), b.address());
        assert_ne!(a.address(), c.address());
    }
}
