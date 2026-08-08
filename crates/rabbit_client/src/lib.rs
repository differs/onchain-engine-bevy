//! RabbitChain JSON-RPC 轻量客户端（游戏客户端专用）。
//!
//! 节点写方法（`rabbit_submitComputeTx` 等）在配置了 `auth_token` 时需携带
//! `Authorization: Bearer <token>` 或 `x-rabbit-token: <token>` 头（见 NETWORKS.md）。
//! 交易/对象结构与 rabbitcore `ComputeTx` / `ObjectOutput` 一致（COMPUTE_JSON_SPEC.md）。

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("rpc error code {code}: {message} (data: {data:?})")]
    Remote {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unexpected response: {0}")]
    BadResponse(String),
}

#[derive(Debug, Clone, Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    result: Option<Value>,
    error: Option<RpcErrorBody>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorBody {
    code: i64,
    message: String,
    data: Option<Value>,
}

/// 连接单个 RabbitChain 节点的 JSON-RPC 客户端。
#[derive(Debug)]
pub struct RpcClient {
    url: String,
    token: Option<String>,
    http: reqwest::Client,
    next_id: std::sync::atomic::AtomicU64,
}

impl RpcClient {
    pub fn new(url: impl Into<String>, token: Option<String>) -> Self {
        Self {
            url: url.into(),
            token,
            http: reqwest::Client::new(),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut req = self
            .http
            .post(&self.url)
            .json(&RpcRequest {
                jsonrpc: "2.0",
                id,
                method,
                params,
            });
        if let Some(tok) = &self.token {
            req = req.header("x-rabbit-token", tok);
        }
        let resp: RpcResponse = req.send().await?.json().await?;
        if let Some(err) = resp.error {
            return Err(RpcError::Remote {
                code: err.code,
                message: err.message,
                data: err.data,
            });
        }
        resp.result.ok_or_else(|| RpcError::BadResponse("missing result".into()))
    }

    /// `rabbit_getObject`：查询逻辑对象最新版本。
    pub async fn get_object(&self, object_id: &str) -> Result<Value, RpcError> {
        self.call("rabbit_getObject", json!([object_id])).await
    }

    /// `rabbit_getOutput`：查询指定物理输出。
    pub async fn get_output(&self, output_id: &str) -> Result<Value, RpcError> {
        self.call("rabbit_getOutput", json!([output_id])).await
    }

    /// `rabbit_simulateComputeTx`：链上干跑（不落账）。
    pub async fn simulate_compute_tx(&self, tx: &Value) -> Result<Value, RpcError> {
        self.call("rabbit_simulateComputeTx", json!([tx])).await
    }

    /// `rabbit_submitComputeTx`：提交交易（写方法，需 token）。
    pub async fn submit_compute_tx(&self, tx: &Value) -> Result<Value, RpcError> {
        self.call("rabbit_submitComputeTx", json!([tx])).await
    }

    /// `rabbit_getComputeTxResult`：查询交易结果（BlockTime：区块执行后才有真结果）。
    pub async fn get_compute_tx_result(&self, tx_id: &str) -> Result<Value, RpcError> {
        self.call("rabbit_getComputeTxResult", json!([tx_id])).await
    }

    /// `rabbit_gasPrice`：查询节点当前 base fee。
    pub async fn gas_price(&self) -> Result<Value, RpcError> {
        self.call("rabbit_gasPrice", json!([])).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_envelope_is_jsonrpc2() {
        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "rabbit_getObject",
            params: json!(["0x1234"]),
        };
        let v: Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "rabbit_getObject");
        assert_eq!(v["params"][0], "0x1234");
    }

    #[test]
    fn parses_remote_error() {
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"token required"}}"#;
        let resp: RpcResponse = serde_json::from_str(body).unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32001);
        assert!(resp.result.is_none());
    }

    #[test]
    fn parses_success_result() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#;
        let resp: RpcResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.result.unwrap()["ok"], true);
    }
}
