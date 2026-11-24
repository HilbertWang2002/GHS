use anyhow::Result;
use ghs::api::{self, Request};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    path::Path,
};

#[derive(PartialEq)]
enum Phase {
    New,
    Negotiated,
    Ready,
}
fn rpc_error(id: &Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn dispatch(msg: Value, phase: &mut Phase, socket: &Path) -> Option<Value> {
    let id = &msg["id"];
    let Some(method) = msg["method"].as_str() else {
        return Some(rpc_error(id, -32600, "missing method"));
    };
    if msg["jsonrpc"] != "2.0" {
        return Some(rpc_error(id, -32600, "expected JSON-RPC 2.0"));
    }
    if msg.get("id").is_none() {
        if method == "notifications/initialized" && *phase == Phase::Negotiated {
            *phase = Phase::Ready;
        }
        return None;
    }
    let result = match method {
        "initialize" if *phase == Phase::New => {
            if !msg["params"]["protocolVersion"].is_string() {
                return Some(rpc_error(id, -32602, "missing protocolVersion"));
            }
            *phase = Phase::Negotiated;
            json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"ghs","version":env!("CARGO_PKG_VERSION")}})
        }
        "ping" => json!({}),
        _ if *phase != Phase::Ready => {
            return Some(rpc_error(id, -32600, "initialize the session first"));
        }
        "tools/list" => api::tool_list(),
        "tools/call" => {
            let params = &msg["params"];
            let name = params["name"].as_str();
            if !api::tool_list()["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t["name"].as_str() == name)
            {
                return Some(rpc_error(id, -32602, "unknown tool"));
            }
            let req = serde_json::from_value::<Request>(
                json!({"method":name,"arguments":params.get("arguments").cloned().unwrap_or_else(||json!({}))}),
            );
            let req = match req {
                Ok(r) => r,
                Err(e) => return Some(rpc_error(id, -32602, &e.to_string())),
            };
            match api::call(socket, &req) {
                Ok(v) => {
                    json!({"structuredContent":v,"content":[{"type":"text","text":v.to_string()}],"isError":false})
                }
                Err(e) => {
                    json!({"content":[{"type":"text","text":format!("{e:#}")}],"isError":true})
                }
            }
        }
        _ => return Some(rpc_error(id, -32601, "method not found")),
    };
    Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
}
pub fn run(socket: &Path) -> Result<()> {
    let mut phase = Phase::New;
    let mut out = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let reply = match serde_json::from_str(&line?) {
            Ok(msg) => dispatch(msg, &mut phase, socket),
            Err(e) => Some(rpc_error(&Value::Null, -32700, &e.to_string())),
        };
        if let Some(reply) = reply {
            serde_json::to_writer(&mut out, &reply)?;
            out.write_all(b"\n")?;
            out.flush()?;
        }
    }
    Ok(())
}
