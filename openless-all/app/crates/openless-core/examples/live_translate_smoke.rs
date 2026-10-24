//! Live Bailian Qwen-Livetranslate session.update smoke test.
//!
//! Probes session.update variants against the real realtime endpoint until the
//! server accepts one, so protocol fixes do not need a full app reinstall.
//!
//! Run from openless-all/app:
//! LIVE_TRANSLATE_KEY_FILE=/tmp/ol-key cargo run -p openless-core --example live_translate_smoke -- 3
//! Optional: --model qwen3.5-livetranslate-flash-realtime

use std::time::Duration;

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::connect_async;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() -> Result<()> {
    let mut model = "qwen3.8-livetranslate-flash-realtime".to_string();
    let mut variant = 1u8;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--model" => model = args.next().expect("value after --model"),
            other => variant = other.parse().expect("variant number"),
        }
    }
    let key_file =
        std::env::var("LIVE_TRANSLATE_KEY_FILE").unwrap_or_else(|_| "/tmp/ol-key".to_string());
    let key = std::fs::read_to_string(&key_file)?.trim().to_string();
    if key.is_empty() {
        return Err("empty key file".into());
    }

    let url = format!("wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={model}");
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {key}")).map_err(|e| format!("key header: {e}"))?,
    );
    request
        .headers_mut()
        .insert("OpenAI-Beta", HeaderValue::from_static("realtime=v1"));

    println!("# model={model} variant={variant}");
    let (ws, _resp) = match tokio::time::timeout(Duration::from_secs(8), connect_async(request))
        .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(format!("connect failed: {e}").into()),
        Err(_) => return Err("connect timeout".into()),
    };
    println!("# connected");
    let (mut write, mut read) = ws.split();

    let update = session_update(variant)?;
    println!("# >>> {update}");
    write.send(Message::Text(update.into())).await?;

    // 收事件：session.updated / error / 其它，最多 6 条或 10 秒
    let mut got_ready = false;
    for _ in 0..6 {
        let msg = match tokio::time::timeout(Duration::from_secs(10), read.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                println!("# stream error: {e}");
                break;
            }
            Ok(None) => {
                println!("# closed");
                break;
            }
            Err(_) => {
                println!("# receive timeout");
                break;
            }
        };
        let Message::Text(text) = msg else { continue };
        let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>")
            .to_string();
        println!("# <<< {kind}: {}", compact(&value));
        if kind == "session.updated" {
            got_ready = true;
            break;
        }
        if kind == "error" {
            break;
        }
    }

    if got_ready {
        // 发 1 秒静音 + session.finish，验证能否正常收尾
        let silence = vec![0u8; 32_000];
        let audio = json!({
            "type": "input_audio_buffer.append",
            "audio": base64::engine::general_purpose::STANDARD.encode(silence),
        })
        .to_string();
        write.send(Message::Text(audio.into())).await?;
        write.send(Message::Text(r#"{"type":"session.finish"}"#.into()))
            .await?;
        for _ in 0..6 {
            let msg = match tokio::time::timeout(Duration::from_secs(10), read.next()).await {
                Ok(Some(Ok(m))) => m,
                _ => break,
            };
            let Message::Text(text) = msg else { continue };
            let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            let kind = value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string();
            println!("# <<< {kind}: {}", compact(&value));
            if kind == "session.finished" {
                break;
            }
        }
    }
    let _ = write.close().await;
    println!("# done (ready={got_ready})");
    Ok(())
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .chars()
        .take(600)
        .collect()
}

fn session_update(variant: u8) -> Result<String> {
    let session = match variant {
        // 1: 当前 App 代码（3.8 结构 + voice default）
        1 => json!({
            "output_modalities": ["text"],
            "translation": {"language": "en"},
            "audio": {"input": {"turn_detection": {"type": "speaker_detection", "threshold": 0.5}}},
            "voice": "default",
        }),
        // 2: 3.8 结构，无 voice（服务端补 Chelsie 的那版）
        2 => json!({
            "output_modalities": ["text"],
            "translation": {"language": "en"},
            "audio": {"input": {"turn_detection": {"type": "speaker_detection", "threshold": 0.5}}},
        }),
        // 3: 3.8 结构 + voice Tina（文档 session.updated 回显的默认音色）
        3 => json!({
            "output_modalities": ["text"],
            "translation": {"language": "en"},
            "audio": {"input": {"turn_detection": {"type": "speaker_detection", "threshold": 0.5}}},
            "voice": "Tina",
        }),
        // 4: 3.5 扁平结构，无 voice
        4 => json!({
            "modalities": ["text"],
            "translation": {"language": "en"},
            "turn_detection": {"type": "server_vad", "threshold": 0.2, "silence_duration_ms": 1000},
        }),
        // 5: 3.5 扁平结构 + voice default（官方 3.5 示例原样，去 corpus/clone）
        5 => json!({
            "modalities": ["text"],
            "voice": "default",
            "translation": {"language": "en"},
            "turn_detection": {"type": "server_vad", "threshold": 0.2, "silence_duration_ms": 1000},
        }),
        // 6: 官方 3.8 示例原样（text+audio 双模态 + voice default）
        6 => json!({
            "output_modalities": ["text", "audio"],
            "translation": {"language": "en"},
            "audio": {"input": {"turn_detection": {"type": "speaker_detection", "threshold": 0.5}}},
            "voice": "default",
        }),
        // 7: 极简：只要译文
        7 => json!({
            "output_modalities": ["text"],
            "translation": {"language": "en"},
        }),
        // 8: 3.8 结构 + voice Ethan
        8 => json!({
            "output_modalities": ["text"],
            "translation": {"language": "en"},
            "audio": {"input": {"turn_detection": {"type": "speaker_detection", "threshold": 0.5}}},
            "voice": "Ethan",
        }),
        _ => return Err("variant must be 1..=8".into()),
    };
    Ok(json!({"type": "session.update", "session": session}).to_string())
}
