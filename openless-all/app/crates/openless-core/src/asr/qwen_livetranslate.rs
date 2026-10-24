//! Alibaba Cloud Bailian Qwen-Livetranslate realtime client (display-only).
//!
//! Speaks the same protocol line as `qwen_realtime.rs`
//! (`/api-ws/v1/realtime?model=...`, Bearer auth, base64
//! `input_audio_buffer.append`, `session.finish` close-out) but asks for a
//! text-only simultaneous-translation session and parses translation events
//! instead of transcription events:
//!
//! - qwen3.8 style: `response.text.delta` carries an incremental `delta`
//!   that accumulates without overwriting earlier text.
//! - qwen3.5 style: `response.text.text` carries the confirmed `text` plus a
//!   tentative `stash`; the pair replaces the current segment snapshot.
//! - `response.text.done` closes one segment with its complete `text`.
//! - `error` / mid-session disconnects hide the preview (`Unavailable`).
//! - `session.finish` flushes the tail segment before `session.finished`.
//!
//! Unlike the ASR clients this never produces a transcript and never
//! implements `TranscriptionSession`: snapshots go to a `LiveTranslationSink`
//! for capsule display only and are never persisted
//! (docs/live-translation-preview-design.md §4).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex as ParkingMutex;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, Notify};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

use crate::config::{TaskSpawner, TokioTaskSpawner};
use crate::live_translation::{
    LiveTranslationSink, LiveTranslationUpdate, LiveTranslatorSession,
};
use crate::provider_rules::QWEN_LIVETRANSLATE_DEFAULT_MODEL;
use crate::types::SessionId;

use super::qwen_realtime::{drain_audio_chunks, join_segments};

pub const DEFAULT_ENDPOINT: &str = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime";
pub const DEFAULT_TARGET_LANGUAGE: &str = "en";

/// 15 s of 16 kHz / s16le / mono audio across the pre-ready buffer, the chunk
/// scratch and the unwritten send queue. Translation audio is droppable, so
/// exceeding the cap stalls the preview instead of growing memory.
pub const MAX_QUEUED_AUDIO_BYTES: usize = 480_000;

/// Full-snapshot publish throttle: the reader merges token-level deltas into
/// one event per interval so the UI is not flooded per token.
pub const PUBLISH_THROTTLE: Duration = Duration::from_millis(80);

const SESSION_READY_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on the WebSocket handshake itself; `open_session` runs on a spawned
/// task, but a hung connect would still hold the (droppable) preview open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = futures_util::stream::SplitSink<WsStream, Message>;
type SharedWriter = Arc<AsyncMutex<Option<WsSink>>>;

#[derive(Clone, Debug)]
pub struct QwenLiveTranslateCredentials {
    pub api_key: String,
    /// Realtime WebSocket endpoint (`wss://.../api-ws/v1/realtime`); empty
    /// selects the public default.
    pub endpoint: String,
    pub model: String,
    pub target_language: String,
}

impl QwenLiveTranslateCredentials {
    pub fn normalized_endpoint(&self) -> String {
        if self.endpoint.trim().is_empty() {
            return DEFAULT_ENDPOINT.to_string();
        }
        self.endpoint.trim().to_string()
    }

    pub fn normalized_model(&self) -> String {
        let model = self.model.trim();
        if model.is_empty() {
            QWEN_LIVETRANSLATE_DEFAULT_MODEL.to_string()
        } else {
            model.to_string()
        }
    }

    pub fn normalized_target_language(&self) -> String {
        let language = self.target_language.trim();
        if language.is_empty() {
            DEFAULT_TARGET_LANGUAGE.to_string()
        } else {
            language.to_string()
        }
    }

    /// Connect URL: `{endpoint}?model={model}`; an endpoint that already
    /// carries a `model=` query is used as-is.
    pub fn connect_url(&self) -> String {
        let endpoint = self.normalized_endpoint();
        if endpoint.contains("model=") {
            return endpoint;
        }
        let sep = if endpoint.contains('?') { '&' } else { '?' };
        format!(
            "{}{}model={}",
            endpoint.trim_end_matches('/'),
            sep,
            self.normalized_model()
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LiveTranslateError {
    #[error("credentials missing")]
    CredentialsMissing,
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("send failed: {0}")]
    SendFailed(String),
    #[error("task failed: {0}")]
    TaskFailed(String),
    #[error("session ready timed out")]
    ReadyTimeout,
}

enum SendItem {
    Audio(Vec<u8>),
    Finish(oneshot::Sender<Result<(), LiveTranslateError>>),
}

#[derive(Default)]
struct TranslateState {
    pending_audio: Vec<u8>,
    audio_scratch: Vec<u8>,
    session_ready: bool,
    ready_error: Option<String>,
    finished: bool,
    stalled: bool,
    completed_segments: Vec<String>,
    partial_text: String,
    last_publish: Option<Instant>,
    send_tx: Option<mpsc::UnboundedSender<SendItem>>,
}

pub struct QwenLiveTranslator {
    credentials: QwenLiveTranslateCredentials,
    session_id: SessionId,
    sink: Arc<dyn LiveTranslationSink>,
    task_spawner: Arc<dyn TaskSpawner>,
    state: Arc<ParkingMutex<TranslateState>>,
    /// Bytes handed to the writer task but not yet on the socket; part of the
    /// stall budget together with the pending buffer and the chunk scratch.
    in_flight: Arc<AtomicUsize>,
    writer: SharedWriter,
    session_ready: Arc<Notify>,
    finished: Arc<Notify>,
    publish_throttle: Duration,
}

impl QwenLiveTranslator {
    pub fn new(
        credentials: QwenLiveTranslateCredentials,
        session_id: SessionId,
        sink: Arc<dyn LiveTranslationSink>,
    ) -> Arc<Self> {
        Self::with_task_spawner(credentials, session_id, sink, Arc::new(TokioTaskSpawner))
    }

    pub fn with_task_spawner(
        credentials: QwenLiveTranslateCredentials,
        session_id: SessionId,
        sink: Arc<dyn LiveTranslationSink>,
        task_spawner: Arc<dyn TaskSpawner>,
    ) -> Arc<Self> {
        Arc::new(Self {
            credentials,
            session_id,
            sink,
            task_spawner,
            state: Arc::new(ParkingMutex::new(TranslateState::default())),
            in_flight: Arc::new(AtomicUsize::new(0)),
            writer: Arc::new(AsyncMutex::new(None)),
            session_ready: Arc::new(Notify::new()),
            finished: Arc::new(Notify::new()),
            publish_throttle: PUBLISH_THROTTLE,
        })
    }

    /// Test hook: disable snapshot merging so every delta publishes.
    #[cfg(test)]
    fn with_publish_throttle(mut self: Arc<Self>, throttle: Duration) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("test translator is unshared")
            .publish_throttle = throttle;
        self
    }

    pub async fn open_session(self: &Arc<Self>) -> Result<(), LiveTranslateError> {
        if self.credentials.api_key.trim().is_empty() {
            return Err(LiveTranslateError::CredentialsMissing);
        }

        let url = self.credentials.connect_url();
        let mut request = url
            .into_client_request()
            .map_err(|e| LiveTranslateError::ConnectionFailed(e.to_string()))?;
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", self.credentials.api_key.trim()))
                .map_err(|e| LiveTranslateError::ConnectionFailed(e.to_string()))?,
        );
        request
            .headers_mut()
            .insert("OpenAI-Beta", HeaderValue::from_static("realtime=v1"));

        let (ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(request))
            .await
            .map_err(|_| {
                LiveTranslateError::ConnectionFailed(format!(
                    "连接超时（{} ms）",
                    CONNECT_TIMEOUT.as_millis()
                ))
            })?
            .map_err(|e| LiveTranslateError::ConnectionFailed(e.to_string()))?;
        let (write, read) = ws.split();
        *self.writer.lock().await = Some(write);

        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<SendItem>();
        {
            let mut st = self.state.lock();
            st.send_tx = Some(send_tx);
        }

        let writer_for_worker = Arc::clone(&self.writer);
        let in_flight_for_worker = Arc::clone(&self.in_flight);
        let weak_self_for_worker = Arc::downgrade(self);
        let task_spawner = Arc::clone(&self.task_spawner);
        task_spawner.spawn(Box::pin(async move {
            while let Some(item) = send_rx.recv().await {
                match item {
                    SendItem::Audio(chunk) => {
                        let len = chunk.len();
                        let result =
                            send_text(&writer_for_worker, append_audio_message(&chunk)).await;
                        in_flight_for_worker.fetch_sub(len, Ordering::Relaxed);
                        if let Err(error) = result {
                            log::warn!("[qwen-livetranslate] audio frame send failed: {error}");
                            if let Some(this) = weak_self_for_worker.upgrade() {
                                this.finish_unavailable();
                            }
                            break;
                        }
                    }
                    SendItem::Finish(done) => {
                        let result = send_text(&writer_for_worker, finish_session_message())
                            .await
                            .map_err(|e| LiveTranslateError::SendFailed(e.to_string()));
                        let _ = done.send(result);
                    }
                }
            }
        }));

        let weak_self = Arc::downgrade(self);
        let task_spawner = Arc::clone(&self.task_spawner);
        task_spawner.spawn(Box::pin(async move {
            let mut read = read;
            while let Some(msg) = read.next().await {
                let Some(this) = weak_self.upgrade() else {
                    break;
                };
                match msg {
                    Ok(Message::Text(text)) => {
                        if !this.handle_text_message(&text) {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        this.fail_session_ready(
                            "websocket closed before session configuration completed",
                        );
                        this.finish_unavailable();
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("[qwen-livetranslate] receive loop error: {e}");
                        this.fail_session_ready(&e.to_string());
                        this.finish_unavailable();
                        break;
                    }
                }
            }
        }));

        let ready = self.session_ready.notified();
        tokio::pin!(ready);
        ready.as_mut().enable();
        if let Err(error) = send_text(
            &self.writer,
            session_update_message(&self.credentials.normalized_target_language()),
        )
        .await
        {
            self.cancel();
            return Err(error);
        }
        let ready_result = if !self.state.lock().session_ready {
            tokio::time::timeout(SESSION_READY_TIMEOUT, ready)
                .await
                .map_err(|_| LiveTranslateError::ReadyTimeout)
        } else {
            Ok(())
        };
        if let Err(error) = ready_result {
            self.cancel();
            return Err(error);
        }
        // Rust 2021 keeps an if-let scrutinee's temporary guard through its
        // body; clone the error out before cancel() takes the same lock.
        let start_error = self.state.lock().ready_error.clone();
        if let Some(error) = start_error {
            self.cancel();
            return Err(LiveTranslateError::TaskFailed(error));
        }
        Ok(())
    }

    fn handle_text_message(&self, text: &str) -> bool {
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[qwen-livetranslate] invalid json event: {e}");
                return true;
            }
        };
        let event = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match event {
            "session.updated" => {
                self.mark_session_ready();
                true
            }
            // qwen3.8: incremental fragments that accumulate.
            "response.text.delta" => {
                self.record_delta(&value);
                true
            }
            // qwen3.5: confirmed `text` plus tentative `stash`; the pair
            // replaces the open segment snapshot.
            "response.text.text" => {
                self.record_confirmed_and_stash(&value);
                true
            }
            "response.text.done" => {
                self.record_done(&value);
                true
            }
            "session.finished" => {
                self.finish_success();
                false
            }
            "error" => {
                let message = value
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("realtime translation error")
                    .to_string();
                log::warn!("[qwen-livetranslate] server error: {message}");
                self.finish_unavailable();
                false
            }
            _ => true,
        }
    }

    fn mark_session_ready(&self) {
        let (send_tx, chunks) = {
            let mut st = self.state.lock();
            st.session_ready = true;
            if !st.pending_audio.is_empty() {
                let pending = std::mem::take(&mut st.pending_audio);
                st.audio_scratch.extend_from_slice(&pending);
            }
            let chunks = drain_audio_chunks(&mut st.audio_scratch);
            (st.send_tx.clone(), chunks)
        };
        self.send_chunks(send_tx.as_ref(), chunks);
        self.session_ready.notify_waiters();
    }

    fn fail_session_ready(&self, error: &str) {
        let mut st = self.state.lock();
        if !st.session_ready && st.ready_error.is_none() {
            st.ready_error = Some(error.to_string());
            self.session_ready.notify_waiters();
        }
    }

    fn record_delta(&self, value: &Value) {
        let Some(delta) = value
            .get("delta")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return;
        };
        {
            let mut st = self.state.lock();
            if st.finished || st.stalled {
                return;
            }
            st.partial_text.push_str(delta);
        }
        self.publish_snapshot(false);
    }

    fn record_confirmed_and_stash(&self, value: &Value) {
        let confirmed = value.get("text").and_then(Value::as_str).unwrap_or_default();
        let stash = value.get("stash").and_then(Value::as_str).unwrap_or_default();
        if confirmed.is_empty() && stash.is_empty() {
            return;
        }
        {
            let mut st = self.state.lock();
            if st.finished || st.stalled {
                return;
            }
            st.partial_text = format!("{confirmed}{stash}");
        }
        self.publish_snapshot(false);
    }

    fn record_done(&self, value: &Value) {
        let final_text = value
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or_default();
        {
            let mut st = self.state.lock();
            if st.finished || st.stalled {
                return;
            }
            let segment = if final_text.is_empty() {
                std::mem::take(&mut st.partial_text)
            } else {
                st.partial_text.clear();
                final_text.to_string()
            };
            let segment = segment.trim();
            if !segment.is_empty() {
                st.completed_segments.push(segment.to_string());
            }
        }
        // A confirmed segment is a meaningful reading checkpoint: publish it
        // immediately instead of waiting out the throttle.
        self.publish_snapshot(true);
    }

    fn finish_success(&self) {
        {
            let mut st = self.state.lock();
            if st.finished {
                return;
            }
            // Flush a still-open tail into the final snapshot before closing.
            if !st.partial_text.is_empty() {
                let tail = std::mem::take(&mut st.partial_text);
                let tail = tail.trim().to_string();
                if !tail.is_empty() {
                    st.completed_segments.push(tail);
                }
            }
        }
        self.publish_snapshot(true);
        self.mark_finished_and_close();
    }

    fn finish_unavailable(&self) {
        self.fail_session_ready("translation session failed");
        let already = {
            let mut st = self.state.lock();
            let already = st.finished;
            st.finished = true;
            already
        };
        if !already {
            self.publish(LiveTranslationUpdate::unavailable());
        }
        self.mark_finished_and_close();
    }

    fn mark_finished_and_close(&self) {
        {
            let mut st = self.state.lock();
            st.finished = true;
            st.send_tx.take();
        }
        self.finished.notify_waiters();
        self.close_on_runtime();
    }

    fn mark_stalled(&self) {
        {
            let mut st = self.state.lock();
            if st.stalled || st.finished {
                return;
            }
            st.stalled = true;
            st.pending_audio.clear();
            st.audio_scratch.clear();
            st.send_tx.take();
            st.finished = true;
        }
        // Byte counts only; audio and translated text never reach the log.
        log::warn!(
            "[qwen-livetranslate] queued audio exceeded {MAX_QUEUED_AUDIO_BYTES} bytes; preview stalled"
        );
        self.publish(LiveTranslationUpdate::unavailable());
        self.finished.notify_waiters();
        self.close_on_runtime();
    }

    fn publish(&self, update: LiveTranslationUpdate) {
        self.sink.publish(self.session_id, update);
    }

    /// Publish the joined full-text snapshot. Throttled unless `force`; the
    /// snapshot is built under the state lock but published outside it so the
    /// audio callback is never blocked on the event bus.
    fn publish_snapshot(&self, force: bool) {
        let snapshot = {
            let mut st = self.state.lock();
            if st.finished || st.stalled {
                return;
            }
            if !force {
                if let Some(last) = st.last_publish {
                    if last.elapsed() < self.publish_throttle {
                        return;
                    }
                }
            }
            st.last_publish = Some(Instant::now());
            let mut segments = st.completed_segments.clone();
            if !st.partial_text.is_empty() {
                segments.push(st.partial_text.clone());
            }
            join_segments(&segments)
        };
        self.publish(LiveTranslationUpdate::streaming(snapshot));
    }

    fn queued_audio_bytes_locked(&self, st: &TranslateState) -> usize {
        st.pending_audio
            .len()
            .saturating_add(st.audio_scratch.len())
            .saturating_add(self.in_flight.load(Ordering::Relaxed))
    }

    fn send_chunks(&self, send_tx: Option<&mpsc::UnboundedSender<SendItem>>, chunks: Vec<Vec<u8>>) {
        let Some(send_tx) = send_tx else { return };
        for chunk in chunks {
            self.in_flight.fetch_add(chunk.len(), Ordering::Relaxed);
            if send_tx.send(SendItem::Audio(chunk)).is_err() {
                break;
            }
        }
    }

    fn close_on_runtime(&self) {
        let writer = Arc::clone(&self.writer);
        self.task_spawner.spawn(Box::pin(async move {
            let _ = close_writer(&writer).await;
        }));
    }

    /// Flush the audio tail and send `session.finish`, then keep receiving late
    /// translation for at most `grace` before closing. Every error is logged
    /// and swallowed: the preview must never surface a failure at stop time.
    fn finish_soft(&self, grace: Duration) -> BoxFuture<'static, ()> {
        let state = Arc::clone(&self.state);
        let in_flight = Arc::clone(&self.in_flight);
        let writer = Arc::clone(&self.writer);
        let finished = Arc::clone(&self.finished);
        let already_finished = self.state.lock().finished;
        Box::pin(async move {
            if !already_finished {
                let outcome = tokio::time::timeout(grace, async {
                    let notified = finished.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let (send_tx, tail) = {
                        let mut st = state.lock();
                        let send_tx = st.send_tx.clone();
                        if !st.pending_audio.is_empty() {
                            let pending = std::mem::take(&mut st.pending_audio);
                            st.audio_scratch.extend_from_slice(&pending);
                        }
                        let tail = if st.audio_scratch.is_empty() {
                            Vec::new()
                        } else {
                            vec![std::mem::take(&mut st.audio_scratch)]
                        };
                        (send_tx, tail)
                    };
                    if let Some(send_tx) = send_tx {
                        for chunk in tail {
                            in_flight.fetch_add(chunk.len(), Ordering::Relaxed);
                            if send_tx.send(SendItem::Audio(chunk)).is_err() {
                                break;
                            }
                        }
                        let (done_tx, done_rx) = oneshot::channel();
                        if send_tx.send(SendItem::Finish(done_tx)).is_ok() {
                            let _ = done_rx.await;
                        }
                    }
                    if !state.lock().finished {
                        notified.await;
                    }
                })
                .await;
                if outcome.is_err() {
                    log::warn!(
                        "[qwen-livetranslate] tail translation did not finish within {grace:?}; closing"
                    );
                }
            }
            {
                let mut st = state.lock();
                st.finished = true;
                st.send_tx.take();
            }
            let _ = close_writer(&writer).await;
        })
    }
}

impl crate::ports::AudioConsumer for QwenLiveTranslator {
    fn consume_pcm_chunk(&self, pcm: &[u8]) {
        if pcm.is_empty() {
            return;
        }
        enum Action {
            Drop,
            Send(Vec<Vec<u8>>),
            Stall,
        }
        let action = {
            let mut st = self.state.lock();
            if st.finished || st.stalled {
                Action::Drop
            } else if self.queued_audio_bytes_locked(&st) + pcm.len() > MAX_QUEUED_AUDIO_BYTES {
                Action::Stall
            } else if !st.session_ready {
                st.pending_audio.extend_from_slice(pcm);
                Action::Drop
            } else {
                st.audio_scratch.extend_from_slice(pcm);
                Action::Send(drain_audio_chunks(&mut st.audio_scratch))
            }
        };
        match action {
            Action::Send(chunks) => {
                let send_tx = self.state.lock().send_tx.clone();
                self.send_chunks(send_tx.as_ref(), chunks);
            }
            Action::Stall => self.mark_stalled(),
            Action::Drop => {}
        }
    }
}

impl LiveTranslatorSession for QwenLiveTranslator {
    fn finish_soft(&self, grace: Duration) -> BoxFuture<'static, ()> {
        QwenLiveTranslator::finish_soft(self, grace)
    }

    fn cancel(&self) {
        {
            let mut st = self.state.lock();
            st.pending_audio.clear();
            st.audio_scratch.clear();
            st.send_tx.take();
            st.finished = true;
        }
        self.finished.notify_waiters();
        self.close_on_runtime();
    }
}

fn session_update_message(target_language: &str) -> String {
    // Text-only output; the model auto-detects the source language. Input is
    // 16 kHz / s16le / mono PCM per the qwen3.8 default, matching the recorder.
    // `voice` must be "Tina": omitted, the server falls back to its own default
    // "Chelsie" and rejects it; "default" is rejected the same way. Verified
    // against the live service 2026-10 (examples/live_translate_smoke.rs).
    json!({
        "type": "session.update",
        "event_id": event_id(),
        "session": {
            "output_modalities": ["text"],
            "translation": { "language": target_language },
            "audio": {
                "input": {
                    "turn_detection": {
                        "type": "speaker_detection",
                        "threshold": 0.5,
                    },
                },
            },
            "voice": "Tina",
        },
    })
    .to_string()
}

fn append_audio_message(pcm: &[u8]) -> String {
    json!({
        "type": "input_audio_buffer.append",
        "event_id": event_id(),
        "audio": base64::engine::general_purpose::STANDARD.encode(pcm),
    })
    .to_string()
}

fn finish_session_message() -> String {
    json!({ "type": "session.finish", "event_id": event_id() }).to_string()
}

fn event_id() -> String {
    format!("event_{}", Uuid::new_v4())
}

async fn send_text(writer: &SharedWriter, text: String) -> Result<(), LiveTranslateError> {
    tokio::time::timeout(WRITE_TIMEOUT, async {
        let mut guard = writer.lock().await;
        let Some(ws) = guard.as_mut() else {
            return Err(LiveTranslateError::ConnectionFailed(
                "websocket writer not available".to_string(),
            ));
        };
        ws.send(Message::Text(text))
            .await
            .map_err(|e| LiveTranslateError::SendFailed(e.to_string()))
    })
    .await
    .map_err(|_| LiveTranslateError::SendFailed("websocket write timed out".to_string()))?
}

async fn close_writer(writer: &SharedWriter) -> Result<(), LiveTranslateError> {
    let mut guard = writer.lock().await;
    if let Some(mut ws) = guard.take() {
        let _ = ws.close().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_translation::LiveTranslationStatus;
    use crate::ports::AudioConsumer;

    #[derive(Default)]
    struct CaptureSink(ParkingMutex<Vec<LiveTranslationUpdate>>);

    impl LiveTranslationSink for CaptureSink {
        fn publish(&self, _session_id: SessionId, update: LiveTranslationUpdate) {
            self.0.lock().push(update);
        }
    }

    impl CaptureSink {
        fn updates(&self) -> Vec<LiveTranslationUpdate> {
            self.0.lock().clone()
        }
    }

    struct NoopSpawner;

    impl TaskSpawner for NoopSpawner {
        fn spawn(&self, _task: BoxFuture<'static, ()>) {}
    }

    fn create_test_translator() -> (Arc<QwenLiveTranslator>, Arc<CaptureSink>) {
        let sink = Arc::new(CaptureSink::default());
        let translator = QwenLiveTranslator::with_task_spawner(
            QwenLiveTranslateCredentials {
                api_key: "sk-test".to_string(),
                endpoint: String::new(),
                model: String::new(),
                target_language: String::new(),
            },
            SessionId::new(),
            sink.clone(),
            Arc::new(NoopSpawner),
        )
        .with_publish_throttle(Duration::ZERO);
        (translator, sink)
    }

    // ---- credentials / URL ----

    #[test]
    fn credentials_apply_defaults_and_connect_url() {
        let creds = QwenLiveTranslateCredentials {
            api_key: "sk-test".to_string(),
            endpoint: String::new(),
            model: String::new(),
            target_language: String::new(),
        };
        assert_eq!(creds.normalized_endpoint(), DEFAULT_ENDPOINT);
        assert_eq!(creds.normalized_model(), "qwen3.8-livetranslate-flash-realtime");
        assert_eq!(creds.normalized_target_language(), "en");
        assert_eq!(
            creds.connect_url(),
            "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.8-livetranslate-flash-realtime"
        );
        let workspace = QwenLiveTranslateCredentials {
            api_key: "sk-test".to_string(),
            endpoint: "wss://llm-xxx.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime/".to_string(),
            model: "custom-model".to_string(),
            target_language: String::new(),
        };
        assert_eq!(
            workspace.connect_url(),
            "wss://llm-xxx.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime?model=custom-model"
        );
        let with_query = QwenLiveTranslateCredentials {
            api_key: "sk-test".to_string(),
            endpoint: "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=pinned".to_string(),
            model: "ignored".to_string(),
            target_language: String::new(),
        };
        assert_eq!(
            with_query.connect_url(),
            "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=pinned"
        );
    }

    // ---- message builders ----

    #[test]
    fn session_update_is_text_only_english() {
        let value: Value = serde_json::from_str(&session_update_message("en")).unwrap();
        assert_eq!(value["type"], "session.update");
        assert_eq!(value["session"]["output_modalities"], json!(["text"]));
        assert_eq!(value["session"]["translation"]["language"], "en");
        assert_eq!(
            value["session"]["audio"]["input"]["turn_detection"]["type"],
            "speaker_detection"
        );
        assert_eq!(
            value["session"]["audio"]["input"]["turn_detection"]["threshold"],
            0.5
        );
        // Omitting voice makes the server pick its own default "Chelsie" and
        // reject it; "default" is rejected the same way. "Tina" is the value
        // verified against the live service.
        assert_eq!(value["session"]["voice"], "Tina");
        assert!(value["event_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("event_")));
    }

    #[test]
    fn append_message_base64_encodes_audio() {
        let value: Value = serde_json::from_str(&append_audio_message(b"\x01\x02\x03")).unwrap();
        assert_eq!(value["type"], "input_audio_buffer.append");
        assert_eq!(value["audio"], "AQID");
    }

    #[test]
    fn finish_message_shape() {
        let value: Value = serde_json::from_str(&finish_session_message()).unwrap();
        assert_eq!(value["type"], "session.finish");
    }

    // ---- qwen3.8 incremental deltas ----

    #[test]
    fn parses_v38_incremental_deltas() {
        let (translator, sink) = create_test_translator();
        for delta in ["Hello", " world", ", how are you?"] {
            translator.handle_text_message(
                &json!({
                    "type": "response.text.delta",
                    "delta": delta,
                })
                .to_string(),
            );
        }
        let updates = sink.updates();
        assert_eq!(
            updates,
            vec![
                LiveTranslationUpdate::streaming("Hello"),
                LiveTranslationUpdate::streaming("Hello world"),
                LiveTranslationUpdate::streaming("Hello world, how are you?"),
            ]
        );
        assert_eq!(translator.state.lock().partial_text, "Hello world, how are you?");
    }

    #[test]
    fn delta_accumulates_across_segments_with_joining() {
        let (translator, sink) = create_test_translator();
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "First sentence."}).to_string(),
        );
        translator.handle_text_message(
            &json!({"type": "response.text.done", "text": "First sentence."}).to_string(),
        );
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "Second one."}).to_string(),
        );
        let updates = sink.updates();
        assert_eq!(updates.last().unwrap().text, "First sentence. Second one.");
        assert_eq!(
            translator.state.lock().completed_segments,
            vec!["First sentence.".to_string()]
        );
    }

    // ---- qwen3.5 text + stash ----

    #[test]
    fn parses_v35_text_and_stash() {
        let (translator, sink) = create_test_translator();
        translator.handle_text_message(
            &json!({"type": "response.text.text", "text": "How are", "stash": " you"})
                .to_string(),
        );
        translator.handle_text_message(
            &json!({"type": "response.text.text", "text": "How are you", "stash": " today?"})
                .to_string(),
        );
        let updates = sink.updates();
        assert_eq!(
            updates,
            vec![
                LiveTranslationUpdate::streaming("How are you"),
                LiveTranslationUpdate::streaming("How are you today?"),
            ]
        );
    }

    #[test]
    fn done_replaces_snapshot() {
        let (translator, sink) = create_test_translator();
        translator.handle_text_message(
            &json!({"type": "response.text.text", "text": "How are", "stash": " yo"}).to_string(),
        );
        translator.handle_text_message(
            &json!({"type": "response.text.done", "text": "How are you today?"}).to_string(),
        );
        // A new open segment starts from scratch; the confirmed text stays.
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "Fine."}).to_string(),
        );
        let updates = sink.updates();
        assert_eq!(updates[1].text, "How are you today?");
        assert_eq!(updates.last().unwrap().text, "How are you today? Fine.");
        assert!(translator.state.lock().completed_segments.len() == 1);
    }

    #[test]
    fn done_without_text_promotes_partial() {
        let (translator, sink) = create_test_translator();
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "Tail text"}).to_string(),
        );
        translator
            .handle_text_message(&json!({"type": "response.text.done", "text": ""}).to_string());
        let st = translator.state.lock();
        assert_eq!(st.completed_segments, vec!["Tail text".to_string()]);
        assert!(st.partial_text.is_empty());
        drop(st);
        assert_eq!(sink.updates().last().unwrap().text, "Tail text");
    }

    // ---- terminal events ----

    #[test]
    fn error_marks_unavailable_and_stops_the_loop() {
        let (translator, sink) = create_test_translator();
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "partial"}).to_string(),
        );
        let keep_going = translator.handle_text_message(
            &json!({"type": "error", "error": {"message": "boom"}}).to_string(),
        );
        assert!(!keep_going);
        let updates = sink.updates();
        assert_eq!(updates.last().unwrap().status, LiveTranslationStatus::Unavailable);
        assert_eq!(updates.last().unwrap().text, "");
        assert!(translator.state.lock().finished);
        // Late events after the terminal are ignored.
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "late"}).to_string(),
        );
        assert_eq!(sink.updates().len(), updates.len());
    }

    #[test]
    fn session_finished_publishes_final_snapshot_including_open_tail() {
        let (translator, sink) = create_test_translator();
        translator.handle_text_message(
            &json!({"type": "response.text.done", "text": "Confirmed part."}).to_string(),
        );
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "open tail"}).to_string(),
        );
        let keep_going = translator
            .handle_text_message(&json!({"type": "session.finished"}).to_string());
        assert!(!keep_going);
        assert_eq!(
            sink.updates().last().unwrap().text,
            "Confirmed part. open tail"
        );
        assert!(translator.state.lock().finished);
    }

    #[test]
    fn only_session_updated_marks_session_ready_and_flushes_pending() {
        let (translator, _sink) = create_test_translator();
        translator.consume_pcm_chunk(&[7u8; 64]);
        assert_eq!(translator.state.lock().pending_audio.len(), 64);
        translator.handle_text_message(&json!({"type": "session.created"}).to_string());
        assert!(!translator.state.lock().session_ready);
        translator.handle_text_message(&json!({"type": "session.updated"}).to_string());
        let st = translator.state.lock();
        assert!(st.session_ready);
        assert!(st.pending_audio.is_empty());
    }

    // ---- backpressure ----

    #[test]
    fn pending_audio_is_capped() {
        let (translator, sink) = create_test_translator();
        translator.consume_pcm_chunk(&[1u8; MAX_QUEUED_AUDIO_BYTES]);
        assert!(!translator.state.lock().stalled);
        // One more byte exceeds the cap: stall, drop, and hide the preview.
        translator.consume_pcm_chunk(&[1u8; 1]);
        {
            let st = translator.state.lock();
            assert!(st.stalled);
            assert!(st.pending_audio.is_empty());
        }
        assert_eq!(
            sink.updates().last().unwrap().status,
            LiveTranslationStatus::Unavailable
        );
        // Later audio is discarded without growing anything.
        let before = sink.updates().len();
        translator.consume_pcm_chunk(&[2u8; 10_000]);
        assert_eq!(translator.state.lock().pending_audio.len(), 0);
        assert_eq!(sink.updates().len(), before);
    }

    #[test]
    fn publish_snapshot_is_throttled_unless_forced() {
        let sink = Arc::new(CaptureSink::default());
        let translator = QwenLiveTranslator::with_task_spawner(
            QwenLiveTranslateCredentials {
                api_key: "sk-test".to_string(),
                endpoint: String::new(),
                model: String::new(),
                target_language: "en".to_string(),
            },
            SessionId::new(),
            sink.clone(),
            Arc::new(NoopSpawner),
        );
        // Default 80 ms throttle: the second delta within the window merges.
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": "one"}).to_string(),
        );
        translator.handle_text_message(
            &json!({"type": "response.text.delta", "delta": " two"}).to_string(),
        );
        assert_eq!(sink.updates().len(), 1);
        // A confirmed segment always publishes immediately.
        translator.handle_text_message(
            &json!({"type": "response.text.done", "text": "one two"}).to_string(),
        );
        assert_eq!(sink.updates().len(), 2);
        assert_eq!(sink.updates().last().unwrap().text, "one two");
    }
}
