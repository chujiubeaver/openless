//! Display-only live translation preview during dictation recordings.
//!
//! The recorder decorator tees the canonical 16 kHz PCM stream: the primary
//! consumer (dictation pipeline) passes through untouched while a secondary
//! translation client streams the same audio to the Bailian live-translate
//! model and publishes full-text snapshots as
//! `BackendEventKind::LiveTranslationUpdate`.
//!
//! Hard guarantees (docs/live-translation-preview-design.md §2.5 / §4):
//! - The preview never joins insertion, history, polish context or engine
//!   results; it only emits its own event kind.
//! - Recorder `start`/`stop` results come exclusively from the inner recorder;
//!   every translator failure is logged and published as `Unavailable`.
//! - Queued translation audio is capped; the preview stalls (and hides itself)
//!   before memory can grow without bound.
//! - With no [`LiveTranslationPlan`] in the session context the decorator is a
//!   pass-through: the original consumer `Arc` reaches the inner recorder
//!   unchanged and no task, credential read or connection happens.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use parking_lot::Mutex as ParkingMutex;
use serde::{Deserialize, Serialize};

use crate::asr::qwen_livetranslate::{QwenLiveTranslateCredentials, QwenLiveTranslator};
use crate::config::TaskSpawner;
use crate::credentials::{
    CredentialKey, CredentialNamespace, CredentialStore, SecretValue, OMNI_API_KEY_ACCOUNT,
    OMNI_ENDPOINT_ACCOUNT,
};
use crate::dictation_context::{DictationAudioSource, DictationContext, DictationStartOptions};
use crate::errors::{BackendError, BackendErrorCode};
use crate::events::{BackendEventKind, BackendEventPublisher};
use crate::ports::{
    ActiveRecording, AudioConsumer, AudioRecorder, CaptureStartRequest, PendingAudioCapture,
    RecordingArchive, RecordingProgressSink,
};
use crate::provider_rules::{
    channel_supports_live_translation, derive_bailian_endpoint, BailianEndpointProtocol,
    QWEN_LIVETRANSLATE_DEFAULT_MODEL,
};
use crate::shared_types::PipelineMode;
use crate::types::SessionId;

/// Target language fixed for v1; the model auto-detects the source language.
pub const LIVE_TRANSLATION_TARGET_LANGUAGE: &str = "en";

/// 15 s of 16 kHz / s16le / mono PCM (32 000 bytes per second). The preview is
/// droppable: exceeding the cap stalls it instead of growing memory.
pub const MAX_PENDING_TRANSLATION_AUDIO_BYTES: usize = 480_000;

/// Tail-translation window after the inner recording has stopped. `finish_soft`
/// runs as a DETACHED task, so this bound never touches the main path's
/// key-release → insert latency: the omni engine encodes and uploads while the
/// preview keeps receiving. The 1.5 s window regularly cut off the server's
/// final translated words after `session.finish` (log: "tail translation did
/// not finish within 1.5s"), truncating the very text the user was reading;
/// 8 s lets the tail finish in parallel while the main path proceeds, and costs
/// nothing but an idle socket plus one small final message.
pub const LIVE_TRANSLATION_FINISH_GRACE: Duration = Duration::from_millis(8_000);

/// Frozen at session start: which channel pays for the preview and which
/// language it translates into. Contains no secrets; the decorator resolves the
/// API key by `provider_id` when the session actually starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTranslationPlan {
    pub provider_id: String,
    pub target_language: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTranslationStatus {
    Streaming,
    Unavailable,
}

/// One preview frame. `text` is always a full snapshot (never incremental) so
/// a dropped event self-heals on the next one; `Unavailable` carries no text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveTranslationUpdate {
    pub text: String,
    pub status: LiveTranslationStatus,
}

impl LiveTranslationUpdate {
    pub fn streaming(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            status: LiveTranslationStatus::Streaming,
        }
    }

    pub fn unavailable() -> Self {
        Self {
            text: String::new(),
            status: LiveTranslationStatus::Unavailable,
        }
    }
}

pub trait LiveTranslationSink: Send + Sync {
    fn publish(&self, session_id: SessionId, update: LiveTranslationUpdate);
}

/// Production sink: forwards to the backend event bus as its own event kind so
/// the preview can never merge into transcript accumulation or history.
pub struct PublisherLiveTranslationSink {
    publisher: BackendEventPublisher,
}

impl PublisherLiveTranslationSink {
    pub fn new(publisher: BackendEventPublisher) -> Arc<Self> {
        Arc::new(Self { publisher })
    }
}

impl LiveTranslationSink for PublisherLiveTranslationSink {
    fn publish(&self, session_id: SessionId, update: LiveTranslationUpdate) {
        self.publisher.publish(
            Some(session_id),
            BackendEventKind::LiveTranslationUpdate(update),
        );
    }
}

pub fn live_translation_sink(publisher: BackendEventPublisher) -> Arc<dyn LiveTranslationSink> {
    PublisherLiveTranslationSink::new(publisher)
}

/// Late-bound sink resolution: the host constructs the recorder decorator
/// before the backend (and therefore the event bus) exists.
pub type LiveTranslationSinkProvider =
    Arc<dyn Fn() -> Option<Arc<dyn LiveTranslationSink>> + Send + Sync>;

/// One live translation connection bound to a recording session. Not a
/// [`crate::ports::TranscriptionSession`]: it produces no transcript and takes
/// part in no engine finalization.
pub trait LiveTranslatorSession: AudioConsumer {
    /// Flush the audio tail, receive late translation for at most `grace`,
    /// then release the connection. Errors are swallowed by contract.
    fn finish_soft(&self, grace: Duration) -> BoxFuture<'static, ()>;

    /// Release the connection immediately without waiting for a tail.
    fn cancel(&self);
}

pub trait LiveTranslatorFactory: Send + Sync {
    /// Build and connect a translator session. The returned future performs
    /// every fallible step (credential read, WebSocket handshake, session
    /// configuration); failures are contained by the caller.
    fn create(
        &self,
        session_id: SessionId,
        plan: &LiveTranslationPlan,
        sink: Arc<dyn LiveTranslationSink>,
    ) -> BoxFuture<'static, Result<Arc<dyn LiveTranslatorSession>, BackendError>>;
}

/// Production factory: reuses the omni channel credentials (`omni.api_key` /
/// `omni.endpoint`) of the planned provider and derives the realtime WebSocket
/// endpoint from them, so no new credential surface is introduced.
pub struct QwenLiveTranslatorFactory {
    credentials: Arc<dyn CredentialStore>,
    task_spawner: Arc<dyn TaskSpawner>,
}

impl QwenLiveTranslatorFactory {
    pub fn new(
        credentials: Arc<dyn CredentialStore>,
        task_spawner: Arc<dyn TaskSpawner>,
    ) -> Self {
        Self {
            credentials,
            task_spawner,
        }
    }

    async fn read_channel_credential(
        &self,
        provider_id: &str,
        account: &str,
    ) -> Result<Option<String>, BackendError> {
        let key = CredentialKey::new(
            CredentialNamespace::Omni,
            Some(provider_id.to_string()),
            account,
        )?;
        self.credentials
            .read(key)
            .await
            .map(|value| value.map(SecretValue::into_exposed))
    }
}

impl LiveTranslatorFactory for QwenLiveTranslatorFactory {
    fn create(
        &self,
        session_id: SessionId,
        plan: &LiveTranslationPlan,
        sink: Arc<dyn LiveTranslationSink>,
    ) -> BoxFuture<'static, Result<Arc<dyn LiveTranslatorSession>, BackendError>> {
        let credentials = Arc::clone(&self.credentials);
        let task_spawner = Arc::clone(&self.task_spawner);
        let factory = QwenLiveTranslatorFactory {
            credentials,
            task_spawner,
        };
        let plan = plan.clone();
        Box::pin(async move {
            let provider_id = plan.provider_id.trim();
            if provider_id.is_empty() {
                return Err(BackendError::new(
                    BackendErrorCode::InvalidArgument,
                    "live translation plan has no provider id",
                ));
            }
            // Both keychain reads run concurrently: the setup task races the
            // microphone open, and every millisecond shaved here shows up as
            // earlier first translation.
            let (api_key_read, endpoint_read) = tokio::join!(
                factory.read_channel_credential(provider_id, OMNI_API_KEY_ACCOUNT),
                factory.read_channel_credential(provider_id, OMNI_ENDPOINT_ACCOUNT),
            );
            let api_key = api_key_read?
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    BackendError::new(
                        BackendErrorCode::Provider,
                        "Omni API key is not configured for live translation",
                    )
                })?;
            let endpoint = endpoint_read?.unwrap_or_default();
            let ws_endpoint =
                derive_bailian_endpoint(&endpoint, BailianEndpointProtocol::QwenRealtime)
                    .map_err(|_| {
                        BackendError::new(
                            BackendErrorCode::InvalidArgument,
                            "omni endpoint is not usable for realtime translation",
                        )
                    })?;
            let translator = QwenLiveTranslator::with_task_spawner(
                QwenLiveTranslateCredentials {
                    api_key,
                    endpoint: ws_endpoint,
                    model: QWEN_LIVETRANSLATE_DEFAULT_MODEL.to_string(),
                    target_language: plan.target_language.clone(),
                },
                session_id,
                sink,
                factory.task_spawner,
            );
            translator
                .open_session()
                .await
                .map_err(|error| BackendError::new(BackendErrorCode::Provider, error.to_string()))?;
            Ok(translator as Arc<dyn LiveTranslatorSession>)
        })
    }
}

enum TeeState {
    /// Translator not attached yet; PCM accumulates in `buffer` (capped).
    Pending { buffer: Vec<u8> },
    Live {
        translator: Arc<dyn LiveTranslatorSession>,
    },
    /// Pending buffer overflowed: everything is dropped and the UI was told.
    Stalled,
    /// Recording stopped (or setup failed); no further audio or events.
    Finished,
}

/// Per-session state shared by the tee consumer, the spawned setup task and
/// the wrapped recording handle.
pub struct LiveTranslationSessionState {
    session_id: SessionId,
    sink: Arc<dyn LiveTranslationSink>,
    state: ParkingMutex<TeeState>,
}

impl LiveTranslationSessionState {
    pub fn new(session_id: SessionId, sink: Arc<dyn LiveTranslationSink>) -> Self {
        Self {
            session_id,
            sink,
            state: ParkingMutex::new(TeeState::Pending {
                buffer: Vec::new(),
            }),
        }
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn sink(&self) -> Arc<dyn LiveTranslationSink> {
        Arc::clone(&self.sink)
    }

    /// Announce "this session has a preview planned" before any text exists, as
    /// an empty streaming snapshot. The capsule uses it to pre-size its window
    /// for the card: growing the window at the moment the first text paints makes
    /// WebKit stretch the previous frame across the new bounds, which reads as
    /// the voice animation flashing through the card's slot.
    pub fn announce_planned(&self) {
        self.sink
            .publish(self.session_id, LiveTranslationUpdate::streaming(""));
    }

    /// Called from the audio callback thread: never awaits, never allocates
    /// beyond the capped buffer, never touches the primary consumer.
    fn consume(&self, pcm: &[u8]) {
        enum Action {
            Discard,
            Forward(Arc<dyn LiveTranslatorSession>),
            Stall,
        }
        let action = {
            let mut state = self.state.lock();
            match &mut *state {
                TeeState::Pending { buffer } => {
                    if buffer.len().saturating_add(pcm.len()) > MAX_PENDING_TRANSLATION_AUDIO_BYTES {
                        Action::Stall
                    } else {
                        buffer.extend_from_slice(pcm);
                        Action::Discard
                    }
                }
                TeeState::Live { translator } => Action::Forward(Arc::clone(translator)),
                TeeState::Stalled | TeeState::Finished => Action::Discard,
            }
        };
        match action {
            Action::Forward(translator) => translator.consume_pcm_chunk(pcm),
            Action::Stall => {
                {
                    let mut state = self.state.lock();
                    if !matches!(*state, TeeState::Pending { .. }) {
                        return;
                    }
                    *state = TeeState::Stalled;
                }
                // Log byte counts only; never the audio or translated text.
                log::warn!(
                    "[live-translation] pending audio exceeded {MAX_PENDING_TRANSLATION_AUDIO_BYTES} bytes; preview stalled"
                );
                self.sink.publish(self.session_id, LiveTranslationUpdate::unavailable());
            }
            Action::Discard => {}
        }
    }

    fn attach(&self, translator: Arc<dyn LiveTranslatorSession>) {
        let mut state = self.state.lock();
        let previous = std::mem::replace(&mut *state, TeeState::Finished);
        match previous {
            TeeState::Pending { buffer } => {
                *state = TeeState::Live {
                    translator: Arc::clone(&translator),
                };
                drop(state);
                if !buffer.is_empty() {
                    translator.consume_pcm_chunk(&buffer);
                }
            }
            TeeState::Stalled => {
                *state = TeeState::Stalled;
                drop(state);
                translator.cancel();
            }
            TeeState::Finished => {
                drop(state);
                translator.cancel();
            }
            live @ TeeState::Live { .. } => {
                *state = live;
                drop(state);
                translator.cancel();
            }
        }
    }

    fn fail(&self, reason: &str) {
        {
            let mut state = self.state.lock();
            if matches!(*state, TeeState::Finished) {
                // Stopped before setup completed; the UI already cleared.
                return;
            }
            *state = TeeState::Finished;
        }
        log::warn!("[live-translation] translator setup failed: {reason}");
        self.sink.publish(self.session_id, LiveTranslationUpdate::unavailable());
    }

    /// The recording never started (inner recorder failed): release the
    /// session silently. An already-attached translator is cancelled; a setup
    /// task still in flight finds `Finished` on attach and cancels too.
    fn abort(&self) {
        let translator = {
            let mut state = self.state.lock();
            if matches!(*state, TeeState::Finished) {
                return;
            }
            let previous = std::mem::replace(&mut *state, TeeState::Finished);
            match previous {
                TeeState::Live { translator } => Some(translator),
                _ => None,
            }
        };
        if let Some(translator) = translator {
            translator.cancel();
        }
    }

    async fn finish_soft(&self, grace: Duration) {
        let translator = {
            let mut state = self.state.lock();
            let previous = std::mem::replace(&mut *state, TeeState::Finished);
            match previous {
                TeeState::Live { translator } => Some(translator),
                _ => None,
            }
        };
        if let Some(translator) = translator {
            translator.finish_soft(grace).await;
        }
    }
}

/// Fan-out consumer: the primary dictation consumer first (its behavior must
/// be bit-identical to the un-decorated recorder), then the capped preview.
pub struct TeeAudioConsumer {
    primary: Arc<dyn AudioConsumer>,
    session: Arc<LiveTranslationSessionState>,
}

impl TeeAudioConsumer {
    pub fn new(
        primary: Arc<dyn AudioConsumer>,
        session: Arc<LiveTranslationSessionState>,
    ) -> Arc<Self> {
        Arc::new(Self { primary, session })
    }
}

impl AudioConsumer for TeeAudioConsumer {
    fn consume_pcm_chunk(&self, pcm: &[u8]) {
        self.primary.consume_pcm_chunk(pcm);
        self.session.consume(pcm);
    }
}

struct LiveTranslationSetup {
    factory: Arc<dyn LiveTranslatorFactory>,
    task_spawner: Arc<dyn TaskSpawner>,
    sink_provider: LiveTranslationSinkProvider,
}

impl LiveTranslationSetup {
    fn begin(
        self: &Arc<Self>,
        session_id: SessionId,
        primary: &Arc<dyn AudioConsumer>,
    ) -> Option<(Arc<TeeAudioConsumer>, Arc<LiveTranslationSessionState>)> {
        let sink = (self.sink_provider)()?;
        let session = Arc::new(LiveTranslationSessionState::new(session_id, sink));
        let tee = TeeAudioConsumer::new(Arc::clone(primary), Arc::clone(&session));
        Some((tee, session))
    }

    fn spawn_setup(
        self: &Arc<Self>,
        session_id: SessionId,
        plan: LiveTranslationPlan,
        session: Arc<LiveTranslationSessionState>,
    ) {
        let factory = Arc::clone(&self.factory);
        let sink = session.sink();
        self.task_spawner.spawn(Box::pin(async move {
            // Per-session marker with no translated content: it is what makes
            // "preview on vs off" attributable in the field log, so main-path
            // timings can be compared between the two (the warnings alone only
            // cover failing sessions).
            let started = std::time::Instant::now();
            match factory.create(session_id, &plan, sink).await {
                Ok(translator) => {
                    log::info!(
                        "[live-translation] preview attached session_id={} setup_ms={}",
                        session_id,
                        started.elapsed().as_millis()
                    );
                    session.attach(translator);
                }
                Err(error) => {
                    log::warn!(
                        "[live-translation] preview unavailable session_id={} after_ms={}",
                        session_id,
                        started.elapsed().as_millis()
                    );
                    session.fail(&error.message);
                }
            }
        }));
    }
}

struct LiveTranslationRecording {
    inner: Box<dyn ActiveRecording>,
    session: Arc<LiveTranslationSessionState>,
    task_spawner: Arc<dyn TaskSpawner>,
}

impl ActiveRecording for LiveTranslationRecording {
    fn archive(&self) -> Option<Arc<dyn RecordingArchive>> {
        self.inner.archive()
    }

    fn stop(self: Box<Self>) -> BoxFuture<'static, Result<(), BackendError>> {
        Box::pin(async move {
            let result = self.inner.stop().await;
            // The tail translation must NEVER delay the main dictation path:
            // the omni engine encodes and uploads the recording right after
            // this future resolves, so the ≤1.5 s grace runs as a detached
            // task and keeps publishing snapshots while the main path works
            // (docs/live-translation-preview-design.md §2.3, §2.5).
            let session = self.session;
            self.task_spawner.spawn(Box::pin(async move {
                session.finish_soft(LIVE_TRANSLATION_FINISH_GRACE).await;
            }));
            result
        })
    }
}

/// Wraps the commit path of a provisional (low-latency) capture so the tee is
/// installed exactly where the consumer is handed over.
struct LiveTranslationPendingCapture {
    inner: Box<dyn PendingAudioCapture>,
    setup: Arc<LiveTranslationSetup>,
}

impl PendingAudioCapture for LiveTranslationPendingCapture {
    fn commit(
        self: Box<Self>,
        session_id: SessionId,
        context: Arc<DictationContext>,
        consumer: Arc<dyn AudioConsumer>,
        progress: Arc<dyn RecordingProgressSink>,
    ) -> BoxFuture<'static, Result<Box<dyn ActiveRecording>, BackendError>> {
        let Some(plan) = context.live_translation.clone() else {
            return self.inner.commit(session_id, context, consumer, progress);
        };
        let Some((tee, session)) = self.setup.begin(session_id, &consumer) else {
            log::warn!("[live-translation] event sink unavailable; preview disabled for this session");
            return self.inner.commit(session_id, context, consumer, progress);
        };
        // Pre-size the capsule window for the card before any text exists (see
        // announce_planned); the first painted text must not resize it.
        session.announce_planned();
        let setup = Arc::clone(&self.setup);
        Box::pin(async move {
            // A provisional capture has already opened the microphone in
            // `arm_capture`, so unlike `AudioRecorder::start` there is no capture
            // bring-up left to race: the handshake can overlap the handover and
            // the preview attaches as early as possible. Buffered PCM covers the
            // gap. A failed inner commit aborts the session, which cancels the
            // (possibly attached) translator.
            setup.spawn_setup(session_id, plan, Arc::clone(&session));
            let recording = match self.inner.commit(session_id, context, tee, progress).await {
                Ok(recording) => recording,
                Err(error) => {
                    session.abort();
                    return Err(error);
                }
            };
            Ok(Box::new(LiveTranslationRecording {
                inner: recording,
                session,
                task_spawner: Arc::clone(&setup.task_spawner),
            }) as Box<dyn ActiveRecording>)
        })
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, Result<(), BackendError>> {
        self.inner.discard()
    }
}

/// [`AudioRecorder`] decorator that tees recording PCM into a display-only
/// live translation session. With no plan in the context it is an identity
/// pass-through (same consumer `Arc`, no side effects).
pub struct LiveTranslationRecorder {
    inner: Arc<dyn AudioRecorder>,
    setup: Arc<LiveTranslationSetup>,
}

impl LiveTranslationRecorder {
    pub fn wrap(
        inner: Arc<dyn AudioRecorder>,
        factory: Arc<dyn LiveTranslatorFactory>,
        task_spawner: Arc<dyn TaskSpawner>,
        sink_provider: LiveTranslationSinkProvider,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            setup: Arc::new(LiveTranslationSetup {
                factory,
                task_spawner,
                sink_provider,
            }),
        })
    }
}

impl AudioRecorder for LiveTranslationRecorder {
    fn arm_capture(
        &self,
        request: CaptureStartRequest,
    ) -> Result<Box<dyn PendingAudioCapture>, BackendError> {
        self.inner.arm_capture(request).map(|capture| {
            Box::new(LiveTranslationPendingCapture {
                inner: capture,
                setup: Arc::clone(&self.setup),
            }) as Box<dyn PendingAudioCapture>
        })
    }

    fn discard_capture(&self, press_id: u64) -> BoxFuture<'static, Result<(), BackendError>> {
        self.inner.discard_capture(press_id)
    }

    fn start(
        &self,
        session_id: SessionId,
        context: Arc<DictationContext>,
        consumer: Arc<dyn AudioConsumer>,
        progress: Arc<dyn RecordingProgressSink>,
    ) -> BoxFuture<'static, Result<Box<dyn ActiveRecording>, BackendError>> {
        let Some(plan) = context.live_translation.clone() else {
            return self.inner.start(session_id, context, consumer, progress);
        };
        let Some((tee, session)) = self.setup.begin(session_id, &consumer) else {
            log::warn!("[live-translation] event sink unavailable; preview disabled for this session");
            return self.inner.start(session_id, context, consumer, progress);
        };
        // Pre-size the capsule window for the card before any text exists (see
        // announce_planned); the first painted text must not resize it.
        session.announce_planned();
        let inner = Arc::clone(&self.inner);
        let setup = Arc::clone(&self.setup);
        Box::pin(async move {
            let recording = match inner.start(session_id, context, tee, progress).await {
                Ok(recording) => recording,
                Err(error) => {
                    session.abort();
                    return Err(error);
                }
            };
            // Spawned only once the capture is up. The setup task reads two
            // keychain entries and performs a TLS handshake; racing that against
            // the microphone open measurably slowed capture bring-up in the
            // field log (cpal stream_built median 43.5ms OFF vs 62.3ms ON, first
            // PCM 86.6ms vs 109.3ms over 25/24 captures). The tee buffers PCM
            // from the very first chunk, so no audio is lost: the preview simply
            // attaches while the main path is already streaming.
            setup.spawn_setup(session_id, plan, Arc::clone(&session));
            Ok(Box::new(LiveTranslationRecording {
                inner: recording,
                session,
                task_spawner: Arc::clone(&setup.task_spawner),
            }) as Box<dyn ActiveRecording>)
        })
    }

    fn feed_pcm(&self, session_id: SessionId, pcm: &[u8]) -> Result<(), BackendError> {
        self.inner.feed_pcm(session_id, pcm)
    }
}

/// Computes the frozen plan for one capture (design §3.2). `None` keeps the
/// recorder decorator completely inert.
pub fn live_translation_plan(
    preview_enabled: bool,
    dictation_purpose: bool,
    options: &DictationStartOptions,
    context: &DictationContext,
) -> Option<LiveTranslationPlan> {
    if !preview_enabled || !dictation_purpose {
        return None;
    }
    if options.audio_source != DictationAudioSource::Microphone {
        return None;
    }
    use crate::dictation_context::DictationOutputTarget;
    if !matches!(
        options.output_target,
        DictationOutputTarget::ForegroundApp
            | DictationOutputTarget::Undecided
            | DictationOutputTarget::QuickNote
    ) {
        return None;
    }
    // v1 channel gate: only the multimodal pipeline on a Bailian omni channel.
    if context.pipeline_mode != PipelineMode::Multimodal
        || !channel_supports_live_translation(&context.omni.provider_type)
    {
        return None;
    }
    if !has_no_live_draft(context) {
        return None;
    }
    Some(LiveTranslationPlan {
        provider_id: context.omni.provider_id.clone(),
        target_language: LIVE_TRANSLATION_TARGET_LANGUAGE.to_string(),
    })
}

/// v1 approximation of "the recognizer shows no live draft": the multimodal
/// pipeline produces text only after stop, and traditional stable mode buffers
/// the whole recording (design §3.2, open question O-3).
fn has_no_live_draft(context: &DictationContext) -> bool {
    context.pipeline_mode == PipelineMode::Multimodal || context.recording.transcribe_after_stop
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictation_context::{DictationOutputTarget, ProviderInvocation};
    use crate::events::{BackendEventKind, BackendEventPublisher, EventBus, EventRecvError};
    use crate::shared_types::PipelineMode;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct VecConsumer(ParkingMutex<Vec<u8>>);

    impl AudioConsumer for VecConsumer {
        fn consume_pcm_chunk(&self, pcm: &[u8]) {
            self.0.lock().extend_from_slice(pcm);
        }
    }

    impl VecConsumer {
        fn bytes(&self) -> Vec<u8> {
            self.0.lock().clone()
        }
    }

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

    #[derive(Default)]
    struct RecordingTranslatorSession {
        bytes: ParkingMutex<Vec<u8>>,
        finishes: AtomicUsize,
        cancels: AtomicUsize,
    }

    impl RecordingTranslatorSession {
        fn bytes(&self) -> Vec<u8> {
            self.bytes.lock().clone()
        }
    }

    impl AudioConsumer for RecordingTranslatorSession {
        fn consume_pcm_chunk(&self, pcm: &[u8]) {
            self.bytes.lock().extend_from_slice(pcm);
        }
    }

    impl LiveTranslatorSession for RecordingTranslatorSession {
        fn finish_soft(&self, _grace: Duration) -> BoxFuture<'static, ()> {
            self.finishes.fetch_add(1, Ordering::AcqRel);
            Box::pin(async {})
        }

        fn cancel(&self) {
            self.cancels.fetch_add(1, Ordering::AcqRel);
        }
    }

    struct CountingFactory {
        calls: AtomicUsize,
        fail: bool,
        /// When set, `create` waits for one notification before resolving, so
        /// tests can deterministically order stop() before attach().
        gate: Option<Arc<tokio::sync::Notify>>,
        created: ParkingMutex<Vec<Arc<RecordingTranslatorSession>>>,
    }

    impl CountingFactory {
        fn ok() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                fail: false,
                gate: None,
                created: ParkingMutex::new(Vec::new()),
            })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                fail: true,
                gate: None,
                created: ParkingMutex::new(Vec::new()),
            })
        }

        fn gated() -> (Arc<Self>, Arc<tokio::sync::Notify>) {
            let gate = Arc::new(tokio::sync::Notify::new());
            (
                Arc::new(Self {
                    calls: AtomicUsize::new(0),
                    fail: false,
                    gate: Some(Arc::clone(&gate)),
                    created: ParkingMutex::new(Vec::new()),
                }),
                gate,
            )
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Acquire)
        }

        fn translator(&self) -> Option<Arc<RecordingTranslatorSession>> {
            self.created.lock().first().cloned()
        }
    }

    impl LiveTranslatorFactory for CountingFactory {
        fn create(
            &self,
            _session_id: SessionId,
            _plan: &LiveTranslationPlan,
            _sink: Arc<dyn LiveTranslationSink>,
        ) -> BoxFuture<'static, Result<Arc<dyn LiveTranslatorSession>, BackendError>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let gate = self.gate.clone();
            if self.fail {
                return Box::pin(async {
                    Err(BackendError::new(
                        BackendErrorCode::Provider,
                        "translator factory down",
                    ))
                });
            }
            let translator = Arc::new(RecordingTranslatorSession::default());
            self.created.lock().push(Arc::clone(&translator));
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                Ok(translator as Arc<dyn LiveTranslatorSession>)
            })
        }
    }

    struct NoopProgress;

    impl RecordingProgressSink for NoopProgress {
        fn publish_level(&self, _elapsed_ms: u64, _level: f32) -> Result<(), BackendError> {
            Ok(())
        }
    }

    struct SpyRecording {
        stops: Arc<AtomicUsize>,
        stop_fails: bool,
    }

    impl ActiveRecording for SpyRecording {
        fn stop(self: Box<Self>) -> BoxFuture<'static, Result<(), BackendError>> {
            self.stops.fetch_add(1, Ordering::AcqRel);
            let stop_fails = self.stop_fails;
            Box::pin(async move {
                if stop_fails {
                    Err(BackendError::new(
                        BackendErrorCode::Provider,
                        "inner recorder stop failed",
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }

    struct SpyRecorder {
        chunks: Vec<Vec<u8>>,
        captured: ParkingMutex<Option<Arc<dyn AudioConsumer>>>,
        stops: Arc<AtomicUsize>,
        stop_fails: bool,
    }

    impl SpyRecorder {
        fn new(chunks: Vec<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                chunks,
                captured: ParkingMutex::new(None),
                stops: Arc::new(AtomicUsize::new(0)),
                stop_fails: false,
            })
        }

        fn with_stop_failure(chunks: Vec<Vec<u8>>) -> Arc<Self> {
            let recorder = Self::new(chunks);
            // SAFETY-free test helper: rebuild with the flag set.
            Arc::new(Self {
                chunks: recorder.chunks.clone(),
                captured: ParkingMutex::new(None),
                stops: Arc::clone(&recorder.stops),
                stop_fails: true,
            })
        }

        fn captured_consumer(&self) -> Option<Arc<dyn AudioConsumer>> {
            self.captured.lock().clone()
        }
    }

    impl AudioRecorder for SpyRecorder {
        fn start(
            &self,
            _session_id: SessionId,
            _context: Arc<DictationContext>,
            consumer: Arc<dyn AudioConsumer>,
            _progress: Arc<dyn RecordingProgressSink>,
        ) -> BoxFuture<'static, Result<Box<dyn ActiveRecording>, BackendError>> {
            *self.captured.lock() = Some(Arc::clone(&consumer));
            let chunks = self.chunks.clone();
            let stops = Arc::clone(&self.stops);
            let stop_fails = self.stop_fails;
            Box::pin(async move {
                for chunk in chunks {
                    consumer.consume_pcm_chunk(&chunk);
                }
                Ok(Box::new(SpyRecording { stops, stop_fails }) as Box<dyn ActiveRecording>)
            })
        }
    }

    fn context_with_plan() -> Arc<DictationContext> {
        let mut context = DictationContext::default();
        context.live_translation = Some(LiveTranslationPlan {
            provider_id: "omni-channel".to_string(),
            target_language: LIVE_TRANSLATION_TARGET_LANGUAGE.to_string(),
        });
        Arc::new(context)
    }

    fn bus_sink_provider() -> (Arc<EventBus>, LiveTranslationSinkProvider) {
        let bus = Arc::new(EventBus::new(64));
        let publisher = BackendEventPublisher::new(Arc::clone(&bus));
        let provider: LiveTranslationSinkProvider =
            Arc::new(move || Some(live_translation_sink(publisher.clone())));
        (bus, provider)
    }

    fn decorator(
        inner: Arc<SpyRecorder>,
        factory: Arc<CountingFactory>,
        sink_provider: LiveTranslationSinkProvider,
    ) -> Arc<LiveTranslationRecorder> {
        let factory: Arc<dyn LiveTranslatorFactory> = factory;
        LiveTranslationRecorder::wrap(
            inner,
            Arc::clone(&factory),
            Arc::new(crate::config::TokioTaskSpawner),
            sink_provider,
        )
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        for _ in 0..400 {
            if ready() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition not met within 2s");
    }

    fn count_live_translation_events(bus: &EventBus) -> usize {
        bus.replay_after(0)
            .events
            .iter()
            .filter(|event| {
                matches!(event.kind, BackendEventKind::LiveTranslationUpdate(_))
            })
            .count()
    }

    // ---- identity when disabled ----

    #[tokio::test]
    async fn recorder_decorator_is_identity_when_plan_is_none() {
        let inner = SpyRecorder::new(vec![vec![1, 0, 2, 0]]);
        let factory = CountingFactory::ok();
        let (bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);
        let mut subscription = bus.subscribe();

        let consumer: Arc<dyn AudioConsumer> = Arc::new(VecConsumer::default());
        let recording = recorder
            .start(
                SessionId::new(),
                DictationContext::default().into(),
                Arc::clone(&consumer),
                Arc::new(NoopProgress),
            )
            .await
            .expect("inner start result must pass through");
        // The inner recorder received the exact same Arc, not a tee.
        let captured = inner.captured_consumer().expect("consumer captured");
        assert!(Arc::ptr_eq(&captured, &consumer));
        assert_eq!(factory.calls(), 0);
        assert_eq!(count_live_translation_events(&bus), 0);

        recording.stop().await.expect("inner stop passes through");
        assert_eq!(factory.calls(), 0);
        assert_eq!(count_live_translation_events(&bus), 0);
        assert!(matches!(
            subscription.try_recv(),
            Err(EventRecvError::Empty)
        ));
    }

    // ---- tee ----

    #[tokio::test]
    async fn recorder_decorator_tees_pcm_without_mutating_primary() {
        let chunks = vec![vec![1u8, 0, 2, 0], vec![3, 0, 4, 0]];
        let inner = SpyRecorder::new(chunks.clone());
        let factory = CountingFactory::ok();
        let (_bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);

        let primary = Arc::new(VecConsumer::default());
        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                primary.clone(),
                Arc::new(NoopProgress),
            )
            .await
            .expect("start succeeds");
        // The inner recorder received the tee, not the primary directly.
        let captured = inner.captured_consumer().expect("consumer captured");
        assert!(!Arc::ptr_eq(
            &captured,
            &(primary.clone() as Arc<dyn AudioConsumer>)
        ));
        // Primary sees the exact byte sequence immediately.
        let expected: Vec<u8> = chunks.concat();
        assert_eq!(primary.bytes(), expected);

        wait_until(|| factory.calls() == 1).await;
        let translator = factory.translator().expect("translator created");
        wait_until(|| !translator.bytes().is_empty()).await;
        // The buffered prefix is flushed on attach, so the translator sees the
        // same byte sequence as the primary consumer.
        assert_eq!(translator.bytes(), expected);

        recording.stop().await.expect("stop succeeds");
        // The tail finish runs detached from stop(); it must still happen.
        wait_until(|| translator.finishes.load(Ordering::Acquire) == 1).await;
        assert_eq!(translator.cancels.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn tee_per_chunk_cost_stays_off_the_blocking_path() {
        // The capture thread feeds every recorded chunk through the tee, so the
        // per-chunk budget is part of the main-path contract: buffering and
        // hand-off must stay memory-only work. A future change that moved
        // keychain reads, base64 of the whole buffer, or a socket write in here
        // would cost dropped audio frames, not just latency — this bound is
        // loose against the ~8µs measured cost, so it only trips on something blocking.
        const CHUNKS: usize = 100;
        const CHUNK_LEN: usize = 3200; // 100 ms of 16 kHz mono i16
        let inner = SpyRecorder::new(vec![vec![7u8; CHUNK_LEN]; CHUNKS]);
        let factory = CountingFactory::ok();
        let (_bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);

        let started = std::time::Instant::now();
        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await
            .expect("start succeeds");
        let per_chunk = started.elapsed() / CHUNKS as u32;

        assert!(
            per_chunk < std::time::Duration::from_micros(500),
            "tee cost per chunk regressed to {per_chunk:?} (budget 500µs)"
        );
        recording.stop().await.expect("stop succeeds");
    }

    #[tokio::test]
    async fn session_start_announces_the_planned_preview_before_any_text() {
        // The capsule pre-sizes its window from this empty streaming snapshot, so
        // it must be on the bus as soon as the plan exists — before the
        // translator attaches and long before the first translated word. Growing
        // the window only when text arrives stretches the previous frame across
        // the new bounds (the voice animation flashes through the card's slot).
        let inner = SpyRecorder::new(vec![vec![1, 2]]);
        let (factory, gate) = CountingFactory::gated();
        let (bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);

        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await
            .expect("start succeeds");

        // The attach is parked on the gate, so the announcement is the only
        // thing that can have been published so far.
        let replay = bus.replay_after(0);
        let updates: Vec<LiveTranslationUpdate> = replay
            .events
            .iter()
            .filter_map(|event| match &event.kind {
                BackendEventKind::LiveTranslationUpdate(update) => Some(update.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            updates,
            vec![LiveTranslationUpdate {
                text: String::new(),
                status: LiveTranslationStatus::Streaming
            }]
        );

        gate.notify_one();
        recording.stop().await.expect("stop succeeds");
    }

    // ---- failure isolation ----

    #[tokio::test]
    async fn translator_failure_does_not_fail_recorder_start() {
        let inner = SpyRecorder::new(vec![vec![9, 9]]);
        let factory = CountingFactory::failing();
        let (bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);

        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await
            .expect("translator failure must never fail the recorder start");

        // The empty "preview planned" announcement is published synchronously at
        // start, so waiting for "any event" would race the failure snapshot.
        wait_until(|| {
            bus.replay_after(0).events.iter().any(|event| {
                matches!(
                    &event.kind,
                    BackendEventKind::LiveTranslationUpdate(update)
                        if update.status == LiveTranslationStatus::Unavailable
                )
            })
        })
        .await;
        let replay = bus.replay_after(0);
        // The first event on the bus is the empty "preview planned" announcement
        // published at start; the failure itself surfaces as the Unavailable
        // snapshot that follows it.
        let update = replay.events.iter().find_map(|event| match &event.kind {
            BackendEventKind::LiveTranslationUpdate(update)
                if update.status == LiveTranslationStatus::Unavailable =>
            {
                Some(update.clone())
            }
            _ => None,
        });
        assert_eq!(
            update,
            Some(LiveTranslationUpdate {
                text: String::new(),
                status: LiveTranslationStatus::Unavailable
            })
        );
        recording.stop().await.expect("stop still succeeds");
    }

    #[tokio::test]
    async fn recorder_start_failure_aborts_the_translator() {
        struct FailingRecorder;
        impl AudioRecorder for FailingRecorder {
            fn start(
                &self,
                _session_id: SessionId,
                _context: Arc<DictationContext>,
                _consumer: Arc<dyn AudioConsumer>,
                _progress: Arc<dyn RecordingProgressSink>,
            ) -> BoxFuture<'static, Result<Box<dyn ActiveRecording>, BackendError>> {
                Box::pin(async {
                    Err(BackendError::new(
                        BackendErrorCode::Provider,
                        "microphone unavailable",
                    ))
                })
            }
        }
        // The preview must never race capture bring-up: the setup task (two
        // keychain reads + a TLS handshake) is spawned only after the inner
        // start resolves, so a failing start leaves the factory untouched and
        // there is no translator to leak.
        let (factory, _gate) = CountingFactory::gated();
        let (_bus, sink_provider) = bus_sink_provider();
        let factory_dyn = Arc::clone(&factory) as Arc<dyn LiveTranslatorFactory>;
        let recorder = LiveTranslationRecorder::wrap(
            Arc::new(FailingRecorder),
            factory_dyn,
            Arc::new(crate::config::TokioTaskSpawner),
            sink_provider,
        );
        let result = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("inner recorder failure must pass through"),
        };
        assert_eq!(error.message, "microphone unavailable");
        // Give a wrongly-early setup task every chance to run before asserting
        // it was never spawned: this is the ordering guarantee that keeps the
        // microphone open off the translator's critical path.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(factory.calls(), 0, "setup must not race the microphone open");
        assert!(factory.translator().is_none());
    }

    #[tokio::test]
    async fn translator_stop_result_never_overrides_inner_stop() {
        let inner = SpyRecorder::with_stop_failure(vec![vec![5, 5]]);
        let factory = CountingFactory::ok();
        let (_bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);

        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await
            .expect("start succeeds");
        wait_until(|| factory.calls() == 1).await;
        let translator = factory.translator().expect("translator created");
        // Wait for the attach flush so stop() deterministically sees a live
        // translator instead of racing the setup task.
        wait_until(|| !translator.bytes().is_empty()).await;

        let error = recording.stop().await.expect_err("inner stop error is returned");
        assert_eq!(error.message, "inner recorder stop failed");
        assert_eq!(inner.stops.load(Ordering::Acquire), 1);
        // The soft finish still runs (detached) even though the inner stop
        // failed.
        wait_until(|| translator.finishes.load(Ordering::Acquire) == 1).await;
    }

    #[tokio::test]
    async fn stop_returns_before_the_tail_translation_finishes() {
        // Regression test for the main-path slowdown: stop() used to await
        // finish_soft inline, delaying the omni encode/upload by up to the
        // 1.5 s grace after every key release.
        struct SlowFinishTranslator {
            bytes: ParkingMutex<Vec<u8>>,
            finishes: AtomicUsize,
        }
        impl AudioConsumer for SlowFinishTranslator {
            fn consume_pcm_chunk(&self, pcm: &[u8]) {
                self.bytes.lock().extend_from_slice(pcm);
            }
        }
        impl LiveTranslatorSession for SlowFinishTranslator {
            fn finish_soft(&self, _grace: Duration) -> BoxFuture<'static, ()> {
                self.finishes.fetch_add(1, Ordering::AcqRel);
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                })
            }
            fn cancel(&self) {}
        }
        struct SlowFinishFactory {
            created: ParkingMutex<Option<Arc<SlowFinishTranslator>>>,
        }
        impl LiveTranslatorFactory for SlowFinishFactory {
            fn create(
                &self,
                _session_id: SessionId,
                _plan: &LiveTranslationPlan,
                _sink: Arc<dyn LiveTranslationSink>,
            ) -> BoxFuture<'static, Result<Arc<dyn LiveTranslatorSession>, BackendError>> {
                let translator = Arc::new(SlowFinishTranslator {
                    bytes: ParkingMutex::new(Vec::new()),
                    finishes: AtomicUsize::new(0),
                });
                *self.created.lock() = Some(Arc::clone(&translator));
                Box::pin(async move { Ok(translator as Arc<dyn LiveTranslatorSession>) })
            }
        }
        let inner = SpyRecorder::new(vec![vec![8, 8]]);
        let factory = Arc::new(SlowFinishFactory {
            created: ParkingMutex::new(None),
        });
        let factory_dyn: Arc<dyn LiveTranslatorFactory> = Arc::clone(&factory) as _;
        let (_bus, sink_provider) = bus_sink_provider();
        let recorder = LiveTranslationRecorder::wrap(
            Arc::clone(&inner) as Arc<SpyRecorder> as Arc<dyn AudioRecorder>,
            factory_dyn,
            Arc::new(crate::config::TokioTaskSpawner),
            sink_provider,
        );
        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await
            .expect("start succeeds");
        // The factory runs in a detached setup task; wait for it to exist.
        wait_until(|| factory.created.lock().is_some()).await;
        let translator = factory
            .created
            .lock()
            .clone()
            .expect("translator created");
        wait_until(|| !translator.bytes.lock().is_empty()).await;

        let began = std::time::Instant::now();
        recording.stop().await.expect("stop succeeds");
        let elapsed = began.elapsed();
        // The 400 ms soft finish must run detached: stop() returns in a
        // fraction of it (the inner fixture stop resolves immediately).
        assert!(
            elapsed < Duration::from_millis(300),
            "stop() must not wait for the tail translation, took {elapsed:?}"
        );
        wait_until(|| translator.finishes.load(Ordering::Acquire) == 1).await;
    }

    #[tokio::test]
    async fn stop_before_attach_cancels_the_late_translator() {
        let inner = SpyRecorder::new(vec![vec![7]]);
        let (factory, gate) = CountingFactory::gated();
        let (_bus, sink_provider) = bus_sink_provider();
        let recorder = decorator(Arc::clone(&inner), Arc::clone(&factory), sink_provider);
        let recording = recorder
            .start(
                SessionId::new(),
                context_with_plan(),
                Arc::new(VecConsumer::default()),
                Arc::new(NoopProgress),
            )
            .await
            .expect("start succeeds");
        // Stop while the setup task is still parked on the gate: the session
        // is Finished before the translator ever attaches.
        recording.stop().await.expect("stop succeeds");
        wait_until(|| factory.calls() == 1).await;
        gate.notify_one();
        let translator = factory.translator().expect("translator created");
        wait_until(|| translator.cancels.load(Ordering::Acquire) > 0).await;
        assert_eq!(translator.finishes.load(Ordering::Acquire), 0);
    }

    // ---- backpressure ----

    #[test]
    fn pending_audio_is_capped() {
        let sink = Arc::new(CaptureSink::default());
        let session = LiveTranslationSessionState::new(SessionId::new(), sink.clone());
        session.consume(&[1u8; MAX_PENDING_TRANSLATION_AUDIO_BYTES]);
        assert!(sink.updates().is_empty());
        session.consume(&[1u8; 1]);
        let updates = sink.updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].status, LiveTranslationStatus::Unavailable);
        // Stalled: later audio neither grows the buffer nor publishes again.
        session.consume(&[2u8; 4096]);
        assert_eq!(sink.updates().len(), 1);
    }

    #[tokio::test]
    async fn stalled_session_cancels_attached_translator() {
        let sink = Arc::new(CaptureSink::default());
        let session = Arc::new(LiveTranslationSessionState::new(
            SessionId::new(),
            sink.clone(),
        ));
        session.consume(&[1u8; MAX_PENDING_TRANSLATION_AUDIO_BYTES + 1]);
        let translator = Arc::new(RecordingTranslatorSession::default());
        session.attach(translator.clone());
        assert_eq!(translator.cancels.load(Ordering::Acquire), 1);
        assert!(translator.bytes().is_empty());
    }

    // ---- plan predicate (design §3.2) ----

    fn multimodal_dashscope_context(provider_type: &str) -> DictationContext {
        let mut context = DictationContext::default();
        context.pipeline_mode = PipelineMode::Multimodal;
        context.omni = ProviderInvocation::new("omni-channel", provider_type);
        context
    }

    #[test]
    fn context_plan_is_none_when_disabled() {
        let context = multimodal_dashscope_context("dashscope-omni");
        let options = DictationStartOptions::default();
        assert!(live_translation_plan(false, true, &options, &context).is_none());
        let plan = live_translation_plan(true, true, &options, &context).expect("plan");
        assert_eq!(plan.provider_id, "omni-channel");
        assert_eq!(plan.target_language, LIVE_TRANSLATION_TARGET_LANGUAGE);
    }

    #[test]
    fn context_plan_is_none_for_non_dictation_purposes() {
        let context = multimodal_dashscope_context("dashscope-omni");
        let options = DictationStartOptions::default();
        // QaVoice / AuxiliaryVoice / QaText all pass purpose=false.
        assert!(live_translation_plan(true, false, &options, &context).is_none());
    }

    #[test]
    fn context_plan_is_none_for_external_audio_or_excluded_targets() {
        let context = multimodal_dashscope_context("dashscope-omni");
        let external = DictationStartOptions {
            audio_source: DictationAudioSource::External,
            ..DictationStartOptions::default()
        };
        assert!(live_translation_plan(true, true, &external, &context).is_none());
        for output_target in [
            DictationOutputTarget::Qa,
            DictationOutputTarget::VoiceEdit,
            DictationOutputTarget::CloudNote,
        ] {
            let options = DictationStartOptions {
                output_target,
                ..DictationStartOptions::default()
            };
            assert!(
                live_translation_plan(true, true, &options, &context).is_none(),
                "{output_target:?} must not enable the preview"
            );
        }
        for output_target in [
            DictationOutputTarget::ForegroundApp,
            DictationOutputTarget::Undecided,
            DictationOutputTarget::QuickNote,
        ] {
            let options = DictationStartOptions {
                output_target,
                ..DictationStartOptions::default()
            };
            assert!(
                live_translation_plan(true, true, &options, &context).is_some(),
                "{output_target:?} keeps the preview"
            );
        }
    }

    #[test]
    fn context_plan_requires_multimodal_dashscope_omni() {
        let options = DictationStartOptions::default();
        // Traditional pipeline (default context) never plans in v1, even in
        // stable mode: the channel gate requires the multimodal pipeline.
        let mut traditional = DictationContext::default();
        traditional.recording.transcribe_after_stop = true;
        assert!(live_translation_plan(true, true, &options, &traditional).is_none());
        // Multimodal with a non-Bailian omni channel: no shared protocol.
        let openai = multimodal_dashscope_context("openai");
        assert!(live_translation_plan(true, true, &options, &openai).is_none());
        let gemini = multimodal_dashscope_context("gemini");
        assert!(live_translation_plan(true, true, &options, &gemini).is_none());
        let custom = multimodal_dashscope_context("custom");
        assert!(live_translation_plan(true, true, &options, &custom).is_none());
        // The supported combination.
        let dashscope = multimodal_dashscope_context("dashscope-omni");
        assert!(live_translation_plan(true, true, &options, &dashscope).is_some());
    }
}
