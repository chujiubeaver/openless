# 录音期间实时英文翻译预览（仅显示、不落地、带总开关）— 评估与设计

- 状态：设计稿 v1（仅评估，未改代码）
- 范围：macOS Tauri 宿主 + `openless-core`；验收基准 = 用户现有配置「多模态管线 + `dashscope-omni` 渠道 + `qwen3.8-omni-flash`」
- 文件引用均相对 `openless-all/app/`；外部协议事实引用阿里云百炼官方文档（见文末「参考」）

---

## 0. 结论摘要

**可行，且改动面可控。** 一句话理由：omni 批量管线的音频已经以 `Arc<dyn AudioConsumer>` 的形式单点注入录音器（`cloud_providers.rs:2128-2135`），在录音器外层加一个"分流"装饰器即可把同一路 16 kHz PCM 复制给一个独立的 DashScope 实时翻译 WebSocket 客户端；该客户端与仓库现有 `asr/qwen_realtime.rs` 走**同一端点、同一鉴权、同一音频事件**（`qwen_realtime.rs:43, 196-203, 691-702`），只需替换 `session.update` 体和服务端事件解析。

关键选型：**② DashScope 实时语音翻译 `qwen3.8-livetranslate-flash-realtime`（text-only 模态，目标语 `en`）**，复用 omni 渠道的 API Key 与主机名，不新增凭据。

必须纠正的侦察线索：

| 线索 | 核实结果 |
|---|---|
| 「录音 PCM 走事件总线订阅制（`events.rs` 的 `subscribe`）」 | **不成立。** `EventBus` 只承载 UI 语义事件，`BackendEventKind` 无任何音频载荷（`events.rs:406-441`）。音频走 `AudioRecorder::start(.., consumer: Arc<dyn AudioConsumer>, ..)` 的单消费者注入（`ports.rs:555-561`），fan-out 只能在 consumer 层做 tee。 |
| 「本地 TextStream 以订阅方式接音频流，是并行消费者范式」 | TextStream 是作为 `TranscriptionSession`（本身 `: AudioConsumer`，`ports.rs:606`）被引擎当作**唯一** consumer 注入的，不是第二路订阅者。仓库内目前**没有**"一路 PCM 喂两个消费者"的既有实现；`BufferedTranscriptionSession`（`dictation_engine.rs:1518-1545`）是缓冲转发，不是广播。 |
| 「`provider_rules.rs` 有模型名→流式/非流式分类」 | 只有百炼/StepFun 的局部分类（`provider_rules.rs:860-949, 965-989`），**没有**跨 provider 的统一"当前 ASR 是否有实时草稿"谓词。v1 用 `pipeline_mode == Multimodal || recording.transcribe_after_stop` 近似（见 §3）。 |

---

## 1. 协议/模型选型

### 1.1 候选对比

| 维度 | ① omni 家族 realtime（`qwen3.8-omni-flash-realtime`） | **② 实时语音翻译 `qwen3.8-livetranslate-flash-realtime`（推荐）** | ③ `qwen3-asr-flash-realtime` + 流式 LLM 翻译（两跳） | 备选：`gummy-realtime-v1` |
|---|---|---|---|---|
| 定位 | 实时音视频**聊天**模型，须以 `response.create`/VAD 触发"回复"；官方描述为"实时音视频聊天模型" | 专用同传模型，源语自动检测，目标语 `translation.language: "en"`，官方标称端到端 ~2.3 s | ASR 分段完成 → LLM 翻译；每段两次网络往返 | 老一代 Gummy 系，走经典 `/api-ws/v1/inference` task 协议 |
| 延迟 | VAD 断句后才生成；需 `instructions` 把聊天模型"驯"成翻译器，输出前有思考 | VAD 内增量 `response.text.delta`，边说边出 | 最差：ASR partial 稳定 → LLM 首 token，叠加两段延迟 | 低，但模型质量弱于 Qwen3.x |
| 成本 | 输入音频 + 作为"回复"的输出 token，单价对标对话模型 | 输入音频 7 token/s，纯文本输出，无音频输出成本 | ASR 音频计费 + LLM token（且需不断重译增长文本） | 低 |
| 协议复杂度 | 与 `qwen_realtime.rs` 同端点，但要处理 `response.create`/对话 item 语义、防止模型"回答"而非"翻译" | **与 `qwen_realtime.rs` 同端点 `/api-ws/v1/realtime`、同 Bearer 鉴权、同 `input_audio_buffer.append`/`session.finish`**；只换 `session.update` 和 3 个服务端事件名 | 两个连接、两套错误路径、增量翻译 prompt 工程（抖动/重译） | 需复用 `bailian.rs` 的 run-task/result-generated 协议（`bailian.rs:446-450, 756-775`），与 omni 渠道凭据通用但协议线不同 |
| 可读性（用户诉求） | 聊天模型易加解释/寒暄，难保证"只输出译文" | 只输出译文；3.8 版增量 delta 直接拼接 | 文本随 ASR 修正反复重译，闪烁明显 | 可读 |
| 对仓库代码复用 | 高（同协议骨架） | **高（同协议骨架 + `derive_bailian_endpoint(.., QwenRealtime)` 直接可用，`provider_rules.rs:999-1035`）** | 中（ASR 客户端现成，LLM 流式客户端现成，但需新编排层） | 中 |

### 1.2 推荐与理由

选 **②**：

1. **同渠道同凭据**：omni 渠道的 `omni.api_key` / `omni.endpoint`（`credentials.rs:151-152`，读取逻辑 `cloud_providers.rs:1734-1774`）即可直连；`https://dashscope.aliyuncs.com/compatible-mode/v1` 经 `derive_bailian_endpoint(endpoint, BailianEndpointProtocol::QwenRealtime)` 得到 `wss://dashscope.aliyuncs.com/api-ws/v1/realtime`（`provider_rules.rs:1020`），专属工作空间域名（`*.maas.aliyuncs.com`）与新加坡域名同样按主机保留，这与官方要求的 `wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime?model=...` 一致。
2. **协议复用度最高**：`Qwen3RealtimeASR` 已经实现了连接超时、`session.updated` 就绪前缓存音频、100 ms 切块、写任务/读任务分离、`session.finish` 收尾、取消时关闭 writer（`qwen_realtime.rs:187-308, 380-392, 618-648`）。新客户端可复制该文件并改三处：`session_update_message()`（`:673-689`）、`handle_text_message()` 分发表（`:406-448`）、最终结果语义（不需要 `RawTranscript`，只要快照回调）。
3. **低延迟 + 可读优先**：3.8 版 `response.text.delta` 为增量片段，客户端累加即可；无需 ①的"把聊天模型当翻译器"的 prompt 风险，也无 ③的重译闪烁。

**v1 固定参数**（无新配置项）：

```json
{ "type": "session.update",
  "session": {
    "output_modalities": ["text"],
    "translation": { "language": "en" },
    "audio": { "input": { "turn_detection": { "type": "speaker_detection", "threshold": 0.5 } } }
  } }
```

连接 URL：`{derived_ws_endpoint}?model=qwen3.8-livetranslate-flash-realtime`，`Authorization: Bearer <omni.api_key>`，输入 16 kHz / s16le / mono（官方 3.8 默认，无需 `sample_rate` 字段；与仓库录音规格一致，`asr/pcm.rs:1-12`）。

**兼容性要求**：解析器同时识别 3.8 风格（`response.text.delta` 增量、`response.text.done`）与 3.5 风格（`response.text.text` 的 `text`+`stash` 累积）——两代事件名不同是官方明确提示的坑；写法可照搬 `record_partial` 对 `text`/`stash` 的处理（`qwen_realtime.rs:479-517`）。

---

## 2. 架构与数据流

### 2.1 现状（omni 批量管线）

```
cpal 回调线程 (recorder.rs:1979 consumer.consume_pcm_chunk)
   └─► OmniPcm (cloud_providers.rs:1938-1942)  ← 仅 append 到内存 Vec
          │
   [热键松开] SharedOmniDictationEngine::finish (cloud_providers.rs:2161-2323)
          ├─ recording.stop()  (:2192)
          ├─ encode WAV (:2207-2216) → provider.complete_streaming(prompt, "", wav) (:2250)
          ├─ PolishDelta 流 → BackendEngineProgress → 插入会话 + 事件 (api.rs:2279-2302)
          └─ EngineResult{raw_text, polished_text} (:2309-2321) → 历史/插入
```

录音期间除 `RecordingLevel` 外**没有任何文本事件**；胶囊 `localAsrText` 为空，`CapsuleTranscriptRail` 不渲染（`Capsule.tsx:577-586, 1023-1030`）。

### 2.2 目标数据流

```
                       DictationContext.live_translation: Option<LiveTranslationPlan>
                       （api.rs capture_dictation_context 在 purpose==Dictation 且条件满足时置 Some）
                                           │
cpal 回调 ──► TeeAudioConsumer ──┬─► OmniPcm（原路，不变）
 (recorder.rs:1979)              └─► LiveTranslator::consume_pcm_chunk   ← 非阻塞：锁内 append + 100ms 切块 + mpsc::send
                                              │
                                     writer task ──► WS input_audio_buffer.append (base64)
                                     reader task ◄── response.text.delta / .done / error
                                              │
                                     LiveTranslationSink::publish(session_id, LiveTranslationUpdate{text快照,status})
                                              │
                                     EventBus (BackendEventKind::LiveTranslationUpdate)  ← 不进 state.transcripts
                                              │
                                     tauri_events.rs:36 app.emit("backend:event")
                                              │
                                     Capsule.tsx 新 reducer applyLiveTranslationEvent → <CapsuleTranscriptRail tone="translate">
```

**挂钩点 = `AudioRecorder` 装饰器**（`LiveTranslationRecorder`），包在 `AudioRecorderRouter` 外层（`core_adapters.rs:257-260`），同一个 `Arc<dyn AudioRecorder>` 同时传给 `PipelineDictationEngine::new`（`:261-265`）与 `SharedOmniDictationEngine::new`（`:267-269`）。理由：

- 两个引擎都只通过 `recorder.start(session_id, context, consumer, progress)`（`dictation_engine.rs:292-294`；`cloud_providers.rs:2133-2135`）或 `PendingAudioCapture::commit(.., consumer, ..)`（`dictation_engine.rs:282-289`；`cloud_providers.rs:2057-2059`）交出 consumer，装饰器在这两处 tee，**引擎零改动**。
- `context` 在两处都可拿到，判定逻辑集中在一处。
- 低延迟录音（`arm_capture` → `commit`）路径天然覆盖：装饰器包装 `arm_capture` 返回值，在 `commit` 时 tee（`ports.rs:515-525`）。
- 不包 `SharedOmniDictationEngine` 内部，避免把翻译耦合进 provider 文件；不包 `DictationEngineRouter`（`provider_registry.rs:215-271`），因为路由器看不到 consumer。

### 2.3 生命周期同步

| 听写事件 | 代码位置 | 翻译消费者动作 |
|---|---|---|
| `start`/`commit` 成功返回 `ActiveRecording` | `cloud_providers.rs:2133-2148` / `dictation_engine.rs:280-304` | 装饰器**先**返回包装后的 `ActiveRecording`（录音启动不等翻译连接），同时 `task_spawner.spawn(translator.open_session())`。就绪前 PCM 进 translator 的 `pending_audio`（上限见 §2.4）。 |
| 录音停止（finish 路径） | `cloud_providers.rs:2192 recording.stop()` | 包装的 `stop()`：先 `inner.stop().await`（结果原样返回），再 `translator.finish_soft()`：发 `session.finish`，最多再接收 **1.5 s** 尾段译文后关闭（omni 主链路此时正在 `complete_streaming`，通常 1–3 s，不阻塞主链路）。 |
| 取消 | `cloud_providers.rs:2394-2413` 同样调 `recording.stop()` | 同上路径；UI 收到 `cancelled` 终态即清除，尾段译文即使到达也不显示。装饰器无法区分 finish/cancel（`ActiveRecording::stop` 无参数，`ports.rs:581`），不需要区分。 |
| 录音启动失败 | `cloud_providers.rs:2137-2141` | 装饰器只在 `inner.start()` 成功后才 spawn 翻译；失败则无任何副作用。 |
| `update_context`（中途切换目标/翻译键） | `cloud_providers.rs:2325-2348` | 不影响：`live_translation` 计划在 start 时冻结；若新 context 的 `output_target` 变为 `Qa`/`VoiceEdit`，仅 UI 侧依据胶囊状态隐藏（v1 不追加处理，见开放问题）。 |

### 2.4 背压策略

- 回调线程只做：`parking_lot::Mutex` 短临界区 append + 满 3 200 字节切块 + `mpsc::UnboundedSender::send`（与 `qwen_realtime.rs:618-648` 一致），**绝不 await / 绝不做 base64**。
- `pending_audio`（就绪前缓存）与 writer 队列合计上限 **15 s 音频（480 KB）**；超限即把 translator 标记 `Failed(Stalled)`、丢弃后续音频并关闭连接——宁可翻译消失，不可内存无界增长。`qwen_realtime.rs` 现无此上限（ASR 必须保音频），翻译链路必须加。
- 事件发布节流：`LiveTranslationUpdate` 为**全文快照**（而非增量），reader task 侧按 ≥ 80 ms 合并后发布，避免每个 token 一次 `app.emit`。

### 2.5 故障隔离

- 装饰器 `start`/`commit` 的 `Result` **只来自内层录音器**；translator 构造/连接/鉴权/协议错误全部 `log::warn!` + 发布 `status: Unavailable`，不向引擎返回 `Err`。
- `stop()` 的 `Result` 只来自内层；translator 收尾错误吞掉。
- translator 不持有 `OmniPcm`/`BufferedTranscriptionSession` 引用，不触碰 `EngineProgressSink`，因此不可能污染 `state.transcripts`（`api.rs:2270-2274`）或 `PolishDelta` 插入路径（`api.rs:2289-2299`）。
- 新事件类型独立于 `TranscriptDelta`，`BackendEngineProgress` 不处理它；前端 `applyTranscriptEvent` 对未知 kind 只推进水位（`backendEvent.ts:206-208`），现有草稿逻辑不受影响。

---

## 3. 开关与触发条件

### 3.1 偏好字段

| 项 | 值 |
|---|---|
| Rust 字段 | `UserPreferences.live_translation_preview_enabled: bool`，`#[serde(default)]`（默认 `false`） |
| 线格式 | `liveTranslationPreviewEnabled` |
| 触点 | `shared_types.rs` 五处镜像：主结构体（`:385-414` 区域，紧邻 `capsule_transcript_enabled :402`）、Wire 结构体（`:969-977`）、to-wire（`:1233-1237`）、from-wire（`:1436-1440`）、`Default`（`:1816-1829`）；参照 `capsule_transcript_preferences_tests`（`:3803-3835`）补"旧 JSON 缺字段 → false"回归测试 |
| 云同步分类 | `cloud_sync_e2ee_documents/registry.rs` 新增 `PreferenceField{ class: Portable, shape: Boolean }`（参照 `:105-111`）。**不登记会被 `tests.rs:110-118` 的"每个持久字段必须有同步分类"断言拦下。** |
| 前端类型 | `src/lib/types.ts:355` 旁新增；`src/lib/ipc/mock-data.ts:50` 旁补默认 `false` |

### 3.2 生效条件（在 `api.rs:7328 capture_dictation_context` 内计算，写入 `DictationContext.live_translation`）

```
Some(plan)  当且仅当
  preferences.live_translation_preview_enabled
  && purpose == DictationContextPurpose::Dictation            // 排除 QaVoice / AuxiliaryVoice（api.rs:112-120）
  && options.audio_source == Microphone                       // 排除远程输入外部音频
  && options.output_target ∈ { ForegroundApp, Undecided, QuickNote }   // 排除 Qa / VoiceEdit / CloudNote
  && no_live_draft(context)                                   // 见下
  && channel_supports_live_translation(context)               // 见 §7
```

`no_live_draft(context)` v1 定义：`context.pipeline_mode == Multimodal || context.recording.transcribe_after_stop`（后者即"稳定模式"，`dictation_context.rs:306-307`）。传统管线 + 天然 batch ASR（whisper 等）在 v1 **不**自动判定（缺统一谓词，见开放问题 O-3）。

`channel_supports_live_translation` v1 定义：`pipeline_mode == Multimodal && context.omni.provider_type == "dashscope-omni"`。传统管线复用 `CredentialNamespace::Asr` 下百炼渠道的 key 留给阶段 5。

`LiveTranslationPlan { provider_id: String /* = context.omni.provider_id */, target_language: "en" }`——不在 context 里存 key；key 由装饰器在 start 时按 `provider_id` 读取（与 `build_omni_provider` 同源：`cloud_providers.rs:1734-1741`）。

### 3.3 关闭时的惰性保证（代码结构层面）

1. **判定集中在 context 构造**：开关关 → `live_translation == None`，整个运行期只多一次 `bool` 判断。
2. **装饰器首行短路**：`if context.live_translation.is_none() { return inner.start(...) }`，consumer 原样透传（可用 `Arc::ptr_eq` 断言）；不构造 tee、不读凭据、不 spawn 任务、不发事件。
3. **无 UI 差异**：前端 reducer 只在收到 `live_translation_update` 时才改状态；无事件则 `liveTranslation.text === ''`，`CapsuleTranscriptRail` 根本不挂载（`LiveTranscriptPill.tsx:116-117` 的空文本返回 `null`）。
4. **无网络**：WebSocket 客户端仅在装饰器 spawn 时构造；`Qwen3RealtimeASR` 式设计中"构造不连接、`open_session` 才连接"（`qwen_realtime.rs:187`）沿用。
5. **可测**：Fixture 录音器 + 计数型 translator 工厂，断言开关关时工厂调用次数为 0、consumer 指针相等、事件总线无 `LiveTranslationUpdate`（§8）。

---

## 4. "不落地"语义边界

译文**禁止**进入的管线（含代码锚点与保障方式）：

| 管线 | 锚点 | 保障 |
|---|---|---|
| 文本插入 | `api.rs:2289-2299`（仅 `PolishDelta`）、`ports.rs:714-727` | 新事件类型不经 `BackendEngineProgress`；无任何 `TextInsertionSession` 引用 |
| 最终出稿 / `EngineResult` | `cloud_providers.rs:2309-2321` | translator 不参与 `finish` |
| 实时草稿累积 `state.transcripts` | `api.rs:2270-2274` | 不复用 `TranscriptDelta` |
| 历史记录 | `api.rs:5485-5493 append_history` | 只有 `DictationSession` 入库，译文无字段 |
| 润色上下文窗口（近期历史回灌 prompt） | `api.rs:7430-7445`、`dictation_context.rs:294-295` | 源于历史，历史无译文 |
| 云同步（历史/偏好文档） | `cloud_sync_e2ee_documents/*` | 无持久结构承载译文；仅开关字段同步 |
| QA / 划词 / Voice Edit 上下文 | `qa_service.rs:578`、`selection_service.rs:484`、`selection_voice_service.rs:733` | 这些会话 `purpose != Dictation`，计划为 `None`（§3.2） |
| Cloud Note webhook | `output_target == CloudNote` 被排除（§3.2） | — |
| 调试录音存档 | `RecordingArchive`（`ports.rs:471-495`）只存音频 | 译文文本不写盘；音频存档行为不变（**注意**：开启 `recordAudioForDebug` 时音频本就落盘，与本功能无关） |
| 原生胶囊快照 | `capsule_snapshot.rs:131-145` 只投影 `TranscriptDelta` | v1 **不**把新事件加入快照投影（代价：胶囊窗口迟到挂载会错过已出译文，下一条全文快照即可补齐） |
| 日志 | — | 译文文本不进 `log::*`（仅记录字节数/事件数/错误码） |

**唯一的内存驻留**：`EventBus` 回放环形缓冲（`events.rs:454 EVENT_REPLAY_CAPACITY = 2048`）会短暂持有最近事件以供窗口重连回放；进程内、有界、不落盘，终态后随滚动淘汰。可接受。

**opt-in"保留译文"设置**：v1 **不提供**。理由：一旦提供就必须定义历史条目结构、同步、详情页展示、删除语义，是另一个功能；用户诉求"顺便学"不需要。列入阶段 5 候选。

---

## 5. UI 契约

### 5.1 胶囊译文行状态机

```
hidden ──(首个 LiveTranslationUpdate{text≠""} 且胶囊 state ∈ {recording})──► streaming
streaming ──(每条 update 替换全文快照)──► streaming
streaming ──(capsule:state 进入 polishing|transcribing)──► settling   // 仍显示，接收 ≤1.5s 尾段
settling / streaming ──(capsule:state ∈ {done, cancelled, error, idle} 或 sessionId 变化)──► cleared(=hidden，text="")
任意态 ──(LiveTranslationUpdate{status: Unavailable})──► hidden（v1 静默，不显示错误）
```

- 可见性谓词与现有草稿一致：`enabled && !selectionPolish && state ∈ {recording, transcribing, polishing}`（`capsuleTranscript.ts:9-18` 同形）。
- 会话门控复用 `TranscriptViewState` 的 `sessionId`/`awaitingSessionStart` 思路（`backendEvent.ts:13-25, 145-222`）：新建 `applyLiveTranslationEvent`，拒绝旧会话迟到事件。

### 5.2 与 omni 出稿瞬间的衔接

omni 最终文本**不在胶囊中展示**，而是流式插入文档并在 `Done` 态显示"已输入"（`tauri_events.rs:190-201`）。因此译文行在 `done` 到达时整体淡出即可，不存在"译文行让位给最终文本"的同窗竞争。`polishing` 期间（1–3 s）译文行继续显示尾段，是用户"读完最后一句"的窗口。

### 5.3 渲染形态

- macOS：复用 `CapsuleTranscriptRail`（`LiveTranscriptPill.tsx:108-130`），新增 `tone="translate"`（CSS 变量级别的色调差异 + 左侧 `EN` 小标），单行滚动与现有草稿 pill 一致，字号沿用 `capsuleTranscriptFontSize`。在 `ClassicCapsule`（`Capsule.tsx:576-586`）与 `TypelessCapsule`/Siri 分支的现有草稿槽位渲染：触发条件下草稿槽位必然为空（无实时草稿正是生效前提），**不做双行叠放**。
- Windows 分离轨道（`Capsule.tsx:711 splitTranscriptRail`、`CapsuleTranscriptOverlay`）：阶段 5。
- **不进主窗口**：主窗口是设置/历史，没有"当前会话"面板；译文不落地则无内容可展示；胶囊是唯一与说话同屏的表面。

### 5.4 设置页位置与文案

位置：`RecordingInputSection.tsx:480-489`「显示流式原文」行之后（同属胶囊显示类）。

| key | zh-CN | en |
|---|---|---|
| `settings.recording.liveTranslationPreviewLabel` | 实时英文翻译预览 | Live English translation preview |
| `settings.recording.liveTranslationPreviewDesc` | 录音期间在胶囊中实时显示所说内容的英文翻译，仅供查看，不插入文档、不进历史，录音结束即清除。音频会额外实时发送到当前百炼渠道的翻译模型（qwen3.8-livetranslate-flash-realtime），使用同一 API Key，按音频时长计费。 | Show a live English translation of what you say in the capsule while recording. Display only: never inserted, never saved to history, cleared when recording ends. Audio is additionally streamed to the translation model (qwen3.8-livetranslate-flash-realtime) on your current Bailian channel using the same API key; billed by audio duration. |
| `settings.recording.liveTranslationPreviewUnavailableOmni` | 需要「多模态模式」且渠道为百炼（dashscope-omni）。 | Requires Multimodal mode with a Bailian (dashscope-omni) channel. |
| `settings.recording.liveTranslationPreviewUnavailableStreaming` | 当前识别已提供实时原文，此功能不生效。 | Your current recognizer already shows live text; this preview is inactive. |
| `capsule.liveTranslationTag` | EN | EN |

i18n 以 `zh-CN` 为唯一内联资源与回退（`i18n/index.ts:66-68`），其余 locale（`src/i18n/{en,ja,...}.ts`）按现有约定补齐。

---

## 6. 隐私

两种情形区分文案与呈现时机：

| 情形 | 判定 | 文案要点 | 时机 |
|---|---|---|---|
| A. 主链路本身是云（用户基准：dashscope-omni） | `pipeline_mode == Multimodal` 且 omni 为云渠道 | "音频会**额外**实时流式发送到同一百炼渠道的翻译模型；主识别已在上传整段音频，本功能新增的是录音期间的实时上传与按秒计费。" | 开关描述文案常驻（§5.4）；无弹窗 |
| B. 主链路本地 ASR（传统 + 本地模型 + 稳定模式；v1 不支持，阶段 5） | `asr.provider_type ∈ 本地集合`（`dictation_context.rs:263-286` 列出的 foundry/sherpa/local-qwen3/whisper 等） | "你的识别在本机完成，但开启本功能后**录音期间的音频会实时发送到云端**（百炼）。" | 开启开关的瞬间弹一次确认（二选一按钮：继续/取消），确认后才写入 `true`；描述文案改为红字提示 |

共同约束：禁止"设置了但没生效"——开关置灰时文案说明原因（§7）；开关生效中，胶囊译文行自带 `EN` 标即为"正在上传"的可感知信号，不再额外加麦克风/云图标。

---

## 7. 不支持的组合

选择：**显示开关但置灰（disabled），下方一行原因文案**；不隐藏、不运行期报错。

| 组合 | 行为 |
|---|---|
| 非多模态管线，或 omni 渠道类型 ≠ `dashscope-omni`（openai/gemini/custom） | 置灰 + `liveTranslationPreviewUnavailableOmni` |
| 传统管线 + 流式 ASR（火山、讯飞、腾讯、百炼 realtime、StepFun stream、TextStream）且非稳定模式 | 置灰 + `liveTranslationPreviewUnavailableStreaming` |
| 开关 `true` 但运行期条件不满足（例如用户随后切回传统模式） | 计划为 `None`，无任何动作；设置页同步显示置灰态（值保留，便于切回时自动恢复） |
| 凭据缺失/鉴权失败/连接超时 | 静默：`log::warn!` + `status: Unavailable`；胶囊不显示错误（主链路不受影响，用户此刻正在说话，报错只会打断） |

理由：隐藏会造成"我记得有这个设置"的找不到；报错发生在录音中，时机最差；置灰 + 原因是三者中唯一能在用户决策时刻给出信息的方案。

---

## 8. 错误与取消语义矩阵

| 事件 | omni 主链路 | 翻译链路 | 胶囊译文行 |
|---|---|---|---|
| 翻译 WS 连接超时 / 401 / 4xx | 不受影响 | `Unavailable`，释放 | hidden |
| 翻译 WS 中途断开 | 不受影响 | `Unavailable`，释放 | 立即 hidden（不保留半截文本，避免误导） |
| 翻译队列超 15 s 积压 | 不受影响 | `Failed(Stalled)`，释放 | hidden |
| 用户取消录音 | `cancel` → `recording.stop()`（`cloud_providers.rs:2406-2408`） | `finish_soft`，尾段到达也无人显示 | cancelled → cleared |
| 录音停止 → omni 成功 | 正常插入 | `finish_soft` ≤1.5 s 后关闭 | polishing 期间 settling → done 时 cleared |
| 录音停止 → omni 失败 | Error 态 | 同上 | error → cleared |
| 录音器启动失败 | 返回 Err | 从未启动 | 无 |
| 偏好中途被关闭（录音中） | — | 本次会话按 start 时冻结继续（`PreferencesChanged` 不介入），下一次会话生效 | 继续到终态 |
| 应用退出 `BackendStopping` | — | task 随 runtime 终止；WS 由对端超时回收 | — |

---

## 9. 文件级改动清单

### 新增

| 文件 | 职责 |
|---|---|
| `crates/openless-core/src/asr/qwen_livetranslate.rs` | `QwenLiveTranslator`：复制 `qwen_realtime.rs` 骨架；`session.update`（text-only、`translation.language`）；解析 `response.text.delta`/`response.text.text(+stash)`/`response.text.done`/`error`/`session.finished`；全文快照回调 `LiveTranslationSink`；`pending`+队列 15 s 上限；`finish_soft(grace)`；`cancel()`。**不实现 `TranscriptionSession`**（它不是 ASR）。 |
| `crates/openless-core/src/live_translation.rs` | `LiveTranslationPlan`、`LiveTranslationUpdate{ text, status }`、`LiveTranslationSink` trait、`TeeAudioConsumer`、`LiveTranslationRecorder`（`AudioRecorder` 装饰器，含 `PendingAudioCapture`/`ActiveRecording` 包装）、`LiveTranslatorFactory` trait（便于测试注入计数工厂）。 |
| `src/lib/liveTranslation.ts` + `.test.ts` | `applyLiveTranslationEvent` reducer、可见性谓词。 |
| `docs/live-translation-preview.md`（实施时） | 用户向说明：计费/隐私/限制。 |

### 修改

| 文件 | 改动 |
|---|---|
| `crates/openless-core/src/shared_types.rs` | 五处镜像新增 `live_translation_preview_enabled`；默认测试 |
| `crates/openless-core/src/cloud_sync_e2ee_documents/registry.rs` | 登记字段 |
| `crates/openless-core/src/dictation_context.rs` | `DictationContext.live_translation: Option<LiveTranslationPlan>`（`:192-212`）；`capture()` 默认 `None` |
| `crates/openless-core/src/api.rs` | `capture_dictation_context`（`:7328`）内按 §3.2 置 `Some`；构造期 `bind_event_publisher` 到 `LiveTranslationRecorder`（参照 `:2838-2844`） |
| `crates/openless-core/src/events.rs` | `BackendEventKind::LiveTranslationUpdate(LiveTranslationUpdate)`（`:406-441`） |
| `crates/openless-core/src/provider_rules.rs` | `pub const QWEN_LIVETRANSLATE_DEFAULT_MODEL`；`channel_supports_live_translation(provider_type)`（紧邻 `default_omni_endpoint :728`） |
| `crates/openless-core/src/lib.rs` | 导出新类型 |
| `contract/backend-2.0.json` | `kinds` 增 `live_translation_update`（`:46-81`）+ `samples` 样本（camelCase） |
| `src-tauri/src/core_adapters.rs` | `:257-260` 之后用 `LiveTranslationRecorder::wrap(recorder, credential_store, task_spawner)` 包装，再传给两个引擎 |
| `src-tauri/src/tauri_events.rs` | `forward_legacy_event` 增 `LiveTranslationUpdate(_) => {}`（`:189` 旁）；无需专用 Tauri 事件名（走 `backend:event`） |
| `linux-egui/src/backend.rs` | `:257-262` 不包装（Linux v1 不启用）；若 `BackendEventKind` 有穷尽匹配需补 `_ => {}` |
| `src/lib/types.ts`、`src/lib/ipc/mock-data.ts` | 新字段 |
| `src/components/Capsule.tsx` | `backend:event` 订阅处（`:837-843`）并行喂 `liveTranslationRef`；在草稿槽位按 §5.3 渲染；终态清理（`:809-819`） |
| `src/components/LiveTranscriptPill.tsx` | `LiveTranscriptTone` 增 `'translate'`；可选 `prefixTag` |
| `src/pages/settings/RecordingInputSection.tsx` | `:489` 后新增行（Toggle + disabled 原因） |
| `src/i18n/zh-CN.ts`（`:1420` 区）及其他 locale | §5.4 文案 |
| `src/styles/*`（胶囊 CSS） | `[data-tone="translate"]` 配色 |

---

## 10. 测试计划

### Rust 单测（`crates/openless-core/src/**` `#[cfg(test)]`）

| 用例 | 断言 |
|---|---|
| `live_translation_preview_defaults_off_and_roundtrips` | 旧 JSON 缺字段 → `false`；`true` 往返保留（仿 `shared_types.rs:3806`） |
| `registry_classifies_live_translation_preview` | `tests.rs:110-118` 自动覆盖；另断言 class=Portable |
| `context_plan_is_none_when_disabled` | 任意管线/渠道，开关关 → `None` |
| `context_plan_is_none_for_non_dictation_purposes` | QaVoice/AuxiliaryVoice/CloudNote/Qa/VoiceEdit/External → `None` |
| `context_plan_requires_no_live_draft_and_dashscope_omni` | 传统+非稳定 → None；多模态+openai → None；多模态+dashscope-omni → Some |
| **`recorder_decorator_is_identity_when_plan_is_none`** | fixture recorder 记录收到的 consumer；`Arc::ptr_eq(received, original)`；translator 工厂计数 == 0；事件总线无 `LiveTranslationUpdate`；`stop()` 后同样为 0 |
| `recorder_decorator_tees_pcm_without_mutating_primary` | 喂 `[1,0,2,0]`：primary 与 translator 收到完全相同字节序列 |
| `translator_failure_does_not_fail_recorder_start` | 工厂返回 Err → `start()` 仍 Ok；发布 `Unavailable` |
| `translator_stop_error_is_swallowed` | `finish_soft` 返回 Err → 包装 `stop()` 仍返回 inner 结果 |
| `pending_audio_is_capped` | 就绪前喂 >15 s → 标记 Stalled，队列不再增长 |
| `parses_v38_incremental_deltas` / `parses_v35_text_and_stash` / `done_replaces_snapshot` / `error_marks_unavailable` | 仿 `qwen_realtime.rs:842-1021` 的 `handle_text_message` 测试风格 |
| `session_update_is_text_only_english` | `output_modalities == ["text"]`, `translation.language == "en"` |

### 契约测试

- `crates/openless-core/tests/contract_2.rs:153-175` 自动要求 `backend-2.0.json` 的 `kinds`/`samples` 一致且 camelCase。
- 新增 `scripts/live-translation-contract.test.mjs`：静态断言 ① `src/lib/backendEvent.ts`/`liveTranslation.ts` 不把 `live_translation_update` 写入 `TranscriptViewState.text`；② `api.rs` 中 `append_history` 调用点附近不出现 `LiveTranslation` 标识符（防止未来误接入历史）；③ 所有 locale 含新 key。风格参照 `scripts/less-computer-opencode-contract.test.mjs:240-242`。

### 前端单测（`npm test` → `scripts/frontend-test-runner.mjs`）

- `src/lib/liveTranslation.test.ts`：旧会话迟到事件被拒；终态清空；`Unavailable` 清空；可见性谓词在 `selectionPolish`/`idle` 为假。

### 手工用例（macOS，用户基准配置）

1. 开关关：录一段，Charles/Proxyman 抓包确认除 omni 一次 HTTPS 外无 WebSocket；CPU 采样无新线程。
2. 开关开 + 中文 10 s：首个英文片段出现时延记录（期望 ≤3 s）；松键后 ≤2 s 译文消失；文档中仅出现 omni 稿，历史仅一条且无英文。
3. 开关开 + 录音 2 s 内取消：无插入、无历史、译文行消失。
4. 开关开 + 断网后开始录音：omni 失败态正常显示，无翻译相关错误弹出。
5. 开关开 + 切回传统模式（火山流式）：设置页开关置灰、原文草稿正常、无翻译事件。
6. 开关开 + 稳定模式 + 百炼 ASR：v1 期望置灰（未支持），阶段 5 回归。
7. 低延迟录音开启（`arm_capture` 路径）：译文同样出现。
8. 设置页中途关闭开关：当前录音继续显示到终态，下一次不再出现。

---

## 11. 分阶段实施计划（PR 粒度）

| PR | 内容 | 验收 |
|---|---|---|
| **PR-1 协议客户端（惰性、未接线）** | `asr/qwen_livetranslate.rs` + 单测；`provider_rules.rs` 常量/谓词 | `cargo test -p openless-core qwen_livetranslate` 全绿；无任何调用方；`cargo check` 无警告 |
| **PR-2 数据面骨架（惰性、无生产者）** | 偏好字段 + 同步登记 + `DictationContext.live_translation` + `BackendEventKind::LiveTranslationUpdate` + 契约 JSON + TS 类型 + i18n key（文案） | 所有既有测试通过；`tests.rs` 注册断言通过；`contract_2.rs` 通过；前端 `tsc` 通过；运行时行为零变化（无人发布该事件） |
| **PR-3 分流装饰器与宿主接线** | `live_translation.rs`（tee/装饰器/工厂）+ `api.rs` 置计划 + `core_adapters.rs` 包装 + `tauri_events.rs` 匹配臂 | 关开关：`recorder_decorator_is_identity_when_plan_is_none` 等全绿；开开关：日志可见 WS 连接与事件计数；手工用例 1、4 |
| **PR-4 胶囊 UI 与设置页** | reducer + `translate` tone + 渲染 + 设置行（含置灰原因）+ 文案 | 手工用例 2、3、5、7、8；`npm test` 全绿；截图对比胶囊三种样式 |
| **PR-5（可选）扩展** | 传统+稳定模式复用 `CredentialNamespace::Asr` 百炼 key；统一 `asr_has_live_draft` 谓词；Windows 分离轨道；本地 ASR 情形 B 的确认弹窗 | 手工用例 6；按子项单独验收 |

PR-1/2 可并行；PR-3 依赖 1、2；PR-4 依赖 2（可用 mock 事件先行）。合并到 PR-3 为止用户不可见；PR-4 合并后默认仍关闭。

---

## 12. 风险与开放问题

### 风险

| # | 风险 | 缓解 |
|---|---|---|
| R-1 | `qwen3.8-livetranslate-flash-realtime` 在用户所在区域/工作空间未开通或模型名变更 | 模型名集中为常量；失败静默；文档标注 |
| R-2 | 官方 3.8 文档的端点示例为 `wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/...`，而 `qwen_realtime.rs` 默认 `wss://dashscope.aliyuncs.com/api-ws/v1/realtime`（该文件注释称两者都服务 `/api-ws/v1/realtime`，`qwen_realtime.rs:18-20`，但那是对 ASR 模型的验证） | PR-1 先用 `cargo run --example` 式冒烟脚本（仿 `examples/gemini_transcribe_smoke.rs`）对两种主机名各验一次 |
| R-3 | 回调线程多一次 `Mutex` 争用；极端情况下 translator 的锁被 reader 线程长时间持有 | 锁内只做 `extend_from_slice`+切块；reader 不持该锁发布事件 |
| R-4 | 费用：每次录音多一路按秒计费 | 文案明示；默认关 |
| R-5 | VAD 分段导致英文句子在胶囊中"跳句"，可读性下降 | 全文快照 + 单行滚动 pill 已是现有草稿的呈现方式；必要时只保留最近 2 段 |
| R-6 | `EventBus` 回放缓冲中残留译文 2048 条以内 | 已评估为可接受（§4） |

### 需要用户拍板

| # | 问题 | 建议默认 |
|---|---|---|
| **O-1** | 录音结束后是否允许 ≤1.5 s 尾段译文继续显示（覆盖 omni 润色期），还是松键即刻清除？ | 允许（更符合"读完最后一句"的学习诉求，且不延长主链路） |
| **O-2** | 流式 ASR 已有实时草稿时，是否允许共存（双行：原文 + 英文）？ | **不允许**（v1）。理由：胶囊单行 pill 布局是既有契约（`Capsule.tsx:577-586`），双行需重做窗口几何（`getClassicCapsuleGeometry`）与 Windows 分离轨道；且有原文时翻译对"学英语"的边际价值靠事后翻译功能已能满足。 |
| **O-3** | 传统管线 + 天然 batch ASR（whisper/gemini/elevenlabs 等）是否纳入 v1 生效条件？ | 不纳入；留 PR-5，先补统一谓词 |
| **O-4** | 翻译模型名是否允许高级覆盖（例如通过 omni 渠道现有 `omni.extra_headers` 之类的 JSON 字段）？ | v1 硬编码常量；不新增配置 |
| **O-5** | 设置页位置：放「录音与输入」胶囊区（本文建议），还是放「高级 → 多模态识别管线」区（`zh-CN.ts:2045-2050`）？ | 录音与输入区（它本质是胶囊显示项，且未来会扩展到传统管线） |
| **O-6** | `custom` 类型 omni 渠道但 base URL 主机为 dashscope 时，是否按渠道能力判定为支持？ | v1 仅 `dashscope-omni`；O-6 为 PR-5 候选 |
| **O-7** | 胶囊迟到挂载是否需要把译文加入原生快照投影（`capsule_snapshot.rs`）？ | 不需要（下一条全文快照自动补齐） |

---

## 参考

- 阿里云百炼《实时语音/音视频翻译-千问》：模型 `qwen3.8-livetranslate-flash-realtime`、`output_modalities: ["text"]`、`translation.language`、输入 16 kHz PCM、`input_audio_buffer.append`、`response.text.delta`/`response.done`、7 token/s 输入计费、2.3 s 时延 — https://help.aliyun.com/zh/model-studio/qwen3-5-livetranslate-flash-realtime
- 阿里云百炼《Qwen-Livetranslate-Realtime 客户端事件》：`session.update` 示例（3.8 / 3.5 两种体）、`session.finish` — https://help.aliyun.com/zh/model-studio/live-translator-client-events
- 阿里云百炼《Qwen-Livetranslate-Realtime 服务端事件》：3.8 `response.text.delta` 增量 vs 3.5 `response.text.text` 的 `text`+`stash`；`response.text.done`；`error`；`session.finished` — https://help.aliyun.com/zh/model-studio/live-translator-server-events
- 阿里云百炼《Realtime API 概览》：`qwen3.8-omni-flash-realtime` 等模型支持矩阵 — https://help.aliyun.com/zh/model-studio/realtime-api-overview
- 阿里云百炼《Qwen-Omni-Realtime》：omni realtime 的对话语义、`instructions`、`response.create` — https://help.aliyun.com/zh/model-studio/realtime
