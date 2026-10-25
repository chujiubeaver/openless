import type { BackendEvent } from './backendEvent';
import type { CapsuleState } from './types';

/**
 * Display-only live translation preview (docs/live-translation-preview-design.md §5).
 *
 * Updates are full-text snapshots published as `live_translation_update`
 * backend events. This reducer deliberately shares NO state with
 * `TranscriptViewState`: the preview must never leak into the transcript rail,
 * insertion, or history. Session gating mirrors the transcript reducer so a
 * late event from a previous session cannot repaint a new one.
 */

export type LiveTranslationStatus = 'streaming' | 'unavailable';

export interface LiveTranslationUpdate {
  text: string;
  status: LiveTranslationStatus;
}

export interface LiveTranslationViewState {
  sessionId: string | null;
  /** Global backend event watermark; shared cadence with the transcript state. */
  sequence: number;
  text: string;
  /** Reject updates until the backend announces the owning session. */
  awaitingSessionStart: boolean;
  sessionStartObserved: boolean;
  /** Sticky per generation: once unavailable the row stays hidden. */
  unavailable: boolean;
  /** Session ended but the card stays on screen until Esc or the next session. */
  lingering: boolean;
  /**
   * The backend announced a preview for this session before any text exists.
   * The capsule pre-sizes its window for the card, so the first painted text
   * does not resize the window underneath an already-painted frame — WebKit
   * stretches the previous frame across the new bounds, which reads as the voice
   * animation flashing through the card's slot.
   */
  armed: boolean;
}

export function createLiveTranslationState(): LiveTranslationViewState {
  return {
    sessionId: null,
    sequence: 0,
    text: '',
    awaitingSessionStart: false,
    sessionStartObserved: false,
    unavailable: false,
    lingering: false,
    armed: false,
  };
}

/**
 * Start a new UI generation at the recording edge. Like the transcript rail, a
 * session already announced by `dictation_state_changed(starting)` is reused so
 * events that arrive before the capsule:state edge are not erased.
 */
export function beginLiveTranslationGeneration(
  state: LiveTranslationViewState,
): LiveTranslationViewState {
  const reuseStartedSession =
    state.sessionStartObserved && state.sessionId !== null && !state.awaitingSessionStart;
  return {
    ...state,
    sessionId: reuseStartedSession ? state.sessionId : null,
    text: reuseStartedSession ? state.text : '',
    unavailable: reuseStartedSession ? state.unavailable : false,
    lingering: false,
    armed: reuseStartedSession ? state.armed : false,
    awaitingSessionStart: !reuseStartedSession,
    sessionStartObserved: false,
  };
}

/** Esc / explicit dismissal: drop the card entirely. */
export function clearLiveTranslation(state: LiveTranslationViewState): LiveTranslationViewState {
  return {
    ...state,
    text: '',
    unavailable: false,
    lingering: false,
    armed: false,
    sessionStartObserved: false,
  };
}

/**
 * The session ended but the user may still be reading: keep the snapshot and
 * mark it lingering so the capsule window stays open showing the card alone.
 */
export function lingerLiveTranslation(state: LiveTranslationViewState): LiveTranslationViewState {
  if (!state.text.trim()) return { ...state, lingering: false, armed: false };
  return { ...state, lingering: true, armed: false };
}

/** Apply the session id carried by capsule:state before backend:event. */
export function applyLiveTranslationSessionId(
  state: LiveTranslationViewState,
  sessionId: string | null | undefined,
): LiveTranslationViewState {
  if (!sessionId || sessionId === state.sessionId) return state;
  return {
    ...state,
    sessionId,
    text: '',
    unavailable: false,
    lingering: false,
    armed: false,
    awaitingSessionStart: false,
    sessionStartObserved: true,
  };
}

export function applyLiveTranslationEvent(
  state: LiveTranslationViewState,
  event: BackendEvent,
): LiveTranslationViewState {
  if (event.sequence <= state.sequence) return state;
  const sequence = event.sequence;
  if (event.kind.type === 'dictation_state_changed') {
    const payload = event.kind.payload as { phase?: string; sessionId?: string | null } | undefined;
    const sessionId = payload?.sessionId ?? event.sessionId;
    if (sessionId && sessionId !== state.sessionId) {
      return {
        ...state,
        sessionId,
        sequence,
        text: '',
        unavailable: false,
        lingering: false,
        armed: false,
        awaitingSessionStart: false,
        sessionStartObserved: true,
      };
    }
    return {
      ...state,
      sessionId: sessionId ?? state.sessionId,
      sequence,
      awaitingSessionStart: payload?.phase === 'starting' ? false : state.awaitingSessionStart,
      sessionStartObserved: payload?.phase === 'starting' ? true : state.sessionStartObserved,
    };
  }
  if (event.kind.type !== 'live_translation_update') {
    return { ...state, sequence };
  }
  // Once the card is lingering the session is over: late updates (notably the
  // Unavailable emitted when the translation socket closes after the tail
  // grace) must not wipe the snapshot — wiping it makes the capsule body flash
  // for one exit-animation frame. A new session resets `lingering` first.
  if (state.lingering) return { ...state, sequence };
  const payload = event.kind.payload as LiveTranslationUpdate | undefined;
  if (!payload || typeof payload.text !== 'string') return { ...state, sequence };
  // Pre-arm signal: an empty streaming snapshot published as soon as the session
  // has a preview plan, so the capsule can size its window for the card before
  // any text exists. It carries no text and is cleared at every generation edge,
  // so it is deliberately exempt from the session-start gate below (the plan is
  // announced from the recorder start, which can precede `starting`).
  if (payload.status === 'streaming' && payload.text === '') {
    return state.armed ? { ...state, sequence } : { ...state, sequence, armed: true };
  }
  // Gate exactly like the transcript rail: drop late events from a session
  // that is not the observed one, and hold updates until the session starts.
  if (state.awaitingSessionStart) return { ...state, sequence };
  if (state.sessionId !== null && event.sessionId !== state.sessionId) {
    return { ...state, sequence };
  }
  if (payload.status === 'unavailable') {
    return {
      ...state,
      sessionId: event.sessionId ?? state.sessionId,
      sequence,
      text: '',
      unavailable: true,
      armed: false,
    };
  }
  // Sticky hidden: a session that reported Unavailable never repaints.
  if (state.unavailable) return { ...state, sequence };
  return {
    ...state,
    sessionId: event.sessionId ?? state.sessionId,
    sequence,
    text: payload.text,
    armed: true,
  };
}

/** Same visibility contract as the transcript rail (capsuleTranscript.ts). */
export function visibleLiveTranslation(
  text: string,
  enabled: boolean,
  state: CapsuleState,
  selectionPolish: boolean,
): string {
  return enabled && !selectionPolish && ['recording', 'transcribing', 'polishing'].includes(state)
    ? text.trim()
    : '';
}
