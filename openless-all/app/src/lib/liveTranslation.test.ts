import type { BackendEvent } from './backendEvent';
import {
  applyLiveTranslationEvent,
  applyLiveTranslationSessionId,
  beginLiveTranslationGeneration,
  clearLiveTranslation,
  createLiveTranslationState,
  lingerLiveTranslation,
  visibleLiveTranslation,
  type LiveTranslationViewState,
} from './liveTranslation';

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function updateEvent(
  sequence: number,
  sessionId: string | null,
  text: string,
  status: 'streaming' | 'unavailable' = 'streaming',
): BackendEvent {
  return {
    sequence,
    sessionId,
    kind: { type: 'live_translation_update', payload: { text, status } },
  };
}

function startingEvent(sequence: number, sessionId: string): BackendEvent {
  return {
    sequence,
    sessionId,
    kind: { type: 'dictation_state_changed', payload: { phase: 'starting', sessionId } },
  };
}

// A streaming update is accepted after the session start was announced and
// replaces the previous snapshot wholesale.
let state: LiveTranslationViewState = createLiveTranslationState();
state = applyLiveTranslationEvent(state, startingEvent(1, 'a'));
state = applyLiveTranslationEvent(state, updateEvent(2, 'a', 'Hello'));
assert(state.text === 'Hello' && state.sequence === 2, 'first snapshot accepted');
state = applyLiveTranslationEvent(state, updateEvent(3, 'a', 'Hello world'));
assert(state.text === 'Hello world', 'snapshot replaces, never appends');

// A late event from an old session is rejected but still advances the watermark.
state = applyLiveTranslationEvent(state, updateEvent(4, 'old-session', 'stale text'));
assert(state.text === 'Hello world' && state.sequence === 4, 'old session rejected');

// Stale sequence numbers are ignored entirely.
const beforeStale = state;
state = applyLiveTranslationEvent(state, updateEvent(4, 'a', 'replay'));
assert(state === beforeStale, 'replayed sequence is a no-op');

// Unavailable clears the row and stays hidden for the rest of the generation.
state = applyLiveTranslationEvent(state, updateEvent(5, 'a', '', 'unavailable'));
assert(state.text === '' && state.unavailable, 'unavailable clears');
state = applyLiveTranslationEvent(state, updateEvent(6, 'a', 'late text'));
assert(state.text === '', 'unavailable is sticky within the generation');

// Terminal capsule states clear the text.
state = clearLiveTranslation(state);
assert(state.text === '' && !state.unavailable, 'terminal clears and re-arms');

// A new recording generation without an announced session rejects updates
// until dictation_state_changed names the session.
state = beginLiveTranslationGeneration(state);
assert(state.awaitingSessionStart && state.sessionId === null, 'generation awaits session');
state = applyLiveTranslationEvent(state, updateEvent(7, 'b', 'too early'));
assert(state.text === '', 'updates held while awaiting session start');
state = applyLiveTranslationEvent(state, startingEvent(8, 'b'));
state = applyLiveTranslationEvent(state, updateEvent(9, 'b', 'Now streaming'));
assert(state.text === 'Now streaming' && state.sessionId === 'b', 'session b streams');

// capsule:state can announce the session before backend:event does.
let raced = beginLiveTranslationGeneration(createLiveTranslationState());
raced = applyLiveTranslationSessionId(raced, 'c');
raced = applyLiveTranslationEvent(raced, updateEvent(1, 'c', 'early text'));
assert(raced.text === 'early text', 'session id from capsule state unlocks updates');

// Events that arrived before the recording edge are kept when the session was
// already announced (backend:event may precede capsule:state).
let early = applyLiveTranslationEvent(createLiveTranslationState(), startingEvent(1, 'd'));
early = applyLiveTranslationEvent(early, updateEvent(2, 'd', 'pre-edge text'));
early = beginLiveTranslationGeneration(early);
assert(early.text === 'pre-edge text' && early.sessionId === 'd', 'started session is reused');

// Unknown event kinds only advance the watermark.
let unknown = applyLiveTranslationEvent(early, {
  sequence: 3,
  sessionId: 'd',
  kind: { type: 'notification', payload: {} },
});
assert(unknown.text === 'pre-edge text' && unknown.sequence === 3, 'unknown kinds are inert');

// Malformed payloads are ignored.
let malformed = applyLiveTranslationEvent(early, {
  sequence: 4,
  sessionId: 'd',
  kind: { type: 'live_translation_update', payload: { status: 'streaming' } },
});
assert(malformed.text === 'pre-edge text', 'payload without text is ignored');

// Visibility predicate mirrors the transcript rail.
assert(
  visibleLiveTranslation(' Hi ', true, 'recording', false) === 'Hi',
  'visible while recording',
);
assert(visibleLiveTranslation('Hi', false, 'recording', false) === '', 'disabled hides');
assert(visibleLiveTranslation('Hi', true, 'recording', true) === '', 'selection polish hides');
assert(visibleLiveTranslation('Hi', true, 'idle', false) === '', 'idle hides');
assert(visibleLiveTranslation('Hi', true, 'done', false) === '', 'done hides');
assert(visibleLiveTranslation('Hi', true, 'polishing', false) === 'Hi', 'settling window shows');

console.log('liveTranslation.test.ts: all assertions passed');

// Lingering: the session end keeps the snapshot readable; Esc clears it.
let linger = applyLiveTranslationEvent(createLiveTranslationState(), startingEvent(1, 'e'));
linger = applyLiveTranslationEvent(linger, updateEvent(2, 'e', 'Read me later'));
linger = lingerLiveTranslation(linger);
assert(linger.lingering && linger.text === 'Read me later', 'terminal lingers with text');
linger = clearLiveTranslation(linger);
assert(linger.text === '' && !linger.lingering, 'Esc dismissal clears text and lingering');
// A late Unavailable (socket close after the tail grace) must not wipe a
// lingering card, or the capsule body flashes for one exit-animation frame.
let late = lingerLiveTranslation(
  applyLiveTranslationEvent(
    applyLiveTranslationEvent(createLiveTranslationState(), startingEvent(1, 'h')),
    updateEvent(2, 'h', 'keep me'),
  ),
);
late = applyLiveTranslationEvent(late, updateEvent(3, 'h', '', 'unavailable'));
assert(late.text === 'keep me' && late.lingering, 'late unavailable keeps lingering card');

// A new session replaces the lingering snapshot (its first update overwrites).
let lingered = lingerLiveTranslation(
  applyLiveTranslationEvent(
    applyLiveTranslationEvent(createLiveTranslationState(), startingEvent(1, 'f')),
    updateEvent(2, 'f', 'old session'),
  ),
);
assert(lingered.lingering, 'old session lingers');
let fresh = applyLiveTranslationEvent(lingered, startingEvent(3, 'g'));
assert(!fresh.lingering && fresh.text === '', 'new session clears lingering snapshot');
fresh = beginLiveTranslationGeneration(fresh);
assert(!fresh.lingering && fresh.text === '', 'new generation keeps it cleared');
// An empty snapshot never lingers (nothing to keep on screen).
assert(!lingerLiveTranslation(createLiveTranslationState()).lingering, 'empty never lingers');

console.log('liveTranslation.test.ts: all assertions passed');
