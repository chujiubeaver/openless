import { isDesktop } from './platform-exports';
import { invokeOrMock, platformCapabilities } from './shared';
import type { CapsuleSnapshot } from '../types';

export function startDictation(): Promise<void> {
  return invokeOrMock('start_dictation', undefined, () => undefined);
}

export function stopDictation(): Promise<void> {
  return invokeOrMock('stop_dictation', undefined, () => undefined);
}

export function cancelDictation(): Promise<void> {
  return invokeOrMock('cancel_dictation', undefined, () => undefined);
}

/**
 * Sync the native rail slot. `railHeight` (logical px) overrides the per-style
 * single-line rail budget — the multi-line live translation card uses it to
 * grow the capsule window; omit it for the transcript rail's default sizing.
 */
export function setCapsuleTranscriptVisible(
  visible: boolean,
  railHeight?: number,
  railWidth?: number,
): Promise<void> {
  if (!isDesktop()) return Promise.resolve();
  return invokeOrMock(
    'set_capsule_transcript_visible',
    { visible, railHeight: railHeight ?? null, railWidth: railWidth ?? null },
    () => undefined,
  );
}

/**
 * Keep the capsule window open after the session ends so the wrapped
 * translation card stays readable until Esc or the next session. The window is
 * swapped for a compact one pinned to the card's measured box: the insets are
 * how far the card sat from the capsule window's top-left while the session was
 * live, so the card cannot move when the session body disappears.
 */
export function setCapsuleTranslationLingering(
  active: boolean,
  cardWidth = 0,
  cardHeight = 0,
  cardTopInset = 0,
  cardLeftInset = 0,
): Promise<void> {
  if (!isDesktop()) return Promise.resolve();
  return invokeOrMock(
    'set_capsule_translation_lingering',
    { active, cardWidth, cardHeight, cardTopInset, cardLeftInset },
    () => undefined,
  );
}

/** Begin a user drag of the lingering translation card (free placement). */
export function beginCapsuleTranslationDrag(): Promise<void> {
  if (!isDesktop()) return Promise.resolve();
  return invokeOrMock('begin_capsule_translation_drag', undefined, () => undefined);
}

/** Move the lingering translation card window by a screen-space delta from drag start. */
export function dragCapsuleTranslation(dx: number, dy: number): Promise<void> {
  if (!isDesktop()) return Promise.resolve();
  return invokeOrMock('drag_capsule_translation', { dx, dy }, () => undefined);
}

/** Latest native capsule frame, used by the separate Windows rail after its webview is ready. */
export function getCapsuleSnapshot(): Promise<CapsuleSnapshot | null> {
  if (!isDesktop()) return Promise.resolve(null);
  return invokeOrMock('get_capsule_snapshot', undefined, () => null);
}

export function handleWindowHotkeyEvent(
  eventType: 'keydown' | 'keyup',
  key: string,
  code: string,
  repeat: boolean,
): Promise<void> {
  return platformCapabilities().then((caps) => {
    if (!caps.supportsDesktopHotkey) {
      return undefined;
    }
    return invokeOrMock(
      'handle_window_hotkey_event',
      { event_type: eventType, key, code, repeat },
      () => undefined,
    );
  });
}

export interface DictationTranslationTarget {
  sessionId: string;
  targetLanguage: string;
  workingLanguages: string[];
  mutable: boolean;
}

export function getDictationTranslationTarget(): Promise<DictationTranslationTarget | null> {
  return invokeOrMock('get_dictation_translation_target', undefined, () => null);
}

export function selectDictationTranslationTarget(
  sessionId: string,
  targetLanguage: string,
): Promise<DictationTranslationTarget> {
  return invokeOrMock('select_dictation_translation_target', { sessionId, targetLanguage }, () => {
    throw new Error('No active native recording');
  });
}

/** Reserve native headroom for the shared Base UI popup; close is session-bound. */
export function setCapsuleLanguageMenu(open: boolean, sessionId: string): Promise<void> {
  return invokeOrMock('set_capsule_language_menu', { open, sessionId }, () => undefined);
}
