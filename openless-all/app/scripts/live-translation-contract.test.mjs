import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const appRoot = fileURLToPath(new URL('..', import.meta.url));
const read = (relativePath) => readFile(join(appRoot, relativePath), 'utf8');

// ① The display-only preview must never merge into the transcript view state:
// its text is not insertable and must not share the transcript reducer.
const backendEvent = await read('src/lib/backendEvent.ts');
assert(
  !backendEvent.includes('live_translation'),
  'backendEvent.ts must not handle live_translation_update; the preview has its own reducer',
);
const liveTranslation = await read('src/lib/liveTranslation.ts');
// Comments may mention the transcript types to document the separation; the
// executable code must not reference them.
const liveTranslationCode = liveTranslation
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .split('\n')
  .filter((line) => !line.trim().startsWith('//') && !line.trim().startsWith('*'))
  .join('\n');
assert(
  !liveTranslationCode.includes('TranscriptViewState') &&
    !liveTranslationCode.includes('TranscriptDelta'),
  'liveTranslation.ts must not reference transcript state types',
);
assert(
  liveTranslation.includes("event.kind.type !== 'live_translation_update'"),
  'the preview reducer must only consume live_translation_update payloads',
);

// ② The translated text never reaches history: no LiveTranslation identifier
// may appear near any append_history call site in api.rs.
const api = await read('crates/openless-core/src/api.rs');
let cursor = 0;
let historySites = 0;
while ((cursor = api.indexOf('append_history', cursor)) !== -1) {
  historySites += 1;
  const window = api.slice(Math.max(0, cursor - 1200), cursor + 1200);
  assert(
    !window.includes('LiveTranslation'),
    `append_history at offset ${cursor} must stay clear of live translation identifiers`,
  );
  cursor += 'append_history'.length;
}
assert(historySites > 0, 'api.rs must keep its append_history call sites');

// ③ Every locale carries the full copy set (zh-CN is the canonical fallback).
const localeFiles = ['zh-CN.ts', 'zh-TW.ts', 'en.ts', 'ja.ts', 'ko.ts', 'de.ts', 'fr.ts', 'es.ts'];
const localizedKeys = [
  'liveTranslationPreviewLabel',
  'liveTranslationPreviewDesc',
  'liveTranslationPreviewUnavailableOmni',
  'liveTranslationPreviewUnavailableStreaming',
  'liveTranslationTag',
];
for (const localeFile of localeFiles) {
  const locale = await read(`src/i18n/${localeFile}`);
  for (const key of localizedKeys) {
    assert(locale.includes(`${key}:`), `${localeFile} is missing ${key}`);
  }
}

// The wire contract, the core event enum and the Tauri bridge stay in sync.
const contract = JSON.parse(await read('contract/backend-2.0.json'));
assert(
  contract.backendEvent.kinds.includes('live_translation_update'),
  'backend-2.0.json kinds must include live_translation_update',
);
assert(
  contract.backendEvent.samples.live_translation_update,
  'backend-2.0.json must carry a live_translation_update sample',
);
assert.equal(
  contract.backendEvent.samples.live_translation_update.kind.type,
  'live_translation_update',
);
const events = await read('crates/openless-core/src/events.rs');
assert(
  events.includes('LiveTranslationUpdate(crate::live_translation::LiveTranslationUpdate)'),
  'BackendEventKind must carry the LiveTranslationUpdate variant',
);
const tauriEvents = await read('src-tauri/src/tauri_events.rs');
assert(
  tauriEvents.includes('BackendEventKind::LiveTranslationUpdate(_) => {}'),
  'the Tauri legacy bridge must explicitly ignore the preview event',
);

// The capsule consumes the preview through its own reducer, and the settings
// toggle is greyed out with a reason instead of hidden (design §7).
const capsule = await read('src/components/Capsule.tsx');
assert(
  capsule.includes('applyLiveTranslationEvent(liveTranslationRef.current') ||
    capsule.includes('applyLiveTranslationEvent('),
  'Capsule.tsx must feed backend:event into the live translation reducer',
);
const settings = await read('src/pages/settings/RecordingInputSection.tsx');
assert(
  settings.includes('liveTranslationPreviewLabel') &&
    settings.includes('disabled={!liveTranslationSupported}') &&
    settings.includes('liveTranslationUnavailableReason'),
  'the settings row must show a disabled toggle with an explicit reason',
);

console.log('live-translation-contract.test.mjs passed');
