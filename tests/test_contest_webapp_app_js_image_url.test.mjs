import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

function extractFunction(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  if (start === -1) {
    throw new Error(`Function not found: ${functionName}`);
  }

  const openBrace = source.indexOf('{', start);
  let depth = 0;

  for (let index = openBrace; index < source.length; index += 1) {
    const char = source[index];
    if (char === '{') depth += 1;
    if (char === '}') depth -= 1;
    if (depth === 0) {
      return source.slice(start, index + 1);
    }
  }

  throw new Error(`Failed to parse function body: ${functionName}`);
}

function buildAppHarness() {
  const appJsPath = resolve(process.cwd(), 'contest_webapp/app.js');
  const source = readFileSync(appJsPath, 'utf8');

  const safeStringFn = extractFunction(source, 'safeString');
  const toAbsoluteUrlFn = extractFunction(source, 'toAbsoluteUrl');
  const isContestImageEndpointUrlFn = extractFunction(source, 'isContestImageEndpointUrl');
  const buildEntryImageUrlFn = extractFunction(source, 'buildEntryImageUrl');

  const factory = new Function(`
    const window = {
      location: { origin: 'https://example.com' },
      CONTEST_API_BASE_URL: '',
      CONTEST_MEDIA_BASE_URL: '',
      MEDIA_BASE_URL: '',
    };

    const API_BASE_URL = window.CONTEST_API_BASE_URL || '';
    const MEDIA_BASE_URL = window.CONTEST_MEDIA_BASE_URL || window.MEDIA_BASE_URL || '';

    ${safeStringFn}
    ${toAbsoluteUrlFn}
    ${isContestImageEndpointUrlFn}
    ${buildEntryImageUrlFn}

    return { isContestImageEndpointUrl, buildEntryImageUrl };
  `);

  return factory();
}

test('isContestImageEndpointUrl matches API contest image endpoint without trailing slash', () => {
  const { isContestImageEndpointUrl } = buildAppHarness();
  assert.equal(isContestImageEndpointUrl('/api/contest/entries/1/image'), true);
});

test('isContestImageEndpointUrl matches API contest image endpoint with trailing slash', () => {
  const { isContestImageEndpointUrl } = buildAppHarness();
  assert.equal(isContestImageEndpointUrl('/api/contest/entries/1/image/'), true);
});

test('isContestImageEndpointUrl supports query string for endpoint URL', () => {
  const { isContestImageEndpointUrl } = buildAppHarness();
  assert.equal(isContestImageEndpointUrl('/api/contest/entries/1/image?size=large'), true);
});

test('canAttemptContestEndpointFallback is enabled for URL returned by buildEntryImageUrl', () => {
  const { isContestImageEndpointUrl, buildEntryImageUrl } = buildAppHarness();
  const imageUrl = buildEntryImageUrl({ id: 1 });
  const blobContentType = 'application/octet-stream';
  const canAttemptContestEndpointFallback =
    isContestImageEndpointUrl(imageUrl) &&
    (!blobContentType || blobContentType === 'application/octet-stream');

  assert.equal(imageUrl, 'https://example.com/api/contest/entries/1/image');
  assert.equal(canAttemptContestEndpointFallback, true);
});