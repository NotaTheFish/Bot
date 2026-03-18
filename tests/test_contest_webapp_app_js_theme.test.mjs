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

function buildClassList() {
  const classes = new Set();
  return {
    add(...tokens) {
      for (const token of tokens) classes.add(token);
    },
    remove(...tokens) {
      for (const token of tokens) classes.delete(token);
    },
    contains(token) {
      return classes.has(token);
    },
    toArray() {
      return [...classes];
    },
  };
}

function buildHarness({ storedTheme = null, tgColorScheme = '', tgThemeParams = {} } = {}) {
  const appJsPath = resolve(process.cwd(), 'contest_webapp/app.js');
  const source = readFileSync(appJsPath, 'utf8');

  const safeStringFn = extractFunction(source, 'safeString');
  const readStoredThemeFn = extractFunction(source, 'readStoredTheme');
  const syncThemeToggleUiFn = extractFunction(source, 'syncThemeToggleUi');
  const applyThemeFn = extractFunction(source, 'applyTheme');
  const getPreferredThemeFn = extractFunction(source, 'getPreferredTheme');
  const toggleThemeFn = extractFunction(source, 'toggleTheme');

  const factory = new Function(`
    const THEME_STORAGE_KEY = 'contest_webapp_theme';
    const LIGHT_THEME = 'light';
    const DARK_THEME = 'dark';
    const storage = new Map();
    if (${JSON.stringify(storedTheme)} !== null) {
      storage.set(THEME_STORAGE_KEY, ${JSON.stringify(storedTheme)});
    }
    const themeToggleEl = {
      textContent: '',
      attributes: {},
      setAttribute(name, value) {
        this.attributes[name] = value;
      },
    };
    const document = {
      body: {
        classList: (${buildClassList.toString()})(),
      },
    };
    const window = {
      localStorage: {
        getItem(key) {
          return storage.has(key) ? storage.get(key) : null;
        },
        setItem(key, value) {
          storage.set(key, value);
        },
      },
    };
    const tg = {
      colorScheme: ${JSON.stringify(tgColorScheme)},
      themeParams: ${JSON.stringify(tgThemeParams)},
    };

    ${safeStringFn}
    ${readStoredThemeFn}
    ${syncThemeToggleUiFn}
    ${applyThemeFn}
    ${getPreferredThemeFn}
    ${toggleThemeFn}

    return { document, storage, themeToggleEl, readStoredTheme, syncThemeToggleUi, applyTheme, getPreferredTheme, toggleTheme };
  `);

  return factory();
}

test('getPreferredTheme prefers saved theme from localStorage', () => {
  const harness = buildHarness({ storedTheme: 'dark', tgColorScheme: 'light' });
  assert.equal(harness.getPreferredTheme(), 'dark');
});

test('getPreferredTheme uses Telegram color scheme when no saved theme exists', () => {
  const harness = buildHarness({ tgColorScheme: 'dark', tgThemeParams: { bg_color: '#ffffff' } });
  assert.equal(harness.getPreferredTheme(), 'dark');
});


test('getPreferredTheme infers dark mode from Telegram theme params when color scheme is unavailable', () => {
  const harness = buildHarness({ tgThemeParams: { bg_color: '#101820' } });
  assert.equal(harness.getPreferredTheme(), 'dark');
});

test('getPreferredTheme falls back to light when no saved theme or Telegram params exist', () => {
  const harness = buildHarness();
  assert.equal(harness.getPreferredTheme(), 'light');
});

test('applyTheme updates body classes, toggle icon, label, and localStorage', () => {
  const harness = buildHarness();
  const appliedTheme = harness.applyTheme('dark');

  assert.equal(appliedTheme, 'dark');
  assert.deepEqual(harness.document.body.classList.toArray(), ['theme-dark']);
  assert.equal(harness.themeToggleEl.textContent, '☀️');
  assert.equal(harness.themeToggleEl.attributes['aria-label'], 'Включить светлую тему');
  assert.equal(harness.storage.get('contest_webapp_theme'), 'dark');
});

test('toggleTheme switches between dark and light themes', () => {
  const harness = buildHarness();
  assert.equal(harness.toggleTheme(), 'dark');
  assert.equal(harness.toggleTheme(), 'light');
  assert.deepEqual(harness.document.body.classList.toArray(), ['theme-light']);
  assert.equal(harness.themeToggleEl.textContent, '🌙');
  assert.equal(harness.themeToggleEl.attributes['aria-label'], 'Включить тёмную тему');
});