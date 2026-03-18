import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

function extractFunction(source, functionName) {
  const asyncMarker = `async function ${functionName}(`;
  const marker = `function ${functionName}(`;
  const start = source.indexOf(asyncMarker) !== -1
    ? source.indexOf(asyncMarker)
    : source.indexOf(marker);
  if (start === -1) {
    throw new Error(`Function not found: ${functionName}`);
  }

  let openParenDepth = 0;
  let openBrace = -1;

  for (let index = start; index < source.length; index += 1) {
    const char = source[index];
    if (char === '(') openParenDepth += 1;
    if (char === ')') {
      openParenDepth -= 1;
      continue;
    }
    if (char === '{' && openParenDepth === 0) {
      openBrace = index;
      break;
    }
  }

  if (openBrace === -1) {
    throw new Error(`Failed to locate function body: ${functionName}`);
  }

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

function extractSnippet(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  if (start === -1) {
    throw new Error(`Snippet start not found: ${startMarker}`);
  }

  const end = source.indexOf(endMarker, start);
  if (end === -1) {
    throw new Error(`Snippet end not found: ${endMarker}`);
  }

  return source.slice(start, end);
}

function buildClassList(initialTokens = []) {
  const classes = new Set(initialTokens);
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
    toggle(token, force) {
      if (force === undefined) {
        if (classes.has(token)) {
          classes.delete(token);
          return false;
        }
        classes.add(token);
        return true;
      }

      if (force) {
        classes.add(token);
        return true;
      }

      classes.delete(token);
      return false;
    },
    toArray() {
      return [...classes];
    },
  };
}

function buildButton({ className = '', dataset = {} } = {}) {
  const handlers = new Map();
  return {
    className,
    dataset: { ...dataset },
    attributes: {},
    disabled: false,
    hidden: false,
    textContent: '',
    classList: buildClassList(className.split(/\s+/).filter(Boolean)),
    addEventListener(type, handler) {
      if (!handlers.has(type)) handlers.set(type, []);
      handlers.get(type).push(handler);
    },
    click() {
      if (this.disabled) return false;
      for (const handler of handlers.get('click') || []) {
        handler({ currentTarget: this, target: this });
      }
      return true;
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    removeAttribute(name) {
      delete this.attributes[name];
    },
  };
}

function buildSpan(initialText = '') {
  return {
    textContent: initialText,
  };
}

function buildThemeHarness() {
  const appJsPath = resolve(process.cwd(), 'contest_webapp/app.js');
  const indexHtmlPath = resolve(process.cwd(), 'contest_webapp/index.html');
  const source = readFileSync(appJsPath, 'utf8');
  const indexHtml = readFileSync(indexHtmlPath, 'utf8');

  const syncThemeToggleUiFn = extractFunction(source, 'syncThemeToggleUi');
  const applyThemeFn = extractFunction(source, 'applyTheme');

  const factory = new Function(`
    const LIGHT_THEME = 'light';
    const DARK_THEME = 'dark';
    const THEME_STORAGE_KEY = 'contest_webapp_theme';
    const storage = new Map();
    const themeToggleEl = {
      dataset: {},
      attributes: {},
      classList: (${buildClassList.toString()})(),
      setAttribute(name, value) {
        this.attributes[name] = String(value);
      },
    };
    const rulesButtonEl = { className: 'rules-button', dataset: {} };
    const actionBarEl = { className: 'action-bar', dataset: {} };
    const actionBarButtonEl = { className: 'vote-button', dataset: {} };
    const document = {
      body: {
        classList: (${buildClassList.toString()})(),
        dataset: {},
      },
    };
    const window = {
      localStorage: {
        setItem(key, value) {
          storage.set(key, value);
        },
      },
    };
    const tg = null;

    ${syncThemeToggleUiFn}
    ${applyThemeFn}

    return { applyTheme, document, storage, themeToggleEl, rulesButtonEl, actionBarEl, actionBarButtonEl };
  `);

  return { harness: factory(), indexHtml };
}

function buildAdminHarness({ submissionOpen = false, votingOpen = false } = {}) {
  const appJsPath = resolve(process.cwd(), 'contest_webapp/app.js');
  const source = readFileSync(appJsPath, 'utf8');

  const updateAdminSwitchFn = extractFunction(source, 'updateAdminSwitch');
  const updateAdminButtonsFn = extractFunction(source, 'updateAdminButtons');
  const getAdminStageBusyTargetFn = extractFunction(source, 'getAdminStageBusyTarget');
  const adminStageFn = extractFunction(source, 'adminStage');
  const listenerSnippet = extractSnippet(
    source,
    'submissionToggleEl?.addEventListener("click", () =>',
    'adminOverviewEl?.addEventListener("click", (event) => {'
  );

  const factory = new Function(`
    ${buildClassList.toString()}
    const state = {
      submissionOpen: ${JSON.stringify(submissionOpen)},
      votingOpen: ${JSON.stringify(votingOpen)},
      busy: false,
      adminStageBusyTarget: '',
      isAdmin: true,
    };
    const fetchCalls = [];
    const statuses = [];
    let refreshCount = 0;
    let pendingRequest = null;
    let resolvePendingRequest = null;
    let rejectPendingRequest = null;

    const submissionToggleEl = (${buildButton.toString()})({ className: 'admin-switch', dataset: { state: 'unchecked' } });
    const votingToggleEl = (${buildButton.toString()})({ className: 'admin-switch', dataset: { state: 'unchecked' } });
    const submissionStatusTextEl = (${buildSpan.toString()})('Закрыт');
    const votingStatusTextEl = (${buildSpan.toString()})('Закрыто');
    const adminOverviewEl = { addEventListener() {} };

    function setStatus(message) {
      statuses.push(message);
    }

    function notifyAdminError(message) {
      statuses.push('alert:' + message);
    }

    function refreshAfterAdminAction() {
      refreshCount += 1;
      return Promise.resolve();
    }

    function fetchJson(path, options = {}) {
      fetchCalls.push({ path, options });
      pendingRequest = new Promise((resolve, reject) => {
        resolvePendingRequest = resolve;
        rejectPendingRequest = reject;
      });
      return pendingRequest;
    }

    ${updateAdminSwitchFn}
    ${updateAdminButtonsFn}
    function setBusy(value) {
      state.busy = value;
      updateAdminButtons();
    }
    ${getAdminStageBusyTargetFn}
    ${adminStageFn}
    ${listenerSnippet}

    updateAdminButtons();

    return {
      state,
      fetchCalls,
      statuses,
      submissionToggleEl,
      votingToggleEl,
      submissionStatusTextEl,
      votingStatusTextEl,
      adminStage,
      updateAdminButtons,
      resolvePendingRequest(value = {}) {
        if (!resolvePendingRequest) throw new Error('No pending request to resolve');
        const resolve = resolvePendingRequest;
        resolvePendingRequest = null;
        rejectPendingRequest = null;
        resolve(value);
        return pendingRequest;
      },
      rejectPendingRequest(error = new Error('boom')) {
        if (!rejectPendingRequest) throw new Error('No pending request to reject');
        const reject = rejectPendingRequest;
        resolvePendingRequest = null;
        rejectPendingRequest = null;
        reject(error);
        return pendingRequest;
      },
      getRefreshCount() {
        return refreshCount;
      },
    };
  `);

  return factory();
}

test('applyTheme keeps theme entry points for body, rules button, and action bar stable in light and dark modes', () => {
  const { harness, indexHtml } = buildThemeHarness();

  const lightTheme = harness.applyTheme('light');
  assert.equal(lightTheme, 'light');
  assert.deepEqual(harness.document.body.classList.toArray(), ['theme-light']);
  assert.equal(harness.document.body.dataset.theme, 'light');
  assert.equal(harness.rulesButtonEl.className, 'rules-button');
  assert.equal(harness.actionBarEl.className, 'action-bar');
  assert.equal(harness.actionBarButtonEl.className, 'vote-button');
  assert.match(indexHtml, /<button class="rules-button" id="rulesButton" type="button">/);
  assert.match(indexHtml, /<footer id="actionBar" class="action-bar" hidden>/);
  assert.match(indexHtml, /<button id="actionBarButton" class="vote-button" type="button"><\/button>/);

  const darkTheme = harness.applyTheme('dark');
  assert.equal(darkTheme, 'dark');
  assert.deepEqual(harness.document.body.classList.toArray(), ['theme-dark']);
  assert.equal(harness.document.body.dataset.theme, 'dark');
  assert.equal(harness.rulesButtonEl.className, 'rules-button');
  assert.equal(harness.actionBarEl.className, 'action-bar');
  assert.equal(harness.actionBarButtonEl.className, 'vote-button');
});

test('admin controls markup exposes one switch per stage with expected accessibility defaults', () => {
  const indexHtmlPath = resolve(process.cwd(), 'contest_webapp/index.html');
  const indexHtml = readFileSync(indexHtmlPath, 'utf8');

  assert.equal((indexHtml.match(/class="admin-control-group"/g) || []).length, 2);
  assert.match(indexHtml, /id="submissionToggle"[\s\S]*?class="admin-switch"[\s\S]*?role="switch"[\s\S]*?aria-checked="false"[\s\S]*?aria-busy="false"[\s\S]*?data-state="unchecked"/);
  assert.match(indexHtml, /id="votingToggle"[\s\S]*?class="admin-switch"[\s\S]*?role="switch"[\s\S]*?aria-checked="false"[\s\S]*?aria-busy="false"[\s\S]*?data-state="unchecked"/);
});

test('updateAdminButtons reflects checked and busy switch state for each admin control', () => {
  const harness = buildAdminHarness({ submissionOpen: true, votingOpen: false });

  assert.equal(harness.submissionToggleEl.attributes['aria-checked'], 'true');
  assert.equal(harness.submissionToggleEl.attributes['aria-busy'], 'false');
  assert.equal(harness.submissionToggleEl.dataset.state, 'checked');
  assert.equal(harness.submissionToggleEl.disabled, false);
  assert.equal(harness.submissionStatusTextEl.textContent, 'Открыт');
  assert.equal(harness.submissionToggleEl.classList.contains('admin-switch--checked'), true);
  assert.equal(harness.submissionToggleEl.classList.contains('admin-switch--busy'), false);

  assert.equal(harness.votingToggleEl.attributes['aria-checked'], 'false');
  assert.equal(harness.votingToggleEl.attributes['aria-busy'], 'false');
  assert.equal(harness.votingToggleEl.dataset.state, 'unchecked');
  assert.equal(harness.votingToggleEl.disabled, false);
  assert.equal(harness.votingStatusTextEl.textContent, 'Закрыт');
  assert.equal(harness.votingToggleEl.classList.contains('admin-switch--checked'), false);

  harness.state.adminStageBusyTarget = 'voting';
  harness.updateAdminButtons();

  assert.equal(harness.votingToggleEl.attributes['aria-checked'], 'false');
  assert.equal(harness.votingToggleEl.attributes['aria-busy'], 'true');
  assert.equal(harness.votingToggleEl.dataset.state, 'unchecked');
  assert.equal(harness.votingToggleEl.disabled, true);
  assert.equal(harness.votingToggleEl.classList.contains('admin-switch--busy'), true);
  assert.equal(harness.votingToggleEl.classList.contains('admin-switch--disabled'), true);

  assert.equal(harness.submissionToggleEl.attributes['aria-busy'], 'false');
  assert.equal(harness.submissionToggleEl.disabled, false);
});

test('submission and voting switches keep calling the same backend stage paths', async () => {
  const submissionHarness = buildAdminHarness({ submissionOpen: false });
  submissionHarness.submissionToggleEl.click();
  assert.deepEqual(submissionHarness.fetchCalls.map((call) => call.path), ['/api/contest/admin/submission/open']);
  await submissionHarness.resolvePendingRequest({ ok: true });

  const submissionCloseHarness = buildAdminHarness({ submissionOpen: true });
  submissionCloseHarness.submissionToggleEl.click();
  assert.deepEqual(submissionCloseHarness.fetchCalls.map((call) => call.path), ['/api/contest/admin/submission/close']);
  await submissionCloseHarness.resolvePendingRequest({ ok: true });

  const votingHarness = buildAdminHarness({ votingOpen: false });
  votingHarness.votingToggleEl.click();
  assert.deepEqual(votingHarness.fetchCalls.map((call) => call.path), ['/api/contest/admin/voting/open']);
  await votingHarness.resolvePendingRequest({ ok: true });

  const votingCloseHarness = buildAdminHarness({ votingOpen: true });
  votingCloseHarness.votingToggleEl.click();
  assert.deepEqual(votingCloseHarness.fetchCalls.map((call) => call.path), ['/api/contest/admin/voting/close']);
  await votingCloseHarness.resolvePendingRequest({ ok: true });
});

test('busy admin switch ignores repeated clicks until the current request finishes', async () => {
  const harness = buildAdminHarness({ submissionOpen: false });

  assert.equal(harness.submissionToggleEl.disabled, false);

  const firstClickTriggered = harness.submissionToggleEl.click();
  const secondClickTriggered = harness.submissionToggleEl.click();

  assert.equal(firstClickTriggered, true);
  assert.equal(secondClickTriggered, false);
  assert.equal(harness.fetchCalls.length, 1);
  assert.equal(harness.fetchCalls[0].path, '/api/contest/admin/submission/open');
  assert.equal(harness.state.adminStageBusyTarget, 'submission');
  assert.equal(harness.submissionToggleEl.disabled, true);
  assert.equal(harness.submissionToggleEl.attributes['aria-busy'], 'true');

  await harness.resolvePendingRequest({ ok: true });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.state.adminStageBusyTarget, '');
  assert.equal(harness.submissionToggleEl.disabled, false);
  assert.equal(harness.submissionToggleEl.attributes['aria-busy'], 'false');
  assert.equal(harness.getRefreshCount(), 1);
});