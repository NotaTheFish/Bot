const API_BASE_URL = window.CONTEST_API_BASE_URL || "";
const MEDIA_BASE_URL = window.CONTEST_MEDIA_BASE_URL || "";

const votesCounterEl = document.getElementById("votesCounter");
const contestStageEl = document.getElementById("contestStage");
const entriesGridEl = document.getElementById("entriesGrid");
const statusMessageEl = document.getElementById("statusMessage");
const rulesLinkEl = document.getElementById("rulesLink");
const rulesModalEl = document.getElementById("rulesModal");
const rulesContentEl = document.getElementById("rulesContent");
const rulesCloseEl = document.getElementById("rulesClose");
const cardTemplate = document.getElementById("entryCardTemplate");
const actionBarEl = document.getElementById("actionBar");
const actionBarTextEl = document.getElementById("actionBarText");
const actionBarButtonEl = document.getElementById("actionBarButton");
const confirmVotesBtnEl = document.getElementById("confirmVotesBtn");
const confirmModalEl = document.getElementById("confirmModal");
const confirmYesEl = document.getElementById("confirmYes");
const confirmNoEl = document.getElementById("confirmNo");
const adminPanelEl = document.getElementById("adminPanel");
const adminOverviewEl = document.getElementById("adminOverview");
const closeSubmissionBtnEl = document.getElementById("closeSubmissionBtn");
const openVotingBtnEl = document.getElementById("openVotingBtn");
const closeVotingBtnEl = document.getElementById("closeVotingBtn");

const tg = window.Telegram?.WebApp;
if (tg) { tg.ready(); tg.expand(); }

const initData = tg?.initData || "";
const authHeaders = initData ? { "X-Telegram-Init-Data": initData } : {};

const state = {
  entries: [],
  draftEntryIds: new Set(),
  confirmedEntryIds: new Set(),
  activeEntryId: null,
  votingOpen: false,
  confirmed: false,
  isAdmin: false,
  isChannelMember: false,
  busy: false,
};

const selectedCount = () => (state.confirmed ? state.confirmedEntryIds.size : state.draftEntryIds.size);

function setStatus(message = "") { statusMessageEl.textContent = message; }

function updateHeader() {
  votesCounterEl.textContent = `Выбрано: ${selectedCount()}/3`;
  contestStageEl.textContent = `Статус: ${state.votingOpen ? "идет голосование" : "голосование закрыто"}`;
}

function buildEntryImageUrl(entry) {
  if (MEDIA_BASE_URL && entry.storage_message_id) {
    return `${MEDIA_BASE_URL.replace(/\/$/, "")}/${entry.storage_message_id}`;
  }
  return "";
}

function entryDisabled(entry) {
  return state.confirmed || !state.votingOpen || entry.is_owned_by_current_user || !state.isChannelMember;
}

function renderEntries() {
  entriesGridEl.innerHTML = "";
  if (!state.entries.length) {
    setStatus("Пока нет работ для голосования.");
    renderActionBar();
    return;
  }

  for (const entry of state.entries) {
    const fragment = cardTemplate.content.cloneNode(true);
    const card = fragment.querySelector(".entry-card");
    const image = fragment.querySelector(".entry-card__image");
    const title = fragment.querySelector(".entry-card__title");
    const meta = fragment.querySelector(".entry-card__meta");

    title.textContent = `Работа #${entry.id}`;
    const inDraft = state.draftEntryIds.has(entry.id);
    const isConfirmed = state.confirmedEntryIds.has(entry.id);
    const isActive = state.activeEntryId === entry.id;

    if (isActive) card.classList.add("entry-card--active");
    if (inDraft || isConfirmed) card.classList.add("entry-card--in-draft");
    if (state.confirmed) card.classList.add("entry-card--readonly");
    if (entryDisabled(entry)) card.classList.add("entry-card--disabled");

    if (entry.is_owned_by_current_user) meta.textContent = "Ваша работа — голосовать нельзя";
    else if (isConfirmed) meta.textContent = "Ваш подтвержденный выбор";
    else if (inDraft) meta.textContent = "Добавлено в выбор";
    else meta.textContent = "Нажмите карточку для выбора";

    const imageUrl = buildEntryImageUrl(entry);
    if (imageUrl) image.src = imageUrl;

    card.addEventListener("click", () => {
      state.activeEntryId = entry.id;
      renderEntries();
    });

    entriesGridEl.appendChild(fragment);
  }

  renderActionBar();
}

function renderActionBar() {
  const entry = state.entries.find((item) => item.id === state.activeEntryId);
  const selected = selectedCount();
  const canConfirm = !state.confirmed && state.draftEntryIds.size === 3;

  confirmVotesBtnEl.hidden = !canConfirm;
  confirmVotesBtnEl.disabled = state.busy || state.draftEntryIds.size !== 3 || state.confirmed;
  actionBarEl.hidden = !entry;

  if (!entry) return;

  const inDraft = state.draftEntryIds.has(entry.id);
  actionBarTextEl.textContent = `Выбрано ${selected}/3`;

  if (state.confirmed) {
    actionBarButtonEl.disabled = true;
    actionBarButtonEl.textContent = "Голоса подтверждены";
    return;
  }
  if (!state.votingOpen) {
    actionBarButtonEl.disabled = true;
    actionBarButtonEl.textContent = "Голосование закрыто";
    return;
  }
  if (!state.isChannelMember) {
    actionBarButtonEl.disabled = true;
    actionBarButtonEl.textContent = "Нужна подписка";
    return;
  }
  if (entry.is_owned_by_current_user) {
    actionBarButtonEl.disabled = true;
    actionBarButtonEl.textContent = "Нельзя за свою";
    return;
  }

  actionBarButtonEl.disabled = state.busy;
  actionBarButtonEl.textContent = inDraft ? "Убрать выбор" : "Выбрать";
}

async function fetchJson(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...authHeaders,
      ...(options.headers || {}),
    },
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) throw new Error(payload.message || "Ошибка API");
  return payload;
}

async function loadData() {
  setStatus("Загрузка...");
  try {
    const [entriesPayload, statePayload] = await Promise.all([
      fetchJson("/api/contest/entries/approved"),
      fetchJson("/api/contest/state"),
    ]);

    state.entries = entriesPayload.items || [];
    state.draftEntryIds = new Set(statePayload.draft_entry_ids || []);
    state.confirmedEntryIds = new Set(statePayload.confirmed_entry_ids || []);
    state.votingOpen = Boolean(statePayload.voting_open);
    state.confirmed = Boolean(statePayload.confirmed);
    state.isAdmin = Boolean(statePayload.is_admin);
    state.isChannelMember = Boolean(statePayload.is_channel_member);

    if (state.isAdmin) {
      adminPanelEl.hidden = false;
      await loadAdminOverview();
    } else {
      adminPanelEl.hidden = true;
    }

    updateHeader();
    renderEntries();
    setStatus("");
  } catch (error) {
    setStatus(error.message || "Не удалось загрузить данные.");
  }
}

async function toggleActiveSelection() {
  const entry = state.entries.find((item) => item.id === state.activeEntryId);
  if (!entry || entryDisabled(entry)) return;

  const inDraft = state.draftEntryIds.has(entry.id);
  if (!inDraft && state.draftEntryIds.size >= 3) {
    setStatus("Можно выбрать только 3 работы.");
    return;
  }

  state.busy = true;
  renderActionBar();
  try {
    const payload = await fetchJson(inDraft ? "/api/contest/draft/unselect" : "/api/contest/draft/select", {
      method: "POST",
      body: JSON.stringify({ entry_id: entry.id }),
    });
    state.draftEntryIds = new Set(payload.entry_ids || []);
    updateHeader();
    renderEntries();
    setStatus(inDraft ? "Выбор убран." : "Работа добавлена в выбор.");
  } catch (error) {
    setStatus(error.message || "Не удалось изменить выбор.");
  } finally {
    state.busy = false;
    renderActionBar();
  }
}

async function confirmVotes() {
  if (state.draftEntryIds.size !== 3 || state.confirmed || state.busy) {
    return;
  }
  confirmModalEl.hidden = false;
}

async function submitConfirmVotes() {
  state.busy = true;
  confirmModalEl.hidden = true;
  renderActionBar();
  try {
    const payload = await fetchJson("/api/contest/votes/confirm", { method: "POST", body: "{}" });
    state.confirmed = Boolean(payload.confirmed);
    state.confirmedEntryIds = new Set(payload.entry_ids || []);
    state.draftEntryIds = new Set();
    updateHeader();
    renderEntries();
    setStatus("Голоса подтверждены.");
  } catch (error) {
    setStatus(error.message || "Не удалось подтвердить голоса.");
  } finally {
    state.busy = false;
    renderActionBar();
  }
}

async function openRules() {
  try {
    rulesContentEl.textContent = "Загрузка правил...";
    rulesModalEl.hidden = false;
    const payload = await fetchJson("/api/contest/rules");
    rulesContentEl.textContent = payload.rules_text || "Правила пока не опубликованы.";
  } catch (error) {
    rulesContentEl.textContent = error.message || "Не удалось загрузить правила.";
  }
}

async function loadAdminOverview() {
  try {
    const payload = await fetchJson("/api/contest/admin/overview");
    adminOverviewEl.innerHTML = (payload.items || []).map((item) => `
      <article class="admin-overview-item">
        <b>#${item.id}</b> · автор ${item.owner_user_id}<br />
        Статус: ${item.status}; подтверждено: ${item.confirmed_votes_count}; penalty: ${item.penalty_votes}; net: ${item.net_votes}<br />
        Suspicious: ${item.suspicious_votes_count}
      </article>
    `).join("");
  } catch (error) {
    adminOverviewEl.textContent = error.message || "Не удалось загрузить обзор.";
  }
}

async function adminStage(path) {
  try {
    await fetchJson(path, { method: "POST", body: "{}" });
    await Promise.all([loadData(), loadAdminOverview()]);
  } catch (error) {
    setStatus(error.message || "Не удалось изменить стадию.");
  }
}

rulesLinkEl?.addEventListener("click", openRules);
rulesCloseEl?.addEventListener("click", () => { rulesModalEl.hidden = true; });
actionBarButtonEl?.addEventListener("click", toggleActiveSelection);
confirmVotesBtnEl?.addEventListener("click", confirmVotes);
confirmNoEl?.addEventListener("click", () => { confirmModalEl.hidden = true; });
confirmYesEl?.addEventListener("click", submitConfirmVotes);
closeSubmissionBtnEl?.addEventListener("click", () => adminStage("/api/contest/admin/submission/close"));
openVotingBtnEl?.addEventListener("click", () => adminStage("/api/contest/admin/voting/open"));
closeVotingBtnEl?.addEventListener("click", () => adminStage("/api/contest/admin/voting/close"));

loadData();