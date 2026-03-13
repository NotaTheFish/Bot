const API_BASE_URL = window.CONTEST_API_BASE_URL || "";
const MEDIA_BASE_URL = window.CONTEST_MEDIA_BASE_URL || window.MEDIA_BASE_URL || "";

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
const adminStatusCardEl = document.getElementById("adminStatusCard");
const adminOverviewEl = document.getElementById("adminOverview");
const openSubmissionBtnEl = document.getElementById("openSubmissionBtn");
const closeSubmissionBtnEl = document.getElementById("closeSubmissionBtn");
const openVotingBtnEl = document.getElementById("openVotingBtn");
const closeVotingBtnEl = document.getElementById("closeVotingBtn");

const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const initData = tg?.initData || "";
const authHeaders = initData ? { "X-Telegram-Init-Data": initData } : {};

const state = {
  entries: [],
  draftEntryIds: new Set(),
  confirmedEntryIds: new Set(),
  activeEntryId: null,
  submissionOpen: false,
  votingOpen: false,
  confirmed: false,
  isAdmin: false,
  isChannelMember: false,
  busy: false,
};

const selectedCount = () =>
  state.confirmed ? state.confirmedEntryIds.size : state.draftEntryIds.size;

function setStatus(message = "") {
  statusMessageEl.textContent = message;
}

function updateHeader() {
  votesCounterEl.textContent = `Выбрано: ${selectedCount()}/3`;
  contestStageEl.textContent = `Статус: ${state.votingOpen ? "идет голосование" : "голосование закрыто"}`;
}

function safeString(value) {
  return typeof value === "string" ? value.trim() : "";
}

function toAbsoluteUrl(rawUrl) {
  const value = safeString(rawUrl);
  if (!value) return "";

  try {
    return new URL(value).toString();
  } catch {
    const apiBase = API_BASE_URL
      ? new URL(API_BASE_URL, window.location.origin).toString()
      : "";
    const base = apiBase || window.location.origin;

    try {
      return new URL(value, base).toString();
    } catch {
      return "";
    }
  }
}

function buildEntryImageUrl(entry) {
  const directUrl =
    toAbsoluteUrl(entry?.image_url) ||
    toAbsoluteUrl(entry?.file_url) ||
    toAbsoluteUrl(entry?.storage_url);

  if (directUrl) {
    return directUrl;
  }

  const storageMessageId =
    entry?.storage_message_id ||
    (Array.isArray(entry?.storage_message_ids) ? entry.storage_message_ids[0] : null);

  if (MEDIA_BASE_URL && storageMessageId) {
    return `${MEDIA_BASE_URL.replace(/\/$/, "")}/${storageMessageId}`;
  }

  return "";
}

function entryDisabled(entry) {
  return (
    state.confirmed ||
    !state.votingOpen ||
    entry.is_owned_by_current_user ||
    !state.isChannelMember
  );
}

function updateAdminButtons() {
  if (!openSubmissionBtnEl || !openVotingBtnEl || !closeVotingBtnEl || !closeSubmissionBtnEl) return;

  openSubmissionBtnEl.disabled = state.submissionOpen || state.busy;
  closeSubmissionBtnEl.disabled = !state.submissionOpen || state.busy;
  openVotingBtnEl.disabled = state.votingOpen || state.busy;
  closeVotingBtnEl.disabled = !state.votingOpen || state.busy;
}

function setBusy(value) {
  state.busy = value;
  renderActionBar();
  updateAdminButtons();
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

    const imageFallback = document.createElement("div");
    imageFallback.className = "entry-card__image-fallback";
    imageFallback.textContent = "Изображение недоступно";
    imageFallback.hidden = true;
    image.insertAdjacentElement("afterend", imageFallback);

    const displayNumber = Number(entry.display_number) > 0 ? entry.display_number : entry.id;
    title.textContent = `Работа #${displayNumber}`;

    const inDraft = state.draftEntryIds.has(entry.id);
    const isConfirmed = state.confirmedEntryIds.has(entry.id);
    const isActive = state.activeEntryId === entry.id;

    if (isActive) card.classList.add("entry-card--active");
    if (inDraft || isConfirmed) card.classList.add("entry-card--in-draft");
    if (state.confirmed) card.classList.add("entry-card--readonly");
    if (entryDisabled(entry)) card.classList.add("entry-card--disabled");

    if (entry.is_owned_by_current_user) {
      meta.textContent = "Ваша работа — голосовать нельзя";
    } else if (isConfirmed) {
      meta.textContent = "Ваш подтвержденный выбор";
    } else if (inDraft) {
      meta.textContent = "Добавлено в выбор";
    } else {
      meta.textContent = "Нажмите карточку для выбора";
    }

    const imageUrl = buildEntryImageUrl(entry);

    const showFallback = () => {
      image.hidden = true;
      imageFallback.hidden = false;
    };

    if (imageUrl) {
      image.hidden = false;
      imageFallback.hidden = true;
      image.onerror = showFallback;
      image.src = imageUrl;
    } else {
      image.removeAttribute("src");
      showFallback();
    }

    card.addEventListener("click", () => {
      state.activeEntryId = entry.id;
      renderEntries();
    });

    entriesGridEl.appendChild(fragment);
  }

  renderActionBar();
}

function renderAdminStatusCard() {
  if (!adminStatusCardEl || !state.isAdmin) return;

  const submissionStateText = state.submissionOpen ? "Открыт" : "Закрыт";
  const votingStateText = state.votingOpen ? "Открыто" : "Закрыто";
  const submissionClass = state.submissionOpen
    ? "admin-status-indicator--open"
    : "admin-status-indicator--closed";
  const votingClass = state.votingOpen
    ? "admin-status-indicator--open"
    : "admin-status-indicator--closed";

  adminStatusCardEl.innerHTML = `
    <h3 class="admin-status-card__title">Статус конкурса</h3>
    <p class="admin-status-item">
      Прием заявок: <span class="admin-status-indicator ${submissionClass}">${submissionStateText}</span>
    </p>
    <p class="admin-status-item">
      Голосование: <span class="admin-status-indicator ${votingClass}">${votingStateText}</span>
    </p>
  `;
}

function renderActionBar() {
  const entry = state.entries.find((item) => item.id === state.activeEntryId);
  const selected = selectedCount();
  const canConfirm = !state.confirmed && state.draftEntryIds.size === 3;

  confirmVotesBtnEl.hidden = !canConfirm;
  confirmVotesBtnEl.disabled =
    state.busy || state.draftEntryIds.size !== 3 || state.confirmed;

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

  const rawText = await response.text();
  let payload = null;

  if (rawText) {
    try {
      payload = JSON.parse(rawText);
    } catch {
      const safeText = rawText.trim();
      const fallbackMessage =
        safeText && safeText.length <= 200
          ? `Сервер вернул некорректный ответ: ${safeText}`
          : "Сервер вернул некорректный ответ";
      throw new Error(fallbackMessage);
    }
  }

  if (!payload || typeof payload !== "object") {
    throw new Error("Сервер вернул пустой ответ");
  }

  if (!response.ok || payload.ok === false) {
    throw new Error(payload.message || "Ошибка API");
  }

  return payload;
}

async function loadAdminOverview(options = {}) {
  const throwOnError = Boolean(options.throwOnError);
  const fallbackMessage = options.fallbackMessage || "Не удалось загрузить админ-обзор";

  try {
    const payload = await fetchJson("/api/contest/admin/overview");

    adminOverviewEl.innerHTML = (payload.items || [])
      .map(
        (item) => `
      <article class="admin-overview-item">
        <b>Работа #${item.display_number || "—"} (entry_id=${item.id})</b> · автор ${item.owner_user_id}<br />
        Статус: ${item.status}; подтверждено: ${item.confirmed_votes_count}; penalty: ${item.penalty_votes}; net: ${item.net_votes}<br />
        Suspicious: ${item.suspicious_votes_count}
      </article>
    `
      )
      .join("");
  } catch (error) {
    adminOverviewEl.textContent = fallbackMessage;

    if (throwOnError) {
      throw new Error(error.message || fallbackMessage);
    }
  }
}

function applyContestState(statePayload, entriesPayload) {
  state.entries = entriesPayload.items || [];
  state.draftEntryIds = new Set(statePayload.draft_entry_ids || []);
  state.confirmedEntryIds = new Set(statePayload.confirmed_entry_ids || []);
  state.submissionOpen = Boolean(statePayload.submission_open);
  state.votingOpen = Boolean(statePayload.voting_open);
  state.confirmed = Boolean(statePayload.confirmed);
  state.isAdmin = Boolean(statePayload.is_admin);
  state.isChannelMember = Boolean(statePayload.is_channel_member);

  if (!state.entries.some((item) => item.id === state.activeEntryId)) {
    state.activeEntryId = null;
  }

  adminPanelEl.hidden = !state.isAdmin;
}

function renderDataState() {
  renderAdminStatusCard();
  updateHeader();
  renderEntries();
  updateAdminButtons();
}

function notifyAdminError(message) {
  if (!state.isAdmin) return;

  if (typeof tg?.showAlert === "function") {
    tg.showAlert(message);
    return;
  }

  window.alert(message);
}

async function loadData(options = {}) {
  const throwOnError = Boolean(options.throwOnError);
  setStatus("Загрузка...");

  try {
    const [entriesPayload, statePayload] = await Promise.all([
      fetchJson("/api/contest/entries/approved"),
      fetchJson("/api/contest/state"),
    ]);

    applyContestState(statePayload, entriesPayload);

    if (state.isAdmin) {
      await loadAdminOverview();
    }

    renderDataState();
    setStatus("");
  } catch (error) {
    const message = error.message || "Не удалось загрузить данные.";
    setStatus(message);

    if (throwOnError) {
      throw new Error(message);
    }
  }
}

async function refreshAfterAdminAction() {
  const statePayload = await fetchJson("/api/contest/state").catch((error) => {
    throw new Error(
      `Не удалось обновить состояние конкурса: ${error.message || "ошибка API"}`
    );
  });

  const entriesPayload = await fetchJson("/api/contest/entries/approved").catch(
    (error) => {
      throw new Error(
        `Не удалось обновить список работ: ${error.message || "ошибка API"}`
      );
    }
  );

  applyContestState(statePayload, entriesPayload);

  if (state.isAdmin) {
    await loadAdminOverview({ fallbackMessage: "Не удалось загрузить админ-обзор" });
  }

  renderDataState();
}

async function toggleActiveSelection() {
  const entry = state.entries.find((item) => item.id === state.activeEntryId);
  if (!entry || entryDisabled(entry)) return;

  const inDraft = state.draftEntryIds.has(entry.id);

  if (!inDraft && state.draftEntryIds.size >= 3) {
    setStatus("Можно выбрать только 3 работы.");
    return;
  }

  setBusy(true);

  try {
    const payload = await fetchJson(
      inDraft ? "/api/contest/draft/unselect" : "/api/contest/draft/select",
      {
        method: "POST",
        body: JSON.stringify({ entry_id: entry.id }),
      }
    );

    state.draftEntryIds = new Set(payload.entry_ids || []);
    updateHeader();
    renderEntries();
    setStatus(inDraft ? "Выбор убран." : "Работа добавлена в выбор.");
  } catch (error) {
    setStatus(error.message || "Не удалось изменить выбор.");
  } finally {
    setBusy(false);
  }
}

async function confirmVotes() {
  if (state.draftEntryIds.size !== 3 || state.confirmed || state.busy) {
    return;
  }

  confirmModalEl.hidden = false;
}

async function submitConfirmVotes() {
  setBusy(true);
  confirmModalEl.hidden = true;

  try {
    const payload = await fetchJson("/api/contest/votes/confirm", {
      method: "POST",
      body: "{}",
    });

    state.confirmed = Boolean(payload.confirmed);
    state.confirmedEntryIds = new Set(payload.entry_ids || []);
    state.draftEntryIds = new Set();
    updateHeader();
    renderEntries();
    setStatus("Голоса подтверждены.");
  } catch (error) {
    setStatus(error.message || "Не удалось подтвердить голоса.");
  } finally {
    setBusy(false);
  }
}

async function openRules() {
  try {
    rulesContentEl.textContent = "Загрузка правил...";
    rulesModalEl.hidden = false;

    const payload = await fetchJson("/api/contest/rules");
    rulesContentEl.textContent =
      payload.rules_text || "Правила пока не опубликованы.";
  } catch (error) {
    rulesContentEl.textContent =
      error.message || "Не удалось загрузить правила.";
  }
}

async function adminStage(path) {
  if (
    (path === "/api/contest/admin/voting/open" && state.votingOpen) ||
    (path === "/api/contest/admin/voting/close" && !state.votingOpen) ||
    (path === "/api/contest/admin/submission/open" && state.submissionOpen) ||
    (path === "/api/contest/admin/submission/close" && !state.submissionOpen)
  ) {
    setStatus("Действие уже не требуется.");
    return;
  }

  setBusy(true);

  try {
    await fetchJson(path, { method: "POST", body: "{}" });
    await refreshAfterAdminAction();
    setStatus("Стадия конкурса обновлена.");
  } catch (error) {
    const message = error.message || "Не удалось изменить стадию.";
    if (/already|уже|не требуется/i.test(message)) {
      setStatus("Действие уже не требуется.");
      notifyAdminError("Действие уже не требуется.");
      return;
    }
    setStatus(message);
    notifyAdminError(message);
  } finally {
    setBusy(false);
    updateAdminButtons();
  }
}

rulesLinkEl?.addEventListener("click", openRules);
rulesCloseEl?.addEventListener("click", () => {
  rulesModalEl.hidden = true;
});
actionBarButtonEl?.addEventListener("click", toggleActiveSelection);
confirmVotesBtnEl?.addEventListener("click", confirmVotes);
confirmNoEl?.addEventListener("click", () => {
  confirmModalEl.hidden = true;
});
confirmYesEl?.addEventListener("click", submitConfirmVotes);
openSubmissionBtnEl?.addEventListener("click", () =>
  adminStage("/api/contest/admin/submission/open")
);
closeSubmissionBtnEl?.addEventListener("click", () =>
  adminStage("/api/contest/admin/submission/close")
);
openVotingBtnEl?.addEventListener("click", () =>
  adminStage("/api/contest/admin/voting/open")
);
closeVotingBtnEl?.addEventListener("click", () =>
  adminStage("/api/contest/admin/voting/close")
);

loadData();