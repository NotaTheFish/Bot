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
const imagePreviewModalEl = document.getElementById("imagePreviewModal");
const imagePreviewCloseEl = document.getElementById("imagePreviewClose");
const imagePreviewTitleEl = document.getElementById("imagePreviewTitle");
const imagePreviewImageEl = document.getElementById("imagePreviewImage");

const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const initData = tg?.initData || "";
const authHeaders = initData ? { "X-Telegram-Init-Data": initData } : {};

const state = {
  entries: [],
  entryImageLoadResults: new Map(),
  entryImageObjectUrls: new Map(),
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

let entriesRenderToken = 0;

function isValidObjectUrl(value) {
  return typeof value === "string" && value.startsWith("blob:");
}

function revokeEntryObjectUrl(entryId) {
  const existingObjectUrl = state.entryImageObjectUrls.get(entryId);
  if (!isValidObjectUrl(existingObjectUrl)) return;
  URL.revokeObjectURL(existingObjectUrl);
}

function setEntryObjectUrl(entryId, objectUrl) {
  revokeEntryObjectUrl(entryId);

  if (isValidObjectUrl(objectUrl)) {
    state.entryImageObjectUrls.set(entryId, objectUrl);
    return;
  }

  state.entryImageObjectUrls.delete(entryId);
}

function cleanupAllEntryObjectUrls() {
  for (const objectUrl of state.entryImageObjectUrls.values()) {
    if (!isValidObjectUrl(objectUrl)) continue;
    URL.revokeObjectURL(objectUrl);
  }

  state.entryImageObjectUrls.clear();
}

function cleanupEntryImageLoadResults() {
  const existingEntryIds = new Set(state.entries.map((entry) => entry.id));

  for (const [entryId] of state.entryImageLoadResults) {
    if (existingEntryIds.has(entryId)) continue;
    revokeEntryObjectUrl(entryId);
    state.entryImageLoadResults.delete(entryId);
  }
}

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
  let value = safeString(rawUrl);
  if (!value) return "";

  if (value.startsWith("//")) {
    value = `https:${value}`;
  } else if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(value) && !value.startsWith("/")) {
    value = `https://${value}`;
  }

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
      console.warn("Failed to normalize media URL", rawUrl);
      return "";
    }
  }
}


function parseContentType(rawValue) {
  const value = safeString(rawValue);
  if (!value) return "";
  return value.split(";", 1)[0].trim().toLowerCase();
}

function guessImageTypeFromBytes(bytes) {
  if (!(bytes instanceof Uint8Array) || !bytes.length) return "";

  if (bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
    return "image/jpeg";
  }
  if (
    bytes.length >= 8 &&
    bytes[0] === 0x89 &&
    bytes[1] === 0x50 &&
    bytes[2] === 0x4e &&
    bytes[3] === 0x47 &&
    bytes[4] === 0x0d &&
    bytes[5] === 0x0a &&
    bytes[6] === 0x1a &&
    bytes[7] === 0x0a
  ) {
    return "image/png";
  }

  if (
    bytes.length >= 6 &&
    bytes[0] === 0x47 &&
    bytes[1] === 0x49 &&
    bytes[2] === 0x46 &&
    bytes[3] === 0x38 &&
    (bytes[4] === 0x37 || bytes[4] === 0x39) &&
    bytes[5] === 0x61
  ) {
    return "image/gif";
  }

  if (
    bytes.length >= 12 &&
    bytes[0] === 0x52 &&
    bytes[1] === 0x49 &&
    bytes[2] === 0x46 &&
    bytes[3] === 0x46 &&
    bytes[8] === 0x57 &&
    bytes[9] === 0x45 &&
    bytes[10] === 0x42 &&
    bytes[11] === 0x50
  ) {
    return "image/webp";
  }

  return "";
}

function isContestImageEndpointUrl(imageUrl) {
  const normalizedImageUrl = safeString(imageUrl);
  if (!normalizedImageUrl) return false;

  try {
    const resolved = new URL(normalizedImageUrl, window.location.origin);
    return /\/contest\/entry-image\//.test(resolved.pathname);
  } catch {
    return /\/contest\/entry-image\//.test(normalizedImageUrl);
  }
}

function buildEntryImageUrl(entry) {
  const directUrl = toAbsoluteUrl(entry?.image_url);
  if (directUrl) return directUrl;

  const fileUrl = toAbsoluteUrl(entry?.file_url);
  if (fileUrl) return fileUrl;

  const storageUrl = toAbsoluteUrl(entry?.storage_url);
  if (storageUrl) return storageUrl;

  if (MEDIA_BASE_URL && entry?.storage_message_id) {
    return toAbsoluteUrl(`${MEDIA_BASE_URL.replace(/\/$/, "")}/${entry.storage_message_id}`);
  }

  return "";
}

async function loadEntryImageIntoElement(imgEl, fallbackEl, imageUrl, entryId) {
  const normalizedImageUrl = safeString(imageUrl);
  if (!normalizedImageUrl) {
    imgEl.removeAttribute("src");
    imgEl.hidden = true;
    fallbackEl.hidden = false;
    console.warn("Contest entry image URL missing", { entryId });
    return null;
  }

  try {
    const response = await fetch(normalizedImageUrl, {
      headers: {
        ...authHeaders,
      },
    });
    const responseContentType = parseContentType(response.headers.get("content-type"));

    if (!response.ok) {
      imgEl.removeAttribute("src");
      imgEl.hidden = true;
      fallbackEl.hidden = false;
      console.warn("Contest entry image fetch failed", {
        entryId,
        imageUrl: normalizedImageUrl,
        status: response.status,
      });
      return null;
    }

    const blob = await response.blob();
    if (!blob.size) {
      imgEl.removeAttribute("src");
      imgEl.hidden = true;
      fallbackEl.hidden = false;
      console.warn("Contest entry image blob is empty", {
        entryId,
        imageUrl: normalizedImageUrl,
        status: response.status,
        contentType: blob.type || responseContentType || "unknown",
      });
      return null;
    }

    const blobContentType = parseContentType(blob.type);
    const isImageBlob = blobContentType.startsWith("image/");
    const canAttemptContestEndpointFallback =
      isContestImageEndpointUrl(normalizedImageUrl) &&
      (!blobContentType || blobContentType === "application/octet-stream");

    let objectBlob = blob;
    let guessedType = "";

    if (!isImageBlob && canAttemptContestEndpointFallback) {
      const bytes = new Uint8Array(await blob.arrayBuffer());
      guessedType = guessImageTypeFromBytes(bytes);
      const fallbackType = guessedType || "image/jpeg";
      objectBlob = new Blob([bytes], { type: fallbackType });
      console.warn("Contest entry image blob has weak content type, trying fallback", {
        entryId,
        imageUrl: normalizedImageUrl,
        status: response.status,
        originalType: blobContentType || responseContentType || "unknown",
        fallbackType,
      });
    }

    if (!isImageBlob && !canAttemptContestEndpointFallback) {
      imgEl.removeAttribute("src");
      imgEl.hidden = true;
      fallbackEl.hidden = false;
      console.warn("Contest entry image returned non-image blob", {
        entryId,
        imageUrl: normalizedImageUrl,
        status: response.status,
        contentType: blobContentType || responseContentType || "unknown",
      });
      return null;
    }

    const objectUrl = URL.createObjectURL(objectBlob);
    imgEl.src = objectUrl;
    imgEl.hidden = false;
    fallbackEl.hidden = true;
    console.debug("Contest entry image loaded via blob", {
      entryId,
      imageUrl: normalizedImageUrl,
      status: response.status,
      contentType: objectBlob.type || blobContentType || responseContentType || "unknown",
    });
    return objectUrl;
  } catch (error) {
    imgEl.removeAttribute("src");
    imgEl.hidden = true;
    fallbackEl.hidden = false;
    console.warn("Contest entry image failed to load", {
      entryId,
      imageUrl: normalizedImageUrl,
      error: error?.message || String(error),
    });
    return null;
  }
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

function openImagePreview(entry, imageUrl) {
  if (!imagePreviewModalEl || !imagePreviewImageEl || !imagePreviewTitleEl || !imageUrl) return;
  const displayNumber = Number(entry.display_number) > 0 ? entry.display_number : entry.id;
  imagePreviewTitleEl.textContent = `Работа #${displayNumber}`;
  imagePreviewImageEl.src = imageUrl;
  imagePreviewModalEl.hidden = false;
}

function closeImagePreview() {
  if (!imagePreviewModalEl || !imagePreviewImageEl) return;
  imagePreviewModalEl.hidden = true;
  imagePreviewImageEl.removeAttribute("src");
}

function renderEntries() {
  entriesRenderToken += 1;
  const currentRenderToken = entriesRenderToken;

  cleanupAllEntryObjectUrls();
  state.entryImageLoadResults.clear();

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
    const previewBtn = fragment.querySelector(".entry-card__preview-btn");

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
    loadEntryImageIntoElement(image, imageFallback, imageUrl, entry.id).then((objectUrl) => {
      if (entriesRenderToken !== currentRenderToken) {
        if (isValidObjectUrl(objectUrl)) {
          URL.revokeObjectURL(objectUrl);
        }
        return;
      }

      if (!isValidObjectUrl(objectUrl)) {
        setEntryObjectUrl(entry.id, null);
        state.entryImageLoadResults.set(entry.id, {
          sourceUrl: imageUrl,
        });
        return;
      }

      setEntryObjectUrl(entry.id, objectUrl);
      state.entryImageLoadResults.set(entry.id, {
        objectUrl,
        sourceUrl: imageUrl,
      });
    });

    image.addEventListener("click", (event) => {
      event.stopPropagation();
      const loadResult = state.entryImageLoadResults.get(entry.id);
      if (!isValidObjectUrl(loadResult?.objectUrl)) return;
      openImagePreview(entry, loadResult.objectUrl);
    });

    previewBtn?.addEventListener("click", (event) => {
      event.stopPropagation();
      const loadResult = state.entryImageLoadResults.get(entry.id);
      if (!isValidObjectUrl(loadResult?.objectUrl)) return;
      openImagePreview(entry, loadResult.objectUrl);
    });

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
  cleanupEntryImageLoadResults();
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
imagePreviewCloseEl?.addEventListener("click", closeImagePreview);
imagePreviewModalEl?.addEventListener("click", (event) => {
  if (event.target === imagePreviewModalEl) {
    closeImagePreview();
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && imagePreviewModalEl && !imagePreviewModalEl.hidden) {
    closeImagePreview();
  }
});
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