const API_BASE_URL = window.CONTEST_API_BASE_URL || "";
const MEDIA_BASE_URL = window.CONTEST_MEDIA_BASE_URL || window.MEDIA_BASE_URL || "";

const votesCounterEl = document.getElementById("votesCounter");
const contestStageEl = document.getElementById("contestStage");
const entriesGridEl = document.getElementById("entriesGrid");
const statusMessageEl = document.getElementById("statusMessage");
const rulesButtonEl = document.getElementById("rulesButton");
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
const submissionToggleEl = document.getElementById("submissionToggle");
const submissionStatusTextEl = document.getElementById("submissionStatusText");
const votingToggleEl = document.getElementById("votingToggle");
const votingStatusTextEl = document.getElementById("votingStatusText");
const imagePreviewModalEl = document.getElementById("imagePreviewModal");
const imagePreviewCloseEl = document.getElementById("imagePreviewClose");
const imagePreviewTitleEl = document.getElementById("imagePreviewTitle");
const imagePreviewImageEl = document.getElementById("imagePreviewImage");
const adminEntryDetailModalEl = document.getElementById("adminEntryDetailModal");
const adminEntryDetailCloseEl = document.getElementById("adminEntryDetailClose");
const adminEntryDetailContentEl = document.getElementById("adminEntryDetailContent");
const themeToggleEl = document.getElementById("themeToggle");

const THEME_STORAGE_KEY = "contest_webapp_theme";
const LIGHT_THEME = "light";
const DARK_THEME = "dark";

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
  phase: "submission",
  confirmed: false,
  activeVotesCount: 0,
  availableVotes: 3,
  isAdmin: false,
  isChannelMember: false,
  busy: false,
  adminStageBusyTarget: "",
};

let entriesRenderToken = 0;
const hydratedRulesContainers = new WeakMap();
let lottieLoadPromise = null;

function cleanupRulesEmoji(container) {
  const cleanup = hydratedRulesContainers.get(container);
  if (typeof cleanup === "function") {
    cleanup();
  }
  hydratedRulesContainers.delete(container);
}

function loadLottieLibrary() {
  if (window.lottie) return Promise.resolve(window.lottie);
  if (lottieLoadPromise) return lottieLoadPromise;

  const sources = [
    "https://cdn.jsdelivr.net/npm/lottie-web@5.12.2/build/player/lottie.min.js",
    "https://unpkg.com/lottie-web@5.12.2/build/player/lottie.min.js",
  ];

  lottieLoadPromise = new Promise((resolve, reject) => {
    let index = 0;
    const tryNext = () => {
      if (window.lottie) {
        resolve(window.lottie);
        return;
      }
      if (index >= sources.length) {
        reject(new Error("lottie-web is unavailable"));
        return;
      }
      const script = document.createElement("script");
      script.src = sources[index++];
      script.async = true;
      script.onload = () => {
        if (window.lottie) {
          resolve(window.lottie);
          return;
        }
        tryNext();
      };
      script.onerror = tryNext;
      document.head.appendChild(script);
    };
    tryNext();
  });

  return lottieLoadPromise;
}

async function hydrateCustomEmoji(container) {
  cleanupRulesEmoji(container);
  if (!container) return;

  const lottieNodes = Array.from(container.querySelectorAll(".tg-inline-emoji--lottie"));
  if (!lottieNodes.length) return;

  let lottie;
  try {
    lottie = await loadLottieLibrary();
  } catch (_error) {
    for (const node of lottieNodes) {
      if (!node.textContent.trim()) {
        node.textContent = "◻️";
      }
      node.classList.add("tg-inline-emoji--fallback");
    }
    return;
  }

  const instances = [];
  for (const node of lottieNodes) {
    if (node.dataset.hydrated === "1") continue;
    const path = safeString(node.dataset.assetUrl);
    if (!path) {
      node.textContent = "◻️";
      node.classList.add("tg-inline-emoji--fallback");
      continue;
    }
    node.dataset.hydrated = "1";
    node.textContent = "";
    const instance = lottie.loadAnimation({
      container: node,
      renderer: "svg",
      loop: true,
      autoplay: true,
      path,
    });
    instances.push(instance);
  }

  hydratedRulesContainers.set(container, () => {
    for (const instance of instances) {
      try {
        instance.destroy();
      } catch (_error) {
        // ignore
      }
    }
    for (const node of lottieNodes) {
      delete node.dataset.hydrated;
      node.textContent = "";
    }
  });
}

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

const selectedCount = () => state.draftEntryIds.size;

function setStatus(message = "") {
  statusMessageEl.textContent = message;
}

function readStoredTheme() {
  try {
    const savedTheme = window.localStorage?.getItem(THEME_STORAGE_KEY);
    return savedTheme === DARK_THEME || savedTheme === LIGHT_THEME ? savedTheme : "";
  } catch (_error) {
    return "";
  }
}

function syncThemeToggleUi(theme) {
  if (!themeToggleEl) return;

  const isDarkTheme = theme === DARK_THEME;
  themeToggleEl.dataset.themeIntent = isDarkTheme ? LIGHT_THEME : DARK_THEME;
  themeToggleEl.classList.toggle("theme-toggle--intent-light", isDarkTheme);
  themeToggleEl.classList.toggle("theme-toggle--intent-dark", !isDarkTheme);
  themeToggleEl.setAttribute(
    "aria-label",
    isDarkTheme ? "Включить светлую тему" : "Включить тёмную тему"
  );
}

function applyTheme(theme) {
  const nextTheme = theme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
  const root = document.body;
  if (!root) return nextTheme;

  root.classList.remove("theme-light", "theme-dark");
  root.classList.add(`theme-${nextTheme}`);
  if (root.dataset) {
    root.dataset.theme = nextTheme;
  }
  syncThemeToggleUi(nextTheme);

  if (tg) {
    tg.setHeaderColor?.(nextTheme === DARK_THEME ? "#20252d" : "#edf1f5");
    tg.setBackgroundColor?.(nextTheme === DARK_THEME ? "#20252d" : "#edf1f5");
  }

  try {
    window.localStorage?.setItem(THEME_STORAGE_KEY, nextTheme);
  } catch (_error) {
    // ignore storage errors
  }

  return nextTheme;
}

function getPreferredTheme() {
  const savedTheme = readStoredTheme();
  if (savedTheme) return savedTheme;

  const telegramThemeParams = tg?.themeParams;
  if (telegramThemeParams && Object.keys(telegramThemeParams).length > 0) {
    const telegramColorScheme = safeString(tg?.colorScheme).toLowerCase();
    if (telegramColorScheme === DARK_THEME || telegramColorScheme === LIGHT_THEME) {
      return telegramColorScheme;
    }

    const bgColor = safeString(telegramThemeParams.bg_color).replace("#", "");
    const secondaryBgColor = safeString(telegramThemeParams.secondary_bg_color).replace("#", "");
    const referenceColor = bgColor || secondaryBgColor;

    if (/^[0-9a-f]{6}$/i.test(referenceColor)) {
      const red = Number.parseInt(referenceColor.slice(0, 2), 16);
      const green = Number.parseInt(referenceColor.slice(2, 4), 16);
      const blue = Number.parseInt(referenceColor.slice(4, 6), 16);
      const brightness = (red * 299 + green * 587 + blue * 114) / 1000;
      return brightness < 128 ? DARK_THEME : LIGHT_THEME;
    }
  }

  return LIGHT_THEME;
}

function toggleTheme() {
  const root = document.body;
  const isDarkTheme = root?.classList.contains("theme-dark");
  return applyTheme(isDarkTheme ? LIGHT_THEME : DARK_THEME);
}

function updateHeader() {
  const active = Math.max(0, Number(state.activeVotesCount) || 0);
  const available = Math.max(0, Number(state.availableVotes) || 0);
  const phaseLabels = {
    submission: "идет прием заявок",
    voting: "идет голосование",
    results: "подведены итоги",
  };
  const stageText = phaseLabels[state.phase] || (state.votingOpen ? "идет голосование" : "голосование закрыто");
  votesCounterEl.textContent =
    available > 0
      ? `Подтверждено: ${active}/3 · Осталось выбрать: ${available}`
      : `Подтверждено: ${active}/3`;
  contestStageEl.textContent = `Статус: ${stageText}`;
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

  const contestImageEndpointPathRegex = /^\/api\/contest\/entries\/\d+\/image\/?$/;

  try {
    const resolved = new URL(normalizedImageUrl, window.location.origin);
    return contestImageEndpointPathRegex.test(resolved.pathname);
  } catch {
    const pathWithoutQuery = normalizedImageUrl.split(/[?#]/, 1)[0];
    const fallbackPath = pathWithoutQuery.replace(/^[a-z][a-z0-9+.-]*:\/\/[^/]+/i, "");
    return contestImageEndpointPathRegex.test(fallbackPath);
  }
}

function buildEntryImageUrl(entry) {
  const directUrl = toAbsoluteUrl(entry?.image_url);
  if (directUrl) return directUrl;

  if (Number(entry?.id) > 0) {
    return toAbsoluteUrl(`/api/contest/entries/${entry.id}/image`);
  }

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
  console.debug("Contest entry image loader invoked", {
    entryId,
    hasUrl: Boolean(normalizedImageUrl),
    imageUrl: normalizedImageUrl,
  });
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
    state.availableVotes <= 0 ||
    state.phase === "results" ||
    !state.votingOpen ||
    entry.is_owned_by_current_user ||
    !state.isChannelMember
  );
}

function updateAdminSwitch(toggleEl, statusEl, options = {}) {
  if (!toggleEl || !statusEl) return;

  const {
    isOpen = false,
    busy = false,
    openText = "Открыт",
    closedText = "Закрыт",
  } = options;

  statusEl.textContent = isOpen ? openText : closedText;
  toggleEl.disabled = busy;
  toggleEl.setAttribute("aria-checked", String(isOpen));
  toggleEl.setAttribute("aria-busy", String(busy));
  toggleEl.dataset.state = isOpen ? "checked" : "unchecked";
  toggleEl.classList.toggle("admin-switch--checked", isOpen);
  toggleEl.classList.toggle("admin-switch--busy", busy);
  toggleEl.classList.toggle("admin-switch--disabled", busy);
}

function updateAdminButtons() {
  updateAdminSwitch(submissionToggleEl, submissionStatusTextEl, {
    isOpen: state.submissionOpen,
    busy: state.adminStageBusyTarget === "submission",
    openText: "Открыт",
    closedText: "Закрыт",
  });

  updateAdminSwitch(votingToggleEl, votingStatusTextEl, {
    isOpen: state.votingOpen,
    busy: state.adminStageBusyTarget === "voting",
    openText: "Открыт",
    closedText: "Закрыт",
  });
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

function setActiveCardState(previousEntryId, nextEntryId) {
  if (previousEntryId != null) {
    const previousCard = entriesGridEl.querySelector(
      `.entry-card[data-entry-id="${String(previousEntryId)}"]`
    );
    previousCard?.classList.remove("entry-card--active");
  }

  if (nextEntryId != null) {
    const nextCard = entriesGridEl.querySelector(
      `.entry-card[data-entry-id="${String(nextEntryId)}"]`
    );
    nextCard?.classList.add("entry-card--active");
  }
}

function clearActiveSelection() {
  if (state.activeEntryId == null) return;
  const previousActiveEntryId = state.activeEntryId;
  state.activeEntryId = null;
  setActiveCardState(previousActiveEntryId, null);
  renderActionBar();
}

function shouldPreserveSelection(target) {
  if (!(target instanceof Element)) return false;

  return Boolean(
    target.closest(
      [
        ".entry-card",
        ".entry-card__preview-btn",
        ".admin-entry-button",
        ".theme-toggle",
        ".rules-button",
        ".admin-panel",
        ".action-bar",
        ".modal",
      ].join(", ")
    )
  );
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

    card.dataset.entryId = String(entry.id);

    if (state.isAdmin) {
      const adminDetailBtn = document.createElement("button");
      adminDetailBtn.type = "button";
      adminDetailBtn.className = "admin-entry-button entry-card__admin-detail-btn";
      adminDetailBtn.textContent = "Подробнее";
      adminDetailBtn.addEventListener("click", (event) => {
        event.stopPropagation();
        openAdminEntryDetail(entry.id);
      });
      fragment.querySelector(".entry-card__body")?.appendChild(adminDetailBtn);
    }

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
    if (state.availableVotes <= 0 || state.phase === "results") card.classList.add("entry-card--readonly");
    if (entryDisabled(entry)) card.classList.add("entry-card--disabled");

    if (entry.is_owned_by_current_user) {
      meta.textContent = "Ваша работа — голосовать нельзя";
    } else if (state.phase === "results") {
      const reward = entry.reward_label || "—";
      const author = entry.author_label || "—";
      const place = Number(entry.place) > 0 ? entry.place : "—";
      const resultVotesSource = entry.effective_net_votes ?? entry.net_votes;
      const votes = Number.isFinite(Number(resultVotesSource)) ? Number(resultVotesSource) : 0;
      const rewardLine = Number(place) >= 1 && Number(place) <= 3 ? `Награда: ${reward}` : reward;
      meta.textContent = `Голосов: ${votes}\nМесто: ${place}\n${rewardLine}\nАвтор: ${author}`;
      if (place === 1) card.classList.add("entry-card--winner-gold");
      if (place === 2) card.classList.add("entry-card--winner-silver");
      if (place === 3) card.classList.add("entry-card--winner-bronze");
    } else if (isConfirmed) {
      meta.textContent = "Ваш подтвержденный выбор";
    } else if (inDraft) {
      meta.textContent = "Добавлено в выбор";
    } else {
      meta.textContent = "Нажмите карточку для выбора";
    }

    const imageUrl = buildEntryImageUrl(entry);
    console.log("contest entry image debug", {
      id: entry.id,
      display_number: entry.display_number,
      image_url: entry.image_url,
      built_url: imageUrl,
    });
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
      const previousActiveEntryId = state.activeEntryId;
      state.activeEntryId = previousActiveEntryId === entry.id ? null : entry.id;
      setActiveCardState(previousActiveEntryId, state.activeEntryId);
      renderActionBar();
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
  const requiredToConfirm = state.availableVotes;
  const canConfirm = requiredToConfirm > 0 && state.draftEntryIds.size === requiredToConfirm;

  confirmVotesBtnEl.hidden = !canConfirm;
  confirmVotesBtnEl.disabled =
    state.busy || requiredToConfirm <= 0 || state.draftEntryIds.size !== requiredToConfirm;

  actionBarEl.hidden = !entry;
  if (!entry) return;

  const inDraft = state.draftEntryIds.has(entry.id);
  actionBarTextEl.textContent =
    state.availableVotes > 0
      ? `Выбрано в добор: ${selected}/${state.availableVotes}`
      : "Голоса подтверждены";

  if (state.phase === "results") {
    actionBarButtonEl.disabled = true;
    actionBarButtonEl.textContent = "Итоги подведены";
    return;
  }

  if (state.availableVotes <= 0) {
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

    const leaderItems = Array.isArray(payload.leaders) && payload.leaders.length
      ? payload.leaders
      : (payload.items || []).slice(0, 5);
    adminOverviewEl.innerHTML = leaderItems.map(renderAdminEntryLog).join("");
  } catch (error) {
    adminOverviewEl.textContent = fallbackMessage;

    if (throwOnError) {
      throw new Error(error.message || fallbackMessage);
    }
  }
}

function renderAdminEntryLog(entry) {
  const displayNumber = Number(entry.display_number) > 0 ? Number(entry.display_number) : "—";
  const place = Number(entry.place) > 0 ? Number(entry.place) : "—";
  const netVotes = Number.isFinite(Number(entry.net_votes)) ? Number(entry.net_votes) : 0;
  const author = Number(entry.owner_user_id) > 0 ? entry.owner_user_id : "—";

  return `
  <div class="admin-entry-log">
    <div class="admin-entry-title">Работа #${displayNumber}</div>
    <div class="admin-entry-stats">
      <div>Голосов: ${netVotes}</div>
      <div>Место: ${place}</div>
      <div>Автор: ${author}</div>
    </div>
    <button class="admin-entry-button" type="button" data-entry-id="${entry.id}">Подробнее</button>
  </div>
  `;
}


function formatAdminEntryDetail(item) {
  if (!item || typeof item !== "object") {
    return "<p class=\"status-message\">Данные недоступны.</p>";
  }

  const rows = [
    ["ID", item.id],
    ["Номер", Number(item.display_number) > 0 ? item.display_number : "—"],
    ["Автор (user_id)", item.owner_user_id || "—"],
    ["Статус", safeString(item.status) || "—"],
    ["Подтвержденные", Number(item.confirmed_votes_count) || 0],
    ["Штраф", Number(item.penalty_votes) || 0],
    ["Итог (net)", Number(item.net_votes) || 0],
    ["Подозрительные", Number(item.suspicious_votes_count) || 0],
    ["Tie-break бонус", Number(item.tie_break_bonus) || 0],
    ["Tie-break итог", Number(item.effective_net_votes) || 0],
  ];

  return `
    <div class="admin-detail-grid">
      ${rows
        .map(
          ([label, value]) =>
            `<div class="admin-detail-grid__row"><span class="admin-detail-grid__label">${label}</span><span class="admin-detail-grid__value">${value}</span></div>`
        )
        .join("")}
    </div>
  `;
}

function closeAdminEntryDetail() {
  if (!adminEntryDetailModalEl) return;
  adminEntryDetailModalEl.hidden = true;
}

async function openAdminEntryDetail(entryId) {
  if (!adminEntryDetailModalEl || !adminEntryDetailContentEl || !state.isAdmin) return;

  adminEntryDetailContentEl.innerHTML = '<p class="status-message">Загрузка...</p>';
  adminEntryDetailModalEl.hidden = false;

  try {
    const payload = await fetchJson(`/api/contest/admin/entries/${entryId}`);
    adminEntryDetailContentEl.innerHTML = formatAdminEntryDetail(payload.item);
  } catch (error) {
    adminEntryDetailContentEl.innerHTML = `<p class="status-message">${error.message || "Не удалось загрузить детали."}</p>`;
  }
}

function applyContestState(statePayload, entriesPayload) {
  state.entries = entriesPayload.items || [];
  cleanupEntryImageLoadResults();
  state.draftEntryIds = new Set(statePayload.draft_entry_ids || []);
  state.confirmedEntryIds = new Set(statePayload.confirmed_entry_ids || []);
  state.submissionOpen = Boolean(statePayload.submission_open);
  state.votingOpen = Boolean(statePayload.voting_open);
  const phase = String(statePayload.phase || entriesPayload.phase || "").trim().toLowerCase();
  state.phase = ["submission", "voting", "results"].includes(phase)
    ? phase
    : (state.votingOpen ? "voting" : (state.submissionOpen ? "submission" : "results"));
  const activeVotesCount = Number(statePayload.active_votes_count);
  const fallbackActiveVotesCount = state.confirmedEntryIds.size;
  state.activeVotesCount = Number.isFinite(activeVotesCount) ? Math.max(0, activeVotesCount) : fallbackActiveVotesCount;
  const availableVotes = Number(statePayload.available_votes);
  const legacyVotesRemaining = Number(statePayload.votes_remaining);
  const fallbackAvailableVotes = Number.isFinite(legacyVotesRemaining)
    ? Math.max(0, legacyVotesRemaining)
    : Math.max(0, 3 - state.activeVotesCount);
  state.availableVotes = Number.isFinite(availableVotes) ? Math.max(0, availableVotes) : fallbackAvailableVotes;
  state.confirmed = state.availableVotes <= 0;
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

  if (!inDraft && state.draftEntryIds.size >= state.availableVotes) {
    setStatus(`Можно выбрать только ${state.availableVotes} работ в текущей сессии.`);
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
  if (state.draftEntryIds.size !== state.availableVotes || state.availableVotes <= 0 || state.busy) {
    return;
  }

  confirmModalEl.hidden = false;
}

async function submitConfirmVotes() {
  setBusy(true);
  confirmModalEl.hidden = true;

  try {
    const [confirmPayload, entriesPayload] = await Promise.all([
      fetchJson("/api/contest/votes/confirm", {
        method: "POST",
        body: "{}",
      }),
      fetchJson("/api/contest/entries/approved"),
    ]);

    applyContestState(confirmPayload, entriesPayload);
    renderDataState();
    setStatus("Голоса подтверждены.");
  } catch (error) {
    setStatus(error.message || "Не удалось подтвердить голоса.");
  } finally {
    setBusy(false);
  }
}

async function openRules() {
  try {
    cleanupRulesEmoji(rulesContentEl);
    rulesContentEl.textContent = "Загрузка правил...";
    rulesModalEl.hidden = false;

    const payload = await fetchJson("/api/contest/rules");
    const rulesHtml = safeString(payload.rules_html);
    if (rulesHtml) {
      rulesContentEl.innerHTML = rulesHtml;
      await hydrateCustomEmoji(rulesContentEl);
      return;
    }

    rulesContentEl.textContent =
      payload.rules_text || "Правила пока не опубликованы.";
  } catch (error) {
    rulesContentEl.textContent =
      error.message || "Не удалось загрузить правила.";
  }
}

function getAdminStageBusyTarget(path) {
  if (path.includes("/submission/")) return "submission";
  if (path.includes("/voting/")) return "voting";
  return "";
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

  state.adminStageBusyTarget = getAdminStageBusyTarget(path);
  setBusy(true);
  updateAdminButtons();

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
    state.adminStageBusyTarget = "";
    setBusy(false);
    updateAdminButtons();
  }
}

themeToggleEl?.addEventListener("click", () => {
  toggleTheme();
});
rulesButtonEl?.addEventListener("click", openRules);
rulesCloseEl?.addEventListener("click", () => {
  cleanupRulesEmoji(rulesContentEl);
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
  if (event.key !== "Escape") return;
  if (imagePreviewModalEl && !imagePreviewModalEl.hidden) {
    closeImagePreview();
    return;
  }
  if (adminEntryDetailModalEl && !adminEntryDetailModalEl.hidden) {
    closeAdminEntryDetail();
  }
});
submissionToggleEl?.addEventListener("click", () =>
  adminStage(
    state.submissionOpen
      ? "/api/contest/admin/submission/close"
      : "/api/contest/admin/submission/open"
  )
);
votingToggleEl?.addEventListener("click", () =>
  adminStage(
    state.votingOpen
      ? "/api/contest/admin/voting/close"
      : "/api/contest/admin/voting/open"
  )
);
adminOverviewEl?.addEventListener("click", (event) => {
  const button = event.target.closest(".admin-entry-button[data-entry-id]");
  if (!button || !adminOverviewEl.contains(button)) return;
  event.stopPropagation();
  const entryId = Number(button.dataset.entryId);
  if (!Number.isFinite(entryId) || entryId <= 0) return;
  openAdminEntryDetail(entryId);
});
adminEntryDetailCloseEl?.addEventListener("click", closeAdminEntryDetail);
adminEntryDetailModalEl?.addEventListener("click", (event) => {
  if (event.target === adminEntryDetailModalEl) {
    closeAdminEntryDetail();
  }
});
document.addEventListener("click", (event) => {
  if (state.activeEntryId == null) return;
  if (shouldPreserveSelection(event.target)) return;
  clearActiveSelection();
});

applyTheme(getPreferredTheme());
loadData();