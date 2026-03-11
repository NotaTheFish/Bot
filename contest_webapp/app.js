const API_BASE_URL = window.CONTEST_API_BASE_URL  ;
const RULES_URL = window.CONTEST_RULES_URL  httpst.me;
const MEDIA_BASE_URL = window.CONTEST_MEDIA_BASE_URL  ;

const votesCounterEl = document.getElementById(votesCounter);
const entriesGridEl = document.getElementById(entriesGrid);
const statusMessageEl = document.getElementById(statusMessage);
const rulesLinkEl = document.getElementById(rulesLink);
const cardTemplate = document.getElementById(entryCardTemplate);

const tg = window.Telegram.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

rulesLinkEl.href = RULES_URL;

const initData = tg.initData  ;
const authHeaders = initData  { X-Telegram-Init-Data initData }  {};

const state = {
  currentUserId null,
  maxVotes 0,
  votesUsed 0,
  votesRemaining 0,
  votedEntryIds new Set(),
  votingOpen false,
  entries [],
  busyEntryId null,
};

function setStatus(message) {
  statusMessageEl.textContent = message  ;
}

function updateVotesCounter() {
  votesCounterEl.textContent = `Осталось голосов ${state.votesRemaining}`;
}

function buildEntryImageUrl(entry) {
  if (MEDIA_BASE_URL && entry.storage_message_id) {
    return `${MEDIA_BASE_URL.replace($, )}${entry.storage_message_id}`;
  }
  return httpsplacehold.co640x640text=Contest+Entry;
}

function getVoteDisableReason(entry) {
  if (!state.votingOpen) return Голосование закрыто;
  if (entry.is_owned_by_current_user) return Своя работа;
  if (state.votedEntryIds.has(entry.id)) return Уже голосовали;
  if (state.votesRemaining = 0) return Лимит исчерпан;
  return ;
}

function renderEntries() {
  entriesGridEl.innerHTML = ;

  if (!state.entries.length) {
    setStatus(Пока нет работ для голосования.);
    return;
  }

  for (const entry of state.entries) {
    const fragment = cardTemplate.content.cloneNode(true);
    const card = fragment.querySelector(.entry-card);
    const image = fragment.querySelector(.entry-card__image);
    const title = fragment.querySelector(.entry-card__title);
    const votes = fragment.querySelector(.entry-card__votes);
    const button = fragment.querySelector(.vote-button);

    image.src = buildEntryImageUrl(entry);
    title.textContent = `Работа #${entry.id}`;
    votes.textContent = `Голосов ${entry.votes_count  0}`;

    const disableReason = getVoteDisableReason(entry);
    const isBusy = state.busyEntryId === entry.id;
    const disabled = Boolean(disableReason)  isBusy;

    if (disabled) {
      card.classList.add(entry-card--disabled);
    }

    button.disabled = disabled;
    button.textContent = isBusy  Отправляем...  disableReason  Проголосовать;
    button.addEventListener(click, () = castVote(entry.id));

    entriesGridEl.appendChild(fragment);
  }
}

async function fetchJson(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...options,
    headers {
      Content-Type applicationjson,
      ...authHeaders,
      ...(options.headers  {}),
    },
  });
  const payload = await response.json();
  if (!response.ok  payload.ok === false) {
    throw new Error(payload.message  Ошибка API);
  }
  return payload;
}

async function loadData() {
  setStatus(Загрузка...);

  try {
    const [entriesPayload, votesPayload] = await Promise.all([
      fetchJson(apicontestentriesapproved),
      fetchJson(apicontestvotesstate),
    ]);

    state.currentUserId = votesPayload.user_id;
    state.maxVotes = votesPayload.max_votes;
    state.votesUsed = votesPayload.votes_used;
    state.votesRemaining = votesPayload.votes_remaining;
    state.votedEntryIds = new Set(votesPayload.voted_entry_ids  []);
    state.votingOpen = Boolean(votesPayload.voting_open);
    state.entries = entriesPayload.items  [];

    updateVotesCounter();
    renderEntries();
    setStatus();
  } catch (error) {
    setStatus(error.message  Не удалось загрузить данные.);
  }
}

async function castVote(entryId) {
  const entry = state.entries.find((item) = item.id === entryId);
  if (!entry  getVoteDisableReason(entry)) {
    return;
  }

  state.busyEntryId = entryId;
  renderEntries();

  try {
    const payload = await fetchJson(apicontestvotescast, {
      method POST,
      body JSON.stringify({ entry_id entryId }),
    });

    state.votesUsed = payload.votes_used;
    state.votesRemaining = payload.votes_remaining;
    state.votedEntryIds.add(entryId);

    const entryRef = state.entries.find((item) = item.id === entryId);
    if (entryRef) {
      entryRef.votes_count = (entryRef.votes_count  0) + 1;
    }

    updateVotesCounter();
    setStatus(Голос принят.);
  } catch (error) {
    setStatus(error.message  Не удалось отправить голос.);
  } finally {
    state.busyEntryId = null;
    renderEntries();
  }
}

loadData();