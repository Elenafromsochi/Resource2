<template>
  <div class="app">
    <header class="header">
      <h1>Telegram Activity Monitor</h1>
    </header>

    <main class="stack">
      <section class="block">
        <h2>Анализ активности</h2>
        <div class="row">
          <input v-model="userListFrom" type="date" />
          <input v-model="userListTo" type="date" />
          <button :disabled="userListLoading" @click="getUsersList">
            Получить список пользователей
          </button>
        </div>
        <div class="table-container analysis-table">
          <table>
            <thead>
              <tr>
                <th class="select-cell">
                  <label class="checkbox">
                    <input
                      type="checkbox"
                      :checked="selectAllChannels"
                      @change="toggleAllChannels"
                    />
                    Все
                  </label>
                </th>
                <th>Название</th>
                <th>Username</th>
                <th>Тип</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="channel in sortedChannelsForSelect" :key="channel.id">
                <td class="select-cell">
                  <input
                    type="checkbox"
                    :value="channel.id"
                    v-model="selectedChannelIds"
                  />
                </td>
                <td>{{ channel.title }}</td>
                <td>{{ channel.username || "-" }}</td>
                <td>{{ channel.channel_type }}</td>
              </tr>
              <tr v-if="sortedChannelsForSelect.length === 0">
                <td colspan="4" class="muted">Нет каналов</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div
          v-if="
            analysisResult &&
            analysisResult.errors &&
            analysisResult.errors.length
          "
          class="analysis-errors"
        >
          <h3>Ошибки анализа</h3>
          <ul>
            <li v-for="(error, index) in analysisResult.errors" :key="index">
              {{ error }}
            </li>
          </ul>
        </div>
      </section>

      <section class="block">
        <h2>Каналы и группы</h2>
        <label class="field-label" for="channel-input">
          Добавить канал или группу
        </label>
        <div class="row compact">
          <input
            id="channel-input"
            v-model="newChannelValue"
            type="text"
            placeholder="@username или https://t.me/..."
          />
          <button :disabled="channelLoading" @click="addChannel">
            Добавить
          </button>
          <button :disabled="channelLoading" @click="importDialogs">
            Импортировать из диалогов
          </button>
        </div>
        <div class="table-container" @scroll="onChannelsScroll">
          <table class="compact-table">
            <thead>
              <tr>
                <th class="sortable" @click="setChannelSort('photo')">
                  Фото
                  <span v-if="channelSort.key === 'photo'" class="sort-indicator">
                    {{ channelSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setChannelSort('id')">
                  ID
                  <span v-if="channelSort.key === 'id'" class="sort-indicator">
                    {{ channelSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setChannelSort('title')">
                  Название
                  <span v-if="channelSort.key === 'title'" class="sort-indicator">
                    {{ channelSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setChannelSort('username')">
                  Username
                  <span v-if="channelSort.key === 'username'" class="sort-indicator">
                    {{ channelSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setChannelSort('channel_type')">
                  Тип
                  <span
                    v-if="channelSort.key === 'channel_type'"
                    class="sort-indicator"
                  >
                    {{ channelSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="channel in sortedChannels"
                :key="channel.id"
                class="clickable-row"
                :class="{ active: selectedChannelDetailsId === channel.id }"
                @click="openChannelDetails(channel.id)"
              >
                <td class="photo-cell">
                  <div
                    class="avatar"
                    :class="{ placeholder: !getChannelAvatarUrl(channel) }"
                  >
                    <img
                      v-if="getChannelAvatarUrl(channel)"
                      :src="getChannelAvatarUrl(channel)"
                      alt=""
                      loading="lazy"
                    />
                  </div>
                </td>
                <td>{{ channel.id }}</td>
                <td>{{ channel.title }}</td>
                <td>{{ channel.username || "-" }}</td>
                <td>{{ channel.channel_type }}</td>
                <td class="actions">
                  <button
                    class="icon-button"
                    type="button"
                    title="Удалить"
                    @click.stop="removeChannel(channel.id)"
                  >
                    <svg viewBox="0 0 24 24" aria-hidden="true">
                      <path
                        d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v9h-2V9zm4 0h2v9h-2V9zM6 9h2v9H6V9z"
                        fill="currentColor"
                      />
                    </svg>
                  </button>
                </td>
              </tr>
              <tr v-if="channelLoading">
                <td colspan="6" class="muted">Загрузка...</td>
              </tr>
              <tr v-if="!channelLoading && channels.length === 0">
                <td colspan="6" class="muted">Нет каналов</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="selectedChannelDetailsId" class="details">
          <h3>Детали канала</h3>
          <div v-if="channelDetailsLoading" class="muted">Загрузка...</div>
          <div v-else-if="channelDetails" class="details-grid">
            <div class="detail-row">
              <span class="detail-label">ID</span>
              <span>{{ channelDetails.id }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Название</span>
              <span>{{ channelDetails.title }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Username</span>
              <span>{{ channelDetails.username || "-" }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Тип</span>
              <span>{{ channelDetails.channel_type }}</span>
            </div>
            <div v-if="channelDetails.link" class="detail-row">
              <span class="detail-label">Ссылка</span>
              <a :href="channelDetails.link" target="_blank" rel="noreferrer">
                {{ channelDetails.link }}
              </a>
            </div>
            <div
              v-if="
                channelDetails.members_count !== null &&
                channelDetails.members_count !== undefined
              "
              class="detail-row"
            >
              <span class="detail-label">Участники</span>
              <span>{{ channelDetails.members_count }}</span>
            </div>
            <div v-if="channelDetails.about" class="detail-row">
              <span class="detail-label">Описание</span>
              <span>{{ channelDetails.about }}</span>
            </div>
          </div>
        </div>
      </section>

      <section class="block">
        <h2>Пользователи</h2>
        <div class="table-container" @scroll="onUsersScroll">
          <table class="compact-table">
            <thead>
              <tr>
                <th class="sortable" @click="setUserSort('photo')">
                  Фото
                  <span v-if="userSort.key === 'photo'" class="sort-indicator">
                    {{ userSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setUserSort('name')">
                  Имя
                  <span v-if="userSort.key === 'name'" class="sort-indicator">
                    {{ userSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setUserSort('username')">
                  Username
                  <span v-if="userSort.key === 'username'" class="sort-indicator">
                    {{ userSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
                <th class="sortable" @click="setUserSort('messages_count')">
                  Сообщений
                  <span
                    v-if="userSort.key === 'messages_count'"
                    class="sort-indicator"
                  >
                    {{ userSort.direction === "asc" ? "^" : "v" }}
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="user in sortedUsers"
                :key="user.id"
                class="clickable-row"
                :class="{ active: selectedUserId === user.id }"
                @click="openUserDetails(user.id)"
              >
                <td class="photo-cell">
                  <div class="avatar" :class="{ placeholder: !getUserAvatarUrl(user) }">
                    <img
                      v-if="getUserAvatarUrl(user)"
                      :src="getUserAvatarUrl(user)"
                      alt=""
                      loading="lazy"
                    />
                  </div>
                </td>
                <td>{{ formatUserName(user) }}</td>
                <td>{{ user.username || "-" }}</td>
                <td>{{ user.messages_count }}</td>
              </tr>
              <tr v-if="userLoading">
                <td colspan="4" class="muted">Загрузка...</td>
              </tr>
              <tr v-if="!userLoading && users.length === 0">
                <td colspan="4" class="muted">Нет данных</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="selectedUserId" class="details">
          <h3>Детали пользователя</h3>
          <div v-if="userDetailsLoading" class="muted">Загрузка...</div>
          <div v-else-if="userDetails" class="details-grid">
            <div class="detail-row">
              <span class="detail-label">ID</span>
              <span>{{ userDetails.id }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Имя</span>
              <span>{{ formatUserName(userDetails) }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Username</span>
              <span>{{ userDetails.username || "-" }}</span>
            </div>
            <div v-if="userDetails.phone" class="detail-row">
              <span class="detail-label">Телефон</span>
              <span>{{ userDetails.phone }}</span>
            </div>
            <div v-if="userDetails.bio" class="detail-row">
              <span class="detail-label">Описание</span>
              <span>{{ userDetails.bio }}</span>
            </div>
          </div>
          <div v-if="userDetails && userDetails.groups.length" class="group-list">
            <h4>Группы</h4>
            <ul>
              <li v-for="group in userDetails.groups" :key="group.id">
                <span class="group-title">{{ group.title }}</span>
                <span class="group-meta">
                  {{ group.username ? `@${group.username}` : group.id }}
                </span>
              </li>
            </ul>
          </div>
          <div
            v-else-if="userDetails && userDetails.groups.length === 0"
            class="muted"
          >
            Нет групп
          </div>
        </div>
      </section>
    </main>
  </div>
</template>

<script setup>
import axios from "axios";
import { computed, onMounted, ref } from "vue";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const api = axios.create({ baseURL: API_BASE });
const TELEGRAM_AVATAR_BASE = "https://t.me/i/userpic/320/";

const channels = ref([]);
const channelOffset = ref(0);
const channelHasMore = ref(true);
const channelLoading = ref(false);
const selectedChannelDetailsId = ref(null);
const channelDetails = ref(null);
const channelDetailsLoading = ref(false);
const channelDetailsRequestId = ref(0);

const users = ref([]);
const userOffset = ref(0);
const userHasMore = ref(true);
const userLoading = ref(false);
const selectedUserId = ref(null);
const userDetails = ref(null);
const userDetailsLoading = ref(false);
const userDetailsRequestId = ref(0);

const newChannelValue = ref("");
const channelsForSelect = ref([]);
const selectedChannelIds = ref([]);

const userListFrom = ref("");
const userListTo = ref("");
const userListLoading = ref(false);
const analysisResult = ref(null);

const channelSort = ref({ key: null, direction: "asc" });
const userSort = ref({ key: null, direction: "asc" });

const selectAllChannels = computed(() => {
  return (
    channelsForSelect.value.length > 0 &&
    selectedChannelIds.value.length === channelsForSelect.value.length
  );
});

const collator = new Intl.Collator("ru", { numeric: true, sensitivity: "base" });

const getUserNameValue = (user) => {
  const parts = [user.first_name, user.last_name].filter(Boolean);
  return parts.join(" ");
};

const formatUserName = (user) => {
  const fullName = getUserNameValue(user);
  return fullName ? fullName : "-";
};

const isHttpUrl = (value) =>
  typeof value === "string" && /^https?:\/\//i.test(value);

const extractHandle = (value) => {
  if (!value) {
    return "";
  }
  const match = value.match(/t\.me\/([a-zA-Z0-9_]+)/);
  return match ? match[1] : "";
};

const buildAvatarUrl = (username, photo) => {
  if (photo && isHttpUrl(photo)) {
    return photo;
  }
  if (username) {
    return `${TELEGRAM_AVATAR_BASE}${username}.jpg`;
  }
  return "";
};

const getChannelAvatarKey = (channel) =>
  channel.username || extractHandle(channel.link);

const getUserAvatarKey = (user) =>
  isHttpUrl(user.photo) ? user.photo : user.username || "";

const getChannelAvatarUrl = (channel) => {
  const handle = getChannelAvatarKey(channel);
  return buildAvatarUrl(handle);
};

const getUserAvatarUrl = (user) => buildAvatarUrl(user.username, user.photo);

const compareValues = (valueA, valueB) => {
  if (valueA === valueB) {
    return 0;
  }
  if (valueA === null || valueA === undefined || valueA === "") {
    return 1;
  }
  if (valueB === null || valueB === undefined || valueB === "") {
    return -1;
  }
  if (typeof valueA === "number" && typeof valueB === "number") {
    return valueA - valueB;
  }
  return collator.compare(String(valueA), String(valueB));
};

const sortRecords = (records, sortState, sorters) => {
  if (!sortState.key || !sorters[sortState.key]) {
    return records;
  }
  return [...records].sort((a, b) => {
    const valueA = sorters[sortState.key](a);
    const valueB = sorters[sortState.key](b);
    const result = compareValues(valueA, valueB);
    return sortState.direction === "asc" ? result : -result;
  });
};

const setSort = (sortRef, key) => {
  const current = sortRef.value;
  if (current.key === key) {
    sortRef.value = {
      key,
      direction: current.direction === "asc" ? "desc" : "asc",
    };
    return;
  }
  sortRef.value = { key, direction: "asc" };
};

const setChannelSort = (key) => setSort(channelSort, key);
const setUserSort = (key) => setSort(userSort, key);

const channelSorters = {
  photo: (channel) => getChannelAvatarKey(channel),
  id: (channel) => channel.id,
  title: (channel) => channel.title || "",
  username: (channel) => channel.username || "",
  channel_type: (channel) => channel.channel_type || "",
};

const userSorters = {
  photo: (user) => getUserAvatarKey(user),
  name: (user) => getUserNameValue(user),
  username: (user) => user.username || "",
  messages_count: (user) => user.messages_count ?? 0,
};

const sortedChannels = computed(() =>
  sortRecords(channels.value, channelSort.value, channelSorters),
);

const sortedUsers = computed(() =>
  sortRecords(users.value, userSort.value, userSorters),
);

const sortedChannelsForSelect = computed(() => {
  return [...channelsForSelect.value].sort((a, b) =>
    collator.compare(a.title || "", b.title || ""),
  );
});

const fetchChannels = async (reset = false) => {
  if (channelLoading.value || (!channelHasMore.value && !reset)) {
    return;
  }
  channelLoading.value = true;
  if (reset) {
    channels.value = [];
    channelOffset.value = 0;
    channelHasMore.value = true;
    selectedChannelDetailsId.value = null;
    channelDetails.value = null;
  }
  const { data } = await api.get("/channels", {
    params: { offset: channelOffset.value, limit: 30 },
  });
  channels.value.push(...data.items);
  if (data.next_offset === null) {
    channelHasMore.value = false;
  } else {
    channelOffset.value = data.next_offset;
  }
  channelLoading.value = false;
};

const fetchUsers = async (reset = false) => {
  if (userLoading.value || (!userHasMore.value && !reset)) {
    return;
  }
  userLoading.value = true;
  if (reset) {
    users.value = [];
    userOffset.value = 0;
    userHasMore.value = true;
    selectedUserId.value = null;
    userDetails.value = null;
  }
  const { data } = await api.get("/users", {
    params: { offset: userOffset.value, limit: 30 },
  });
  users.value.push(...data.items);
  if (data.next_offset === null) {
    userHasMore.value = false;
  } else {
    userOffset.value = data.next_offset;
  }
  userLoading.value = false;
};

const openUserDetails = async (userId) => {
  if (selectedUserId.value === userId) {
    selectedUserId.value = null;
    userDetails.value = null;
    userDetailsLoading.value = false;
    userDetailsRequestId.value += 1;
    return;
  }
  const requestId = userDetailsRequestId.value + 1;
  userDetailsRequestId.value = requestId;
  selectedUserId.value = userId;
  userDetails.value = null;
  userDetailsLoading.value = true;
  try {
    const { data } = await api.get(`/users/${userId}`);
    if (userDetailsRequestId.value !== requestId) {
      return;
    }
    userDetails.value = data;
  } finally {
    if (userDetailsRequestId.value === requestId) {
      userDetailsLoading.value = false;
    }
  }
};

const fetchChannelsForSelect = async () => {
  const { data } = await api.get("/channels/all");
  channelsForSelect.value = data;
};

const addChannel = async () => {
  const value = newChannelValue.value.trim();
  if (!value) {
    return;
  }
  await api.post("/channels", { value });
  newChannelValue.value = "";
  await fetchChannels(true);
  await fetchChannelsForSelect();
};

const removeChannel = async (channelId) => {
  await api.delete(`/channels/${channelId}`);
  await fetchChannels(true);
  await fetchChannelsForSelect();
  if (selectedChannelDetailsId.value === channelId) {
    selectedChannelDetailsId.value = null;
    channelDetails.value = null;
  }
};

const importDialogs = async () => {
  await api.post("/channels/import-dialogs");
  await fetchChannels(true);
  await fetchChannelsForSelect();
};

const getUsersList = async () => {
  if (!userListFrom.value || !userListTo.value) {
    return;
  }
  userListLoading.value = true;
  analysisResult.value = null;
  const dateFrom = new Date(`${userListFrom.value}T00:00:00Z`).toISOString();
  const dateTo = new Date(`${userListTo.value}T23:59:59Z`).toISOString();
  const payload = {
    date_from: dateFrom,
    date_to: dateTo,
    channel_ids:
      selectedChannelIds.value.length > 0 ? selectedChannelIds.value : null,
  };
  try {
    const { data } = await api.post("/users/analyze", payload);
    analysisResult.value = data;
    await fetchUsers(true);
  } finally {
    userListLoading.value = false;
  }
};

const toggleAllChannels = (event) => {
  if (event.target.checked) {
    selectedChannelIds.value = channelsForSelect.value.map(
      (channel) => channel.id,
    );
  } else {
    selectedChannelIds.value = [];
  }
};

const onChannelsScroll = (event) => {
  const target = event.target;
  if (target.scrollTop + target.clientHeight >= target.scrollHeight - 40) {
    fetchChannels();
  }
};

const openChannelDetails = async (channelId) => {
  if (selectedChannelDetailsId.value === channelId) {
    selectedChannelDetailsId.value = null;
    channelDetails.value = null;
    channelDetailsLoading.value = false;
    channelDetailsRequestId.value += 1;
    return;
  }
  const requestId = channelDetailsRequestId.value + 1;
  channelDetailsRequestId.value = requestId;
  selectedChannelDetailsId.value = channelId;
  channelDetails.value = null;
  channelDetailsLoading.value = true;
  try {
    const { data } = await api.get(`/channels/${channelId}`);
    if (channelDetailsRequestId.value !== requestId) {
      return;
    }
    channelDetails.value = data;
  } finally {
    if (channelDetailsRequestId.value === requestId) {
      channelDetailsLoading.value = false;
    }
  }
};

const onUsersScroll = (event) => {
  const target = event.target;
  if (target.scrollTop + target.clientHeight >= target.scrollHeight - 40) {
    fetchUsers();
  }
};

onMounted(async () => {
  await fetchChannels(true);
  await fetchChannelsForSelect();
  await fetchUsers(true);
});
</script>

<style scoped>
.app {
  font-family: Arial, sans-serif;
  padding: 20px;
  padding-left: 20%;
  padding-right: 20%;
  color: #1f2933;
}

.header {
  margin-bottom: 12px;
}

.stack {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.block {
  border: 1px solid #e4e7eb;
  border-radius: 8px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.block h2 {
  margin: 0;
  font-size: 16px;
}

.field-label {
  font-weight: 600;
  font-size: 12px;
}

.row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.row.compact {
  gap: 6px;
}

input[type="text"],
input[type="date"] {
  padding: 5px 8px;
  border: 1px solid #cbd2d9;
  border-radius: 4px;
  min-width: 200px;
  font-size: 12px;
}

button {
  padding: 6px 10px;
  border: none;
  border-radius: 4px;
  background: #2f80ed;
  color: #fff;
  cursor: pointer;
  font-size: 12px;
}

button:disabled {
  background: #b0b7c3;
  cursor: not-allowed;
}

.table-container {
  max-height: 380px;
  overflow-y: auto;
  border: 1px solid #e4e7eb;
  border-radius: 6px;
}

.analysis-table {
  max-height: 260px;
}

.analysis-errors {
  border: 1px solid #f2b8b5;
  background: #fff4f4;
  color: #9b1c1c;
  border-radius: 6px;
  padding: 8px;
  font-size: 12px;
}

.analysis-errors h3 {
  margin: 0 0 6px;
  font-size: 12px;
}

.analysis-errors ul {
  margin: 0;
  padding-left: 16px;
  display: grid;
  gap: 4px;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.compact-table {
  font-size: 11px;
}

th,
td {
  padding: 6px 8px;
  border-bottom: 1px solid #f0f2f5;
  text-align: left;
  vertical-align: middle;
}

.compact-table th,
.compact-table td {
  padding: 4px 6px;
}

th {
  background: #f5f7fa;
  position: sticky;
  top: 0;
  z-index: 1;
}

.sortable {
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}

.sort-indicator {
  margin-left: 4px;
  font-size: 10px;
  color: #6b7280;
}

.checkbox {
  display: inline-flex;
  gap: 6px;
  align-items: center;
  font-size: 12px;
}

.select-cell {
  width: 60px;
}

.photo-cell {
  width: 44px;
}

.avatar {
  width: 24px;
  height: 24px;
  border-radius: 50%;
  overflow: hidden;
  background: #f0f2f5;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.avatar img {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.avatar.placeholder {
  background: #e4e7eb;
}

.actions {
  text-align: right;
}

.icon-button {
  padding: 0;
  width: 26px;
  height: 26px;
  background: #fbe9e7;
  color: #c0392b;
  border: 1px solid #f4c6c0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.icon-button svg {
  width: 14px;
  height: 14px;
}

.clickable-row {
  cursor: pointer;
}

.clickable-row.active {
  background: #f0f6ff;
}

.details {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.details h3 {
  margin: 0;
  font-size: 13px;
}

.details-grid {
  display: grid;
  gap: 6px;
}

.detail-row {
  display: grid;
  grid-template-columns: 80px 1fr;
  gap: 8px;
  font-size: 12px;
}

.detail-label {
  color: #6b7280;
  font-weight: 600;
}

.group-list h4 {
  margin: 6px 0 4px;
  font-size: 12px;
}

.group-list ul {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  gap: 4px;
}

.group-list li {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
}

.group-title {
  font-weight: 600;
}

.group-meta {
  color: #6b7280;
  font-size: 11px;
}

.muted {
  text-align: center;
  color: #8c94a1;
}
</style>
