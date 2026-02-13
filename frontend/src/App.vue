<template>
  <div class="app">
    <header class="header">
      <h1>Telegram Activity Monitor</h1>
    </header>

    <main class="stack">
      <section class="block">
        <h2>Анализ активности</h2>
        <div class="row analysis-controls">
          <div class="range-control">
            <div class="range-info">
              <span class="range-label">Период</span>
              <span class="range-date">{{ analysisRangeLabel }}</span>
            </div>
            <div class="range-slider">
              <input
                class="range-input range-input-start"
                type="range"
                min="0"
                :max="analysisRangeMaxDays"
                step="1"
                :value="rangeStartDays"
                aria-label="До (дней назад)"
                @input="updateRangeStart"
              />
              <input
                class="range-input range-input-end"
                type="range"
                min="0"
                :max="analysisRangeMaxDays"
                step="1"
                :value="rangeEndDays"
                aria-label="От (дней назад)"
                @input="updateRangeEnd"
              />
            </div>
            <div class="range-values">
              <span>От: {{ rangeFromLabel }}</span>
              <span>До: {{ rangeToLabel }}</span>
              <span class="range-scale">0 — {{ analysisRangeMaxDays }} дн.</span>
            </div>
          </div>
          <div class="analysis-actions">
            <button
              type="button"
              :disabled="
                cacheRefreshing ||
                userListLoading ||
                userStatsRefreshing ||
                renderMessagesLoading
              "
              @click="refreshCache"
            >
              Выгрузка сообщений
            </button>
            <button
              type="button"
              :disabled="
                cacheRefreshing ||
                userListLoading ||
                userStatsRefreshing ||
                renderMessagesLoading ||
                !canRenderMessages
              "
              @click="renderMessages"
            >
              Вывести сообщения
            </button>
          </div>
        </div>
        <div class="table-container analysis-table">
          <table class="compact-table">
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
                <th>Фото</th>
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
                <td>{{ channel.title }}</td>
                <td>{{ channel.username || "-" }}</td>
                <td>{{ channel.channel_type }}</td>
              </tr>
              <tr v-if="sortedChannelsForSelect.length === 0">
                <td colspan="5" class="muted">Нет каналов</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="cacheRefreshResult" class="cache-refresh">
          <div class="cache-refresh-header">
            <h3>Результат выгрузки сообщений</h3>
            <span class="cache-refresh-summary">
              Всего сообщений: {{ cacheRefreshResult.total }},
              добавлено: {{ cacheRefreshResult.created }},
              обновлено: {{ cacheRefreshResult.updated }}
            </span>
            <button
              type="button"
              class="cache-refresh-close"
              aria-label="Скрыть результат выгрузки сообщений"
              @click="dismissCacheRefreshResult"
            >
              Скрыть
            </button>
          </div>
          <div v-if="cacheRefreshChannels.length > 0" class="table-container cache-refresh-table">
            <table class="compact-table">
              <thead>
                <tr>
                  <th>Канал</th>
                  <th>Всего</th>
                  <th>Добавлено</th>
                  <th>Обновлено</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="channelStat in cacheRefreshChannels"
                  :key="channelStat.channel_id"
                >
                  <td>
                    <div class="cache-channel-title">
                      {{
                        channelStat.channel_title ||
                        formatSelectedChannelLabel(channelStat.channel_id)
                      }}
                    </div>
                    <div class="cache-channel-meta">ID: {{ channelStat.channel_id }}</div>
                  </td>
                  <td>{{ channelStat.total }}</td>
                  <td>{{ channelStat.created }}</td>
                  <td>{{ channelStat.updated }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div v-if="renderMessagesLoading" class="muted">
          Загрузка сообщений...
        </div>
        <div v-if="renderMessagesResult" class="render-messages">
          <div class="render-messages-header">
            <h3>Сообщения</h3>
            <span class="render-messages-summary">
              Канал: {{ formatSelectedChannelLabel(renderMessagesResult.channel_id) }},
              сообщений: {{ renderMessagesResult.messages.length }}
            </span>
            <button
              type="button"
              class="cache-refresh-close"
              aria-label="Скрыть сообщения"
              @click="dismissRenderMessages"
            >
              Скрыть
            </button>
          </div>
          <div class="render-messages-body">
            <pre class="render-messages-content">{{ renderMessagesText }}</pre>
          </div>
        </div>
        <div v-if="renderMessagesError" class="analysis-errors">
          <h3>Ошибка вывода сообщений</h3>
          <div>{{ renderMessagesError }}</div>
        </div>
      </section>

      <section class="block">
        <h2>Промпты</h2>
        <label class="field-label" for="prompt-title">Название</label>
        <div class="row compact">
          <div class="input-with-clear prompt-input">
            <input
              id="prompt-title"
              v-model="promptTitle"
              type="text"
              placeholder="Название промпта"
            />
            <button
              v-if="promptTitle"
              type="button"
              class="clear-button"
              aria-label="Очистить название"
              @click="promptTitle = ''"
            >
              ×
            </button>
          </div>
        </div>
        <label class="field-label" for="prompt-text">Текст</label>
        <textarea
          id="prompt-text"
          v-model="promptText"
          class="prompt-textarea"
          placeholder="Текст промпта"
        ></textarea>
        <div class="row compact prompt-actions">
          <button
            type="button"
            :disabled="promptSaving || !canSavePrompt"
            @click="savePrompt"
          >
            {{ isEditingPrompt ? "Сохранить" : "Добавить" }}
          </button>
          <button
            v-if="isEditingPrompt"
            type="button"
            class="secondary-button"
            :disabled="promptSaving"
            @click="resetPromptForm"
          >
            Отменить
          </button>
        </div>
        <div class="table-container prompt-table">
          <table class="compact-table">
            <thead>
              <tr>
                <th>Название</th>
                <th>Текст</th>
                <th class="actions">Действия</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="prompt in prompts" :key="prompt.id">
                <td>{{ prompt.title }}</td>
                <td>
                  <pre class="prompt-text-preview">{{ prompt.text }}</pre>
                </td>
                <td class="actions prompt-actions-cell">
                  <button
                    type="button"
                    class="secondary-button"
                    :disabled="promptSaving"
                    @click="startEditPrompt(prompt)"
                  >
                    Редактировать
                  </button>
                  <button
                    type="button"
                    class="danger-button"
                    :disabled="promptDeletingId === prompt.id"
                    @click="removePrompt(prompt.id)"
                  >
                    Удалить
                  </button>
                </td>
              </tr>
              <tr v-if="promptsLoading">
                <td colspan="3" class="muted">Загрузка...</td>
              </tr>
              <tr v-else-if="prompts.length === 0">
                <td colspan="3" class="muted">Нет промптов</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="block">
        <h2>Каналы и группы</h2>
        <label class="field-label" for="channel-input">
          Добавить канал или группу
        </label>
        <div class="row compact">
          <div class="input-with-clear">
            <input
              id="channel-input"
              v-model="newChannelValue"
              type="text"
              placeholder="@username или https://t.me/..."
            />
            <button
              v-if="newChannelValue"
              type="button"
              class="clear-button"
              aria-label="Очистить поле"
              @click="newChannelValue = ''"
            >
              ×
            </button>
          </div>
          <button :disabled="channelLoading" @click="addChannel">
            Добавить
          </button>
          <button :disabled="channelLoading" @click="importDialogs">
            Импортировать из диалогов
          </button>
        </div>
        <label class="field-label" for="channel-filter">
          Фильтр каналов
        </label>
        <div class="row compact">
          <div class="input-with-clear">
            <input
              id="channel-filter"
              v-model="channelSearch"
              type="text"
              placeholder="Поиск по ID, названию, username"
            />
            <button
              v-if="channelSearch"
              type="button"
              class="clear-button"
              aria-label="Очистить фильтр"
              @click="channelSearch = ''"
            >
              ×
            </button>
          </div>
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
        <label class="field-label" for="user-filter">
          Фильтр пользователей
        </label>
        <div class="row compact">
          <div class="input-with-clear">
            <input
              id="user-filter"
              v-model="userSearch"
              type="text"
              placeholder="Поиск по ID, имени, username"
            />
            <button
              v-if="userSearch"
              type="button"
              class="clear-button"
              aria-label="Очистить фильтр"
              @click="userSearch = ''"
            >
              ×
            </button>
          </div>
          <button
            class="align-right"
            :disabled="userStatsRefreshing || userListLoading"
            @click="refreshUserStats"
          >
            Обновить
          </button>
        </div>
        <div v-if="userStatsRefreshResult" class="user-stats-refresh">
          <div class="user-stats-header">
            <h3>Статистика сообщений обновлена</h3>
            <span class="user-stats-summary">
              Пользователей: {{ userStatsRefreshResult.users_updated }},
              каналов: {{ userStatsRefreshResult.channels_with_messages }},
              сообщений: {{ userStatsRefreshResult.messages_total }}
            </span>
          </div>
          <div
            v-if="
              userStatsRefreshResult.errors &&
              userStatsRefreshResult.errors.length
            "
            class="analysis-errors"
          >
            <h3>Ошибки обновления статистики</h3>
            <ul>
              <li v-for="(error, index) in userStatsRefreshResult.errors" :key="index">
                {{ error }}
              </li>
            </ul>
          </div>
        </div>
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
                <th class="sortable" @click="setUserSort('messages_total')">
                  Сообщений
                  <span
                    v-if="userSort.key === 'messages_total'"
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
                <td>{{ formatUserChannelMessages(user) }}</td>
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
import { computed, onMounted, ref, watch } from "vue";

import { ANALYSIS_RANGE_MAX_DAYS } from "./config";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const api = axios.create({ baseURL: API_BASE });
const TELEGRAM_AVATAR_BASE = "https://t.me/i/userpic/320/";

const channels = ref([]);
const channelOffset = ref(0);
const channelHasMore = ref(true);
const channelLoading = ref(false);
const channelListRequestId = ref(0);
const selectedChannelDetailsId = ref(null);
const channelDetails = ref(null);
const channelDetailsLoading = ref(false);
const channelDetailsRequestId = ref(0);
const channelSearch = ref("");

const users = ref([]);
const userOffset = ref(0);
const userHasMore = ref(true);
const userLoading = ref(false);
const userListRequestId = ref(0);
const selectedUserId = ref(null);
const userDetails = ref(null);
const userDetailsLoading = ref(false);
const userDetailsRequestId = ref(0);
const userSearch = ref("");

const newChannelValue = ref("");
const channelsForSelect = ref([]);
const selectedChannelIds = ref([]);

const analysisRangeMaxDays = ANALYSIS_RANGE_MAX_DAYS;
const rangeStartDays = ref(0);
const DEFAULT_ANALYSIS_LAST_DAYS = 3;
const rangeEndDays = ref(
  Math.min(analysisRangeMaxDays, DEFAULT_ANALYSIS_LAST_DAYS - 1),
);
const userListLoading = ref(false);
const cacheRefreshing = ref(false);
const userStatsRefreshing = ref(false);
const cacheRefreshResult = ref(null);
const userStatsRefreshResult = ref(null);
const renderMessagesLoading = ref(false);
const renderMessagesResult = ref(null);
const renderMessagesError = ref(null);

const prompts = ref([]);
const promptsLoading = ref(false);
const promptSaving = ref(false);
const promptDeletingId = ref(null);
const editingPromptId = ref(null);
const promptTitle = ref("");
const promptText = ref("");

const channelSort = ref({ key: null, direction: "asc" });
const userSort = ref({ key: null, direction: "asc" });

const selectAllChannels = computed(() => {
  return (
    channelsForSelect.value.length > 0 &&
    selectedChannelIds.value.length === channelsForSelect.value.length
  );
});

const collator = new Intl.Collator("ru", { numeric: true, sensitivity: "base" });
const dateFormatter = new Intl.DateTimeFormat("ru-RU", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  timeZone: "UTC",
});

const getUserNameValue = (user) => {
  const parts = [user.first_name, user.last_name].filter(Boolean);
  return parts.join(" ");
};

const formatUserName = (user) => {
  const fullName = getUserNameValue(user);
  return fullName ? fullName : "-";
};

const getUserTotalMessages = (user) => {
  if (!user?.channel_messages || user.channel_messages.length === 0) {
    return 0;
  }
  return user.channel_messages.reduce(
    (sum, entry) => sum + (entry?.messages_count ?? 0),
    0,
  );
};

const formatUserChannelMessages = (user) => {
  const total = getUserTotalMessages(user);
  if (!user?.channel_messages || user.channel_messages.length === 0) {
    return String(total);
  }
  const sorted = [...user.channel_messages].sort(
    (a, b) => a.channel_id - b.channel_id,
  );
  const perChannel = sorted
    .map((entry) => String(entry.messages_count ?? 0))
    .join(", ");
  return `${total} (${perChannel})`;
};

const formatSelectedChannelLabel = (channelId) => {
  const entry = channelsForSelect.value.find((channel) => channel.id === channelId);
  if (entry?.title) {
    return entry.title;
  }
  if (channelId !== null && channelId !== undefined) {
    return `Канал ${channelId}`;
  }
  return "Канал";
};

const toCount = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.trunc(numeric);
};

const formatDaysAgo = (days) =>
  days === 0 ? "сегодня" : `${days} дн. назад`;

const getUtcStartDate = (daysAgo) => {
  const now = new Date();
  return new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - daysAgo),
  );
};

const getUtcEndDate = (daysAgo) => {
  const date = getUtcStartDate(daysAgo);
  date.setUTCHours(23, 59, 59, 999);
  return date;
};

const analysisRangeLabel = computed(() => {
  const from = dateFormatter.format(getUtcStartDate(rangeEndDays.value));
  const to = dateFormatter.format(getUtcStartDate(rangeStartDays.value));
  return `${from} — ${to}`;
});

const rangeFromLabel = computed(() => formatDaysAgo(rangeEndDays.value));
const rangeToLabel = computed(() => formatDaysAgo(rangeStartDays.value));
const selectedChannelIdForMessages = computed(() => {
  if (selectedChannelIds.value.length === 1) {
    return selectedChannelIds.value[0];
  }
  return null;
});
const canRenderMessages = computed(() => selectedChannelIdForMessages.value !== null);
const cacheRefreshChannels = computed(() => {
  const channels = cacheRefreshResult.value?.channels;
  if (!Array.isArray(channels)) {
    return [];
  }
  return [...channels]
    .map((entry) => ({
      channel_id: toCount(entry?.channel_id),
      channel_title:
        typeof entry?.channel_title === "string" ? entry.channel_title : null,
      total: toCount(entry?.total),
      created: toCount(entry?.created),
      updated: toCount(entry?.updated),
    }))
    .sort((a, b) => {
      const totalDiff = b.total - a.total;
      if (totalDiff !== 0) {
        return totalDiff;
      }
      return a.channel_id - b.channel_id;
    });
});
const renderMessagesText = computed(() => {
  const messages = renderMessagesResult.value?.messages;
  if (!Array.isArray(messages)) {
    return "";
  }
  return messages.join("\n");
});

const isEditingPrompt = computed(() => editingPromptId.value !== null);
const canSavePrompt = computed(() => {
  return Boolean(promptTitle.value.trim() && promptText.value.trim());
});

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

const updateRangeStart = (event) => {
  const value = Number(event.target.value);
  if (Number.isNaN(value)) {
    return;
  }
  if (value > rangeEndDays.value) {
    rangeEndDays.value = value;
  }
  rangeStartDays.value = value;
};

const updateRangeEnd = (event) => {
  const value = Number(event.target.value);
  if (Number.isNaN(value)) {
    return;
  }
  if (value < rangeStartDays.value) {
    rangeStartDays.value = value;
  }
  rangeEndDays.value = value;
};

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

const mergeUniqueById = (existing, incoming) => {
  if (!Array.isArray(incoming) || incoming.length === 0) {
    return existing;
  }
  const map = new Map();
  for (const item of existing) {
    if (!item) {
      continue;
    }
    const id = item.id;
    if (id === null || id === undefined) {
      continue;
    }
    map.set(id, item);
  }
  for (const item of incoming) {
    if (!item) {
      continue;
    }
    const id = item.id;
    if (id === null || id === undefined) {
      continue;
    }
    const current = map.get(id);
    map.set(id, current ? { ...current, ...item } : item);
  }
  return Array.from(map.values());
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
  messages_total: (user) => getUserTotalMessages(user),
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
  if (!reset && (channelLoading.value || !channelHasMore.value)) {
    return;
  }
  const requestId = channelListRequestId.value + 1;
  channelListRequestId.value = requestId;
  channelLoading.value = true;
  if (reset) {
    channels.value = [];
    channelOffset.value = 0;
    channelHasMore.value = true;
    selectedChannelDetailsId.value = null;
    channelDetails.value = null;
  }
  const searchValue = channelSearch.value.trim();
  const params = { offset: channelOffset.value, limit: 30 };
  if (searchValue) {
    params.search = searchValue;
  }
  try {
    const { data } = await api.get("/channels", { params });
    if (channelListRequestId.value !== requestId) {
      return;
    }
    channels.value.push(...data.items);
    if (data.next_offset === null) {
      channelHasMore.value = false;
    } else {
      channelOffset.value = data.next_offset;
    }
  } finally {
    if (channelListRequestId.value === requestId) {
      channelLoading.value = false;
    }
  }
};

const fetchUsers = async (reset = false) => {
  if (!reset && (userLoading.value || !userHasMore.value)) {
    return;
  }
  const requestId = userListRequestId.value + 1;
  userListRequestId.value = requestId;
  userLoading.value = true;
  if (reset) {
    users.value = [];
    userOffset.value = 0;
    userHasMore.value = true;
    selectedUserId.value = null;
    userDetails.value = null;
  }
  const searchValue = userSearch.value.trim();
  const params = { offset: userOffset.value, limit: 30 };
  if (searchValue) {
    params.search = searchValue;
  }
  try {
    const { data } = await api.get("/users", { params });
    if (userListRequestId.value !== requestId) {
      return;
    }
    users.value = mergeUniqueById(users.value, data.items);
    if (data.next_offset === null) {
      userHasMore.value = false;
    } else {
      userOffset.value = data.next_offset;
    }
  } finally {
    if (userListRequestId.value === requestId) {
      userLoading.value = false;
    }
  }
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

const fetchPrompts = async () => {
  promptsLoading.value = true;
  try {
    const { data } = await api.get("/prompts");
    prompts.value = Array.isArray(data) ? data : [];
  } finally {
    promptsLoading.value = false;
  }
};

const resetPromptForm = () => {
  editingPromptId.value = null;
  promptTitle.value = "";
  promptText.value = "";
};

const startEditPrompt = (prompt) => {
  if (!prompt) {
    return;
  }
  editingPromptId.value = prompt.id;
  promptTitle.value = prompt.title || "";
  promptText.value = prompt.text || "";
};

const savePrompt = async () => {
  const title = promptTitle.value.trim();
  const text = promptText.value.trim();
  if (!title || !text) {
    return;
  }
  promptSaving.value = true;
  try {
    if (isEditingPrompt.value) {
      await api.put(`/prompts/${editingPromptId.value}`, { title, text });
    } else {
      await api.post("/prompts", { title, text });
    }
    await fetchPrompts();
    resetPromptForm();
  } finally {
    promptSaving.value = false;
  }
};

const removePrompt = async (promptId) => {
  if (!window.confirm("Удалить промпт?")) {
    return;
  }
  promptDeletingId.value = promptId;
  try {
    await api.delete(`/prompts/${promptId}`);
    await fetchPrompts();
    if (editingPromptId.value === promptId) {
      resetPromptForm();
    }
  } finally {
    if (promptDeletingId.value === promptId) {
      promptDeletingId.value = null;
    }
  }
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

const refreshCache = async () => {
  cacheRefreshing.value = true;
  cacheRefreshResult.value = null;
  const dateFrom = getUtcStartDate(rangeEndDays.value).toISOString();
  const dateTo = getUtcEndDate(rangeStartDays.value).toISOString();
  const payload = {
    date_from: dateFrom,
    date_to: dateTo,
    channel_ids:
      selectedChannelIds.value.length > 0 ? selectedChannelIds.value : null,
  };
  try {
    const { data } = await api.post("/channels/refresh-messages", payload);
    cacheRefreshResult.value = data;
  } finally {
    cacheRefreshing.value = false;
  }
};

const renderMessages = async () => {
  const channelId = selectedChannelIdForMessages.value;
  if (!channelId) {
    renderMessagesError.value = "Выберите один канал для вывода сообщений.";
    renderMessagesResult.value = null;
    return;
  }
  renderMessagesLoading.value = true;
  renderMessagesError.value = null;
  renderMessagesResult.value = null;
  const dateFrom = getUtcStartDate(rangeEndDays.value).toISOString();
  const dateTo = getUtcEndDate(rangeStartDays.value).toISOString();
  const payload = { channel_id: channelId, date_from: dateFrom, date_to: dateTo };
  try {
    const { data } = await api.post("/channels/render-messages", payload);
    renderMessagesResult.value = data;
  } catch (error) {
    const detail = error?.response?.data?.detail;
    renderMessagesError.value = detail || error?.message || "Ошибка запроса.";
  } finally {
    renderMessagesLoading.value = false;
  }
};

const refreshUserStats = async () => {
  userStatsRefreshing.value = true;
  userStatsRefreshResult.value = null;
  try {
    const { data } = await api.post("/users/refresh-message-stats");
    userStatsRefreshResult.value = data;
  } finally {
    userStatsRefreshing.value = false;
  }
};

const dismissCacheRefreshResult = () => {
  cacheRefreshResult.value = null;
};

const dismissRenderMessages = () => {
  renderMessagesResult.value = null;
  renderMessagesError.value = null;
};

const SEARCH_DEBOUNCE_MS = 300;
let channelSearchTimeout;
let userSearchTimeout;

watch(channelSearch, () => {
  if (channelSearchTimeout) {
    clearTimeout(channelSearchTimeout);
  }
  channelSearchTimeout = setTimeout(() => {
    fetchChannels(true);
  }, SEARCH_DEBOUNCE_MS);
});

watch(userSearch, () => {
  if (userSearchTimeout) {
    clearTimeout(userSearchTimeout);
  }
  userSearchTimeout = setTimeout(() => {
    fetchUsers(true);
  }, SEARCH_DEBOUNCE_MS);
});

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
  await fetchPrompts();
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
  background: #f6f8fb;
  min-height: 100vh;
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
  border: 1px solid #e6ecf5;
  border-radius: 8px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  background: #ffffff;
  box-shadow: 0 2px 8px rgba(31, 41, 51, 0.06);
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

.align-right {
  margin-left: auto;
}

.input-with-clear {
  position: relative;
  display: inline-flex;
  align-items: center;
}

.input-with-clear input {
  padding-right: 26px;
}

.clear-button {
  position: absolute;
  right: 6px;
  top: 50%;
  transform: translateY(-50%);
  width: 18px;
  height: 18px;
  border: none;
  border-radius: 50%;
  background: transparent;
  color: #6b7280;
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
  padding: 0;
}

.clear-button:hover {
  color: #1f2933;
}

.analysis-controls {
  align-items: flex-start;
  justify-content: space-between;
}

.analysis-actions {
  margin-left: auto;
  align-self: flex-start;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.range-control {
  flex: 0 1 360px;
  max-width: 360px;
  min-width: 220px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.range-info {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 6px;
  font-size: 12px;
}

.range-label {
  font-weight: 600;
}

.range-date {
  color: #1f2933;
}

.range-days {
  color: #6b7280;
  font-size: 11px;
}

.range-slider {
  position: relative;
  width: 100%;
  height: 28px;
}

.range-input {
  position: absolute;
  left: 0;
  right: 0;
  top: 50%;
  width: 100%;
  margin: 0;
  background: transparent;
  pointer-events: none;
  direction: rtl;
  transform: translateY(-50%);
  -webkit-appearance: none;
  appearance: none;
}

.range-input-start {
  z-index: 2;
}

.range-input-end {
  z-index: 3;
}

.range-input::-webkit-slider-runnable-track {
  height: 4px;
  background: #d9e2ec;
  border-radius: 999px;
}

.range-input::-webkit-slider-thumb {
  -webkit-appearance: none;
  pointer-events: auto;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  background: #2f80ed;
  border: 1px solid #1b4f9a;
}

.range-input::-moz-range-track {
  height: 4px;
  background: #d9e2ec;
  border-radius: 999px;
}

.range-input::-moz-range-thumb {
  pointer-events: auto;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  background: #2f80ed;
  border: 1px solid #1b4f9a;
}

.range-input-end::-webkit-slider-runnable-track {
  background: transparent;
}

.range-input-end::-moz-range-track {
  background: transparent;
}

.range-values {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  font-size: 11px;
  color: #6b7280;
}

.range-scale {
  margin-left: auto;
}

input[type="text"],
input[type="date"] {
  padding: 5px 8px;
  border: 1px solid #cbd2d9;
  border-radius: 4px;
  min-width: 200px;
  font-size: 12px;
}

textarea {
  padding: 5px 8px;
  border: 1px solid #cbd2d9;
  border-radius: 4px;
  font-size: 12px;
  font-family: inherit;
  width: 100%;
  min-height: 120px;
  resize: vertical;
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

.secondary-button {
  background: #ffffff;
  color: #1f2933;
  border: 1px solid #cbd2d9;
}

.secondary-button:hover {
  background: #f1f4f9;
}

.danger-button {
  background: #fbe9e7;
  color: #c0392b;
  border: 1px solid #f4c6c0;
}

.danger-button:hover {
  background: #f6d8d6;
}

.table-container {
  max-height: 380px;
  overflow-y: auto;
  border: 1px solid #e6ecf5;
  border-radius: 6px;
  background: #ffffff;
}

.analysis-table {
  max-height: 260px;
}

.prompt-input {
  width: 100%;
  max-width: 420px;
}

.prompt-input input {
  width: 100%;
}

.prompt-textarea {
  min-height: 140px;
  box-sizing: border-box;
  line-height: 1.45;
}

.prompt-actions {
  align-items: center;
}

.prompt-table {
  max-height: 260px;
  overflow-x: auto;
}

.prompt-text-preview {
  margin: 0;
  padding: 6px 8px;
  border: 1px solid #e6ecf5;
  border-radius: 6px;
  background: #f8fafc;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  word-break: break-word;
  line-height: 1.4;
  font-family: "Courier New", monospace;
  font-size: 11px;
  max-width: 100%;
  box-sizing: border-box;
  max-height: 110px;
  overflow: auto;
}

.prompt-actions-cell {
  display: flex;
  justify-content: flex-end;
  gap: 6px;
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

.cache-refresh {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.cache-refresh-header {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.cache-refresh-header h3 {
  margin: 0;
  font-size: 13px;
}

.cache-refresh-summary {
  font-size: 12px;
  color: #6b7280;
}

.cache-refresh-close {
  margin-left: auto;
  padding: 4px 8px;
  border-radius: 999px;
  border: 1px solid #cbd2d9;
  background: #ffffff;
  color: #6b7280;
  font-size: 11px;
}

.cache-refresh-close:hover {
  background: #f1f4f9;
  color: #1f2933;
}

.cache-refresh-table {
  max-height: 240px;
}

.render-messages {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.render-messages-header {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 8px;
}

.render-messages-header h3 {
  margin: 0;
  font-size: 13px;
}

.render-messages-summary {
  font-size: 12px;
  color: #6b7280;
}

.render-messages-body {
  border: 1px solid #e6ecf5;
  border-radius: 6px;
  max-height: 260px;
  overflow: auto;
  background: #f9fafb;
}

.render-messages-content {
  margin: 0;
  padding: 8px;
  white-space: pre-wrap;
  font-family: "Courier New", monospace;
  font-size: 11px;
}

.user-stats-refresh {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.user-stats-header {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 8px;
}

.user-stats-header h3 {
  margin: 0;
  font-size: 13px;
}

.user-stats-summary {
  font-size: 12px;
  color: #6b7280;
}

.cache-channel-title {
  font-weight: 600;
}

.cache-channel-meta {
  font-size: 10px;
  color: #6b7280;
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
  background: #f1f4f9;
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
  margin-right: 4px;
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
