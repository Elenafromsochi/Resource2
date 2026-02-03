<template>
  <div class="app">
    <header class="header">
      <h1>Telegram Activity Monitor</h1>
    </header>

    <section class="controls">
      <div class="control-group">
        <label for="channel-input">Добавить канал или группу</label>
        <div class="row">
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
      </div>

      <div class="control-group">
        <label>Анализ активности</label>
        <div class="row">
          <input v-model="analyzeFrom" type="date" />
          <input v-model="analyzeTo" type="date" />
          <button :disabled="analysisLoading" @click="analyzeUsers">
            Запустить анализ
          </button>
        </div>
        <div class="channel-selector">
          <span>Каналы для анализа:</span>
          <label class="checkbox">
            <input
              type="checkbox"
              :checked="selectAllChannels"
              @change="toggleAllChannels"
            />
            Все
          </label>
          <label
            v-for="channel in channelsForSelect"
            :key="channel.id"
            class="checkbox"
          >
            <input
              type="checkbox"
              :value="channel.id"
              v-model="selectedChannelIds"
            />
            {{ channel.title }}
          </label>
        </div>
      </div>
    </section>

    <section class="tables">
      <div class="table-block">
        <h2>Каналы и группы</h2>
        <div class="table-container" @scroll="onChannelsScroll">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Название</th>
                <th>Username</th>
                <th>Тип</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="channel in channels" :key="channel.id">
                <td>{{ channel.id }}</td>
                <td>{{ channel.title }}</td>
                <td>{{ channel.username || "-" }}</td>
                <td>{{ channel.channel_type }}</td>
                <td class="actions">
                  <button @click="removeChannel(channel.id)">Удалить</button>
                </td>
              </tr>
              <tr v-if="channelLoading">
                <td colspan="5" class="muted">Загрузка...</td>
              </tr>
              <tr v-if="!channelLoading && channels.length === 0">
                <td colspan="5" class="muted">Нет каналов</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="table-block">
        <h2>Пользователи</h2>
        <div class="table-container" @scroll="onUsersScroll">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Имя</th>
                <th>Username</th>
                <th>Сообщений</th>
                <th>Обновлено</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="user in users" :key="user.id">
                <td>{{ user.id }}</td>
                <td>{{ formatUserName(user) }}</td>
                <td>{{ user.username || "-" }}</td>
                <td>{{ user.messages_count }}</td>
                <td>{{ formatDate(user.updated_at) }}</td>
              </tr>
              <tr v-if="userLoading">
                <td colspan="5" class="muted">Загрузка...</td>
              </tr>
              <tr v-if="!userLoading && users.length === 0">
                <td colspan="5" class="muted">Нет данных</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import axios from "axios";
import { computed, onMounted, ref } from "vue";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const api = axios.create({ baseURL: API_BASE });

const channels = ref([]);
const channelOffset = ref(0);
const channelHasMore = ref(true);
const channelLoading = ref(false);

const users = ref([]);
const userOffset = ref(0);
const userHasMore = ref(true);
const userLoading = ref(false);

const newChannelValue = ref("");
const channelsForSelect = ref([]);
const selectedChannelIds = ref([]);

const analyzeFrom = ref("");
const analyzeTo = ref("");
const analysisLoading = ref(false);

const selectAllChannels = computed(() => {
  return (
    channelsForSelect.value.length > 0 &&
    selectedChannelIds.value.length === channelsForSelect.value.length
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
};

const importDialogs = async () => {
  await api.post("/channels/import-dialogs");
  await fetchChannels(true);
  await fetchChannelsForSelect();
};

const analyzeUsers = async () => {
  if (!analyzeFrom.value || !analyzeTo.value) {
    return;
  }
  analysisLoading.value = true;
  const dateFrom = new Date(`${analyzeFrom.value}T00:00:00Z`).toISOString();
  const dateTo = new Date(`${analyzeTo.value}T23:59:59Z`).toISOString();
  const payload = {
    date_from: dateFrom,
    date_to: dateTo,
    channel_ids:
      selectedChannelIds.value.length > 0 ? selectedChannelIds.value : null,
  };
  await api.post("/users/analyze", payload);
  await fetchUsers(true);
  analysisLoading.value = false;
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

const onUsersScroll = (event) => {
  const target = event.target;
  if (target.scrollTop + target.clientHeight >= target.scrollHeight - 40) {
    fetchUsers();
  }
};

const formatUserName = (user) => {
  const parts = [user.first_name, user.last_name].filter(Boolean);
  return parts.length ? parts.join(" ") : "-";
};

const formatDate = (value) => {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString();
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
  color: #1f2933;
}

.header {
  margin-bottom: 16px;
}

.controls {
  display: grid;
  gap: 16px;
  margin-bottom: 24px;
}

.control-group label {
  display: block;
  font-weight: 600;
  margin-bottom: 8px;
}

.row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

input[type="text"],
input[type="date"] {
  padding: 6px 8px;
  border: 1px solid #cbd2d9;
  border-radius: 4px;
  min-width: 220px;
}

button {
  padding: 6px 12px;
  border: none;
  border-radius: 4px;
  background: #2f80ed;
  color: #fff;
  cursor: pointer;
}

button:disabled {
  background: #b0b7c3;
  cursor: not-allowed;
}

.channel-selector {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: center;
  margin-top: 8px;
}

.checkbox {
  display: flex;
  gap: 4px;
  align-items: center;
  font-size: 12px;
}

.tables {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 20px;
}

.table-block h2 {
  margin-bottom: 8px;
  font-size: 16px;
}

.table-container {
  max-height: 420px;
  overflow-y: auto;
  border: 1px solid #e4e7eb;
  border-radius: 6px;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

th,
td {
  padding: 6px 8px;
  border-bottom: 1px solid #f0f2f5;
  text-align: left;
}

th {
  background: #f5f7fa;
  position: sticky;
  top: 0;
  z-index: 1;
}

.actions button {
  background: #e74c3c;
}

.muted {
  text-align: center;
  color: #8c94a1;
}
</style>
