## Telegram Activity Monitor

Минимальный стек: FastAPI, Telethon, AsyncPG, PostgreSQL, Vue, Docker Compose.

### Переменные окружения

`.env` содержит секреты и параметры подключения.

Обязательные переменные:

- `POSTGRES_URL`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELETHON_SESSION`
- `DEEPSEEK_API_KEY`

Пример `.env.example` содержит Telegram/DeepSeek ключи.

### Запуск через Docker Compose

```bash
docker compose up --build
```

- Backend: http://localhost:8000
- Frontend: http://localhost:5173

### Основные API

- `POST /api/channels` — добавить канал по username или ссылке
- `DELETE /api/channels/{id}` — удалить канал
- `GET /api/channels` — список каналов с пагинацией
- `POST /api/channels/import-dialogs` — импорт из диалогов
- `POST /api/users/analyze` — анализ активности пользователей
- `GET /api/users` — список пользователей с пагинацией
