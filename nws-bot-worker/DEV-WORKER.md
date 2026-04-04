# Dev-бот и окружение `dev`

Рабочий (прод) воркер: **`nwsnumbot`** — деплой: `wrangler deploy` или push в `main` (GitHub Actions).

Тестовый воркер: **`nwsnumbot-dev`** — отдельные KV и Durable Objects, свой токен бота.

## Мини-приложение: два URL

| | URL на GitHub Pages | Файл в репо | API |
|---|---------------------|-------------|-----|
| **Прод** | `https://krivetka13011.github.io/nws-app/` | `index.html` | `nwsnumbot` |
| **Dev** | `https://krivetka13011.github.io/nws-app/dev/` | `dev/index.html` | `nwsnumbot-dev` |

У dev отдельные ключи `localStorage` (`nws_history_dev`), чтобы история в браузере не смешивалась с продом.

В **`wrangler.toml`** у прод задаётся `APP_URL` без `/dev/`, у `[env.dev.vars]` — `APP_URL` с `/dev/`, чтобы кнопка «Приложение» у каждого бота открывала свою страницу.

## Перенос на основного бота

- **Воркер:** после проверки — merge в `main` и обычный деплой прод (CI уже деплоит и прод, и dev).
- **Мини-приложение:** переносить содержимое **`dev/index.html` → корневой `index.html`** только когда ты **явно** скажешь, что обновление готово к продакшену. До этого правки только в `dev/`.

## Один раз (dev-бот)

1. Токен dev-бота из **@BotFather**.
2. `npx wrangler secret put BOT_TOKEN --env dev`
3. `npx wrangler deploy --env dev` (или дождаться CI после push).
4. Webhook:
   ```text
   https://api.telegram.org/bot<DEV_BOT_TOKEN>/setWebhook?url=https://nwsnumbot-dev.krivetkagames.workers.dev/webhook/nws-dev-webhook-secret
   ```
5. В **BotFather** у dev-бота Web App URL: **`https://krivetka13011.github.io/nws-app/dev/`** (или с `?uid=` не нужен — `uid` добавляет бот из `APP_URL`).

## Важно

- **GROUP_ID** в dev может совпадать с продом — тесты попадут в ту же группу. Для изоляции — отдельная супергруппа в `[env.dev.vars]`.
- Счётчики заказов и темы в dev изолированы (свои DO).

## Публикация из CI

При push в `main` при изменении `nws-bot-worker/**` workflow **Deploy Worker** деплоит сначала **прод**, затем **dev**. Публикация **статики** Pages — любой push в репозиторий (в т.ч. `dev/index.html`).

Ручной деплой только воркера dev: **Actions → Deploy Worker (dev)**.
