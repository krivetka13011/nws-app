# Dev-бот и окружение `dev`

Рабочий (прод) воркер: **`nwsnumbot`** — деплой как раньше: `wrangler deploy` из этой папки или push в `main` (GitHub Actions).

Тестовый воркер: **`nwsnumbot-dev`** — отдельные KV и Durable Objects, свой токен бота.

## Один раз

1. В **@BotFather** создайте второго бота, скопируйте **токен**.
2. В каталоге `nws-bot-worker` выполните:
   ```bash
   npx wrangler secret put BOT_TOKEN --env dev
   ```
   Вставьте токен **dev**-бота (не прод!).
3. Деплой dev:
   ```bash
   npx wrangler deploy --env dev
   ```
4. Повесьте webhook на **dev**-бота (подставьте свой токен и при необходимости другой секрет из `wrangler.toml` → `[env.dev.vars]` `WEBHOOK_SECRET`):
   ```text
   https://api.telegram.org/bot<DEV_BOT_TOKEN>/setWebhook?url=https://nwsnumbot-dev.krivetkagames.workers.dev/webhook/nws-dev-webhook-secret
   ```
5. В **BotFather** → dev-бот → **Menu Button / Web App** можно указать ту же страницу GitHub Pages; кнопка «Приложение» из чата dev-бота откроет URL с `?worker=…` (задаётся через `APP_URL` в `[env.dev.vars]`), мини-приложение будет ходить в **dev**-воркер.

## Важно

- **GROUP_ID** в dev сейчас совпадает с продом: тестовые заказы попадут в ту же группу. Для изоляции создайте тестовую супергруппу, пропишите её ID в `wrangler.toml` → `[env.dev.vars]` → `GROUP_ID` и задеплойте снова.
- Счётчик заказов и темы в dev — **отдельные** (свои DO), номера заказов в dev не совпадают с продом.
- Перенос на прод: после проверки в dev — обычный PR/merge и **`wrangler deploy`** без `--env dev` (или push в `main`).

## Ручной деплой только dev (CI)

В репозитории: **Actions → Deploy Worker (dev)** → Run workflow.
