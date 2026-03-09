# Настройка автодеплоя (один раз)

## Что уже сделано
- Git upstream настроен (`main` → `origin/main`)
- `git push` работает без дополнительных флагов
- GitHub Actions: при каждом push в `nws-bot-worker/index.js` или `wrangler.toml` запускается деплой

## Одноразовая настройка

### 1. Токены в GitHub Secrets
1. Откройте https://github.com/krivetka13011/nws-app/settings/secrets/actions
2. **CLOUDFLARE_API_TOKEN** — API-токен Cloudflare (шаблон "Edit Cloudflare Workers")
3. **BOT_TOKEN** — токен Telegram-бота (для уведомления "Деплой прошёл успешно" в личку менеджеру)

### 2. Дальнейшие деплои
После настройки токена:
- **Автоматически:** при каждом `git push` (если менялись файлы в `nws-bot-worker/`)
- **Вручную:** GitHub → Actions → Deploy Worker → Run workflow

### 3. Локальный push
Запустите `git-push.bat` или:
```cmd
cd "c:\Users\User\Downloads\tg bot cursor"
git add -A
git commit -m "Update"
git push
```
