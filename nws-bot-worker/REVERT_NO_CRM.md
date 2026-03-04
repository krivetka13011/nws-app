# Откат к версии без CRM

Если CRM с темами работает некорректно, можно вернуться к предыдущей версии.

## Шаги

1. **Замените файлы:**
   ```powershell
   cd nws-bot-worker
   copy index.js.no-crm index.js
   copy wrangler.toml.no-crm wrangler.toml
   ```

2. **Удалите переменную GROUP_ID** из Cloudflare Dashboard (если добавлена там).

3. **Деплой:**
   ```powershell
   npx wrangler deploy
   ```

После отката бот будет отправлять заказы и сообщения только в личку менеджеру (MANAGER_ID).
