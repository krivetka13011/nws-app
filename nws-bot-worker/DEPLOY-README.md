# Деплой Worker при ошибке "fetch failed"

## Проблема
`npx wrangler deploy` падает с `fetch failed` — часто из-за VPN, firewall или провайдера.

## Решения

### 1. Отключить VPN
Если включен VPN — отключите и повторите `npx wrangler deploy`.

### 2. Другой DNS
Попробуйте DNS 1.1.1.1 (Cloudflare) или 8.8.8.8 (Google).

### 3. Деплой через REST API (curl)

**Создайте API Token:**
1. https://dash.cloudflare.com/profile/api-tokens
2. Create Token → Edit Cloudflare Workers
3. Или Custom: права **Workers Scripts: Edit**, **Workers KV Storage: Edit**

**Запуск:**
```powershell
cd "c:\Users\User\Downloads\tg bot cursor\nws-bot-worker"
$env:CLOUDFLARE_API_TOKEN = "ваш_токен"
.\deploy-curl.ps1
```

### 4. Деплой с другого компьютера/сети
Скопируйте папку `nws-bot-worker` на другой ПК или используйте мобильный интернет и выполните `npx wrangler deploy` там.

### 5. Cloudflare Dashboard
Зайдите в https://dash.cloudflare.com → Workers & Pages → nwsnumbot → Quick Edit. Вставьте код из `index.js` и Save and Deploy.  
**Важно:** в Settings добавьте KV namespaces (CLIENTS, ORDERS_KV) и переменные (MANAGER_ID, APP_URL и т.д.).
