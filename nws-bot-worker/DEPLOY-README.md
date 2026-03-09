# Деплой Worker

## R2 (фото без лимитов)
Чтобы убрать лимиты ImgBB, включите R2 и запустите:
```powershell
.\setup-r2-and-deploy.ps1
```
Скрипт откроет Dashboard для включения R2 (если нужно), создаст bucket и задеплоит.

## Токен
Токен хранится в `.env.deploy` (не в git). Для деплоя используется он.

## Способы деплоя

### 1. GitHub Actions (рекомендуется при проблемах с сетью)

1. Добавьте секрет в репозиторий:
   - https://github.com/krivetka13011/nws-app/settings/secrets/actions
   - New repository secret
   - Name: `CLOUDFLARE_API_TOKEN`
   - Value: токен из `.env.deploy`

2. Запуск: Actions → Deploy Worker → Run workflow  
   Или при push в `nws-bot-worker/index.js` деплой запустится автоматически.

### 2. Локально: deploy-curl.ps1

```powershell
cd "c:\Users\User\Downloads\tg bot cursor\nws-bot-worker"
.\deploy-curl.ps1
```

Токен берётся из `.env.deploy` или `$env:CLOUDFLARE_API_TOKEN`.

### 3. wrangler deploy

```powershell
cd "c:\Users\User\Downloads\tg bot cursor\nws-bot-worker"
$env:CLOUDFLARE_API_TOKEN = (Get-Content .env.deploy | Select-String "CLOUDFLARE_API_TOKEN=(.+)" | ForEach-Object { $_.Matches.Groups[1].Value })
npx wrangler deploy
```

### 4. При ошибке "fetch failed"
Отключите VPN, смените DNS на 1.1.1.1, попробуйте мобильный интернет или используйте GitHub Actions.
