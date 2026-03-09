# Настройка R2 и деплой Worker
# 1. Включает R2 (если ещё не включён) — откроет браузер
# 2. Создаёт bucket nws-images
# 3. Деплоит Worker

$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $dir

Write-Host "=== NWS Worker: R2 + Deploy ===" -ForegroundColor Cyan

# Шаг 1: Создать bucket
Write-Host "`n1. Creating R2 bucket nws-images..." -ForegroundColor Yellow
$create = npx wrangler r2 bucket create nws-images 2>&1
if ($LASTEXITCODE -ne 0) {
    if ($create -match "enable R2|10042") {
        Write-Host "R2 not enabled. Opening Cloudflare Dashboard..." -ForegroundColor Yellow
        Start-Process "https://dash.cloudflare.com/abd3a9f30b070ba7b27946ecb6b82945/r2/overview"
        Write-Host "Enable R2 in Dashboard (Enable R2 or Get started), then press Enter." -ForegroundColor Cyan
        Read-Host
        $create = npx wrangler r2 bucket create nws-images 2>&1
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: $create" -ForegroundColor Red
        exit 1
    }
}
Write-Host "OK: Bucket created" -ForegroundColor Green

# Шаг 2: Deploy (с R2)
Write-Host "`n2. Deploying Worker (with R2)..." -ForegroundColor Yellow
npx wrangler deploy --config wrangler.r2.toml
if ($LASTEXITCODE -ne 0) {
    Write-Host "Try: .\deploy-curl.ps1 (if wrangler fetch failed)" -ForegroundColor Yellow
    exit 1
}
Write-Host "`nDone! Photos now stored in R2 (no limits)." -ForegroundColor Green
