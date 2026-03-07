# Deploy Worker via Cloudflare REST API (обход wrangler "fetch failed")
# Требуется: API Token - https://dash.cloudflare.com/profile/api-tokens
# Права: Account - Workers Scripts: Edit, Workers KV Storage: Edit

$ErrorActionPreference = "Stop"
$AccountId = "abd3a9f30b070ba7b27946ecb6b82945"
$ScriptName = "nwsnumbot"
$ApiToken = $env:CLOUDFLARE_API_TOKEN

if (-not $ApiToken) {
    $ConfigPath = "$env:APPDATA\xdg.config\.wrangler\config\default.toml"
    if (Test-Path $ConfigPath) {
        $content = Get-Content $ConfigPath -Raw
        if ($content -match 'oauth_token\s*=\s*"([^"]+)"') {
            $ApiToken = $Matches[1]
            Write-Host "Using token from wrangler config" -ForegroundColor Gray
        }
    }
}

if (-not $ApiToken) {
    Write-Host "Set CLOUDFLARE_API_TOKEN or create API Token:" -ForegroundColor Yellow
    Write-Host "  https://dash.cloudflare.com/profile/api-tokens" -ForegroundColor Cyan
    Write-Host "  Permissions: Workers Scripts Edit, Workers KV Storage Edit" -ForegroundColor Gray
    exit 1
}

$ScriptPath = Join-Path $PSScriptRoot "index.js"
$metadata = '{"main_module":"index.js","compatibility_date":"2026-03-02","bindings":[{"type":"kv_namespace","name":"CLIENTS","namespace_id":"c638f4e8ab67463c9882857c8b93c063"},{"type":"kv_namespace","name":"ORDERS_KV","namespace_id":"25e034244f544889b450c9993e3c5370"},{"type":"plain_text","name":"MANAGER_ID","text":"1159166497"},{"type":"plain_text","name":"APP_URL","text":"https://krivetka13011.github.io/nws-app/"},{"type":"plain_text","name":"WEBHOOK_SECRET","text":"nws-secret-123"},{"type":"plain_text","name":"GROUP_ID","text":"-1003737384929"},{"type":"plain_text","name":"GENERAL_TOPIC_ID","text":"1"}]}'

$uri = "https://api.cloudflare.com/client/v4/accounts/$AccountId/workers/scripts/$ScriptName"

Write-Host "Uploading Worker via REST API..." -ForegroundColor Cyan
try {
    if ($PSVersionTable.PSVersion.Major -ge 7) {
        $form = @{
            metadata = $metadata
            "index.js" = Get-Item -Path $ScriptPath
        }
        $response = Invoke-RestMethod -Uri $uri -Method Put -Headers @{"Authorization" = "Bearer $ApiToken"} -Form $form
    } else {
        $boundary = [System.Guid]::NewGuid().ToString()
        $LF = "`r`n"
        $fileBytes = [System.IO.File]::ReadAllBytes($ScriptPath)
        $fileEnc = [System.Text.Encoding]::GetEncoding("ISO-8859-1").GetString($fileBytes)
        $body = @(
            "--$boundary",
            "Content-Disposition: form-data; name=`"metadata`"$LF",
            $metadata,
            "--$boundary",
            "Content-Disposition: form-data; name=`"index.js`"; filename=`"index.js`"",
            "Content-Type: application/javascript+module$LF",
            $fileEnc,
            "--$boundary--$LF"
        ) -join $LF
        $response = Invoke-RestMethod -Uri $uri -Method Put -Headers @{"Authorization" = "Bearer $ApiToken"} -ContentType "multipart/form-data; boundary=$boundary" -Body $body
    }
    Write-Host "OK: Worker deployed!" -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) { Write-Host $_.ErrorDetails.Message }
    exit 1
}
