# Deploy via curl (обход wrangler "fetch failed")
# Токен берётся из .env.deploy или CLOUDFLARE_API_TOKEN

$ApiToken = $env:CLOUDFLARE_API_TOKEN
if (-not $ApiToken -and (Test-Path "$PSScriptRoot\.env.deploy")) {
    $ApiToken = (Get-Content "$PSScriptRoot\.env.deploy" -Raw | Select-String -Pattern 'CLOUDFLARE_API_TOKEN=(.+)').Matches.Groups[1].Value.Trim()
}
if (-not $ApiToken) {
    $cfg = "$env:APPDATA\xdg.config\.wrangler\config\default.toml"
    if (Test-Path $cfg) {
        $t = Select-String -Path $cfg -Pattern 'oauth_token\s*=\s*"([^"]+)"'
        if ($t) { $ApiToken = $t.Matches.Groups[1].Value }
    }
}
if (-not $ApiToken) {
    Write-Host "Set CLOUDFLARE_API_TOKEN" -ForegroundColor Yellow
    exit 1
}

$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$metadata = '{"main_module":"index.js","compatibility_date":"2026-03-02","bindings":[{"type":"kv_namespace","name":"CLIENTS","namespace_id":"c638f4e8ab67463c9882857c8b93c063"},{"type":"kv_namespace","name":"ORDERS_KV","namespace_id":"25e034244f544889b450c9993e3c5370"},{"type":"plain_text","name":"MANAGER_ID","text":"1159166497"},{"type":"plain_text","name":"APP_URL","text":"https://krivetka13011.github.io/nws-app/"},{"type":"plain_text","name":"WEBHOOK_SECRET","text":"nws-secret-123"},{"type":"plain_text","name":"GROUP_ID","text":"-1003737384929"},{"type":"plain_text","name":"GENERAL_TOPIC_ID","text":"1"},{"type":"plain_text","name":"IMGBB_KEY","text":"412d2791dd8620104d0849b745f489e0"},{"type":"plain_text","name":"UPLOADME_KEY","text":"chv_g1_56b22d590f2a9504aaafb8b9fac193e11ffc648888dd50f6748e4c29b3f16b3388d63f0c2206070ea985b19272f3328d2b10e0cd7b2a32e9d8fa1c3cf7796325"}]}'

Write-Host "Deploying via curl..." -ForegroundColor Cyan
$outFile = Join-Path $env:TEMP "cf-deploy-result.json"
$null = curl.exe -s -4 --connect-timeout 60 --max-time 600 -o $outFile -X PUT "https://api.cloudflare.com/client/v4/accounts/abd3a9f30b070ba7b27946ecb6b82945/workers/scripts/nwsnumbot" `
    -H "Authorization: Bearer $ApiToken" `
    -F "metadata=$metadata" `
    -F "index.js=@$dir\index.js;type=application/javascript+module"
$result = if (Test-Path $outFile) { Get-Content $outFile -Raw } else { "" }

if ([string]::IsNullOrWhiteSpace($result)) {
    Write-Host "Empty response from curl. Check network, VPN, or token." -ForegroundColor Red
    exit 1
}

try {
    $json = $result | ConvertFrom-Json
} catch {
    Write-Host "Parse error. Raw response:" -ForegroundColor Red
    Write-Host $result
    exit 1
}
if ($json.success) {
    Write-Host "OK: Worker deployed!" -ForegroundColor Green
} else {
    Write-Host "Error:" -ForegroundColor Red
    if ($json.errors -and $json.errors.Count -gt 0) {
        $json.errors | ForEach-Object { Write-Host $_.message }
    } else {
        Write-Host "Full response: $result"
    }
    exit 1
}
