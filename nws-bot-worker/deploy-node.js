#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const TOKEN = process.env.CLOUDFLARE_API_TOKEN || 
  (() => { try { return fs.readFileSync('.env.deploy','utf8').match(/CLOUDFLARE_API_TOKEN=(.+)/)?.[1]?.trim(); } catch { return null; }})();

if (!TOKEN) {
  console.error('Set CLOUDFLARE_API_TOKEN');
  process.exit(1);
}

const metadata = JSON.stringify({
  main_module: 'index.js',
  compatibility_date: '2026-03-02',
  bindings: [
    { type: 'kv_namespace', name: 'CLIENTS', namespace_id: 'c638f4e8ab67463c9882857c8b93c063' },
    { type: 'kv_namespace', name: 'ORDERS_KV', namespace_id: '25e034244f544889b450c9993e3c5370' },
    { type: 'plain_text', name: 'MANAGER_ID', text: '1159166497' },
    { type: 'plain_text', name: 'APP_URL', text: 'https://krivetka13011.github.io/nws-app/' },
    { type: 'plain_text', name: 'WEBHOOK_SECRET', text: 'nws-secret-123' },
    { type: 'plain_text', name: 'GROUP_ID', text: '-1003737384929' },
    { type: 'plain_text', name: 'GENERAL_TOPIC_ID', text: '1' }
  ]
});

const script = fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8');
const boundary = '----FormBoundary' + Math.random().toString(36).slice(2);
const body = [
  `--${boundary}`,
  'Content-Disposition: form-data; name="metadata"',
  '',
  metadata,
  `--${boundary}`,
  'Content-Disposition: form-data; name="index.js"; filename="index.js"',
  'Content-Type: application/javascript+module',
  '',
  script,
  `--${boundary}--`
].join('\r\n');

const url = 'https://api.cloudflare.com/client/v4/accounts/abd3a9f30b070ba7b27946ecb6b82945/workers/scripts/nwsnumbot';

fetch(url, {
  method: 'PUT',
  headers: {
    'Authorization': `Bearer ${TOKEN}`,
    'Content-Type': `multipart/form-data; boundary=${boundary}`
  },
  body
}).then(r => r.json()).then(d => {
  if (d.success) console.log('OK: Worker deployed!');
  else { console.error('Error:', d.errors || d); process.exit(1); }
}).catch(e => { console.error(e); process.exit(1); });
