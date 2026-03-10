// NWS Logistics Bot
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400'
};

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS }
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // Health-check
    if (request.method === 'GET' && url.pathname === '/') {
      return new Response('Bot is running', { status: 200, headers: CORS_HEADERS });
    }

    // GET /api/history?userId=12345 — получить историю пользователя
    if (request.method === 'GET' && url.pathname === '/api/history') {
      const userId = url.searchParams.get('userId');
      if (!userId || !env.ORDERS_KV) {
        return jsonResponse({ ok: false, error: 'userId required' }, 400);
      }
      try {
        const raw = await env.ORDERS_KV.get(`history_${userId}`);
        const arr = raw ? JSON.parse(raw) : [];
        return jsonResponse(Array.isArray(arr) ? arr : []);
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 500);
      }
    }

    // POST /api/history — добавить запись в историю
    if (request.method === 'POST' && url.pathname === '/api/history') {
      if (!env.ORDERS_KV) {
        return jsonResponse({ ok: false, error: 'ORDERS_KV not configured' }, 500);
      }
      let body;
      try {
        body = await request.json();
      } catch (_) {
        return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
      }
      const { userId, orderData } = body;
      if (!userId || !orderData) {
        return jsonResponse({ ok: false, error: 'userId and orderData required' }, 400);
      }
      try {
        const key = `history_${userId}`;
        const raw = await env.ORDERS_KV.get(key);
        let arr = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(arr)) arr = [];
        arr.unshift(orderData);
        if (arr.length > 20) arr = arr.slice(0, 20);
        await env.ORDERS_KV.put(key, JSON.stringify(arr));
        return jsonResponse({ ok: true });
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 500);
      }
    }

    // POST /api/create-order — создание заказа через API (Вариант А)
    // Быстрый ответ + фоновая обработка через ctx.waitUntil()
    if (request.method === 'POST' && url.pathname === '/api/create-order') {
      if (!env.CLIENTS) return jsonResponse({ ok: false, error: 'Not configured' }, 500);
      let body;
      try {
        body = await request.json();
      } catch (_) {
        return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
      }
      const { userId, username, firstName = '', lastName = '', items, deliveryType, timestamp } = body;
      if (!userId || !items || !Array.isArray(items) || items.length === 0) {
        return jsonResponse({ ok: false, error: 'userId and items required' }, 400);
      }
      try {
        const chatId = Number(userId);
        const isWhite = /белая|white/i.test(deliveryType || '');
        const user = { id: chatId, username, first_name: firstName, last_name: lastName };

        const totalRub = items.reduce((sum, it) => sum + calcRub(it.price ?? it.resYuan ?? 0, isWhite), 0);
        const totalYuan = items.reduce((sum, it) => {
          const v = Number(it.resYuan ?? it.price ?? 0);
          return sum + (Number.isNaN(v) ? 0 : v);
        }, 0);

        const usernameStr = username ? `@${username}` : (firstName || lastName ? `${(firstName || '').trim()} ${(lastName || '').trim()}`.trim() : 'Клиент');
        const orderNumber = await getAndIncrementOrderCounter(env);
        const deliveryInfo = deliveryType || '';
        const summaryText =
          `  🔥  <b>ЗАКАЗ №${orderNumber} НА ВЫКУП</b>\n` +
          `  👤  Клиент: ${usernameStr}\n` +
          `  💵  Сумма: <b>${totalRub} ₽</b> (${totalYuan.toFixed(2)} ¥)\n` +
          `  📦  Товаров: ${items.length}` +
          (deliveryInfo ? `\n  🚚  Доставка: ${deliveryInfo}` : '');

        const ts = timestamp || Date.now();
        const orderId = `${chatId}_${ts}`;
        const orderData = {
          clientId: chatId,
          orderNumber,
          orderPaid: false,
          deliveryPaid: false,
          deliveryAmount: null,
          summaryText,
          totalRub,
          pinnedMsgId: null,
          createdAt: ts
        };

        // Topic creation, summary, pin — synchronously
        const { topicId } = await getOrCreateTopic(env, chatId, user);
        const groupId = getGroupId(env);
        let dest = topicId
          ? { chat_id: groupId, message_thread_id: topicId }
          : { chat_id: Number(env.MANAGER_ID) };

        const lockAcquired = await acquireOrderLock(env, chatId, orderId);
        if (!lockAcquired) {
          await env.CLIENTS.put(`order_${orderId}`, JSON.stringify(orderData));
          await env.CLIENTS.put(`order_by_num_${orderNumber}`, orderId);
          await env.CLIENTS.put(`pending_job_${orderId}`, JSON.stringify({
            type: 'order', orderId, orderData, items, dest, isWhite, workerUrl: url.origin
          }));
          return jsonResponse({ ok: true, orderId, orderNumber, queued: true });
        }

        const statusText = buildOrderStatus(orderData);
        const keyboard = buildOrderKeyboard(orderId, orderData);

        let sent = await sendWithRetry(env, 'sendMessage', {
          ...dest, text: summaryText + statusText, parse_mode: 'HTML', reply_markup: keyboard
        });
        if (isThreadNotFound(sent)) {
          const newTopicId = await invalidateAndRecreateTopic(env, chatId, user);
          dest = newTopicId
            ? { chat_id: groupId, message_thread_id: newTopicId }
            : { chat_id: Number(env.MANAGER_ID) };
          sent = await sendWithRetry(env, 'sendMessage', {
            ...dest, text: summaryText + statusText, parse_mode: 'HTML', reply_markup: keyboard
          });
        }

        if (sent?.result?.message_id) {
          orderData.pinnedMsgId = sent.result.message_id;
          if (dest.message_thread_id) {
            await callTelegram(env, 'pinChatMessage', {
              chat_id: groupId, message_id: sent.result.message_id, disable_notification: true
            });
          }
        }

        await env.CLIENTS.put(`order_${orderId}`, JSON.stringify(orderData));
        await env.CLIENTS.put(`order_by_num_${orderNumber}`, orderId);
        await addToBroadcastList(env, chatId);

        // Save items for continuation processing
        await env.CLIENTS.put(`pending_items_${orderId}`, JSON.stringify({
          type: 'order', items, dest, isWhite, clientId: chatId,
          orderNumber, orderId, workerUrl: url.origin, nextIdx: 0
        }));

        return jsonResponse({ ok: true, orderId, orderNumber, totalItems: items.length, processed: 0 });
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 500);
      }
    }

    // POST /api/continue-order — process next batch of items
    if (request.method === 'POST' && url.pathname === '/api/continue-order') {
      let body;
      try { body = await request.json(); } catch (_) { return jsonResponse({ error: 'bad json' }, 400); }
      const { orderId } = body;
      if (!orderId) return jsonResponse({ error: 'orderId required' }, 400);

      const raw = await env.CLIENTS.get(`pending_items_${orderId}`);
      if (!raw) return jsonResponse({ ok: true, done: true, processed: 0 });

      const job = JSON.parse(raw);
      const { items, dest, isWhite, clientId, nextIdx = 0 } = job;
      const ITEMS_PER_CALL = 4;
      const end = Math.min(nextIdx + ITEMS_PER_CALL, items.length);

      for (let idx = nextIdx; idx < end; idx++) {
        if (idx > nextIdx) await sleep(80);
        let lastErr;
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            if (job.type === 'order') {
              await sendOrderItem(env, items[idx], idx, dest, isWhite);
            } else {
              await sendSearchItem(env, items[idx], idx, dest);
            }
            lastErr = null;
            break;
          } catch (e) {
            lastErr = e;
            console.error(`continue-order item ${idx} attempt ${attempt + 1}:`, e);
            if (attempt < 2) await sleep(2000);
          }
        }
      }

      if (end < items.length) {
        job.nextIdx = end;
        await env.CLIENTS.put(`pending_items_${orderId}`, JSON.stringify(job));
        return jsonResponse({ ok: true, done: false, processed: end, total: items.length });
      }

      // All items done
      await env.CLIENTS.delete(`pending_items_${orderId}`);
      try {
        if (job.type === 'order') {
          await finishOrder(env, job);
        } else {
          await sendWithRetry(env, 'sendMessage', {
            chat_id: clientId,
            text: '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
            parse_mode: 'HTML'
          });
        }
      } catch (e) {
        console.error('continue-order finish error:', e);
      }
      await releaseOrderLock(env, clientId, job.workerUrl);
      return jsonResponse({ ok: true, done: true, processed: items.length, total: items.length });
    }

    // PUT /api/history — заменить всю историю (для очистки и обновления orderNumber)
    if (request.method === 'PUT' && url.pathname === '/api/history') {
      if (!env.ORDERS_KV) {
        return jsonResponse({ ok: false, error: 'ORDERS_KV not configured' }, 500);
      }
      let body;
      try {
        body = await request.json();
      } catch (_) {
        return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
      }
      const { userId, history } = body;
      if (!userId || !Array.isArray(history)) {
        return jsonResponse({ ok: false, error: 'userId and history array required' }, 400);
      }
      try {
        const key = `history_${userId}`;
        await env.ORDERS_KV.put(key, JSON.stringify(history));
        return jsonResponse({ ok: true });
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 500);
      }
    }

    // Простая проверка отправки сообщений из воркера:
    // GET /test отправляет сообщение менеджеру. Нужна только для диагностики.
    if (request.method === 'GET' && url.pathname === '/test') {
      try {
        await callTelegram(env, 'sendMessage', {
          chat_id: Number(env.MANAGER_ID),
          text: 'Test from Cloudflare Worker: OK'
        });
        return new Response('Test message sent', { status: 200 });
      } catch (e) {
        console.error('Test route error:', e);
        return new Response('Test failed', { status: 500 });
      }
    }

    // Диагностика создания темы: GET /debug-topic?secret=...
    if (request.method === 'GET' && url.pathname === '/debug-topic') {
      const secret = url.searchParams.get('secret');
      if (secret !== env.WEBHOOK_SECRET) return jsonResponse({ ok: false }, 403);
      const res = await callTelegram(env, 'createForumTopic', {
        chat_id: Number(env.GROUP_ID),
        name: 'Тест — удалите'
      });
      return new Response(JSON.stringify(res, null, 2), {
        status: 200, headers: { 'Content-Type': 'application/json' }
      });
    }

    // Диагностика CRM
    if (request.method === 'GET' && url.pathname === '/debug-crm') {
      const groupId = env.GROUP_ID;
      const info = { GROUP_ID: groupId, GROUP_ID_number: Number(groupId), HAS_KV: !!env.CLIENTS };
      try {
        const chatRes = await callTelegram(env, 'getChat', { chat_id: Number(groupId) });
        info.getChat = chatRes;
      } catch (e) { info.getChatError = String(e); }
      try {
        const topicRes = await callTelegram(env, 'createForumTopic', {
          chat_id: Number(groupId), name: 'TEST — удалите эту тему'
        });
        info.createTopic = topicRes;
      } catch (e) { info.createTopicError = String(e); }
      return new Response(JSON.stringify(info, null, 2), {
        status: 200, headers: { 'Content-Type': 'application/json' }
      });
    }

    // Очистка кэша тем (client_*, topic_*)
    // GET /clear-topic-cache?secret=WEBHOOK_SECRET
    // Опционально: ?clientId=123 — очистить только кэш этого клиента
    if (request.method === 'GET' && url.pathname === '/clear-topic-cache') {
      const secret = url.searchParams.get('secret');
      if (secret !== env.WEBHOOK_SECRET || !env.CLIENTS) {
        return jsonResponse({ ok: false }, 403);
      }
      try {
        let deleted = 0;
        let clientId = url.searchParams.get('clientId');
        const topicIdParam = url.searchParams.get('topicId');

        if (topicIdParam) {
          const cid = await env.CLIENTS.get(`topic_${topicIdParam}`);
          if (cid) clientId = cid;
        }
        if (clientId) {
          const key = `client_${clientId}`;
          const stored = await env.CLIENTS.get(key);
          if (stored) {
            try {
              const { topicId } = JSON.parse(stored);
              await env.CLIENTS.delete(`topic_${topicId}`);
            } catch (_) {}
            await env.CLIENTS.delete(key);
            deleted += 2;
          }
          return jsonResponse({ ok: true, deleted });
        }
        try {
          for (const prefix of ['client_', 'topic_']) {
            let list = await env.CLIENTS.list({ prefix });
            do {
              for (const k of list.keys) {
                await env.CLIENTS.delete(k.name);
                deleted++;
              }
              if (list.list_complete) break;
              list = await env.CLIENTS.list({ prefix, cursor: list.cursor });
            } while (true);
          }
          return jsonResponse({ ok: true, deleted });
        } catch (listErr) {
          const msg = String(listErr);
          if (msg.includes('limit exceeded') || msg.includes('KV list')) {
            return jsonResponse({
              ok: false,
              error: msg,
              hint: 'Используйте ?clientId=ID или ?topicId=1480 (число из ссылки t.me/c/3737384929/1480)'
            }, 429);
          }
          throw listErr;
        }
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 500);
      }
    }

    // API списка заказов клиента для WebApp (история)
    if (request.method === 'GET' && (url.pathname === '/orders' || url.pathname === '/api/orders')) {
      const clientId = url.searchParams.get('clientId');
      if (!clientId || !env.CLIENTS) {
        return jsonResponse({ ok: false }, 400);
      }
      try {
        const list = await env.CLIENTS.list({ prefix: `order_${clientId}_` });
        const orders = [];
        for (const key of list.keys) {
          const stored = await env.CLIENTS.get(key.name);
          if (!stored) continue;
          try {
            const od = JSON.parse(stored);
            const orderId = key.name.replace('order_', '');
            orders.push({
              orderId,
              orderNumber: od.orderNumber || null,
              totalRub: od.totalRub || 0,
              orderPaid: od.orderPaid || false,
              deliveryPaid: od.deliveryPaid || false,
              deliveryAmount: od.deliveryAmount || null,
              timestamp: od.createdAt || null
            });
          } catch (_) {}
        }
        return jsonResponse({ ok: true, orders });
      } catch (e) {
        return jsonResponse({ ok: false }, 500);
      }
    }

    // API статуса заказа для WebApp (история)
    if (request.method === 'GET' && (url.pathname === '/order-status' || url.pathname === '/api/order-status')) {
      const orderId = url.searchParams.get('orderId');
      const orderNumber = url.searchParams.get('orderNumber');
      if ((!orderId && !orderNumber) || !env.CLIENTS) {
        return jsonResponse({ ok: false }, 400);
      }
      try {
        let stored = null;
        if (orderNumber) {
          const ref = await env.CLIENTS.get(`order_by_num_${orderNumber}`);
          if (ref) stored = await env.CLIENTS.get(`order_${ref}`);
        }
        if (!stored && orderId) {
          stored = await env.CLIENTS.get(`order_${orderId}`);
        }
        if (!stored) {
          return jsonResponse({ ok: false }, 404);
        }
        const od = JSON.parse(stored);
        return jsonResponse({
          ok: true,
          orderNumber: od.orderNumber || null,
          orderPaid: od.orderPaid || false,
          deliveryPaid: od.deliveryPaid || false,
          deliveryAmount: od.deliveryAmount || null
        });
      } catch (e) {
        return jsonResponse({ ok: false }, 500);
      }
    }

    // POST /api/upload-image — только ImgBB
    if (request.method === 'POST' && url.pathname === '/api/upload-image') {
      const key = env.IMGBB_KEY;
      if (!key) return jsonResponse({ ok: false, error: 'IMGBB_KEY not configured' }, 500);
      try {
        const contentType = request.headers.get('Content-Type') || '';
        if (!contentType.includes('multipart/form-data')) {
          return jsonResponse({ ok: false, error: 'Expected multipart/form-data' }, 400);
        }
        const formData = await request.formData();
        const image = formData.get('image');
        if (!image || !(image instanceof Blob)) {
          return jsonResponse({ ok: false, error: 'No image in form' }, 400);
        }
        const fd = new FormData();
        fd.append('image', image, image.name || 'image.jpg');
        const r = await fetch(`https://api.imgbb.com/1/upload?key=${key}`, { method: 'POST', body: fd });
        const data = await r.json();
        return jsonResponse(data, r.ok ? 200 : 400);
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e.message || e) }, 500);
      }
    }

    // POST /api/delete-images — удаление фото с ImgBB по delete_url
    if (request.method === 'POST' && url.pathname === '/api/delete-images') {
      let body;
      try {
        body = await request.json();
      } catch (_) {
        return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
      }
      const { deleteUrls } = body;
      if (!Array.isArray(deleteUrls) || deleteUrls.length === 0) {
        return jsonResponse({ ok: true });
      }
      const validUrls = deleteUrls.filter(u => typeof u === 'string' && u.includes('ibb.co'));
      for (const delUrl of validUrls) {
        try {
          const m = delUrl.match(/ibb\.co\/([^/]+)\/([^/?#]+)/);
          if (!m) continue;
          const [, imageId, imageHash] = m;
          const fd = new FormData();
          fd.append('pathname', `/${imageId}/${imageHash}`);
          fd.append('action', 'delete');
          fd.append('delete', 'image');
          fd.append('from', 'resource');
          fd.append('deleting[id]', imageId);
          fd.append('deleting[hash]', imageHash);
          await fetch('https://ibb.co/json', { method: 'POST', body: fd });
        } catch (_) { /* ignore per-image errors */ }
      }
      return jsonResponse({ ok: true });
    }

    // Переустановить webhook (включая callback_query)
    if (request.method === 'GET' && url.pathname === '/set-webhook') {
      const webhookUrl = `${url.origin}/webhook/${env.WEBHOOK_SECRET}`;
      const res = await callTelegram(env, 'setWebhook', {
        url: webhookUrl,
        allowed_updates: ['message', 'callback_query']
      });
      return new Response(JSON.stringify(res, null, 2), {
        status: 200, headers: { 'Content-Type': 'application/json' }
      });
    }

    // Webhook: /webhook/<WEBHOOK_SECRET>
    if (
      request.method === 'POST' &&
      url.pathname === `/webhook/${env.WEBHOOK_SECRET}`
    ) {
      let update;
      try {
        update = await request.json();
      } catch (e) {
        return new Response('OK', { status: 200 });
      }
      ctx.waitUntil(handleUpdate(update, env, url.origin).catch(e => console.error('Update error:', e)));
      return new Response('OK', { status: 200 });
    }

    return new Response('Not found', { status: 404, headers: CORS_HEADERS });
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(processPendingOrdersCron(env));
  }
};

async function processPendingOrdersCron(env) {
  if (!env.CLIENTS) return;
  try {
    const list = await env.CLIENTS.list({ prefix: 'pending_items_' });
    if (!list.keys.length) return;
    const key = list.keys[0].name;
    const orderId = key.replace('pending_items_', '');
    const raw = await env.CLIENTS.get(key);
    if (!raw) return;
    const job = JSON.parse(raw);
    const { items, dest, isWhite, clientId, nextIdx = 0 } = job;
    const ITEMS_PER_CALL = 4;
    const end = Math.min(nextIdx + ITEMS_PER_CALL, items.length);

    for (let idx = nextIdx; idx < end; idx++) {
      if (idx > nextIdx) await sleep(500);
      let lastErr;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          if (job.type === 'order') {
            await sendOrderItem(env, items[idx], idx, dest, isWhite);
          } else {
            await sendSearchItem(env, items[idx], idx, dest);
          }
          lastErr = null;
          break;
        } catch (e) {
          lastErr = e;
          console.error('cron item', idx, 'attempt', attempt + 1, e);
          if (attempt < 2) await sleep(2000);
        }
      }
    }

    if (end < items.length) {
      job.nextIdx = end;
      await env.CLIENTS.put(key, JSON.stringify(job));
      return;
    }

    await env.CLIENTS.delete(key);
    try {
      if (job.type === 'order') {
        await finishOrder(env, job);
      } else {
        await sendWithRetry(env, 'sendMessage', {
          chat_id: clientId,
          text: '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
          parse_mode: 'HTML'
        });
      }
    } catch (e) {
      console.error('cron finish error:', e);
    }
    const workerUrl = job.workerUrl || env.WORKER_URL || '';
    await releaseOrderLock(env, clientId, workerUrl);
  } catch (e) {
    console.error('processPendingOrdersCron:', e);
  }
}

// ===== Telegram helpers =====

async function callTelegram(env, method, payload) {
  const res = await fetch(
    `https://api.telegram.org/bot${env.BOT_TOKEN}/${method}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }
  );

  let data = {};
  try {
    data = await res.json();
  } catch (_) {}

  if (!data.ok) {
    console.error('Telegram API error:', method, data);
  }

  return data;
}

async function sendWithRetry(env, method, payload, retries = 5, delayMs = 1500) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const data = await callTelegram(env, method, payload);
      if (data.ok) return data;

      // Non-retryable errors — return immediately
      if (data.error_code === 400) return data;
      if (data.error_code === 403) return data;

      const retryAfter = data.parameters?.retry_after;
      if (retryAfter && attempt < retries - 1) {
        await sleep((retryAfter + 1) * 1000);
        continue;
      }
      if (data.error_code === 429 && attempt < retries - 1) {
        await sleep(3000 * (attempt + 1));
        continue;
      }

      if (attempt === retries - 1) return data;
      await sleep(delayMs * (attempt + 1));
    } catch (e) {
      console.error(`sendWithRetry ${method} attempt ${attempt + 1}/${retries}:`, e);
      if (attempt === retries - 1) throw e;
      await sleep(delayMs * (attempt + 1));
    }
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }


async function sendOrderItem(env, item, idx, dest, isWhite) {
  const resYuan = Number(item.resYuan ?? item.price ?? 0) || 0;
  const resRub = calcRub(item.price ?? resYuan, isWhite);
  const link = item.link || 'Не указана';
  const textMsg =
    `  📍  <b>ТОВАР №${idx + 1}</b>\n` +
    `  🔗  ${link}\n` +
    `  💰  Цена: ${item.price ?? '—'} ¥\n` +
    `  💲  С комиссией: ${resYuan.toFixed(2)} ¥ / ${resRub} ₽`;

  await sendItemMedia(env, item, textMsg, dest);
}

async function sendSearchItem(env, item, idx, dest) {
  const textMsg =
    `  📍  <b>ПОЗИЦИЯ №${idx + 1}</b>\n` +
    `  💬  ${item.comment || 'Без комментария'}`;

  await sendItemMedia(env, item, textMsg, dest);
}

async function sendItemMedia(env, item, textMsg, dest) {
  const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];

  if (!imgUrls.length) {
    await sendWithRetry(env, 'sendMessage', {
      ...dest, text: `${textMsg}\n  📸  Фото: Нет`,
      parse_mode: 'HTML', disable_web_page_preview: true
    });
    return;
  }

  const mediaBatchSize = 10;
  for (let start = 0; start < imgUrls.length; start += mediaBatchSize) {
    if (start > 0) await sleep(50);
    const batch = imgUrls.slice(start, start + mediaBatchSize);

    let sent = await sendMediaGroupWithUpload(env, batch, textMsg, dest);
    if (!sent) {
      await sleep(200);
      sent = await sendMediaGroupWithUpload(env, batch, textMsg, dest);
    }
    if (sent) continue;

    const media = batch.map((url, i) => {
      if (i === 0) return { type: 'photo', media: url, caption: textMsg, parse_mode: 'HTML' };
      return { type: 'photo', media: url };
    });
    const result = await sendWithRetry(env, 'sendMediaGroup', { ...dest, media });
    if (result?.ok) continue;

    if (batch.length === 1) {
      await sendWithRetry(env, 'sendPhoto', {
        ...dest, photo: batch[0], caption: textMsg, parse_mode: 'HTML'
      });
    } else {
      for (let i = 0; i < batch.length; i++) {
        await callTelegram(env, 'sendPhoto', {
          ...dest, photo: batch[i], caption: i === 0 ? textMsg : undefined, parse_mode: 'HTML'
        });
        if (i < batch.length - 1) await sleep(30);
      }
    }
  }
}

async function sendMediaGroupWithUpload(env, imgUrls, caption, dest) {
  const media = [];
  const formData = new FormData();
  formData.append('chat_id', String(dest.chat_id));
  if (dest.message_thread_id) formData.append('message_thread_id', String(dest.message_thread_id));

  for (let i = 0; i < imgUrls.length; i++) {
    try {
      const r = await fetch(imgUrls[i]);
      if (!r.ok) return false;
      const blob = await r.blob();
      const name = `photo${i}`;
      formData.append(name, blob, `${name}.jpg`);
      media.push({
        type: 'photo',
        media: `attach://${name}`,
        ...(i === 0 ? { caption, parse_mode: 'HTML' } : {})
      });
    } catch (_) {
      return false;
    }
  }
  formData.append('media', JSON.stringify(media));

  const res = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMediaGroup`, {
    method: 'POST',
    body: formData
  });
  const data = await res.json().catch(() => ({}));
  return !!data?.ok;
}

async function finishOrder(env, job) {
  const { clientId, orderNumber, orderId } = job;

  await sendWithRetry(env, 'sendMessage', {
    chat_id: clientId,
    text: `✅ Ваш заказ №${orderNumber} успешно принят! Менеджер получил информацию и скоро свяжется с вами или вы можете написать ему самостоятельно прямо в этот чат.`,
    parse_mode: 'HTML'
  });

  const raw = await env.CLIENTS.get(`order_${orderId}`);
  if (!raw) return;
  const orderData = JSON.parse(raw);

  const clientStatusText = buildClientStatusText(orderData);
  const clientStatusSent = await sendWithRetry(env, 'sendMessage', {
    chat_id: clientId,
    text: clientStatusText,
    parse_mode: 'HTML'
  });

  if (clientStatusSent?.result?.message_id) {
    orderData.clientStatusMsgId = clientStatusSent.result.message_id;
    await env.CLIENTS.put(`order_${orderId}`, JSON.stringify(orderData));
  }
}

// ===== Order queue: per-client lock =====

async function acquireOrderLock(env, clientId, orderId) {
  const lockKey = `order_lock_${clientId}`;
  const existing = await env.CLIENTS.get(lockKey);
  if (existing && existing !== orderId) {
    const queueKey = `order_queue_${clientId}`;
    const raw = await env.CLIENTS.get(queueKey);
    const queue = raw ? JSON.parse(raw) : [];
    queue.push(orderId);
    await env.CLIENTS.put(queueKey, JSON.stringify(queue));
    return false;
  }
  // TTL 300s safety net — auto-unlock if stuck
  await env.CLIENTS.put(lockKey, orderId, { expirationTtl: 300 });
  return true;
}

async function releaseOrderLock(env, clientId, workerUrl) {
  const lockKey = `order_lock_${clientId}`;
  await env.CLIENTS.delete(lockKey);

  const queueKey = `order_queue_${clientId}`;
  const raw = await env.CLIENTS.get(queueKey);
  if (!raw) return;

  const queue = JSON.parse(raw);
  if (!queue.length) return;

  const nextOrderId = queue.shift();
  if (queue.length) {
    await env.CLIENTS.put(queueKey, JSON.stringify(queue));
  } else {
    await env.CLIENTS.delete(queueKey);
  }

  // Запускаем следующий заказ из очереди
  const jobRaw = await env.CLIENTS.get(`pending_job_${nextOrderId}`);
  if (!jobRaw) return;

  const job = JSON.parse(jobRaw);
  await env.CLIENTS.delete(`pending_job_${nextOrderId}`);

  await env.CLIENTS.put(`order_lock_${clientId}`, nextOrderId, { expirationTtl: 300 });

  if (job.type === 'order') {
    const { orderId, orderData, items, dest, isWhite } = job;
    const statusText = buildOrderStatus(orderData);
    const keyboard = buildOrderKeyboard(orderId, orderData);

    const sent = await sendWithRetry(env, 'sendMessage', {
      ...dest,
      text: orderData.summaryText + statusText,
      parse_mode: 'HTML',
      reply_markup: keyboard
    });

    if (sent?.result?.message_id) {
      orderData.pinnedMsgId = sent.result.message_id;
      const groupId = getGroupId(env);
      if (groupId && dest.message_thread_id) {
        await callTelegram(env, 'pinChatMessage', {
          chat_id: groupId,
          message_id: sent.result.message_id,
          disable_notification: true
        });
      }
    }

    await env.CLIENTS.put(`order_${orderId}`, JSON.stringify(orderData));
    await env.CLIENTS.put(`order_by_num_${orderData.orderNumber}`, orderId);

    for (let idx = 0; idx < items.length; idx++) {
      if (idx > 0) await sleep(80);
      for (let a = 0; a < 3; a++) {
        try { await sendOrderItem(env, items[idx], idx, dest, isWhite); break; } catch (e) {
          if (a < 2) await sleep(2000);
        }
      }
    }

    try {
      await finishOrder(env, { clientId, orderNumber: orderData.orderNumber, orderId, dest });
    } catch (_) {}
    await releaseOrderLock(env, clientId, workerUrl);
  } else if (job.type === 'search') {
    const { orderId, items, dest, header } = job;

    await sendWithRetry(env, 'sendMessage', { ...dest, text: header, parse_mode: 'HTML' });

    for (let idx = 0; idx < items.length; idx++) {
      if (idx > 0) await sleep(80);
      for (let a = 0; a < 3; a++) {
        try { await sendSearchItem(env, items[idx], idx, dest); break; } catch (e) {
          if (a < 2) await sleep(2000);
        }
      }
    }

    await sendWithRetry(env, 'sendMessage', {
      chat_id: clientId,
      text: '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
      parse_mode: 'HTML'
    });
    await releaseOrderLock(env, clientId, workerUrl);
  }
}

// ===== CRM: группа с темами =====

function getGroupId(env) {
  const g = env.GROUP_ID;
  if (!g || g === '' || g === '0') return null;
  const n = Number(g);
  return Number.isNaN(n) ? null : n;
}

function getGeneralTopicId(env) {
  const t = env.GENERAL_TOPIC_ID;
  if (!t || t === '') return 1;
  const n = Number(t);
  return Number.isNaN(n) ? 1 : n;
}

function clientName(from) {
  if (!from || typeof from !== 'object') return 'Клиент | —';
  const fn = String(from.first_name || '').trim();
  const ln = String(from.last_name || '').trim();
  const fullName = `${fn} ${ln}`.trim() || 'Клиент';
  const name = fullName.replace(/\s+/g, ' ').slice(0, 50);
  const username = from.username ? `@${from.username}` : '—';
  return `${name} | ${username}`;
}

async function getOrCreateTopic(env, clientChatId, from) {
  const groupId = getGroupId(env);
  if (!groupId || !env.CLIENTS) return { topicId: null, created: false };

  const key = `client_${clientChatId}`;
  let stored = await env.CLIENTS.get(key);
  if (stored) {
    try {
      const parsed = JSON.parse(stored);
      const topicId = parsed.topicId;
      const check = await callTelegram(env, 'sendChatAction', {
        chat_id: groupId,
        message_thread_id: topicId,
        action: 'typing'
      });
      if (check && check.ok && !isGeneralTopic(env, topicId)) return { topicId, created: false };
      // Тема удалена/закрыта или это General — инвалидируем кэш
      await env.CLIENTS.delete(`topic_${topicId}`);
    } catch (_) {}
    await env.CLIENTS.delete(key);
  }

  const name = clientName(from || {});
  const res = await callTelegram(env, 'createForumTopic', {
    chat_id: Number(groupId),
    name
  });
  if (!res.ok || !res.result) {
    console.error('createForumTopic failed:', res.error_code, res.description);
    await callTelegram(env, 'sendMessage', {
      chat_id: Number(env.MANAGER_ID),
      text: `⚠️ Не удалось создать тему для клиента ${clientChatId}:\n${res.error_code || ''} ${res.description || ''}\n\nПроверьте: бот — админ с правом «Управление темами», группа — режим форума включён.`
    });
    return { topicId: null, created: false };
  }

  const topicId = res.result.message_thread_id;
  if (topicId === 1 || topicId === getGeneralTopicId(env)) {
    console.error('createForumTopic returned General topic id:', topicId);
    return { topicId: null, created: false };
  }
  await env.CLIENTS.put(key, JSON.stringify({ topicId, name }));
  await env.CLIENTS.put(`topic_${topicId}`, String(clientChatId));
  return { topicId, created: true };
}

function isGeneralTopic(env, topicId) {
  return !topicId || topicId === 1 || topicId === getGeneralTopicId(env);
}

async function invalidateAndRecreateTopic(env, clientChatId, from) {
  const groupId = getGroupId(env);
  if (!groupId || !env.CLIENTS) return null;
  const key = `client_${clientChatId}`;
  const stored = await env.CLIENTS.get(key);
  if (stored) {
    try {
      const { topicId: old } = JSON.parse(stored);
      await env.CLIENTS.delete(`topic_${old}`);
    } catch (_) {}
    await env.CLIENTS.delete(key);
  }
  const name = clientName(from || {});
  const res = await callTelegram(env, 'createForumTopic', { chat_id: Number(groupId), name });
  if (!res.ok || !res.result) return null;
  const topicId = res.result.message_thread_id;
  if (isGeneralTopic(env, topicId)) return null;
  await env.CLIENTS.put(key, JSON.stringify({ topicId, name }));
  await env.CLIENTS.put(`topic_${topicId}`, String(clientChatId));
  return topicId;
}

function isThreadNotFound(result) {
  if (!result || result.ok) return false;
  const d = (result.description || '').toLowerCase();
  return (result.error_code === 400 || result.error_code === 404) &&
    (d.includes('thread not found') || d.includes('message thread not found') ||
     d.includes('forum topic') || d.includes('topic not found'));
}

function sentToGeneral(result, expectedTopicId) {
  if (!result || !result.ok || !expectedTopicId) return false;
  const got = result.result?.message_thread_id;
  return got === 1;
}

async function getClientForTopic(env, topicId) {
  if (!env.CLIENTS) return null;
  const clientId = await env.CLIENTS.get(`topic_${topicId}`);
  return clientId ? Number(clientId) : null;
}

async function sendToTopic(env, topicId, method, payload) {
  const groupId = getGroupId(env);
  if (!groupId || !topicId) return null;
  return callTelegram(env, method, {
    ...payload,
    chat_id: groupId,
    message_thread_id: topicId
  });
}

async function addToBroadcastList(env, chatId) {
  if (!env.CLIENTS) return;
  const key = 'broadcast_users';
  let list = [];
  try {
    const stored = await env.CLIENTS.get(key);
    if (stored) list = JSON.parse(stored);
  } catch (_) {}
  if (!Array.isArray(list)) list = [];
  const id = Number(chatId);
  if (!list.includes(id)) {
    list.push(id);
    await env.CLIENTS.put(key, JSON.stringify(list));
  }
}

function parseBroadcastButtons(text) {
  if (!text || typeof text !== 'string') return { text: text || '', buttons: [] };
  const re = /\[([^\]]+?)\s*\+\s*(https?:\/\/[^\s\]\]]+)\]/g;
  const buttons = [];
  let m;
  while ((m = re.exec(text)) !== null) {
    buttons.push({ text: m[1].trim(), url: m[2].trim() });
  }
  const cleaned = text.replace(re, '').replace(/\n{3,}/g, '\n\n').trim();
  return { text: cleaned, buttons };
}

function buildBroadcastKeyboard(buttons) {
  if (!buttons || !buttons.length) return undefined;
  return {
    inline_keyboard: buttons.map(b => [{ text: b.text, url: b.url }])
  };
}

async function broadcastToAllUsers(env, msg) {
  if (!env.CLIENTS) return;
  let list = [];
  try {
    const stored = await env.CLIENTS.get('broadcast_users');
    if (stored) list = JSON.parse(stored);
  } catch (_) {}
  if (!Array.isArray(list) || list.length === 0) return;

  const rawText = msg.text || msg.caption || '';
  const { text: cleanedText, buttons } = parseBroadcastButtons(rawText);
  const keyboard = buildBroadcastKeyboard(buttons);
  const hasButtons = keyboard && keyboard.inline_keyboard.length > 0;

  let toRemove = [];

  for (const chatId of list) {
    let res;
    if (hasButtons) {
      const opts = { chat_id: chatId, parse_mode: 'HTML', reply_markup: keyboard };
      if (msg.photo && msg.photo.length) {
        const photo = msg.photo[msg.photo.length - 1];
        res = await callTelegram(env, 'sendPhoto', { ...opts, photo: photo.file_id, caption: cleanedText });
      } else if (msg.video) {
        res = await callTelegram(env, 'sendVideo', { ...opts, video: msg.video.file_id, caption: cleanedText });
      } else if (msg.document) {
        res = await callTelegram(env, 'sendDocument', { ...opts, document: msg.document.file_id, caption: cleanedText });
      } else {
        res = await callTelegram(env, 'sendMessage', { ...opts, text: cleanedText });
      }
    } else {
      res = await callTelegram(env, 'copyMessage', {
        chat_id: chatId,
        from_chat_id: msg.chat.id,
        message_id: msg.message_id
      });
    }
    if (res && !res.ok) {
      const d = (res.description || '').toLowerCase();
      if (d.includes('blocked') || d.includes('deactivated') || d.includes('chat not found')) {
        toRemove.push(chatId);
      }
    }
    await new Promise((r) => setTimeout(r, 50));
  }

  if (toRemove.length) {
    list = list.filter((id) => !toRemove.includes(id));
    await env.CLIENTS.put('broadcast_users', JSON.stringify(list));
  }
}

// ===== Order numbering =====

async function getAndIncrementOrderCounter(env) {
  if (!env.CLIENTS) return 1;
  const key = 'order_counter';
  let n = 1;
  try {
    const v = await env.CLIENTS.get(key);
    if (v) n = parseInt(v, 10) || 1;
  } catch (_) {}
  await env.CLIENTS.put(key, String(n + 1));
  return n;
}

// ===== Payment helpers =====

function buildOrderStatus(orderData) {
  const op = orderData.orderPaid ? '🟢' : '🔴';
  const opText = orderData.orderPaid ? 'Оплачен' : 'Не оплачен';

  let deliveryLine;
  if (orderData.deliveryAmount) {
    const dp = orderData.deliveryPaid ? '🟢' : '🔴';
    const dpText = orderData.deliveryPaid ? 'Оплачена' : 'Не оплачена';
    deliveryLine = `${dp} Доставка (${orderData.deliveryAmount} ₽): ${dpText}`;
  } else {
    deliveryLine = '🔴 Доставка: Счёт не выставлен';
  }

  return `\n\n${op} <b>Заказ: ${opText}</b>\n${deliveryLine}`;
}

function buildClientStatusText(orderData) {
  const totalRub = orderData.totalRub || 0;
  const op = orderData.orderPaid ? '🟢' : '🔴';
  const opText = orderData.orderPaid ? 'Оплачен' : 'Не оплачен';

  let lines = `📋 <b>Статус вашего заказа</b>\n\n`;
  lines += `💵 Сумма заказа: <b>${totalRub} ₽</b>\n`;
  lines += `${op} <b>Заказ: ${opText}</b>\n`;

  if (orderData.deliveryAmount) {
    if (orderData.deliveryPaid) {
      lines += `🟢 <b>Доставка: Оплачена</b>`;
    } else {
      lines += `🔴 <b>Доставка: ${orderData.deliveryAmount} ₽ — Не оплачена</b>`;
    }
  } else {
    lines += `⏳ Доставка: Счёт ещё не выставлен`;
  }

  return lines;
}

async function updateClientStatusMsg(env, orderData, kvKey) {
  if (!orderData.clientId) return;
  const text = buildClientStatusText(orderData);

  if (orderData.clientStatusMsgId) {
    const res = await callTelegram(env, 'editMessageText', {
      chat_id: orderData.clientId,
      message_id: orderData.clientStatusMsgId,
      text,
      parse_mode: 'HTML'
    });
    if (res && res.ok) return;
  }

  // Фоллбэк: отправить новое сообщение (для старых заказов без clientStatusMsgId)
  const sent = await callTelegram(env, 'sendMessage', {
    chat_id: orderData.clientId,
    text,
    parse_mode: 'HTML'
  });
  if (sent?.result?.message_id && env.CLIENTS && kvKey) {
    orderData.clientStatusMsgId = sent.result.message_id;
    await env.CLIENTS.put(kvKey, JSON.stringify(orderData));
  }
}

function buildOrderKeyboard(orderId, orderData) {
  const orderBtnText = orderData.orderPaid
    ? '🟢 Заказ: Оплачен'
    : '🔴 Заказ: Не оплачен';

  let delBtnText;
  if (orderData.deliveryAmount) {
    delBtnText = orderData.deliveryPaid
      ? '🟢 Доставка: Оплачена'
      : `🔴 Доставка: ${orderData.deliveryAmount} ₽ — Не оплачена`;
  } else {
    delBtnText = '🔴 Доставка: Счёт не выставлен';
  }

  return {
    inline_keyboard: [
      [{ text: orderBtnText, callback_data: `op_${orderId}` }],
      [{ text: delBtnText, callback_data: `dp_${orderId}` }]
    ]
  };
}

async function handleCallbackQuery(env, query) {
  const data = query.data || '';
  const msg = query.message;
  if (!msg || !env.CLIENTS) {
    await callTelegram(env, 'answerCallbackQuery', { callback_query_id: query.id });
    return;
  }

  const msgId = msg.message_id;
  const chatId = msg.chat.id;
  let orderId;
  let field;

  if (data.startsWith('op_')) {
    orderId = data.slice(3);
    field = 'orderPaid';
  } else if (data.startsWith('dp_')) {
    orderId = data.slice(3);
    field = 'deliveryPaid';
  } else {
    await callTelegram(env, 'answerCallbackQuery', { callback_query_id: query.id });
    return;
  }

  const kvKey = `order_${orderId}`;
  let orderData;
  try {
    const stored = await env.CLIENTS.get(kvKey);
    if (!stored) {
      await callTelegram(env, 'answerCallbackQuery', {
        callback_query_id: query.id,
        text: 'Заказ не найден'
      });
      return;
    }
    orderData = JSON.parse(stored);
  } catch (_) {
    await callTelegram(env, 'answerCallbackQuery', { callback_query_id: query.id });
    return;
  }

  if (field === 'deliveryPaid' && !orderData.deliveryAmount) {
    await callTelegram(env, 'answerCallbackQuery', {
      callback_query_id: query.id,
      text: 'Сначала выставите счёт: /d 1500'
    });
    return;
  }

  orderData[field] = !orderData[field];
  await env.CLIENTS.put(kvKey, JSON.stringify(orderData));

  const newStatus = buildOrderStatus(orderData);
  const newText = orderData.summaryText + newStatus;
  const newKeyboard = buildOrderKeyboard(orderId, orderData);

  await callTelegram(env, 'editMessageText', {
    chat_id: chatId,
    message_id: msgId,
    text: newText,
    parse_mode: 'HTML',
    reply_markup: newKeyboard
  });

  // Обновить статусное сообщение у клиента
  await updateClientStatusMsg(env, orderData, kvKey);

  const answerText = orderData[field] ? '✅ Отмечено как оплачено' : '❌ Отмечено как не оплачено';
  await callTelegram(env, 'answerCallbackQuery', {
    callback_query_id: query.id,
    text: answerText
  });
}

async function handleDeliveryCommand(env, msg, threadId) {
  const match = msg.text.match(/^\/d\s+(\d+)\s+(\d+)/);
  if (!match) {
    await callTelegram(env, 'sendMessage', {
      chat_id: msg.chat.id,
      message_thread_id: threadId,
      text: '⚠️ Формат: /d <номер_заказа> <сумма>\nНапример: /d 1 1500'
    });
    return;
  }

  const orderNum = Number(match[1]);
  const amount = Number(match[2]);
  if (!orderNum || !amount) {
    await callTelegram(env, 'sendMessage', {
      chat_id: msg.chat.id,
      message_thread_id: threadId,
      text: '⚠️ Формат: /d 1 1500 (номер заказа и сумма в рублях)'
    });
    return;
  }

  const clientId = await getClientForTopic(env, threadId);
  if (!clientId) return;

  let foundKey = null;
  let orderData = null;

  const orderIdRef = await env.CLIENTS.get(`order_by_num_${orderNum}`);
  if (orderIdRef) {
    foundKey = `order_${orderIdRef}`;
    const stored = await env.CLIENTS.get(foundKey);
    if (stored) {
      try {
        orderData = JSON.parse(stored);
        if (Number(orderData.clientId) === Number(clientId)) {
          // OK
        } else {
          orderData = null;
          foundKey = null;
        }
      } catch (_) {
        orderData = null;
        foundKey = null;
      }
    }
  }

  if (!foundKey || !orderData) {
    await callTelegram(env, 'sendMessage', {
      chat_id: msg.chat.id,
      message_thread_id: threadId,
      text: `⚠️ Заказ №${orderNum} не найден или не принадлежит этому клиенту`
    });
    return;
  }

  orderData.deliveryAmount = amount;

  const groupId = getGroupId(env);

  // Обновить закреплённое сообщение в группе (добавить кнопку доставки)
  if (orderData.pinnedMsgId && groupId) {
    const newStatus = buildOrderStatus(orderData);
    const newText = orderData.summaryText + newStatus;
    const orderId = foundKey.replace('order_', '');
    const newKeyboard = buildOrderKeyboard(orderId, orderData);

    await callTelegram(env, 'editMessageText', {
      chat_id: groupId,
      message_id: orderData.pinnedMsgId,
      text: newText,
      parse_mode: 'HTML',
      reply_markup: newKeyboard
    });
  }

  await env.CLIENTS.put(foundKey, JSON.stringify(orderData));

  // Обновить статусное сообщение у клиента (если не обновилось — отправит новое)
  await updateClientStatusMsg(env, orderData, foundKey);

  // Отправить клиенту уведомление о выставленном счёте
  await callTelegram(env, 'sendMessage', {
    chat_id: clientId,
    text: `📦 <b>Счёт за доставку: ${amount} ₽</b>\n\nОплатите доставку и сообщите менеджеру.`,
    parse_mode: 'HTML'
  });

  await callTelegram(env, 'sendMessage', {
    chat_id: msg.chat.id,
    message_thread_id: threadId,
    text: `✅ Счёт за доставку ${amount} ₽ отправлен клиенту`
  });
}

// ===== Update router =====

async function handleUpdate(update, env, workerUrl) {
  // Обработка нажатий на inline-кнопки
  if (update.callback_query) {
    await handleCallbackQuery(env, update.callback_query);
    return;
  }

  const msg = update.message;
  if (!msg) return;

  // Удаляем системные уведомления о закреплённых сообщениях
  if (msg.pinned_message) {
    await callTelegram(env, 'deleteMessage', {
      chat_id: msg.chat.id,
      message_id: msg.message_id
    }).catch(() => {});
    return;
  }

  const chatId = msg.chat.id;
  const groupId = getGroupId(env);
  const isFromGroup = msg.chat.type === 'supergroup' && groupId && Number(msg.chat.id) === Number(groupId);

  if (isFromGroup && msg.from && !msg.from.is_bot) {
    const threadId = msg.message_thread_id || null;
    const generalTopicId = getGeneralTopicId(env);

    // General тема (thread_id отсутствует или равен generalTopicId) → рассылка
    if (!threadId || threadId === generalTopicId) {
      await broadcastToAllUsers(env, msg);
      return;
    }

    // Команда /d в теме клиента
    if (msg.text && msg.text.startsWith('/d') && threadId) {
      await handleDeliveryCommand(env, msg, threadId);
      return;
    }

    // Тема клиента → переслать клиенту
    const clientId = await getClientForTopic(env, threadId);
    if (clientId) {
      await forwardManagerReplyToClient(env, msg, clientId);
    }
    return;
  }

  // Игнорируем остальные сообщения из групп
  if (!isFromGroup && msg.chat.type !== 'private') return;

  // /start
  if (msg.text && msg.text.startsWith('/start')) {
    await handleStart(env, chatId, msg.from);
    return;
  }

  // WebApp data
  if (msg.web_app_data && msg.web_app_data.data) {
    let data;
    try {
      data = JSON.parse(msg.web_app_data.data);
    } catch (e) {
      console.error('JSON parse error:', e);
      await callTelegram(env, 'sendMessage', {
        chat_id: chatId,
        text: 'Произошла ошибка при чтении данных из приложения.'
      });
      return;
    }

    try {
      if (data.type === 'calc') {
        await handleCalc(env, chatId, data, msg.from);
      } else if (data.type === 'order') {
        await handleOrder(env, chatId, msg.from, data, workerUrl);
      } else if (data.type === 'search') {
        await handleSearch(env, chatId, msg.from, data, workerUrl);
      }
    } catch (e) {
      console.error('Handle web_app_data error:', e);
      await callTelegram(env, 'sendMessage', {
        chat_id: chatId,
        text: `Произошла ошибка при обработке данных: ${e?.message || String(e)}`
      });
    }
    return;
  }

  // Обычное сообщение от клиента → переслать в тему + реакция OK
  let { topicId } = await getOrCreateTopic(env, chatId, msg.from);
  if (topicId && !isGeneralTopic(env, topicId)) {
    let sent = await forwardClientMessageToTopic(env, msg, topicId);
    const needRetry = (sent && !sent.ok && isThreadNotFound(sent)) ||
      (sent && sent.ok && sentToGeneral(sent, topicId));
    if (needRetry) {
      if (sent?.ok && sent.result?.message_id) {
        await callTelegram(env, 'deleteMessage', {
          chat_id: getGroupId(env),
          message_id: sent.result.message_id
        });
      }
      topicId = await invalidateAndRecreateTopic(env, chatId, msg.from);
      if (topicId && !isGeneralTopic(env, topicId)) {
        await forwardClientMessageToTopic(env, msg, topicId);
      }
    }
  }

  await callTelegram(env, 'setMessageReaction', {
    chat_id: chatId,
    message_id: msg.message_id,
    reaction: [{ type: 'emoji', emoji: '👌' }]
  });
}

async function forwardClientMessageToTopic(env, msg, topicId) {
  const groupId = getGroupId(env);
  if (!groupId || !topicId || isGeneralTopic(env, topicId)) return null;

  const from = msg.from || {};
  const tag = from.username ? `@${from.username}` : clientName(from);
  const opts = { chat_id: groupId, message_thread_id: topicId };

  let res = null;
  if (msg.text) {
    res = await callTelegram(env, 'sendMessage', {
      ...opts,
      text: `${msg.text}\n\n— ${tag}`
    });
  } else if (msg.photo && msg.photo.length) {
    const photo = msg.photo[msg.photo.length - 1];
    const caption = msg.caption ? `${msg.caption}\n\n— ${tag}` : `— ${tag}`;
    res = await callTelegram(env, 'sendPhoto', { ...opts, photo: photo.file_id, caption });
  } else if (msg.document) {
    res = await callTelegram(env, 'sendDocument', {
      ...opts,
      document: msg.document.file_id,
      caption: `— ${tag}`
    });
  } else if (msg.voice) {
    res = await callTelegram(env, 'sendVoice', {
      ...opts,
      voice: msg.voice.file_id,
      caption: `— ${tag}`
    });
  } else if (msg.video) {
    res = await callTelegram(env, 'sendVideo', {
      ...opts,
      video: msg.video.file_id,
      caption: `— ${tag}`
    });
  } else if (msg.sticker) {
    res = await callTelegram(env, 'sendSticker', { ...opts, sticker: msg.sticker.file_id });
  }
  return res;
}

const MANAGER_MSG_PREFIX = 'Сообщение от менеджера:\n\n';

async function forwardManagerReplyToClient(env, msg, clientId) {
  const text = msg.text || msg.caption || '';
  if (msg.text) {
    await callTelegram(env, 'sendMessage', {
      chat_id: clientId,
      text: MANAGER_MSG_PREFIX + text
    });
  } else if (msg.photo && msg.photo.length) {
    const photo = msg.photo[msg.photo.length - 1];
    await callTelegram(env, 'sendPhoto', {
      chat_id: clientId,
      photo: photo.file_id,
      caption: text ? MANAGER_MSG_PREFIX + text : 'Сообщение от менеджера:'
    });
  } else if (msg.document || msg.voice || msg.audio || msg.video) {
    await callTelegram(env, 'sendMessage', {
      chat_id: clientId,
      text: 'Сообщение от менеджера:'
    });
    await callTelegram(env, 'copyMessage', {
      chat_id: clientId,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    });
  } else if (text) {
    await callTelegram(env, 'sendMessage', {
      chat_id: clientId,
      text: MANAGER_MSG_PREFIX + text
    });
  }
}

// ===== Handlers =====

async function handleStart(env, chatId, from) {
  let { topicId } = await getOrCreateTopic(env, chatId, from);
  if (topicId && !isGeneralTopic(env, topicId)) {
    const ping = await callTelegram(env, 'sendMessage', {
      chat_id: getGroupId(env),
      message_thread_id: topicId,
      text: '\u200B'
    });
    const badTopic = (ping && !ping.ok && isThreadNotFound(ping)) || (ping?.ok && sentToGeneral(ping, topicId));
    if (badTopic) {
      if (ping?.ok && ping.result?.message_id) {
        await callTelegram(env, 'deleteMessage', {
          chat_id: getGroupId(env),
          message_id: ping.result.message_id
        });
      }
      await invalidateAndRecreateTopic(env, chatId, from);
    } else if (ping?.ok && ping.result?.message_id) {
      await callTelegram(env, 'deleteMessage', {
        chat_id: getGroupId(env),
        message_id: ping.result.message_id
      });
    }
  }

  const text =
    '🌊  Приветствуем в NWS LOGISTICS!\n\n' +
    ' ⬇️  Используйте кнопку ниже чтобы открыть приложение.\n\n' +
    ' ✅️  В этом телеграм боте вы можете рассчитать стоимость доставки и оформить заказ.\n\n' +
    ' ❗️  Стоимость доставки рассчитывается до Москвы, а далее мы отправим по России, ' +
    'в Беларусь или Казахстан в любой город, любым способом ' +
    '(расчеты смотрите на сайте CDEK или любой другой транспортной компании)\n\n' +
    '👤 Вопросы, чеки оплаты отправляйте в чат бота❗️\n\n' +
    '🔗 Для быстрой работы бота используйте VPN';

  const appUrl = env.APP_URL.includes('?')
    ? `${env.APP_URL}&uid=${chatId}`
    : `${env.APP_URL}?uid=${chatId}`;

  const keyboard = {
    keyboard: [
      [
        {
          text: ' 📦  Приложение',
          web_app: { url: appUrl }
        }
      ]
    ],
    resize_keyboard: true
  };

  await callTelegram(env, 'sendMessage', {
    chat_id: chatId,
    text,
    reply_markup: keyboard
  });

  await addToBroadcastList(env, chatId);
}

async function handleCalc(env, chatId, data, user) {
  const text =
    '  💠  <b>РАСЧЕТ ДОСТАВКИ NWS</b>\n\n' +
    `  📦  Коробка: ${data.boxName}\n` +
    `  📏  Габариты: ${data.l}x${data.w}x${data.h} см\n` +
    `  ⚖️  Вес: ${data.weight || '—'} кг\n` +
    `  🪵  Обрешетка: ${data.hasCrate ? '  ✅  ' : '  ❌  '}\n` +
    `  💰  Предв. итог: ${data.packPrice} ₽`;

  const isCustom = data.boxName === 'Свой размер';
  if (isCustom) {
    const { topicId } = await getOrCreateTopic(env, chatId, user);
    const groupId = getGroupId(env);
    const dest = topicId && groupId
      ? { chat_id: groupId, message_thread_id: topicId }
      : { chat_id: Number(env.MANAGER_ID) };
    const tag = user?.username ? `@${user.username}` : (user?.first_name || user?.last_name ? `${(user.first_name || '').trim()} ${(user.last_name || '').trim()}`.trim() : 'Клиент');
    const msgToManager = text + `\n\n  👤  Клиент: ${tag}\n  ⬇️  Ответьте на это сообщение — стоимость придёт клиенту.`;

    await callTelegram(env, 'sendMessage', {
      ...dest,
      text: msgToManager,
      parse_mode: 'HTML'
    });
    await callTelegram(env, 'sendMessage', {
      chat_id: chatId,
      text: '✅ Запрос на расчёт отправлен менеджеру. Прямо в этом чате вы можете общаться с ним.',
      parse_mode: 'HTML'
    });
  } else {
    await callTelegram(env, 'sendMessage', {
      chat_id: chatId,
      text,
      parse_mode: 'HTML'
    });
  }
}

// Серая: (цена + 30) × 1.05 × 13
// Белая: (цена + 30) × 1.10 × 13
function calcRub(priceYuan, isWhite) {
  const p = Number(priceYuan) || 0;
  const mult = isWhite ? 1.10 : 1.05;
  return Math.ceil((p + 30) * mult * 13);
}

async function handleOrder(env, chatId, user, data, workerUrl) {
  const items = Array.isArray(data.items) ? data.items : [];
  const isWhite = /белая|white/i.test(data.deliveryType || '');
  const { topicId } = await getOrCreateTopic(env, chatId, user);
  const groupId = getGroupId(env);

  const totalRub = items.reduce((sum, it) => sum + calcRub(it.price ?? it.resYuan ?? 0, isWhite), 0);
  const totalYuan = items.reduce((sum, it) => {
    const v = Number(it.resYuan ?? it.price ?? 0);
    return sum + (Number.isNaN(v) ? 0 : v);
  }, 0);

  const username = user?.username ? `@${user.username}` : (user?.first_name || user?.last_name ? `${(user.first_name || '').trim()} ${(user.last_name || '').trim()}`.trim() : 'Клиент');
  const orderNumber = await getAndIncrementOrderCounter(env);
  const deliveryInfo = data.deliveryType || '';
  const summaryText =
    `  🔥  <b>ЗАКАЗ №${orderNumber} НА ВЫКУП</b>\n` +
    `  👤  Клиент: ${username}\n` +
    `  💵  Сумма: <b>${totalRub} ₽</b> (${totalYuan.toFixed(2)} ¥)\n` +
    `  📦  Товаров: ${items.length}` +
    (deliveryInfo ? `\n  🚚  Доставка: ${deliveryInfo}` : '');

  const orderData = {
    clientId: chatId,
    orderNumber,
    orderPaid: false,
    deliveryPaid: false,
    deliveryAmount: null,
    summaryText,
    totalRub,
    pinnedMsgId: null,
    createdAt: Date.now()
  };

  let dest = topicId
    ? { chat_id: groupId, message_thread_id: topicId }
    : { chat_id: Number(env.MANAGER_ID) };

  const orderId = `${chatId}_${data.timestamp || Date.now()}`;

  const lockAcquired = await acquireOrderLock(env, chatId, orderId);
  if (!lockAcquired) {
    await env.CLIENTS.put(`pending_job_${orderId}`, JSON.stringify({
      type: 'order', orderId, orderData, items, dest, isWhite, workerUrl
    }));
    return;
  }

  const statusText = buildOrderStatus(orderData);
  const keyboard = buildOrderKeyboard(orderId, orderData);

  let sent = await sendWithRetry(env, 'sendMessage', {
    ...dest,
    text: summaryText + statusText,
    parse_mode: 'HTML',
    reply_markup: keyboard
  });

  if (isThreadNotFound(sent)) {
    const newTopicId = await invalidateAndRecreateTopic(env, chatId, user);
    dest = newTopicId
      ? { chat_id: groupId, message_thread_id: newTopicId }
      : { chat_id: Number(env.MANAGER_ID) };
    sent = await sendWithRetry(env, 'sendMessage', {
      ...dest,
      text: summaryText + statusText,
      parse_mode: 'HTML',
      reply_markup: keyboard
    });
  }

  if (sent?.result?.message_id) {
    orderData.pinnedMsgId = sent.result.message_id;
    if (dest.message_thread_id) {
      await callTelegram(env, 'pinChatMessage', {
        chat_id: groupId,
        message_id: sent.result.message_id,
        disable_notification: true
      });
    }
  }

  if (env.CLIENTS) {
    await env.CLIENTS.put(`order_${orderId}`, JSON.stringify(orderData));
    await env.CLIENTS.put(`order_by_num_${orderNumber}`, orderId);
  }

  await addToBroadcastList(env, chatId);

  for (let idx = 0; idx < items.length; idx++) {
    if (idx > 0) await sleep(200);
    for (let a = 0; a < 3; a++) {
      try {
        await sendOrderItem(env, items[idx], idx, dest, isWhite);
        break;
      } catch (e) {
        console.error(`handleOrder item ${idx} attempt ${a + 1}:`, e);
        if (a < 2) await sleep(2000);
      }
    }
  }

  try {
    const job = { clientId: chatId, orderNumber, orderId, dest };
    await finishOrder(env, job);
  } catch (e) {
    console.error('handleOrder finishOrder error:', e);
  }

  await releaseOrderLock(env, chatId, workerUrl);
}

async function handleSearch(env, chatId, user, data, workerUrl) {
  const items = Array.isArray(data.items) ? data.items : [];
  if (!items.length) {
    await callTelegram(env, 'sendMessage', { chat_id: chatId, text: 'Заявка на поиск пуста.' });
    return;
  }

  const { topicId } = await getOrCreateTopic(env, chatId, user);
  const groupId = getGroupId(env);
  let dest = topicId
    ? { chat_id: groupId, message_thread_id: topicId }
    : { chat_id: Number(env.MANAGER_ID) };

  const username = user?.username ? `@${user.username}` : (user?.first_name || user?.last_name ? `${(user.first_name || '').trim()} ${(user.last_name || '').trim()}`.trim() : 'Клиент');
  const header =
    '  🔍  <b>НОВАЯ ЗАЯВКА НА ПОИСК</b>\n' +
    `  👤  Клиент: ${username}\n` +
    `  📦  Позиций: ${items.length}`;

  const orderId = `search_${chatId}_${Date.now()}`;

  const lockAcquired = await acquireOrderLock(env, chatId, orderId);
  if (!lockAcquired) {
    await env.CLIENTS.put(`pending_job_${orderId}`, JSON.stringify({
      type: 'search', orderId, items, dest, clientId: chatId, header, workerUrl
    }));
    return;
  }

  let headerSent = await sendWithRetry(env, 'sendMessage', { ...dest, text: header, parse_mode: 'HTML' });
  if (isThreadNotFound(headerSent)) {
    const newTopicId = await invalidateAndRecreateTopic(env, chatId, user);
    dest = newTopicId
      ? { chat_id: groupId, message_thread_id: newTopicId }
      : { chat_id: Number(env.MANAGER_ID) };
    await sendWithRetry(env, 'sendMessage', { ...dest, text: header, parse_mode: 'HTML' });
  }

  for (let idx = 0; idx < items.length; idx++) {
    if (idx > 0) await sleep(200);
    for (let a = 0; a < 3; a++) {
      try {
        await sendSearchItem(env, items[idx], idx, dest);
        break;
      } catch (e) {
        console.error(`handleSearch item ${idx} attempt ${a + 1}:`, e);
        if (a < 2) await sleep(2000);
      }
    }
  }

  await sendWithRetry(env, 'sendMessage', {
    chat_id: chatId,
    text: '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
    parse_mode: 'HTML'
  });

  await releaseOrderLock(env, chatId, workerUrl);
}