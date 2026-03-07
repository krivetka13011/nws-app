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

    // POST /api/payment-proof — клиент отмечает оплату + скриншот
    if (request.method === 'POST' && url.pathname === '/api/payment-proof') {
      if (!env.CLIENTS) return jsonResponse({ ok: false, error: 'Not configured' }, 500);
      let body;
      try {
        body = await request.json();
      } catch (_) {
        return jsonResponse({ ok: false, error: 'Invalid JSON' }, 400);
      }
      const { clientId, orderNumber, orderId, type, screenshotUrl } = body;
      if (!clientId || !type || !['order', 'delivery'].includes(type)) {
        return jsonResponse({ ok: false, error: 'clientId and type (order|delivery) required' }, 400);
      }
      try {
        let foundKey = null;
        let orderData = null;
        if (orderNumber) {
          const ref = await env.CLIENTS.get(`order_by_num_${orderNumber}`);
          if (ref) {
            foundKey = `order_${ref}`;
            const stored = await env.CLIENTS.get(foundKey);
            if (stored) orderData = JSON.parse(stored);
          }
        }
        if (!orderData && orderId) {
          foundKey = `order_${orderId}`;
          const stored = await env.CLIENTS.get(foundKey);
          if (stored) orderData = JSON.parse(stored);
        }
        if (!orderData || Number(orderData.clientId) !== Number(clientId)) {
          return jsonResponse({ ok: false, error: 'Order not found or access denied' }, 404);
        }
        if (type === 'order') {
          orderData.orderPaid = true;
        } else {
          if (!orderData.deliveryAmount) return jsonResponse({ ok: false, error: 'Delivery amount not set' }, 400);
          orderData.deliveryPaid = true;
        }
        await env.CLIENTS.put(foundKey, JSON.stringify(orderData));

        const groupId = getGroupId(env);
        const topicId = await getOrCreateTopic(env, Number(clientId), { id: clientId, first_name: 'Клиент' });
        if (topicId && groupId) {
          const typeText = type === 'order' ? 'заказа' : 'доставки';
          let text = `✅ Клиент отметил оплату ${typeText} по заказу №${orderData.orderNumber}`;
          if (screenshotUrl) text += `\n\n📎 Скриншот: ${screenshotUrl}`;
          await callTelegram(env, 'sendMessage', {
            chat_id: groupId,
            message_thread_id: topicId,
            text,
            disable_web_page_preview: !screenshotUrl
          });
        }
        await updateClientStatusMsg(env, orderData, foundKey);
        return jsonResponse({ ok: true });
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 500);
      }
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

    // API списка заказов клиента для WebApp (история)
    if (request.method === 'GET' && url.pathname === '/orders') {
      const clientId = url.searchParams.get('clientId');
      if (!clientId || !env.CLIENTS) {
        return jsonResponse({ ok: false }, 400);
      }
      try {
        const list = await env.CLIENTS.list({ prefix: 'order_' });
        const orders = [];
        for (const key of list.keys) {
          const stored = await env.CLIENTS.get(key.name);
          if (!stored) continue;
          try {
            const od = JSON.parse(stored);
            if (Number(od.clientId) === Number(clientId)) {
              const orderId = key.name.replace('order_', '');
              orders.push({
                orderId,
                orderNumber: od.orderNumber || null,
                totalRub: od.totalRub || 0,
                orderPaid: od.orderPaid || false,
                deliveryPaid: od.deliveryPaid || false,
                deliveryAmount: od.deliveryAmount || null
              });
            }
          } catch (_) {}
        }
        return jsonResponse({ ok: true, orders });
      } catch (e) {
        return jsonResponse({ ok: false }, 500);
      }
    }

    // API статуса заказа для WebApp (история)
    if (request.method === 'GET' && url.pathname === '/order-status') {
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
      try {
        const update = await request.json();
        await handleUpdate(update, env);
      } catch (e) {
        console.error('Update error:', e);
      }
      return new Response('OK', { status: 200 });
    }

    return new Response('Not found', { status: 404, headers: CORS_HEADERS });
  }
};

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

async function sendWithRetry(env, method, payload, retries = 3, delayMs = 1000) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await callTelegram(env, method, payload);
    } catch (e) {
      console.error(`Retry ${attempt + 1}/${retries} failed:`, e);
      if (attempt === retries - 1) throw e;
      await new Promise((r) => setTimeout(r, delayMs));
    }
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
  if (!from) return 'Клиент';
  const parts = [];
  if (from.first_name) parts.push(from.first_name);
  if (from.last_name) parts.push(from.last_name);
  const name = parts.length ? parts.join(' ').trim() : 'Клиент';
  const suffix = from.username ? ` @${from.username}` : ` ID: ${from.id}`;
  return `${name} |${suffix}`.slice(0, 128);
}

async function getOrCreateTopic(env, clientChatId, from) {
  const groupId = getGroupId(env);
  if (!groupId || !env.CLIENTS) return null;

  const key = `client_${clientChatId}`;
  let stored = await env.CLIENTS.get(key);
  if (stored) {
    try {
      const { topicId } = JSON.parse(stored);
      return topicId;
    } catch (_) {}
  }

  const name = clientName(from);
  const res = await callTelegram(env, 'createForumTopic', {
    chat_id: groupId,
    name
  });
  if (!res.ok || !res.result) return null;

  const topicId = res.result.message_thread_id;
  await env.CLIENTS.put(key, JSON.stringify({ topicId, name }));
  await env.CLIENTS.put(`topic_${topicId}`, String(clientChatId));
  return topicId;
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

async function broadcastToAllUsers(env, msg) {
  if (!env.CLIENTS) return;
  let list = [];
  try {
    const stored = await env.CLIENTS.get('broadcast_users');
    if (stored) list = JSON.parse(stored);
  } catch (_) {}
  if (!Array.isArray(list) || list.length === 0) return;

  let toRemove = [];

  for (const chatId of list) {
    const res = await callTelegram(env, 'copyMessage', {
      chat_id: chatId,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    });
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

async function handleUpdate(update, env) {
  // Обработка нажатий на inline-кнопки
  if (update.callback_query) {
    await handleCallbackQuery(env, update.callback_query);
    return;
  }

  const msg = update.message;
  if (!msg) return;

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
        await handleCalc(env, chatId, data);
      } else if (data.type === 'order') {
        await handleOrder(env, chatId, msg.from, data);
      } else if (data.type === 'search') {
        await handleSearch(env, chatId, msg.from, data);
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

  // Обычное сообщение от клиента → переслать в тему
  const topicId = await getOrCreateTopic(env, chatId, msg.from);
  if (topicId) {
    await forwardClientMessageToTopic(env, msg, topicId);
  } else {
    // Fallback: отправить менеджеру в личку
    await callTelegram(env, 'forwardMessage', {
      chat_id: Number(env.MANAGER_ID),
      from_chat_id: chatId,
      message_id: msg.message_id
    });
  }
}

async function forwardClientMessageToTopic(env, msg, topicId) {
  const groupId = getGroupId(env);
  if (!groupId) return;

  const from = msg.from || {};
  const tag = from.username ? `@${from.username}` : clientName(from);
  const opts = { chat_id: groupId, message_thread_id: topicId };

  if (msg.text) {
    await callTelegram(env, 'sendMessage', {
      ...opts,
      text: `${msg.text}\n\n— ${tag}`
    });
  } else if (msg.photo && msg.photo.length) {
    const photo = msg.photo[msg.photo.length - 1];
    const caption = msg.caption ? `${msg.caption}\n\n— ${tag}` : `— ${tag}`;
    await callTelegram(env, 'sendPhoto', { ...opts, photo: photo.file_id, caption });
  } else if (msg.document) {
    await callTelegram(env, 'sendDocument', {
      ...opts,
      document: msg.document.file_id,
      caption: `— ${tag}`
    });
  } else if (msg.voice) {
    await callTelegram(env, 'sendVoice', {
      ...opts,
      voice: msg.voice.file_id,
      caption: `— ${tag}`
    });
  } else if (msg.video) {
    await callTelegram(env, 'sendVideo', {
      ...opts,
      video: msg.video.file_id,
      caption: `— ${tag}`
    });
  } else if (msg.sticker) {
    await callTelegram(env, 'sendSticker', { ...opts, sticker: msg.sticker.file_id });
  }
}

async function forwardManagerReplyToClient(env, msg, clientId) {
  const text = msg.text || msg.caption || '';
  if (msg.text) {
    await callTelegram(env, 'sendMessage', {
      chat_id: clientId,
      text: `📩 ${text}`
    });
  } else if (msg.photo && msg.photo.length) {
    const photo = msg.photo[msg.photo.length - 1];
    await callTelegram(env, 'sendPhoto', {
      chat_id: clientId,
      photo: photo.file_id,
      caption: text ? `📩 ${text}` : undefined
    });
  } else if (msg.document || msg.voice || msg.audio || msg.video) {
    await callTelegram(env, 'copyMessage', {
      chat_id: clientId,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    });
  } else if (text) {
    await callTelegram(env, 'sendMessage', {
      chat_id: clientId,
      text: `📩 ${text}`
    });
  }
}

// ===== Handlers =====

async function handleStart(env, chatId, from) {
  const topicId = await getOrCreateTopic(env, chatId, from);

  const text =
    ' 🌊  Приветствуем в NWS LOGISTICS!\n\n' +
    ' ⬇️  Используйте кнопку ниже чтобы открыть приложение.\n\n' +
    ' ✅️  В этом телеграм боте вы можете рассчитать стоимость доставки и оформить заказ.\n\n' +
    ' ❗️  Стоимость доставки рассчитывается до Москвы, а далее мы отправим по России, ' +
    'в Беларусь или Казахстан в любой город, любым способом ' +
    '(расчеты смотрите на сайте Транспортной компании)\n\n' +
    'Связь @Krivetka1301';

  const keyboard = {
    keyboard: [
      [
        {
          text: ' 📦  Приложение',
          web_app: { url: env.APP_URL }
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

async function handleCalc(env, chatId, data) {
  const text =
    '  💠  <b>РАСЧЕТ ДОСТАВКИ NWS</b>\n\n' +
    `  📦  Коробка: ${data.boxName}\n` +
    `  📏  Габариты: ${data.l}x${data.w}x${data.h} см\n` +
    `  🪵  Обрешетка: ${data.hasCrate ? '  ✅  ' : '  ❌  '}\n` +
    `  💰  <b>ИТОГО: ${data.packPrice} ₽</b>`;

  await callTelegram(env, 'sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'HTML'
  });
}

// Серая: (цена + 30) × 1.05 × 13
// Белая: (цена + 30) × 1.10 × 13
function calcRub(priceYuan, isWhite) {
  const p = Number(priceYuan) || 0;
  const mult = isWhite ? 1.10 : 1.05;
  return Math.ceil((p + 30) * mult * 13);
}

async function handleOrder(env, chatId, user, data) {
  const items = Array.isArray(data.items) ? data.items : [];
  const isWhite = /белая|white/i.test(data.deliveryType || '');
  const topicId = await getOrCreateTopic(env, chatId, user);
  const groupId = getGroupId(env);

  const totalRub = items.reduce((sum, it) => sum + calcRub(it.price ?? it.resYuan ?? 0, isWhite), 0);
  const totalYuan = items.reduce((sum, it) => {
    const v = Number(it.resYuan ?? it.price ?? 0);
    return sum + (Number.isNaN(v) ? 0 : v);
  }, 0);

  const username = user?.username ? `@${user.username}` : `ID: ${user?.id}`;

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

  const dest = topicId
    ? { chat_id: groupId, message_thread_id: topicId }
    : { chat_id: Number(env.MANAGER_ID) };

  const orderId = `${chatId}_${data.timestamp || Date.now()}`;
  const statusText = buildOrderStatus(orderData);
  const keyboard = buildOrderKeyboard(orderId, orderData);

  const sent = await sendWithRetry(env, 'sendMessage', {
    ...dest,
    text: summaryText + statusText,
    parse_mode: 'HTML',
    reply_markup: keyboard
  });

  if (sent?.result?.message_id) {
    orderData.pinnedMsgId = sent.result.message_id;

    if (topicId) {
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

  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];
    const resYuan = Number(item.resYuan ?? item.price ?? 0) || 0;
    const resRub = calcRub(item.price ?? resYuan, isWhite);
    const link = item.link || 'Не указана';

    const textMsg =
      `  📍  <b>ТОВАР №${idx + 1}</b>\n` +
      `  🔗  ${link}\n` +
      `  💰  Цена: ${item.price ?? '—'} ¥\n` +
      `  💲  С комиссией: ${resYuan.toFixed(2)} ¥ / ${resRub} ₽`;

    const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];

    if (imgUrls.length) {
      // Фото + текст одним сообщением (caption на первом фото)
      const batchSize = 9;
      for (let start = 0; start < imgUrls.length; start += batchSize) {
        const batch = imgUrls.slice(start, start + batchSize);
        const media = batch.map((url, i) => {
          if (start === 0 && i === 0) {
            return { type: 'photo', media: url, caption: textMsg, parse_mode: 'HTML' };
          }
          return { type: 'photo', media: url };
        });

        try {
          await sendWithRetry(env, 'sendMediaGroup', {
            ...dest,
            media
          });
        } catch (err) {
          console.error('sendMediaGroup error (order):', err);
          await sendWithRetry(env, 'sendMessage', {
            ...dest,
            text: textMsg,
            parse_mode: 'HTML',
            disable_web_page_preview: true
          });
          for (const url of batch) {
            try {
              await sendWithRetry(env, 'sendPhoto', {
                ...dest,
                photo: url
              });
            } catch (photoErr) {
              await callTelegram(env, 'sendMessage', {
                ...dest,
                text: `📸 <a href="${url}">Фото</a>`,
                parse_mode: 'HTML'
              });
            }
          }
        }
      }
    } else {
      await sendWithRetry(env, 'sendMessage', {
        ...dest,
        text: `${textMsg}\n  📸  Фото: Нет`,
        parse_mode: 'HTML',
        disable_web_page_preview: true
      });
    }
  }

  await callTelegram(env, 'sendMessage', {
    chat_id: chatId,
    text:
      `  ✅  <b>Ваш заказ №${orderNumber} успешно принят!</b>\n\n` +
      'Менеджер получил информацию и скоро свяжется с вами или вы можете написать ему самостоятельно @Krivetka1301.',
    parse_mode: 'HTML'
  });

  // Отправить клиенту статусное сообщение (будет обновляться при смене статуса)
  const clientStatusText = buildClientStatusText(orderData);
  const clientStatusSent = await callTelegram(env, 'sendMessage', {
    chat_id: chatId,
    text: clientStatusText,
    parse_mode: 'HTML'
  });

  if (clientStatusSent?.result?.message_id && env.CLIENTS) {
    orderData.clientStatusMsgId = clientStatusSent.result.message_id;
    await env.CLIENTS.put(`order_${orderId}`, JSON.stringify(orderData));
  }
}

async function handleSearch(env, chatId, user, data) {
  const items = Array.isArray(data.items) ? data.items : [];
  if (!items.length) {
    await callTelegram(env, 'sendMessage', {
      chat_id: chatId,
      text: 'Заявка на поиск пуста.'
    });
    return;
  }

  const topicId = await getOrCreateTopic(env, chatId, user);
  const groupId = getGroupId(env);
  const dest = topicId
    ? { chat_id: groupId, message_thread_id: topicId }
    : { chat_id: Number(env.MANAGER_ID) };

  const username = user?.username ? `@${user.username}` : `ID: ${user?.id}`;

  const header =
    '  🔍  <b>НОВАЯ ЗАЯВКА НА ПОИСК</b>\n' +
    `  👤  Клиент: ${username}\n` +
    `  📦  Позиций: ${items.length}`;

  await sendWithRetry(env, 'sendMessage', {
    ...dest,
    text: header,
    parse_mode: 'HTML'
  });

  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];
    const textMsg =
      `  📍  <b>ПОЗИЦИЯ №${idx + 1}</b>\n` +
      `  💬  ${item.comment || 'Без комментария'}`;

    const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];

    if (imgUrls.length) {
      const batchSize = 9;
      for (let start = 0; start < imgUrls.length; start += batchSize) {
        const batch = imgUrls.slice(start, start + batchSize);
        const media = batch.map((url, i) => {
          if (start === 0 && i === 0) {
            return { type: 'photo', media: url, caption: textMsg, parse_mode: 'HTML' };
          }
          return { type: 'photo', media: url };
        });

        try {
          await sendWithRetry(env, 'sendMediaGroup', {
            ...dest,
            media
          });
        } catch (err) {
          console.error('sendMediaGroup error (search):', err);
          await sendWithRetry(env, 'sendMessage', {
            ...dest,
            text: textMsg,
            parse_mode: 'HTML'
          });
          for (const url of batch) {
            try {
              await sendWithRetry(env, 'sendPhoto', {
                ...dest,
                photo: url
              });
            } catch (photoErr) {
              await callTelegram(env, 'sendMessage', {
                ...dest,
                text: `📸 <a href="${url}">Фото</a>`,
                parse_mode: 'HTML'
              });
            }
          }
        }
      }
    } else {
      await sendWithRetry(env, 'sendMessage', {
        ...dest,
        text: `${textMsg}\n  📸  Фото: Нет`,
        parse_mode: 'HTML'
      });
    }
  }

  await callTelegram(env, 'sendMessage', {
    chat_id: chatId,
    text: '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
    parse_mode: 'HTML'
  });
}