export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Health-check
    if (request.method === 'GET' && url.pathname === '/') {
      return new Response('Bot is running', { status: 200 });
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

    return new Response('Not found', { status: 404 });
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

// ===== Update router =====

async function handleUpdate(update, env) {
  const msg = update.message;
  if (!msg) return;

  const chatId = msg.chat.id;
  const isPrivate = msg.chat.type === 'private';
  const isGroupTopic = msg.chat.type === 'supergroup' && msg.message_thread_id && msg.is_topic_message;
  const groupId = getGroupId(env);

  // Сообщение из группы (ответ менеджера в теме) → переслать клиенту
  if (isGroupTopic && groupId && Number(msg.chat.id) === Number(groupId)) {
    const clientId = await getClientForTopic(env, msg.message_thread_id);
    if (clientId && msg.from && !msg.from.is_bot) {
      await forwardManagerReplyToClient(env, msg, clientId);
    }
    return;
  }

  // Только личные сообщения от клиентов
  if (!isPrivate) return;

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

  const opts = { chat_id: groupId, message_thread_id: topicId };

  if (msg.text) {
    await callTelegram(env, 'sendMessage', {
      ...opts,
      text: `💬 *От клиента:*\n\n${msg.text}`,
      parse_mode: 'Markdown'
    });
  } else if (msg.photo && msg.photo.length) {
    const photo = msg.photo[msg.photo.length - 1];
    const caption = msg.caption ? `💬 От клиента: ${msg.caption}` : '💬 Фото от клиента';
    await callTelegram(env, 'sendPhoto', {
      ...opts,
      photo: photo.file_id,
      caption
    });
  } else if (msg.document) {
    await callTelegram(env, 'forwardMessage', {
      ...opts,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    });
  } else if (msg.voice) {
    await callTelegram(env, 'forwardMessage', {
      ...opts,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    });
  } else {
    await callTelegram(env, 'forwardMessage', {
      ...opts,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    });
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

  if (topicId) {
    await sendToTopic(env, topicId, 'sendMessage', {
      text: `🆕 Новый клиент написал /start\n${clientName(from)}`,
      parse_mode: 'HTML'
    });
  }
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

  const deliveryInfo = data.deliveryType || '';
  const summary =
    '  🔥  <b>НОВЫЙ ЗАКАЗ НА ВЫКУП</b>\n' +
    `  👤  Клиент: ${username}\n` +
    `  💵  Сумма: <b>${totalRub} ₽</b> (${totalYuan.toFixed(2)} ¥)\n` +
    `  📦  Товаров: ${items.length}` +
    (deliveryInfo ? `\n  🚚  Доставка: ${deliveryInfo}` : '') +
    '\n\n  🔴 <i>Не оплачен</i>';

  const dest = topicId
    ? { chat_id: groupId, message_thread_id: topicId }
    : { chat_id: Number(env.MANAGER_ID) };

  const sent = await sendWithRetry(env, 'sendMessage', {
    ...dest,
    text: summary,
    parse_mode: 'HTML'
  });

  if (topicId && sent?.result?.message_id) {
    await callTelegram(env, 'pinChatMessage', {
      chat_id: groupId,
      message_id: sent.result.message_id,
      disable_notification: true
    });
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
      '  ✅  <b>Ваш заказ успешно принят!</b>\n\n' +
      'Менеджер получил информацию и скоро свяжется с вами или вы можете написать ему самостоятельно @Krivetka1301.',
    parse_mode: 'HTML'
  });
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