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

// ===== Update router =====

async function handleUpdate(update, env) {
  const msg = update.message;
  if (!msg) return;

  const chatId = msg.chat.id;

  // /start
  if (msg.text && msg.text.startsWith('/start')) {
    await handleStart(env, chatId);
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
  }
}

// ===== Handlers =====

async function handleStart(env, chatId) {
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

async function handleOrder(env, chatId, user, data) {
  const items = Array.isArray(data.items) ? data.items : [];
  const totalYuan = items.reduce((sum, it) => {
    const v = Number(it.resYuan || 0);
    return sum + (Number.isNaN(v) ? 0 : v);
  }, 0);
  const totalRub = Math.ceil(totalYuan * 13);

  const username = user?.username ? `@${user.username}` : `ID: ${user?.id}`;

  const deliveryInfo = data.deliveryType || '';
  const summary =
    '  🔥  <b>НОВЫЙ ЗАКАЗ НА ВЫКУП</b>\n' +
    `  👤  Клиент: ${username}\n` +
    `  💵  Сумма: <b>${totalRub} ₽</b> (${totalYuan.toFixed(2)} ¥)\n` +
    `  📦  Товаров: ${items.length}` +
    (deliveryInfo ? `\n  🚚  Доставка: ${deliveryInfo}` : '');

  await sendWithRetry(env, 'sendMessage', {
    chat_id: Number(env.MANAGER_ID),
    text: summary,
    parse_mode: 'HTML'
  });

  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];
    const resYuan = Number(item.resYuan ?? item.price ?? 0) || 0;
    const resRub = Math.ceil(resYuan * 13);
    const link = item.link || 'Не указана';

    const textMsg =
      `  📍  <b>ТОВАР №${idx + 1}</b>\n` +
      `  🔗  ${link}\n` +
      `  💰  Цена: ${item.price ?? '—'} ¥\n` +
      `  💲  С комиссией: ${resYuan.toFixed(2)} ¥ / ${resRub} ₽`;

    // 1. Всегда отправляем текст с информацией о товаре
    await sendWithRetry(env, 'sendMessage', {
      chat_id: Number(env.MANAGER_ID),
      text: textMsg,
      parse_mode: 'HTML',
      disable_web_page_preview: true
    });

    // 2. Отправляем фото (если есть)
    const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];
    if (imgUrls.length) {
      const batchSize = 9;
      for (let start = 0; start < imgUrls.length; start += batchSize) {
        const batch = imgUrls.slice(start, start + batchSize);
        const media = batch.map((url) => ({ type: 'photo', media: url }));

        try {
          await sendWithRetry(env, 'sendMediaGroup', {
            chat_id: Number(env.MANAGER_ID),
            media
          });
        } catch (err) {
          console.error('sendMediaGroup error (order):', err);
          for (const url of batch) {
            try {
              await sendWithRetry(env, 'sendPhoto', {
                chat_id: Number(env.MANAGER_ID),
                photo: url
              });
            } catch (photoErr) {
              console.error('sendPhoto error (order):', photoErr);
              await callTelegram(env, 'sendMessage', {
                chat_id: Number(env.MANAGER_ID),
                text: `📸 <a href="${url}">Фото товара №${idx + 1}</a>`,
                parse_mode: 'HTML'
              });
            }
          }
        }
      }
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

  const username = user?.username ? `@${user.username}` : `ID: ${user?.id}`;

  const header =
    '  🔍  <b>НОВАЯ ЗАЯВКА НА ПОИСК</b>\n' +
    `  👤  Клиент: ${username}\n` +
    `  📦  Позиций: ${items.length}`;

  await sendWithRetry(env, 'sendMessage', {
    chat_id: Number(env.MANAGER_ID),
    text: header,
    parse_mode: 'HTML'
  });

  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];
    const textMsg =
      `  📍  <b>ПОЗИЦИЯ №${idx + 1}</b>\n` +
      `  💬  ${item.comment || 'Без комментария'}`;

    // 1. Всегда отправляем текст
    await sendWithRetry(env, 'sendMessage', {
      chat_id: Number(env.MANAGER_ID),
      text: textMsg,
      parse_mode: 'HTML'
    });

    // 2. Отправляем фото (если есть)
    const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];
    if (imgUrls.length) {
      const batchSize = 9;
      for (let start = 0; start < imgUrls.length; start += batchSize) {
        const batch = imgUrls.slice(start, start + batchSize);
        const media = batch.map((url) => ({ type: 'photo', media: url }));

        try {
          await sendWithRetry(env, 'sendMediaGroup', {
            chat_id: Number(env.MANAGER_ID),
            media
          });
        } catch (err) {
          console.error('sendMediaGroup error (search):', err);
          for (const url of batch) {
            try {
              await sendWithRetry(env, 'sendPhoto', {
                chat_id: Number(env.MANAGER_ID),
                photo: url
              });
            } catch (photoErr) {
              await callTelegram(env, 'sendMessage', {
                chat_id: Number(env.MANAGER_ID),
                text: `📸 <a href="${url}">Фото позиции №${idx + 1}</a>`,
                parse_mode: 'HTML'
              });
            }
          }
        }
      }
    }
  }

  await callTelegram(env, 'sendMessage', {
    chat_id: chatId,
    text: '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
    parse_mode: 'HTML'
  });
}