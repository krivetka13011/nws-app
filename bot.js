'use strict';

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const TelegramBot = require('node-telegram-bot-api');

const API_TOKEN = process.env.BOT_TOKEN;
const APP_URL = process.env.APP_URL || 'https://krivetka13011.github.io/nws-app/';
const MANAGER_ID = Number(process.env.MANAGER_ID) || 1159166497;

// Файл для хранения состояний заказов (оплата заказа и доставки)
const ORDERS_FILE = path.join(__dirname, 'orders-state.json');

function loadOrders() {
  try {
    if (!fs.existsSync(ORDERS_FILE)) {
      return {};
    }
    const raw = fs.readFileSync(ORDERS_FILE, 'utf8');
    return raw ? JSON.parse(raw) : {};
  } catch (e) {
    console.error('Не удалось прочитать orders-state.json:', e);
    return {};
  }
}

function saveOrders(orders) {
  try {
    fs.writeFileSync(ORDERS_FILE, JSON.stringify(orders, null, 2), 'utf8');
  } catch (e) {
    console.error('Не удалось сохранить orders-state.json:', e);
  }
}

/** @type {Record<string, any>} */
let orders = loadOrders();

if (!API_TOKEN) {
  console.error('Ошибка: переменная окружения BOT_TOKEN не задана.');
  console.error('Создайте файл .env рядом с bot.js и добавьте строку:');
  console.error('BOT_TOKEN=ВАШ_ТОКЕН_БОТА');
  process.exit(1);
}

const bot = new TelegramBot(API_TOKEN, { polling: true });

const START_TEXT =
  ' 🌊  Приветствуем в NWS LOGISTICS!\n\n' +
  ' ⬇️  Используйте кнопку ниже чтобы открыть приложение.\n\n' +
  ' ✅️  В этом телеграм боте вы можете рассчитать стоимость доставки и оформить заказ.\n\n' +
  ' ❗️  Стоимость доставки рассчитывается до Москвы, а далее мы отправим по России, ' +
  'в Беларусь или Казахстан в любой город, любым способом ' +
  '(расчеты смотрите на сайте Транспортной компании)\n\n' +
  'Связь @Krivetka1301';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function sendWithRetry(fn, ...args) {
  const retries = 3;
  const delayMs = 1000;

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await fn(...args);
    } catch (err) {
      console.error(`Retry attempt ${attempt + 1}/${retries} failed:`, err?.message || err);
      if (attempt === retries - 1) {
        throw err;
      }
      await sleep(delayMs);
    }
  }
}

bot.onText(/\/start/, (msg) => {
  const chatId = msg.chat.id;

  const keyboard = {
    keyboard: [
      [
        {
          text: ' 📦  Приложение',
          web_app: { url: APP_URL }
        }
      ]
    ],
    resize_keyboard: true
  };

  bot.sendMessage(chatId, START_TEXT, {
    reply_markup: keyboard
  });
});

bot.on('message', async (msg) => {
  const chatId = msg.chat.id;

  if (!msg.web_app_data || !msg.web_app_data.data) {
    return;
  }

  let data;

  try {
    data = JSON.parse(msg.web_app_data.data);
  } catch (err) {
    console.error('Ошибка парсинга JSON из web_app_data:', err);
    await bot.sendMessage(chatId, 'Произошла ошибка при чтении данных из приложения.');
    return;
  }

  try {
    if (data.type === 'calc') {
      await handleCalc(chatId, data);
    } else if (data.type === 'order') {
      await handleOrder(chatId, msg.from, data);
    } else if (data.type === 'search') {
      await handleSearch(chatId, msg.from, data);
    }
  } catch (err) {
    console.error('Ошибка обработки web_app_data:', err);
    await bot.sendMessage(
      chatId,
      `Произошла ошибка при обработке данных: ${err?.message || String(err)}`
    );
  }
});

async function handleCalc(chatId, data) {
  const res =
    '  💠  <b>РАСЧЕТ ДОСТАВКИ NWS</b>\n\n' +
    `  📦  Коробка: ${data.boxName}\n` +
    `  📏  Габариты: ${data.l}x${data.w}x${data.h} см\n` +
    `  🪵  Обрешетка: ${data.hasCrate ? '  ✅  ' : '  ❌  '}\n` +
    `  💰  <b>ИТОГО: ${data.packPrice} ₽</b>`;

  await bot.sendMessage(chatId, res, { parse_mode: 'HTML' });
}

async function handleOrder(chatId, user, data) {
  const items = Array.isArray(data.items) ? data.items : [];

  const totalYuan = items.reduce((sum, item) => {
    const val = Number(item.resYuan || 0);
    return sum + (Number.isNaN(val) ? 0 : val);
  }, 0);

  const totalRub = Math.ceil(totalYuan * 13);

  const username = user?.username ? `@${user.username}` : `ID: ${user?.id}`;
  const orderId = Date.now().toString();

  orders[orderId] = {
    id: orderId,
    userId: user?.id,
    chatId,
    username,
    totalYuan,
    totalRub,
    items,
    createdAt: new Date().toISOString(),
    orderPaid: false,
    deliveryPriceRub: null,
    deliveryPaid: false
  };
  saveOrders(orders);

  const summary =
    '  🔥  <b>НОВЫЙ ЗАКАЗ НА ВЫКУП</b>\n' +
    `  🆔  Заказ: <code>${orderId}</code>\n` +
    `  👤  Клиент: ${username}\n` +
    `  💵  Сумма: <b>${totalRub} ₽</b> (${totalYuan.toFixed(2)} ¥)\n` +
    `  📦  Товаров: ${items.length}`;

  await sendWithRetry(
    bot.sendMessage.bind(bot),
    MANAGER_ID,
    summary,
    {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: '✅ Отметить ОПЛАТУ заказа',
              callback_data: `order_paid:${orderId}`
            }
          ]
        ]
      }
    }
  );

  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];

    const resYuan = Number(item.resYuan || 0);
    const resRub = Math.ceil(resYuan * 13);

    const caption =
      `  📍  <b>ТОВАР №${idx + 1}</b>\n` +
      `  🔗  ${item.link}\n` +
      `  💰  Цена: ${item.price} ¥\n` +
      `  💲  С комиссией: ${resYuan.toFixed(2)} ¥ / ${resRub} ₽`;

    const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];

    if (imgUrls.length) {
      const batchSize = 9;

      for (let start = 0; start < imgUrls.length; start += batchSize) {
        const batch = imgUrls.slice(start, start + batchSize);

        const media = batch.map((url, i) => {
          if (start === 0 && i === 0) {
            return {
              type: 'photo',
              media: url,
              caption,
              parse_mode: 'HTML'
            };
          }

          return {
            type: 'photo',
            media: url
          };
        });

        try {
          await sendWithRetry(
            bot.sendMediaGroup.bind(bot),
            MANAGER_ID,
            media
          );
        } catch (err) {
          console.error('Ошибка отправки медиа-группы:', err);

          await sendWithRetry(
            bot.sendMessage.bind(bot),
            MANAGER_ID,
            caption,
            { parse_mode: 'HTML' }
          );

          for (const url of batch) {
            try {
              await sendWithRetry(
                bot.sendPhoto.bind(bot),
                MANAGER_ID,
                url
              );
            } catch (photoErr) {
              console.error('Ошибка отправки отдельного фото:', photoErr);
              await bot.sendMessage(
                MANAGER_ID,
                `<a href="${url}">Фото (ссылка)</a>`,
                { parse_mode: 'HTML' }
              );
            }
          }
        }
      }
    } else {
      await bot.sendMessage(
        MANAGER_ID,
        `${caption}\n  📸  Фото: Нет фото`,
        {
          parse_mode: 'HTML',
          disable_web_page_preview: true
        }
      );
    }
  }

  await bot.sendMessage(
    chatId,
    '  ✅  <b>Ваш заказ успешно принят!</b>\n\n' +
      `Номер вашего заказа: <code>${orderId}</code>.\n` +
      'После оплаты менеджер отметит заказ как оплаченный, и вы получите уведомление.\n\n' +
      'Менеджер уже получил информацию и скоро свяжется с вами, либо вы можете написать ему самостоятельно @Krivetka1301.',
    { parse_mode: 'HTML' }
  );
}

// Обработка callback-кнопок администратора
bot.on('callback_query', async (query) => {
  try {
    const { from, data, message } = query;
    if (!data) {
      return;
    }

    const isManager = from.id === MANAGER_ID;
    if (!isManager) {
      await bot.answerCallbackQuery(query.id, { text: 'Недостаточно прав.', show_alert: true });
      return;
    }

    if (data.startsWith('order_paid:')) {
      const orderId = data.split(':')[1];
      const order = orders[orderId];
      if (!order) {
        await bot.answerCallbackQuery(query.id, { text: 'Заказ не найден.', show_alert: true });
        return;
      }

      if (order.orderPaid) {
        await bot.answerCallbackQuery(query.id, { text: 'Заказ уже помечен как оплаченный.' });
        return;
      }

      order.orderPaid = true;
      saveOrders(orders);

      await bot.answerCallbackQuery(query.id, { text: 'Статус заказа обновлён.' });

      // Обновляем сообщение менеджеру
      if (message) {
        const newText =
          `${message.text || ''}\n\n` +
          '✅ Статус платежа: <b>ЗАКАЗ ОПЛАЧЕН</b>';
        await bot.editMessageText(newText, {
          chat_id: message.chat.id,
          message_id: message.message_id,
          parse_mode: 'HTML',
          reply_markup: {
            inline_keyboard: [
              [
                {
                  text: '🚚 Установить стоимость доставки',
                  callback_data: `set_delivery:${orderId}`
                }
              ]
            ]
          }
        });
      }

      // Уведомляем клиента
      if (order.chatId) {
        await bot.sendMessage(
          order.chatId,
          `  ✅  <b>Оплата заказа принята</b>\n\n` +
            `Ваш заказ <code>${orderId}</code> был отмечен менеджером как оплаченный.`,
          { parse_mode: 'HTML' }
        );
      }
      return;
    }

    if (data.startsWith('set_delivery:')) {
      const orderId = data.split(':')[1];
      const order = orders[orderId];
      if (!order) {
        await bot.answerCallbackQuery(query.id, { text: 'Заказ не найден.', show_alert: true });
        return;
      }

      await bot.answerCallbackQuery(query.id, { text: 'Введите стоимость доставки в ₽ одним сообщением.' });

      // Следующее сообщение менеджера с числом интерпретируем как стоимость доставки
      const managerChatId = message?.chat.id || MANAGER_ID;

      const listener = async (msg) => {
        if (msg.chat.id !== managerChatId || msg.from.id !== MANAGER_ID) return;

        const value = Number(String(msg.text).replace(/\s+/g, ''));
        if (!value || value <= 0) {
          await bot.sendMessage(managerChatId, 'Введите положительное число (стоимость доставки в ₽).');
          return;
        }

        bot.removeListener('message', listener);

        order.deliveryPriceRub = value;
        order.deliveryPaid = false;
        saveOrders(orders);

        await bot.sendMessage(
          managerChatId,
          `Стоимость доставки для заказа <code>${orderId}</code> установлена: <b>${value} ₽</b>.`,
          {
            parse_mode: 'HTML',
            reply_markup: {
              inline_keyboard: [
                [
                  {
                    text: '✅ Отметить ОПЛАТУ доставки',
                    callback_data: `delivery_paid:${orderId}`
                  }
                ]
              ]
            }
          }
        );

        if (order.chatId) {
          await bot.sendMessage(
            order.chatId,
            `  🚚  <b>Стоимость доставки рассчитана</b>\n\n` +
              `Заказ: <code>${orderId}</code>\n` +
              `Сумма к оплате за доставку: <b>${value} ₽</b>.\n` +
              `Статус: <b>ожидает оплаты</b>.`,
            { parse_mode: 'HTML' }
          );
        }
      };

      bot.on('message', listener);
      return;
    }

    if (data.startsWith('delivery_paid:')) {
      const orderId = data.split(':')[1];
      const order = orders[orderId];
      if (!order) {
        await bot.answerCallbackQuery(query.id, { text: 'Заказ не найден.', show_alert: true });
        return;
      }

      if (order.deliveryPaid) {
        await bot.answerCallbackQuery(query.id, { text: 'Доставка уже помечена как оплаченная.' });
        return;
      }

      order.deliveryPaid = true;
      saveOrders(orders);

      await bot.answerCallbackQuery(query.id, { text: 'Статус доставки обновлён.' });

      if (message) {
        const newText =
          `${message.text || ''}\n\n` +
          '✅ Статус доставки: <b>ОПЛАЧЕНО</b>';
        await bot.editMessageText(newText, {
          chat_id: message.chat.id,
          message_id: message.message_id,
          parse_mode: 'HTML'
        });
      }

      if (order.chatId) {
        await bot.sendMessage(
          order.chatId,
          `  ✅  <b>Оплата доставки принята</b>\n\n` +
            `Для заказа <code>${orderId}</code> доставка отмечена как оплаченная.`,
          { parse_mode: 'HTML' }
        );
      }
      return;
    }
  } catch (e) {
    console.error('Ошибка обработки callback_query:', e);
    if (query.id) {
      try {
        await bot.answerCallbackQuery(query.id, { text: 'Ошибка обработки действия.', show_alert: true });
      } catch (_) {}
    }
  }
});

async function handleSearch(chatId, user, data) {
  const items = Array.isArray(data.items) ? data.items : [];

  if (!items.length) {
    await bot.sendMessage(chatId, 'Заявка на поиск пуста.');
    return;
  }

  const username = user?.username ? `@${user.username}` : `ID: ${user?.id}`;

  const header =
    '  🔍  <b>НОВАЯ ЗАЯВКА НА ПОИСК</b>\n' +
    `  👤  Клиент: ${username}\n` +
    `  📦  Позиций: ${items.length}`;

  await sendWithRetry(
    bot.sendMessage.bind(bot),
    MANAGER_ID,
    header,
    { parse_mode: 'HTML' }
  );

  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];

    const caption =
      `  📍  <b>ПОЗИЦИЯ №${idx + 1}</b>\n` +
      `  💬  ${item.comment || 'Без комментария'}`;

    const imgUrls = Array.isArray(item.imgUrls) ? item.imgUrls : [];

    if (imgUrls.length) {
      const batchSize = 9;

      for (let start = 0; start < imgUrls.length; start += batchSize) {
        const batch = imgUrls.slice(start, start + batchSize);

        const media = batch.map((url, i) => {
          if (start === 0 && i === 0) {
            return {
              type: 'photo',
              media: url,
              caption,
              parse_mode: 'HTML'
            };
          }

          return {
            type: 'photo',
            media: url
          };
        });

        try {
          await sendWithRetry(
            bot.sendMediaGroup.bind(bot),
            MANAGER_ID,
            media
          );
        } catch (err) {
          console.error('Ошибка отправки медиа-группы (поиск):', err);

          await sendWithRetry(
            bot.sendMessage.bind(bot),
            MANAGER_ID,
            caption,
            { parse_mode: 'HTML' }
          );

          for (const url of batch) {
            try {
              await sendWithRetry(
                bot.sendPhoto.bind(bot),
                MANAGER_ID,
                url
              );
            } catch (photoErr) {
              console.error('Ошибка отправки отдельного фото (поиск):', photoErr);
              await bot.sendMessage(
                MANAGER_ID,
                `<a href="${url}">Фото (ссылка)</a>`,
                { parse_mode: 'HTML' }
              );
            }
          }
        }
      }
    } else {
      await bot.sendMessage(
        MANAGER_ID,
        `${caption}\n  📸  Фото: Нет фото`,
        {
          parse_mode: 'HTML',
          disable_web_page_preview: true
        }
      );
    }
  }

  await bot.sendMessage(
    chatId,
    '  ✅  <b>Заявка на поиск отправлена менеджеру.</b>',
    { parse_mode: 'HTML' }
  );
}

console.log('Bot is running (Node.js, polling)...');

