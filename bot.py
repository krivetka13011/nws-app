const API_TOKEN = '8576868412:AAEN5EV5rtomRdHO1eINRXzof51u2S0hK7w';
const APP_URL = "https://krivetka13011.github.io/nws-app/";
const MANAGER_ID = 1159166497;
const TELEGRAM_API = `https://api.telegram.org/bot${API_TOKEN}`;
const BASE_RATE = 13;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper: Format number with spaces (1 000 000)
function formatPrice(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
}

async function sendTelegram(method, body) {
  const response = await fetch(`${TELEGRAM_API}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return response.json();
}

async function handleStart(chatId) {
  // ЯВНО сбрасываем кнопку меню на стандартную (чтобы убрать Web App кнопку слева)
  await sendTelegram('setChatMenuButton', {
    chat_id: chatId,
    menu_button: { type: "default" }
  });

  const markup = {
    keyboard: [[{ text: " 📦  Приложение", web_app: { url: APP_URL } }]],
    resize_keyboard: true,
  };

  const text = (
    " 🌊  Приветствуем в NWS LOGISTICS!\n\n" +
    " ⬇️  Используйте кнопку ниже, чтобы открыть приложение.\n\n" +
    " ✅️  В этом телеграм боте вы можете рассчитать стоимость доставки и оформить заказ.\n\n" +
    " ❗️  Стоимость доставки рассчитывается до Москвы, а далее мы отправим по России, " +
    "в Беларусь или Казахстан в любой город, любым способом " +
    "(расчеты смотрите на сайте Транспортной компании)\n\n" +
    "Связь: @Krivetka1301"
  );

  await sendTelegram('sendMessage', { chat_id: chatId, text: text, reply_markup: markup });
}

async function handleWebAppData(chatId, fromUser, webAppData) {
  try {
    const data = JSON.parse(webAppData);
    const username = fromUser.username ? `@${fromUser.username}` : `ID: ${fromUser.id}`;

    // --- 1. КАЛЬКУЛЯТОР ---
    if (data.type === 'calc') {
        
      // СЦЕНАРИЙ: СВОЙ РАЗМЕР (Запрос менеджеру)
      if (data.boxName === 'Свой размер') {
          let text = `📝 <b>ЗАКАЗ НА РАСЧЕТ ДОСТАВКИ</b>\n\n` +
                     `👤 Клиент: ${username}\n` +
                     `📏 Габариты: ${data.l}x${data.w}x${data.h} см\n` +
                     `⚖️ Вес: ${data.weight} кг\n`;
          
          if (data.hasCrate) {
              text += `🪵 Обрешетка: Да\n`;
          } else {
              text += `🪵 Обрешетка: Нет\n`;
          }

          // 1. Уведомление менеджеру
          await sendTelegram('sendMessage', { 
              chat_id: MANAGER_ID, 
              text: text, 
              parse_mode: "HTML" 
          });

          // 2. Ответ клиенту
          await sendTelegram('sendMessage', { 
              chat_id: chatId, 
              text: "✅ <b>Запрос на расчет принят!</b>\n\nМенеджер свяжется с вами в ближайшее время для уточнения стоимости.\n\nСвязь: @Krivetka1301", 
              parse_mode: "HTML" 
          });

      } else {
          // СЦЕНАРИЙ: СТАНДАРТНАЯ КОРОБКА (Ответ клиенту с ценой)
          let res = " 💠  <b>РАСЧЕТ ДОСТАВКИ NWS</b>\n\n" +
                    ` 📦  Упаковка: ${data.boxName}\n` +
                    ` 📏  Габариты: ${data.l}x${data.w}x${data.h} см\n`;
          
          if (data.weight) {
              res += ` ⚖️  Вес: ${data.weight} кг\n`;
          }

          if (data.hasCrate) {
              res += ` 🪵  Обрешетка: Да (+299₽)\n`;
          }
          
          // packPrice приходит уже строкой, но может быть без пробелов, если это просто число.
          // В index.html мы отправляем innerText, который уже отформатирован.
          // Но на всякий случай можно просто вывести как есть.
          res += ` 💰  <b>ИТОГО: ${data.packPrice} ₽</b>`;
          
          await sendTelegram('sendMessage', { chat_id: chatId, text: res, parse_mode: "HTML" });
      }
    } 
    
    // --- 2. ЗАКАЗ (ВЫКУП) ---
    else if (data.type === 'order') {
      const items = data.items;
      
      const deliveryType = data.deliveryType || 'Серая (20-35 дн)';
      const isWhite = deliveryType.toLowerCase().includes('белая');
      
      // Процент комиссии: Белая = 0.10 (10%), Серая = 0.05 (5%)
      const commissionPercent = isWhite ? 0.10 : 0.05;

      let total_rub = 0;
      let total_yuan = 0;

      items.forEach(item => {
          const priceYuan = parseFloat(item.price || 0);
          total_yuan += priceYuan;
          
          if (priceYuan > 0) {
              // Формула: (Цена + 30 + Комиссия) * 13
              const priceWithShipping = priceYuan + 30;
              const commission = priceWithShipping * commissionPercent;
              const priceRub = Math.ceil((priceWithShipping + commission) * BASE_RATE);
              
              total_rub += priceRub;
          }
      });

      // Сообщение менеджеру (сводка)
      await sendTelegram('sendMessage', {
        chat_id: MANAGER_ID,
        text: `🔥 <b>НОВЫЙ ЗАКАЗ</b>\n\n👤 Клиент: ${username}\n🚚 Доставка: ${deliveryType}\n💵 Итого: ${formatPrice(total_rub)} ₽ (${formatPrice(total_yuan)} ¥)\n📦 Товаров: ${items.length}`,
        parse_mode: "HTML"
      });

      // Отправка товаров менеджеру
      for (const [idx, item] of items.entries()) {
        const priceYuan = parseFloat(item.price || 0);
        let itemRub = 0;
        
        if (priceYuan > 0) {
             const priceWithShipping = priceYuan + 30;
             const commission = priceWithShipping * commissionPercent;
             itemRub = Math.ceil((priceWithShipping + commission) * BASE_RATE);
        }
        
        const caption = `📍 <b>ТОВАР №${idx + 1}</b>\n🔗 ${item.link}\n💰 Цена: ${formatPrice(itemRub)} ₽ (${formatPrice(priceYuan)} ¥)`;
        
        // Отправка фото (поддержка нескольких фото в альбоме)
        // Если больше 9 фото, берем последние 9 (как просили в запросе "оставляй последние 9")
        // Телеграм медиагруппа максимум 10, но мы ограничимся 9.
        if (item.imgUrls && item.imgUrls.length > 0) {
          
          const photosToSend = item.imgUrls.length >= 10 ? item.imgUrls.slice(-9) : item.imgUrls;

          const mediaGroup = photosToSend.map((url, i) => ({
            type: 'photo',
            media: url,
            caption: i === 0 ? caption : undefined, // Подпись только к первой фото
            parse_mode: 'HTML'
          }));
          await sendTelegram('sendMediaGroup', { chat_id: MANAGER_ID, media: mediaGroup });
        } else {
          // Если фото нет, просто текст
          await sendTelegram('sendMessage', { 
            chat_id: MANAGER_ID, 
            text: caption, 
            parse_mode: "HTML", 
            disable_web_page_preview: true 
          });
        }

        // Небольшая задержка между товарами
        if (items.length > 1 && idx < items.length - 1) {
          await sleep(1000); 
        }
      }

      // Ответ клиенту
      await sendTelegram('sendMessage', {
        chat_id: chatId,
        text: " ✅  <b>Ваш заказ успешно принят!</b>\n\n" +
              `🚚 Тип доставки: ${deliveryType}\n` +
              `💰 Сумма заказа: ${formatPrice(total_rub)} ₽\n\n` +
              "Менеджер свяжется с вами для подтверждения.\n\nСвязь: @Krivetka1301",
        parse_mode: "HTML"
      });
    }

    // --- 3. ПОИСК ---
    else if (data.type === 'search') {
      const items = data.items;

      await sendTelegram('sendMessage', {
        chat_id: MANAGER_ID,
        text: `🔍 <b>ЗАЯВКА НА ПОИСК</b>\n👤 Клиент: ${username}\n📦 Позиций: ${items.length}`,
        parse_mode: "HTML"
      });

      for (const [idx, item] of items.entries()) {
        const caption = `🕵️ <b>ПОИСК №${idx + 1}</b>\n📝 Комментарий: ${item.comment || 'Нет'}`;
        
        if (item.imgUrls && item.imgUrls.length > 0) {
          // Тоже ограничиваем до последних 9
          const photosToSend = item.imgUrls.length >= 10 ? item.imgUrls.slice(-9) : item.imgUrls;

          const mediaGroup = photosToSend.map((url, i) => ({
            type: 'photo',
            media: url,
            caption: i === 0 ? caption : undefined,
            parse_mode: 'HTML'
          }));
          await sendTelegram('sendMediaGroup', { chat_id: MANAGER_ID, media: mediaGroup });
        } else {
          await sendTelegram('sendMessage', { 
            chat_id: MANAGER_ID, 
            text: caption, 
            parse_mode: "HTML" 
          });
        }

        if (items.length > 1 && idx < items.length - 1) {
          await sleep(1000); 
        }
      }

      await sendTelegram('sendMessage', {
        chat_id: chatId,
        text: " 🕵️  <b>Заявка на поиск принята!</b>\n\n" +
              "Мы получили ваши фото. Менеджер начнет поиск.\n\nСвязь: @Krivetka1301",
        parse_mode: "HTML"
      });
    }

  } catch (e) {
    console.error(e);
    await sendTelegram('sendMessage', { chat_id: chatId, text: `Ошибка: ${e.message}` });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/webhook' && request.method === 'POST') {
      try {
        const update = await request.json();
        if (update.message) {
          const { chat, from, text, web_app_data } = update.message;
          if (text === '/start') {
            await handleStart(chat.id);
          } else if (web_app_data) {
            ctx.waitUntil(handleWebAppData(chat.id, from, web_app_data.data));
          }
        }
      } catch (err) {
        console.error(err);
      }
      return new Response('OK', { status: 200 });
    }
    return new Response('Bot is running', { status: 200 });
  }
};