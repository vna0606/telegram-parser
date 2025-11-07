# server.py — устойчивая версия с логированием и обработкой ошибок
import os
import asyncio
import logging
from flask import Flask, request, jsonify
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("telegram-parser")

app = Flask(__name__)

# Читаем переменные окружения
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
STRING_SESSION = os.environ.get("STRING_SESSION")  # если используешь StringSession
PORT = int(os.environ.get("PORT", 10000))

if not API_ID or not API_HASH:
    log.error("API_ID or API_HASH is missing in environment variables")
    # не бросаем исключение, но /parse будет возвращать понятную ошибку

# Преобразуем api_id в int безопасно
try:
    API_ID_INT = int(API_ID) if API_ID else None
except Exception as e:
    log.error("Invalid API_ID: %s", e)
    API_ID_INT = None

# Инициализация клиента — будем создавать клиент на лету в обработчике,
# чтобы ошибки авторизации/сессии можно было ловить и логировать.
async def create_client():
    if not API_ID_INT or not API_HASH:
        raise RuntimeError("API_ID/API_HASH not configured on server.")
    # Если указан STRING_SESSION — используем его, иначе создаём временную сессию
    if STRING_SESSION:
        client = TelegramClient(StringSession(STRING_SESSION), API_ID_INT, API_HASH)
    else:
        client = TelegramClient('anon_session', API_ID_INT, API_HASH)
    await client.connect()
    return client

@app.route("/")
def home():
    return "Telegram Parser is running!"

@app.route("/parse", methods=["POST"])
def parse_handler():
    try:
        payload = request.get_json(force=True)
    except Exception as e:
        log.exception("Bad JSON: %s", e)
        return jsonify({"error": "Bad JSON payload", "details": str(e)}), 400

    # Поддерживаем оба ключа: "channel" и "url"
    channel = payload.get("channel") or payload.get("url") or payload.get("link")
    limit = int(payload.get("limit", 10))

    if not channel:
        return jsonify({"error": "No channel provided. Use field 'channel' or 'url'."}), 400

    log.info("Parse request: channel=%s limit=%s", channel, limit)

    async def _work():
        client = None
        try:
            client = await create_client()
            # Пробуем получить entity — Telethon поддерживает @username, t.me/.. и id
            try:
                entity = await client.get_entity(channel)
            except Exception as e_entity:
                # Попытка обработать как приглашение/ссылка
                log.warning("get_entity failed: %s — trying as username", e_entity)
                # если get_entity упал — всё равно попробуем iter_messages(channel,...)
                entity = channel

            messages = []
            async for msg in client.iter_messages(entity, limit=limit):
                messages.append({
                    "id": getattr(msg, "id", None),
                    "date": getattr(msg, "date", None).isoformat() if getattr(msg, "date", None) else None,
                    "text": msg.message or "",
                    "views": getattr(msg, "views", None),
                    "forwards": getattr(msg, "forwards", None),
                    "has_media": bool(getattr(msg, "media", None))
                })
            return {"channel": str(channel), "count": len(messages), "messages": messages}
        except Exception as e:
            log.exception("Error while parsing channel %s: %s", channel, e)
            return {"error": "Server error while parsing channel", "details": str(e)}
        finally:
            if client and client.is_connected():
                await client.disconnect()

    # Запускаем асинхронную задачу и ждём результата
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_work())
    finally:
        loop.close()

    # Если это ошибка — вернём 500 с описанием (чтобы в логах видно было)
    if result.get("error"):
        return jsonify(result), 500

    return jsonify(result), 200

if __name__ == "__main__":
    log.info("Starting Flask on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT)
