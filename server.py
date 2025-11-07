import os
import asyncio
from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession

app = Flask(__name__)

# 🔹 Настройки Telegram API
api_id = int(os.environ.get("API_ID"))
api_hash = os.environ.get("API_HASH")
string_session = os.environ.get("STRING_SESSION")

client = TelegramClient(StringSession(string_session), api_id, api_hash)

@app.route('/')
def home():
    return "Telegram Parser is running!"

@app.route('/parse', methods=['POST'])
async def parse_channel():
    data = request.get_json()
    channel = data.get('channel')
    limit = int(data.get('limit', 10))
    result = []

    async with client:
        async for msg in client.iter_messages(channel, limit=limit):
            result.append({
                'date': str(msg.date),
                'text': msg.text
            })
    return jsonify(result)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
