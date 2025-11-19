from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.tl.types import User, Channel, Chat
from datetime import datetime, timedelta
import os
import asyncio

app = Flask(__name__)

# Настройки из переменных окружения
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE = os.getenv('PHONE')
SESSION_NAME = os.getenv('SESSION_NAME', 'parser_session')

client = None

async def init_client():
    global client
    if client is None:
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await client.start(phone=PHONE)
    return client

async def parse_messages(channel_id, limit=100, days_back=None, date_from=None):
    """
    Парсит сообщения из чата за указанный период
    """
    try:
        client = await init_client()
        
        # Определяем дату начала
        if date_from:
            offset_date = datetime.fromisoformat(date_from.replace('Z', '+00:00'))
        elif days_back:
            offset_date = datetime.now() - timedelta(days=days_back)
        else:
            offset_date = None
        
        messages = []
        
        # Получаем сообщения
        async for message in client.iter_messages(
            channel_id, 
            limit=limit,
            offset_date=offset_date,
            reverse=False  # От новых к старым
        ):
            # Пропускаем служебные сообщения
            if not message.message and not message.media:
                continue
            
            # Получаем информацию об отправителе
            sender_name = ""
            if message.sender:
                if isinstance(message.sender, User):
                    sender_name = message.sender.first_name or ""
                    if message.sender.last_name:
                        sender_name += f" {message.sender.last_name}"
                    if message.sender.username:
                        sender_name += f" (@{message.sender.username})"
                elif isinstance(message.sender, (Channel, Chat)):
                    sender_name = message.sender.title or ""
            
            msg_data = {
                'id': message.id,
                'date': message.date.isoformat() if message.date else None,
                'text': message.message or "",
                'sender': sender_name,
                'from_user': sender_name,
                'views': message.views or 0,
                'forwards': message.forwards or 0,
                'has_media': bool(message.media),
                'media_type': type(message.media).__name__ if message.media else None,
            }
            
            messages.append(msg_data)
        
        return messages
    
    except Exception as e:
        raise Exception(f"Ошибка при парсинге: {str(e)}")

@app.route('/parse', methods=['POST'])
def parse():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Не передан JSON'}), 400
        
        channel = data.get('channel')
        limit = int(data.get('limit', 100))
        days_back = data.get('days_back')
        date_from = data.get('date_from')
        
        if not channel:
            return jsonify({'error': 'Не указан channel'}), 400
        
        # Запускаем парсинг
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        messages = loop.run_until_complete(
            parse_messages(channel, limit, days_back, date_from)
        )
        
        return jsonify({
            'success': True,
            'messages': messages,
            'count': len(messages),
            'channel': channel,
            'period_days': days_back
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'details': 'Проверьте правильность ID чата и доступ к нему'
        }), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'OK',
        'message': 'Telegram Parser Server is running'
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
