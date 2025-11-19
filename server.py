from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import User, Channel, Chat
from datetime import datetime, timedelta
import os
import asyncio
import logging
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Получаем данные из переменных окружения
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
SESSION_STRING = os.getenv('SESSION_STRING', '')

client = None
client_lock = asyncio.Lock()

async def get_client():
    """Получает или создает подключенного клиента"""
    global client
    
    async with client_lock:
        if client is None:
            logger.info("Инициализация Telegram клиента...")
            
            if not SESSION_STRING:
                raise Exception("SESSION_STRING не найдена в переменных окружения")
            
            if not API_ID or API_ID == 0:
                raise Exception("API_ID не найден в переменных окружения")
            
            if not API_HASH:
                raise Exception("API_HASH не найден в переменных окружения")
            
            try:
                client = TelegramClient(
                    StringSession(SESSION_STRING), 
                    API_ID, 
                    API_HASH
                )
                
                await client.connect()
                
                if not await client.is_user_authorized():
                    raise Exception("Сессия недействительна. Требуется получить новую SESSION_STRING")
                
                me = await client.get_me()
                logger.info(f"✅ Клиент подключен: {me.first_name} (@{me.username or 'без username'})")
                
            except Exception as e:
                logger.error(f"❌ Ошибка подключения клиента: {e}")
                raise
        
        elif not client.is_connected():
            logger.info("Переподключение клиента...")
            await client.connect()
        
        return client

async def parse_messages(channel_id, limit=100, days_back=None, date_from=None):
    """Парсит сообщения из чата за указанный период"""
    try:
        tg_client = await get_client()
        
        # Определяем дату начала
        if date_from:
            try:
                offset_date = datetime.fromisoformat(date_from.replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Не удалось разобрать дату {date_from}: {e}")
                offset_date = datetime.now() - timedelta(days=7)
        elif days_back:
            offset_date = datetime.now() - timedelta(days=int(days_back))
        else:
            offset_date = None
        
        logger.info(f"📥 Парсинг канала {channel_id}")
        logger.info(f"   Лимит: {limit}, Период с: {offset_date}")
        
        messages = []
        channel_id_int = int(channel_id)
        
        message_count = 0
        
        # Получаем сообщения
        async for message in tg_client.iter_messages(channel_id_int, limit=limit):
            message_count += 1
            
            # Проверяем дату
            if offset_date and message.date:
                msg_date = message.date.replace(tzinfo=None)
                offset_date_naive = offset_date.replace(tzinfo=None)
                
                if msg_date < offset_date_naive:
                    logger.info(f"   Достигнута дата {msg_date}, останавливаемся")
                    break
            
            # Получаем информацию об отправителе
            sender_name = ""
            try:
                if message.sender:
                    if isinstance(message.sender, User):
                        sender_name = message.sender.first_name or ""
                        if message.sender.last_name:
                            sender_name += f" {message.sender.last_name}"
                        if message.sender.username:
                            sender_name += f" (@{message.sender.username})"
                    elif isinstance(message.sender, (Channel, Chat)):
                        sender_name = message.sender.title or ""
            except Exception as e:
                logger.warning(f"Не удалось получить отправителя: {e}")
                sender_name = "Unknown"
            
            # Формируем данные сообщения
            msg_data = {
                'id': message.id,
                'date': message.date.strftime('%Y-%m-%d %H:%M:%S') if message.date else '',
                'text': message.message or "",
                'sender': sender_name,
                'views': message.views or 0,
                'forwards': message.forwards or 0,
                'has_media': bool(message.media),
                'media_type': type(message.media).__name__ if message.media else None,
            }
            
            messages.append(msg_data)
        
        logger.info(f"✅ Собрано {len(messages)} сообщений (просмотрено {message_count})")
        return messages
    
    except Exception as e:
        logger.error(f"❌ Ошибка при парсинге: {str(e)}", exc_info=True)
        raise Exception(f"Ошибка при парсинге: {str(e)}")

def run_async(coro):
    """Запускает async функцию в синхронном контексте"""
    try:
        # Пробуем получить текущий event loop
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            # Если закрыт - создаем новый
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        # Если loop нет - создаем новый
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(coro)

@app.route('/parse', methods=['POST'])
def parse():
    """Endpoint для парсинга канала"""
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
        
        logger.info(f"📨 Получен запрос: channel={channel}, limit={limit}, days_back={days_back}")
        
        # Запускаем парсинг через run_async
        messages = run_async(parse_messages(channel, limit, days_back, date_from))
        
        return jsonify({
            'success': True,
            'messages': messages,
            'count': len(messages),
            'channel': channel,
            'period_days': days_back
        })
    
    except Exception as e:
        logger.error(f"❌ Ошибка в /parse: {str(e)}")
        return jsonify({
            'error': str(e),
            'details': 'Проверьте правильность ID чата и доступ к нему'
        }), 500

@app.route('/health', methods=['GET'])
def health():
    """Проверка работоспособности сервера"""
    return jsonify({
        'status': 'OK',
        'message': 'Telegram Parser Server is running',
        'config': {
            'api_id_set': bool(API_ID and API_ID != 0),
            'api_hash_set': bool(API_HASH),
            'session_set': bool(SESSION_STRING)
        }
    })

@app.route('/test', methods=['GET'])
def test():
    """Тестовый endpoint для проверки подключения"""
    try:
        async def check_connection():
            tg_client = await get_client()
            me = await tg_client.get_me()
            return {
                'connected': True,
                'user': {
                    'id': me.id,
                    'name': f"{me.first_name} {me.last_name or ''}".strip(),
                    'username': me.username,
                    'phone': me.phone
                }
            }
        
        result = run_async(check_connection())
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            'connected': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    logger.info(f"🚀 Запуск сервера на порту {port}")
    app.run(host='0.0.0.0', port=port)
