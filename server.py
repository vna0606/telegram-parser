from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.tl.types import User, Channel, Chat
from datetime import datetime, timedelta
import os
import asyncio
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Настройки из переменных окружения
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
PHONE = os.getenv('PHONE', '')
SESSION_NAME = os.getenv('SESSION_NAME', 'parser_session')

# Глобальный клиент
client = None
client_lock = asyncio.Lock()

async def get_client():
    """
    Получает или создает подключенного клиента
    """
    global client
    
    async with client_lock:
        if client is None:
            logger.info("Создание нового клиента...")
            client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
            await client.connect()
            
            if not await client.is_user_authorized():
                logger.error("Клиент не авторизован!")
                raise Exception("Telegram клиент не авторизован. Требуется первичная настройка.")
            
            logger.info("Клиент успешно подключен")
        
        elif not client.is_connected():
            logger.info("Переподключение клиента...")
            await client.connect()
        
        return client

async def parse_messages(channel_id, limit=100, days_back=None, date_from=None):
    """
    Парсит сообщения из чата за указанный период
    """
    try:
        tg_client = await get_client()
        
        # Определяем дату начала
        if date_from:
            try:
                offset_date = datetime.fromisoformat(date_from.replace('Z', '+00:00'))
            except:
                offset_date = datetime.now() - timedelta(days=7)
        elif days_back:
            offset_date = datetime.now() - timedelta(days=int(days_back))
        else:
            offset_date = None
        
        logger.info(f"Парсинг канала {channel_id}, лимит: {limit}, дата с: {offset_date}")
        
        messages = []
        
        # Преобразуем ID в integer если нужно
        try:
            channel_id_int = int(channel_id)
        except:
            channel_id_int = channel_id
        
        # Получаем сообщения
        message_count = 0
        async for message in tg_client.iter_messages(
            channel_id_int, 
            limit=limit,
            reverse=False  # От новых к старым
        ):
            message_count += 1
            
            # Проверяем дату если указана
            if offset_date and message.date:
                if message.date.replace(tzinfo=None) < offset_date.replace(tzinfo=None):
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
            except:
                sender_name = "Unknown"
            
            # Формируем данные сообщения
            msg_data = {
                'id': message.id,
                'date': message.date.strftime('%Y-%m-%d %H:%M:%S') if message.date else '',
                'text': message.message or "",
                'sender': sender_name,
                'from_user': sender_name,
                'views': message.views or 0,
                'forwards': message.forwards or 0,
                'has_media': bool(message.media),
                'media_type': type(message.media).__name__ if message.media else None,
            }
            
            messages.append(msg_data)
        
        logger.info(f"Собрано {len(messages)} сообщений из {message_count} просмотренных")
        return messages
    
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {str(e)}", exc_info=True)
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
        
        logger.info(f"Получен запрос: channel={channel}, limit={limit}, days_back={days_back}")
        
        # Запускаем парсинг
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            messages = loop.run_until_complete(
                parse_messages(channel, limit, days_back, date_from)
            )
        finally:
            loop.close()
        
        return jsonify({
            'success': True,
            'messages': messages,
            'count': len(messages),
            'channel': channel,
            'period_days': days_back
        })
    
    except Exception as e:
        logger.error(f"Ошибка в /parse: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'details': 'Проверьте правильность ID чата и доступ к нему'
        }), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'OK',
        'message': 'Telegram Parser Server is running',
        'api_id_set': bool(API_ID),
        'api_hash_set': bool(API_HASH),
        'phone_set': bool(PHONE)
    })

@app.route('/status', methods=['GET'])
async def status():
    """
    Проверка статуса подключения
    """
    try:
        tg_client = await get_client()
        is_connected = tg_client.is_connected()
        is_authorized = await tg_client.is_user_authorized()
        
        me = None
        if is_authorized:
            me = await tg_client.get_me()
        
        return jsonify({
            'connected': is_connected,
            'authorized': is_authorized,
            'user': {
                'id': me.id if me else None,
                'username': me.username if me else None,
                'phone': me.phone if me else None
            } if me else None
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'connected': False,
            'authorized': False
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
