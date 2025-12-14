import asyncio
import logging
import sys
import os
from pathlib import Path

# ВАЖНО: загружаем .env ПЕРВЫМ делом!
from dotenv import load_dotenv

# Находим .env файл относительно этого файла
current_dir = Path(__file__).parent
env_path = current_dir / '.env'

if env_path.exists():
    print(f"✅ Загружаем .env из: {env_path}")
    load_dotenv(env_path)
else:
    print(f"❌ .env файл не найден по пути: {env_path}")
    print("Создайте .env в той же папке, где main.py")
    sys.exit(1)

# Теперь импортируем config
try:
    from config import config
    print(f"✅ Конфиг загружен. Токен: {config.TELEGRAM_BOT_TOKEN[:10]}...")
except ImportError as e:
    print(f"❌ Ошибка импорта config: {e}")
    sys.exit(1)

from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from database.init_db import DatabaseInitializer
from database.crud import DatabaseManager
from nlp.query_parser import NaturalLanguageParser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def create_bot():
    """Создание объекта бота с проверкой токена"""
    token = config.TELEGRAM_BOT_TOKEN
    
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN не установлен!")
        sys.exit(1)
    
    if ':' not in token:
        logger.error(f"Токен не содержит ':' : {token[:20]}...")
        sys.exit(1)
    
    print(f"🔄 Создаю бота с токеном: {token[:10]}...")
    
    try:
        bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        print("✅ Объект бота создан")
        return bot
    except Exception as e:
        logger.error(f"Ошибка создания бота: {e}")
        sys.exit(1)

# Создаем бота
bot = create_bot()
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Инициализация парсера NLP
nlp_parser = NaturalLanguageParser()

async def initialize_database():
    """Инициализация базы данных"""
    try:
        initializer = DatabaseInitializer()
        json_file = "data/videos_data.json"
        
        if os.path.exists(json_file):
            await initializer.initialize(json_file)
        else:
            logger.warning(f"JSON файл не найден: {json_file}")
            logger.info("Создаю таблицы без данных...")
            await initializer.create_tables()
        
        await initializer.close()
        logger.info("✅ База данных инициализирована")
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        raise

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    welcome_text = (
        "👋 <b>Привет! Я бот для аналитики видео.</b>\n\n"
        "Задайте мне вопрос на естественном языке, например:\n"
        "• Сколько всего видео есть в системе?\n"
        "• Сколько видео у креатора с id ... вышло с 1 по 5 ноября 2025?\n"
        "• Сколько видео набрало больше 100000 просмотров?\n"
        "• На сколько просмотров в сумме выросли все видео 28 ноября 2025?\n"
        "• Сколько разных видео получали новые просмотры 27 ноября 2025?\n\n"
        "Я верну вам ответ в виде одного числа."
    )
    await message.answer(welcome_text)

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Обработчик команды /help"""
    help_text = (
        "📊 <b>Примеры запросов:</b>\n\n"
        "1. <b>Подсчет видео:</b>\n"
        "   • 'Сколько всего видео есть в системе?'\n"
        "   • 'Сколько видео у креатора с id abc123?'\n\n"
        "2. <b>Запросы по датам:</b>\n"
        "   • 'Сколько видео вышло с 1 по 5 ноября 2025?'\n"
        "   • 'Сколько видео опубликовано 28 ноября 2025?'\n\n"
        "3. <b>Аналитика просмотров:</b>\n"
        "   • 'Сколько видео набрало больше 100000 просмотров?'\n"
        "   • 'На сколько просмотров выросли все видео вчера?'\n\n"
        "4. <b>Динамика просмотров:</b>\n"
        "   • 'Сколько разных видео получали новые просмотры 27 ноября 2025?'\n\n"
        "Просто напишите вопрос, и я постараюсь на него ответить!"
    )
    await message.answer(help_text)

@dp.message()
async def handle_text_query(message: types.Message):
    """Обработчик текстовых запросов"""
    user_query = message.text.strip()
    user_id = message.from_user.id
    
    # Отправляем сообщение о обработке
    processing_msg = await message.answer("🔄 <i>Обрабатываю запрос...</i>")
    
    try:
        # Создаем движок базы данных
        engine = create_async_engine(
            config.database_url.replace("postgresql://", "postgresql+asyncpg://"),
            echo=False
        )
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        # Парсим запрос в SQL
        sql_query, params = nlp_parser.parse_query_to_sql(user_query)
        logger.info(f"User {user_id}: {user_query} -> SQL: {sql_query}")
        
        # Выполняем запрос к базе данных
        async with async_session() as session:
            db_manager = DatabaseManager(session)
            result = await db_manager.execute_custom_query(sql_query, params)
            
            if result is not None:
                # Проверяем, что результат - число
                if isinstance(result, (int, float)):
                    # Форматируем число с разделителями тысяч
                    formatted_result = "{:,}".format(int(result)).replace(",", " ")
                    await processing_msg.edit_text(f"📊 <b>Результат:</b> {formatted_result}")
                else:
                    # Если не число, показываем как есть
                    await processing_msg.edit_text(f"📊 <b>Результат:</b> {result}")
            else:
                await processing_msg.edit_text(
                    "❌ <b>Не удалось получить результат.</b>\n"
                    "Проверьте формулировку запроса."
                )
        
        await engine.dispose()
        
    except Exception as e:
        logger.error(f"Error processing query from user {user_id}: {e}")
        await processing_msg.edit_text(
            "❌ <b>Произошла ошибка при обработке запроса.</b>\n"
            "Пожалуйста, проверьте формулировку или попробуйте другой запрос."
        )

async def main():
    """Основная функция запуска бота"""
    
    # Инициализация базы данных
    logger.info("Инициализация базы данных...")
    await initialize_database()
    
    logger.info("Запуск бота...")
    
    # Тестируем подключение бота
    try:
        bot_info = await bot.get_me()
        print(f"✅ Бот успешно подключен: @{bot_info.username}")
    except Exception as e:
        print(f"❌ Ошибка подключения бота: {e}")
        print("Проверьте токен и интернет-соединение")
        return
    
    # Запуск polling
    print("🔄 Запускаю polling...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    # Создаем директорию для данных если её нет
    os.makedirs("data", exist_ok=True)
    
    # Запуск асинхронного приложения
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")