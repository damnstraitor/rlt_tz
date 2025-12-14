from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession
from database.crud import DatabaseManager
from nlp.query_parser import NaturalLanguageParser
from datetime import datetime
import re

class BotHandlers:
    def __init__(self, dp: Dispatcher, session_maker):
        self.dp = dp
        self.session_maker = session_maker
        self.parser = NaturalLanguageParser()
        
    async def register_handlers(self):
        self.dp.register_message_handler(self.start_command, commands=['start'])
        self.dp.register_message_handler(self.help_command, commands=['help'])
        self.dp.register_message_handler(self.handle_text_query, content_types=types.ContentType.TEXT)
    
    async def start_command(self, message: types.Message):
        """Обработчик команды /start"""
        welcome_text = (
            "👋 Привет! Я бот для аналитики видео.\n\n"
            "Задайте мне вопрос на естественном языке, например:\n"
            "• Сколько всего видео есть в системе?\n"
            "• Сколько видео у креатора с id ... вышло с 1 по 5 ноября 2025?\n"
            "• Сколько видео набрало больше 100000 просмотров?\n"
            "• На сколько просмотров в сумме выросли все видео 28 ноября 2025?\n"
            "• Сколько разных видео получали новые просмотры 27 ноября 2025?\n\n"
            "Я верну вам ответ в виде одного числа."
        )
        await message.answer(welcome_text)
    
    async def help_command(self, message: types.Message):
        """Обработчик команды /help"""
        help_text = (
            "📊 Примеры запросов:\n\n"
            "1. Подсчет видео:\n"
            "   • 'Сколько всего видео есть в системе?'\n"
            "   • 'Сколько видео у креатора с id abc123?'\n\n"
            "2. Запросы по датам:\n"
            "   • 'Сколько видео вышло с 1 по 5 ноября 2025?'\n"
            "   • 'Сколько видео опубликовано 28 ноября 2025?'\n\n"
            "3. Аналитика просмотров:\n"
            "   • 'Сколько видео набрало больше 100000 просмотров?'\n"
            "   • 'На сколько просмотров выросли все видео вчера?'\n\n"
            "4. Динамика просмотров:\n"
            "   • 'Сколько разных видео получали новые просмотры 27 ноября 2025?'\n\n"
            "Просто напишите вопрос, и я постараюсь на него ответить!"
        )
        await message.answer(help_text)
    
    async def handle_text_query(self, message: types.Message):
        """Обработчик текстовых запросов"""
        user_query = message.text.strip()
        
        # Отправляем сообщение о обработке
        processing_msg = await message.answer("🔄 Обрабатываю запрос...")
        
        try:
            # Парсим запрос в SQL
            sql_query, params = self.parser.parse_query_to_sql(user_query)
            
            # Выполняем запрос к базе данных
            async with self.session_maker() as session:
                db_manager = DatabaseManager(session)
                result = await db_manager.execute_custom_query(sql_query, params)
                
                if result is not None:
                    # Форматируем число с разделителями тысяч
                    formatted_result = format(result, ',d').replace(',', ' ')
                    await processing_msg.edit_text(f"📊 Результат: {formatted_result}")
                else:
                    await processing_msg.edit_text("❌ Не удалось получить результат. Проверьте формулировку запроса.")
        
        except Exception as e:
            print(f"Error processing query: {e}")
            await processing_msg.edit_text(
                "❌ Произошла ошибка при обработке запроса. "
                "Пожалуйста, проверьте формулировку или попробуйте другой запрос."
            )