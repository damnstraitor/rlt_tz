import re
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any, Union
import pytz
from openai import OpenAI
from config import config

class NaturalLanguageParser:
    def __init__(self):
        # Всегда инициализируем клиент для API
        self.client = OpenAI(
            api_key=config.MISTRAL_API_KEY,
            base_url="https://api.mistral.ai/v1"
        )
        self.model = config.MISTRAL_MODEL if hasattr(config, 'MISTRAL_MODEL') else "mistral-small"
        
        # Улучшенный системный промпт
        self.system_prompt = """Ты преобразуешь русские запросы в SQL для PostgreSQL.
        
БАЗА ДАННЫХ:
1. Таблица videos:
   - id (text) - ID видео
   - creator_id (text) - ID креатора  
   - video_created_at (timestamp) - дата публикации
   - views_count (bigint) - просмотры
   - likes_count (bigint) - лайки
   - comments_count (bigint) - комментарии
   - reports_count (bigint) - жалобы
   - created_at, updated_at (timestamp)

2. Таблица video_snapshots:
   - id (text) - ID снапшота
   - video_id (text) - ссылка на видео
   - views_count, likes_count, comments_count, reports_count (bigint)
   - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (bigint) - приращения
   - created_at (timestamp) - время замера

ВАЖНЫЕ ПРАВИЛА:
1. Для дат используй DATE() для сравнения дат без времени
2. Всегда подставляй КОНКРЕТНЫЕ значения из запроса в SQL
3. НЕ используй параметры типа :param_name
4. Для диапазонов дат используй BETWEEN
5. Всегда возвращай запрос, который возвращает ОДНО число

Примеры:
Вопрос: Сколько всего видео есть в системе?
SQL: SELECT COUNT(*) FROM videos;

Вопрос: Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: Сколько видео набрало больше 100000 просмотров за всё время?
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
SQL: SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: Сколько разных видео получали новые просмотры 27 ноября 2025?
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

ВОЗВРАЩАЙ ТОЛЬКО SQL ЗАПРОС, БЕЗ ОБЪЯСНЕНИЙ!"""

    def extract_parameters(self, query: str) -> Dict[str, Any]:
        """Извлечение параметров из запроса для помощи модели"""
        params = {}
        
        # Извлекаем ID креатора
        id_match = re.search(r'id\s+([\w-]+)', query.lower())
        if id_match:
            params['creator_id'] = id_match.group(1)
        
        # Извлекаем числа
        numbers = re.findall(r'\b\d[\d\s]*\d\b', query)
        if numbers:
            # Убираем пробелы в числах (типа "100 000")
            clean_numbers = [n.replace(' ', '').replace(',', '').replace('.', '') for n in numbers]
            params['numbers'] = [int(n) for n in clean_numbers if n.isdigit()]
        
        # Извлекаем даты
        months = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12',
            'январь': '01', 'февраль': '02', 'март': '03', 'апрель': '04',
            'май': '05', 'июнь': '06', 'июль': '07', 'август': '08',
            'сентябрь': '09', 'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'
        }
        
        # Паттерн для одной даты
        date_match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', query.lower())
        if date_match:
            day = date_match.group(1).zfill(2)
            month_str = date_match.group(2)
            year = date_match.group(3)
            
            if month_str in months:
                params['date'] = f"{year}-{months[month_str]}-{day}"
        
        return params
    
    def parse_query_to_sql(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """Основной метод преобразования запроса в SQL"""
        print(f"\n📝 Запрос к LLM: {query}")
        
        # Извлекаем параметры для информативности
        extracted_params = self.extract_parameters(query)
        if extracted_params:
            print(f"🔍 Извлечены параметры: {extracted_params}")
        
        try:
            # Запрос к Mistral API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": query}
                ],
                temperature=0.1,
                max_tokens=200
            )
            
            # Получаем и чистим SQL
            sql_query = response.choices[0].message.content.strip()
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            
            # Убираем возможные параметры
            sql_query = re.sub(r':\w+', 'NULL', sql_query)
            
            print(f"✅ LLM сгенерировал SQL: {sql_query}")
            
            # Проверяем, что запрос корректный
            if not any(keyword in sql_query.upper() for keyword in ['SELECT', 'COUNT', 'SUM']):
                print("⚠️ LLM вернул некорректный SQL, использую fallback")
                return self._generate_fallback_sql(query, extracted_params), {}
            
            return sql_query, {}
            
        except Exception as e:
            print(f"❌ Ошибка LLM API: {e}")
            print("🔄 Использую fallback парсинг...")
            return self._generate_fallback_sql(query, extracted_params), {}
    
    def _generate_fallback_sql(self, query: str, params: Dict[str, Any]) -> str:
        """Генерация SQL при ошибке API"""
        query_lower = query.lower()
        
        # Базовые правила fallback
        if 'сколько всего видео' in query_lower:
            return "SELECT COUNT(*) FROM videos"
        
        elif 'сколько видео у креатора' in query_lower or 'креатора с id' in query_lower:
            if 'creator_id' in params:
                return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{params['creator_id']}'"
        
        elif 'сколько видео набрало больше' in query_lower and 'просмотров' in query_lower:
            if 'numbers' in params and params['numbers']:
                return f"SELECT COUNT(*) FROM videos WHERE views_count > {params['numbers'][0]}"
        
        elif 'на сколько просмотров' in query_lower and 'выросли' in query_lower:
            if 'date' in params:
                return f"SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = '{params['date']}'"
        
        elif 'сколько разных видео получали новые просмотры' in query_lower:
            if 'date' in params:
                return f"SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '{params['date']}' AND delta_views_count > 0"
        
        # Самый простой fallback
        return "SELECT COUNT(*) FROM videos"