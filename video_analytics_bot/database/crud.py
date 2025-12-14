from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional

class DatabaseManager:
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def execute_custom_query(self, sql_query: str, params: dict = None) -> Optional[int]:
        """Выполнить пользовательский SQL запрос"""
        if params is None:
            params = {}
        
        try:
            result = await self.session.execute(text(sql_query), params)
            row = result.fetchone()
            
            if row:
                value = row[0]
                if value is None:
                    return 0
                
                # Преобразуем в int если возможно
                try:
                    # Пробуем преобразовать в число
                    if isinstance(value, (int, float)):
                        return int(value)
                    else:
                        # Если строка, пробуем преобразовать
                        return int(float(str(value)))
                except (ValueError, TypeError):
                    # Если не число, возвращаем как есть (будет ошибка форматирования)
                    return value
            
            return 0
            
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            print(f"   Query: {sql_query}")
            print(f"   Params: {params}")
            return None