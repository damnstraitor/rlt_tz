import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from config import config

async def reset_database():
    """Полная очистка базы данных"""
    engine = create_async_engine(
        config.database_url.replace("postgresql://", "postgresql+asyncpg://"),
        echo=True
    )
    
    async with engine.begin() as conn:
        print("🔄 Очищаю базу данных...")
        
        # Удаляем таблицы с каскадом
        await conn.execute(text("DROP TABLE IF EXISTS video_snapshots CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS videos CASCADE"))
        
        print("✅ Таблицы удалены")
    
    await engine.dispose()
    print("🎯 База данных готова для новой загрузки")

if __name__ == "__main__":
    asyncio.run(reset_database())