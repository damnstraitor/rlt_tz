import json
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from datetime import datetime
from .models import Base, Video, VideoSnapshot
from config import config
import pytz

class DatabaseInitializer:
    def __init__(self):
        self.engine = create_async_engine(
            config.database_url.replace("postgresql://", "postgresql+asyncpg://"),
            echo=False
        )
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
    
    async def create_tables(self):
        """Создание таблиц в базе данных"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def load_json_data(self, json_file_path: str):
        """Загрузка данных из JSON файла с проверкой дубликатов"""
        try:
            print(f"📂 Загружаем JSON файл: {json_file_path}")
            
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # ВАЖНО: JSON может быть объектом с ключом "videos"
            if isinstance(data, dict) and 'videos' in data:
                print(f"✅ Найден ключ 'videos' в JSON объекте")
                videos_list = data['videos']
            elif isinstance(data, list):
                print(f"✅ JSON является массивом")
                videos_list = data
            else:
                print(f"❌ Неизвестный формат JSON: {type(data)}")
                print(f"   Доступные ключи: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                return
            
            print(f"📊 Количество видео для обработки: {len(videos_list)}")
            
            async with self.async_session() as session:
                videos_processed = 0
                snapshots_processed = 0
                duplicates_skipped = 0
                
                for i, video_data in enumerate(videos_list):
                    try:
                        # Проверяем структуру видео
                        if not isinstance(video_data, dict):
                            print(f"⚠️ Пропускаем элемент {i}: не словарь (тип: {type(video_data)})")
                            continue
                        
                        if 'id' not in video_data:
                            print(f"⚠️ Пропускаем элемент {i}: нет поля 'id'")
                            continue
                        
                        video_id = str(video_data['id'])
                        
                        # Проверяем, не существует ли уже это видео
                        existing = await session.execute(
                            text("SELECT id FROM videos WHERE id = :video_id"),
                            {"video_id": video_id}
                        )
                        
                        if existing.fetchone():
                            duplicates_skipped += 1
                            if duplicates_skipped <= 5:  # Показываем только первые 5 дубликатов
                                print(f"⚠️ Видео {video_id} уже существует, пропускаем")
                            continue
                        
                        # Создаем видео
                        video = Video(
                            id=video_id,
                            creator_id=str(video_data.get('creator_id', 'unknown')),
                            video_created_at=self.parse_datetime(video_data.get('video_created_at')),
                            views_count=int(video_data.get('views_count', 0)),
                            likes_count=int(video_data.get('likes_count', 0)),
                            comments_count=int(video_data.get('comments_count', 0)),
                            reports_count=int(video_data.get('reports_count', 0)),
                            created_at=self.parse_datetime(video_data.get('created_at')),
                            updated_at=self.parse_datetime(video_data.get('updated_at'))
                        )
                        
                        session.add(video)
                        
                        # Добавляем снапшоты если есть
                        snapshots = video_data.get('snapshots', [])
                        if isinstance(snapshots, list):
                            for j, snapshot_data in enumerate(snapshots):
                                if isinstance(snapshot_data, dict):
                                    snapshot = VideoSnapshot(
                                        id=str(snapshot_data.get('id', f"snap_{video_id}_{j}")),
                                        video_id=video_id,
                                        views_count=int(snapshot_data.get('views_count', 0)),
                                        likes_count=int(snapshot_data.get('likes_count', 0)),
                                        comments_count=int(snapshot_data.get('comments_count', 0)),
                                        reports_count=int(snapshot_data.get('reports_count', 0)),
                                        delta_views_count=int(snapshot_data.get('delta_views_count', 0)),
                                        delta_likes_count=int(snapshot_data.get('delta_likes_count', 0)),
                                        delta_comments_count=int(snapshot_data.get('delta_comments_count', 0)),
                                        delta_reports_count=int(snapshot_data.get('delta_reports_count', 0)),
                                        created_at=self.parse_datetime(snapshot_data.get('created_at')),
                                        updated_at=self.parse_datetime(snapshot_data.get('updated_at', datetime.utcnow()))
                                    )
                                    session.add(snapshot)
                                    snapshots_processed += 1
                        
                        videos_processed += 1
                        
                        # Коммитим каждые 20 видео для производительности
                        if videos_processed % 20 == 0:
                            await session.commit()
                            print(f"🔄 Обработано {videos_processed} видео и {snapshots_processed} снапшотов...")
                            
                    except Exception as e:
                        print(f"❌ Ошибка при обработке видео {i}: {e}")
                        print(f"   ID видео: {video_data.get('id', 'unknown')}")
                        # Откатываем транзакцию и продолжаем
                        await session.rollback()
                        continue
                
                # Финальный коммит
                await session.commit()
                print(f"✅ Всего обработано: {videos_processed} видео и {snapshots_processed} снапшотов")
                if duplicates_skipped > 0:
                    print(f"⚠️ Пропущено дубликатов: {duplicates_skipped}")
                
        except FileNotFoundError:
            print(f"❌ Файл не найден: {json_file_path}")
            print("Создайте папку 'data' и поместите туда videos_data.json")
            
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON: {e}")
            print(f"Строка {e.lineno}, столбец {e.colno}: {e.msg}")
            
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    def parse_datetime(self, dt_str: str) -> datetime:
        """Парсинг строки даты-времени"""
        if not dt_str:
            return datetime.utcnow()
        
        try:
            # Различные форматы дат
            formats = [
                '%Y-%m-%dT%H:%M:%S.%f%z',  # 2025-11-26T11:00:08.983295+00:00
                '%Y-%m-%dT%H:%M:%S%z',     # 2025-08-19T08:54:35+00:00
                '%Y-%m-%dT%H:%M:%S.%f',    # 2025-11-26T11:00:08.983295
                '%Y-%m-%dT%H:%M:%S',       # 2025-11-26T11:00:09
                '%Y-%m-%d %H:%M:%S',       # 2025-11-26 11:00:09
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    # Если есть информация о временной зоне, конвертируем в UTC
                    if dt.tzinfo is not None:
                        dt = dt.astimezone(pytz.UTC).replace(tzinfo=None)
                    return dt
                except ValueError:
                    continue
            
            # Если ни один формат не подошел, возвращаем текущее время
            print(f"⚠️ Не удалось распарсить дату: {dt_str}")
            return datetime.utcnow()
            
        except Exception as e:
            print(f"⚠️ Ошибка парсинга даты '{dt_str}': {e}")
            return datetime.utcnow()
    
    async def initialize(self, json_file_path: str):
        """Полная инициализация базы данных"""
        print("Creating tables...")
        await self.create_tables()
        
        print("Loading JSON data...")
        await self.load_json_data(json_file_path)
        
        print("Database initialization completed!")
    
    async def close(self):
        """Закрытие соединения"""
        await self.engine.dispose()