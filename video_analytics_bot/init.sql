-- Создание индексов для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_videos_views_count ON videos(views_count);

CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_delta_views ON video_snapshots(delta_views_count);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at_delta ON video_snapshots(created_at, delta_views_count);