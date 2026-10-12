-- Phạm Văn Kiên - 2A202602590
-- Trip nào có tỷ lệ khung bị loại cao nhất, cần kiểm tra chất lượng trước?
SELECT drive_id, trip_id, lighting, conditions, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct,
       avg_speed_kmh, split
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, frames_dropped DESC, trip_id;
