-- Top 10 most completed episodes in the past 7 days.
-- "Today" is anchored to the latest event in the data because the sample set
-- ends 2024-04-30. In production replace the anchor with current_date.
WITH anchor AS (SELECT max(event_date) AS as_of FROM core.fact_events)
SELECT
    d.episode_id,
    d.episode_title,
    d.podcast_id,
    count(*) AS completions
FROM core.fact_events f
JOIN core.dim_episodes d USING (episode_id)
CROSS JOIN anchor a
WHERE f.event_type = 'complete'
  AND f.event_date >  a.as_of - INTERVAL 7 DAY
  AND f.event_date <= a.as_of
GROUP BY d.episode_id, d.episode_title, d.podcast_id
ORDER BY completions DESC, d.episode_id
LIMIT 10;
