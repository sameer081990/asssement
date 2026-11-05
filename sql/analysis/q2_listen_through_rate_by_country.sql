-- Average listen-through rate = completion duration / episode duration, by country.
-- Computed over 'complete' events. The sample data contains completions longer
-- than the episode, so each ratio is capped at 1.0 (see README assumptions).
SELECT
    u.country,
    count(*)                                                       AS completions,
    round(avg(least(f.duration_seconds * 1.0 / d.duration_seconds, 1.0)), 4) AS avg_listen_through_rate,
    round(avg(f.duration_seconds * 1.0 / d.duration_seconds), 4)   AS avg_uncapped_rate
FROM core.fact_events f
JOIN core.dim_users u USING (user_id)
JOIN core.dim_episodes d USING (episode_id)
WHERE f.event_type = 'complete'
  AND f.duration_seconds IS NOT NULL
GROUP BY u.country
ORDER BY avg_listen_through_rate DESC;
