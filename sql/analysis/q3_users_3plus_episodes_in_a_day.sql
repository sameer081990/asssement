-- Distinct users who listened to 3+ different episodes on a single day.
-- "Listened" = any play or complete event for the episode.
WITH per_day AS (
    SELECT f.user_id, f.event_date, count(DISTINCT f.episode_id) AS episodes
    FROM core.fact_events f
    WHERE f.event_type IN ('play', 'complete')
    GROUP BY f.user_id, f.event_date
    HAVING count(DISTINCT f.episode_id) >= 3
)
SELECT count(DISTINCT user_id) AS users_with_3plus_episodes_in_a_day
FROM per_day;
