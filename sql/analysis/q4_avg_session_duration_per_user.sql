-- Average session duration per user. A session = a user's consecutive events
-- with no gap longer than 30 minutes; duration = last event - first event.
WITH flagged AS (
    SELECT user_id, event_ts,
           CASE WHEN event_ts - lag(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts)
                     <= INTERVAL 30 MINUTE THEN 0 ELSE 1 END AS is_new
    FROM core.fact_events
), numbered AS (
    SELECT *, sum(is_new) OVER (PARTITION BY user_id ORDER BY event_ts) AS session_no
    FROM flagged
), sessions AS (
    SELECT user_id, date_diff('second', min(event_ts), max(event_ts)) AS duration_seconds
    FROM numbered GROUP BY user_id, session_no
)
SELECT user_id, count(*) AS sessions, round(avg(duration_seconds), 1) AS avg_session_seconds
FROM sessions
GROUP BY user_id
ORDER BY avg_session_seconds DESC
LIMIT 20;
