{# Analysis-ready event model: valid events joined to user and episode attributes. #}
{{ config(materialized='view') }}

select
    f.event_id,
    f.event_type,
    f.event_ts,
    f.event_date,
    f.duration_seconds,
    f.user_id,
    u.country,
    u.signup_date,
    f.episode_id,
    e.podcast_id,
    e.episode_title,
    e.release_date,
    e.duration_seconds as episode_duration_seconds
from {{ ref('fact_events') }} f
join {{ ref('dim_users') }}    u using (user_id)
join {{ ref('dim_episodes') }} e using (episode_id)
