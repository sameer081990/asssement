{{ config(severity='warn') }}
select f.* from {{ ref('fact_events') }} f
join {{ ref('dim_episodes') }} e using (episode_id)
where f.event_type = 'complete' and f.duration_seconds > e.duration_seconds
