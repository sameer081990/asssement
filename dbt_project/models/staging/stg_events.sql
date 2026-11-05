{#
  Typed + normalised events. Every row is kept; invalid rows get a reject_reason
  (the first failing rule) so nothing is silently dropped. Downstream:
    core.fact_events        -> reject_reason is null
    core.quarantined_events -> reject_reason is not null
#}
with norm as (
    select
        lower(nullif(trim(event_type), ''))                                   as event_type,
        nullif(trim(user_id), '')                                             as user_id,
        nullif(trim(episode_id), '')                                          as episode_id,
        try_cast(nullif(trim("timestamp"), '') as timestamp)                  as event_ts,
        try_cast(try_cast(nullif(trim(duration), '') as double) as integer)   as duration_seconds,
        event_type  as raw_event_type,
        user_id     as raw_user_id,
        episode_id  as raw_episode_id,
        "timestamp" as raw_timestamp,
        duration    as raw_duration,
        _source_file,
        _loaded_at
    from {{ source('raw', 'event_logs') }}
)

select
    n.*,
    case
        when n.event_type is null and n.user_id is null and n.episode_id is null
             and n.event_ts is null and nullif(trim(n.raw_timestamp), '') is null
            then 'empty_or_unparseable_record'
        when n.user_id is null                                         then 'missing_user_id'
        when n.episode_id is null                                      then 'missing_episode_id'
        when n.event_type is null
             or n.event_type not in ('play', 'pause', 'seek', 'complete') then 'invalid_event_type'
        when n.event_ts is null                                        then 'malformed_timestamp'
        when n.event_ts < cast('{{ var("min_event_date") }}' as timestamp)
             or n.event_ts > current_timestamp + interval 1 day        then 'timestamp_out_of_range'
        when u.user_id is null                                         then 'unknown_user'
        when e.episode_id is null                                      then 'unknown_episode'
        when n.event_type in ('play', 'complete')
             and n.duration_seconds is null                            then 'missing_duration'
        when n.duration_seconds < 0                                    then 'negative_duration'
    end as reject_reason
from norm n
left join {{ ref('stg_users') }}    u on n.user_id    = u.user_id
left join {{ ref('stg_episodes') }} e on n.episode_id = e.episode_id
