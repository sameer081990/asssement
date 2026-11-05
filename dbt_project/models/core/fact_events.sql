{#
  Grain: one valid user interaction event.
  Incremental on _loaded_at; unique_key = event_id (md5 of user|episode|type|ts) so
  replaying a file, or a duplicate event within/between files, upserts one row.
#}
{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'fail',
    contract = {'enforced': true}
) }}

select
    md5(concat_ws('|', user_id, episode_id, event_type, cast(event_ts as varchar))) as event_id,
    user_id,
    episode_id,
    event_type,
    event_ts,
    cast(event_ts as date)                                                          as event_date,
    case when event_type in ('play', 'complete') then duration_seconds end          as duration_seconds,
    _source_file,
    _loaded_at
from {{ ref('stg_events') }}
where reject_reason is null
{% if is_incremental() %}
  and _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
qualify row_number() over (
    partition by user_id, episode_id, event_type, event_ts
    order by _loaded_at desc
) = 1
