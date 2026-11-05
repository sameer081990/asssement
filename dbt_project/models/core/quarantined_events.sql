{# Rejected rows with the reason, kept for audit / replay. #}
select
    reject_reason,
    raw_event_type, raw_user_id, raw_episode_id, raw_timestamp, raw_duration,
    _source_file, _loaded_at
from {{ ref('stg_events') }}
where reject_reason is not null
