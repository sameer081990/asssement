-- Batch gate (error): fail if more than max_reject_rate of raw events were rejected.
select rejected, total
from (
    select count(*) filter (where reject_reason is not null) as rejected, count(*) as total
    from {{ ref('stg_events') }}
)
where rejected > {{ var('max_reject_rate') }} * total
