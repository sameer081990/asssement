-- error: duration must be present on play/complete and absent on pause/seek
select * from {{ ref('fact_events') }}
where (event_type in ('pause', 'seek')    and duration_seconds is not null)
   or (event_type in ('play', 'complete') and duration_seconds is null)
