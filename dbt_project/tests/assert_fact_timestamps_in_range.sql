-- error: no loaded event outside [min_event_date, tomorrow]
select * from {{ ref('fact_events') }}
where event_ts < cast('{{ var("min_event_date") }}' as timestamp)
   or event_ts > current_timestamp + interval 1 day
