select
    trim(episode_id)                           as episode_id,
    trim(podcast_id)                           as podcast_id,
    title                                      as episode_title,
    try_cast(release_date as date)             as release_date,
    try_cast(duration_seconds as integer)      as duration_seconds
from read_csv('{{ var("episodes_csv") }}', header = true, all_varchar = true)
where nullif(trim(episode_id), '') is not null
  and try_cast(duration_seconds as integer) > 0
