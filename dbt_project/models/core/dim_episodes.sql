select episode_id, podcast_id, episode_title, release_date, duration_seconds
from {{ ref('stg_episodes') }}
