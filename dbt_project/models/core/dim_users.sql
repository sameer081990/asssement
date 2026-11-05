select user_id, signup_date, country
from {{ ref('stg_users') }}
