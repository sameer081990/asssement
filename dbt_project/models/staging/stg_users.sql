select
    trim(user_id)                      as user_id,
    try_cast(signup_date as date)      as signup_date,
    upper(trim(country))               as country
from read_csv('{{ var("users_csv") }}', header = true, all_varchar = true)
where nullif(trim(user_id), '') is not null
  and try_cast(signup_date as date) is not null
  and nullif(trim(country), '') is not null
