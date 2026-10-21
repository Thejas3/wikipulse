select
    event_date,
    user_name,
    bool_or(is_bot) as is_bot,
    count(*) as edits,
    count(distinct page_key) as pages_touched,
    sum(byte_delta) as net_bytes
from {{ ref('fct_edits') }}
where user_name is not null
group by event_date, user_name
