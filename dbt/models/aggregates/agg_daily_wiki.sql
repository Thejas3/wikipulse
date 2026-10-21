select
    event_date,
    wiki,
    count(*) as edits,
    count(distinct user_key) as editors,
    round(avg(cast(is_bot as double)), 4) as bot_share,
    round(avg(cast(is_anonymous as double)), 4) as anon_share
from {{ ref('fct_edits') }}
group by event_date, wiki
