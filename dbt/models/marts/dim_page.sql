-- One row per page that has ever been edited.
select
    page_key,
    wiki,
    any_value(namespace) as namespace,
    title,
    min(event_time) as first_seen,
    max(event_time) as last_seen
from {{ ref('fct_edits') }}
group by page_key, wiki, title
