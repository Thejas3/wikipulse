-- Silver edits with surrogate keys. A view: incremental models filter it by event_date so
-- DuckDB only reads recent Delta files.
select
    edit_id,
    event_time,
    event_date,
    md5(wiki || ':' || title) as page_key,
    md5(coalesce(user_name, '')) as user_key,
    md5(wiki) as wiki_key,
    wiki,
    domain,
    language,
    namespace,
    is_article,
    title,
    user_name,
    is_anonymous,
    is_bot,
    is_minor,
    edit_type,
    byte_delta,
    length_old,
    length_new,
    revision_old,
    revision_new,
    page_url,
    silver_processed_at
from {{ source('lake', 'silver_edits') }}
