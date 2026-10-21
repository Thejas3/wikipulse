{{
  config(
    materialized='incremental',
    unique_key='edit_id',
    incremental_strategy='delete+insert',
  )
}}

-- One row per edit, with a heuristic vandalism score (PRD §6.4). Weights, out of 100:
--   anonymous editor (IP or temporary account)                          30
--   blanking: removes >= 500 bytes, or >= 80% of a page over 1,000 bytes  30
--   page under a revert war: >= 2 revert-like edits on it in the last hour  20
--   edit burst: the same human made >= 10 edits in the last 10 minutes      20
-- Bot edits score 0. A "revert-like" edit restores the page length that it had right
-- before a different user's change (Silver carries no edit comments to detect reverts by text).
-- Incremental runs reprocess the last hour so the windowed signals see their full context;
-- delete+insert on edit_id keeps that overlap idempotent.

{% set lookback = "interval 1 hour" %}

with source as (
    select * from {{ ref('stg_edits') }}
    {% if is_incremental() %}
    where event_date >= (select cast(max(event_time) - {{ lookback }} - interval 1 hour as date) from {{ this }})
      and event_time > (select max(event_time) - {{ lookback }} - interval 1 hour from {{ this }})
    {% endif %}
),

with_prev as (
    select
        *,
        lag(length_old) over page_order as prev_length_old,
        lag(user_name) over page_order as prev_user
    from source
    window page_order as (partition by page_key order by event_time, edit_id)
),

signals as (
    select
        *,
        -- coalesce: page creations have no length_old, and a NULL signal would make the
        -- whole score NULL (and DuckDB's least(100, NULL) returns 100).
        coalesce(prev_length_old is not null and length_new = prev_length_old and user_name <> prev_user
            and edit_type = 'edit', false) as is_revert_like,
        coalesce(byte_delta <= -500 or (length_old >= 1000 and length_new <= 0.2 * length_old), false)
            as is_blanking
    from with_prev
),

windowed as (
    select
        *,
        count(*) filter (where is_revert_like) over (
            partition by page_key order by event_time
            range between interval 1 hour preceding and current row
        ) as page_reverts_last_hour,
        count(*) over (
            partition by user_key order by event_time
            range between interval 10 minutes preceding and current row
        ) as user_edits_last_10min
    from signals
)

select
    edit_id,
    event_time,
    event_date,
    page_key,
    user_key,
    wiki_key,
    wiki,
    domain,
    language,
    namespace,
    title,
    user_name,
    edit_type,
    is_article,
    is_bot,
    is_minor,
    is_anonymous,
    byte_delta,
    length_old,
    length_new,
    revision_old,
    revision_new,
    page_url,
    is_revert_like,
    is_blanking,
    page_reverts_last_hour,
    user_edits_last_10min,
    case when is_bot then 0 else least(100,
          30 * cast(coalesce(is_anonymous, false) as int)
        + 30 * cast(is_blanking as int)
        + 20 * cast(page_reverts_last_hour >= 2 as int)
        + 20 * cast(user_edits_last_10min >= 10 as int)
    ) end as vandalism_score
from windowed
{% if is_incremental() %}
-- Only (re)write rows inside the reprocessed window that come after the context hour.
where event_time > (select max(event_time) - {{ lookback }} from {{ this }})
{% endif %}
