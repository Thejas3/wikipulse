{% snapshot dim_user %}

{{
  config(
    unique_key='user_key',
    strategy='check',
    check_cols=['is_bot', 'is_anonymous'],
  )
}}

-- SCD Type 2 (PRD §7): when an editor's bot flag changes, the old row is closed (dbt_valid_to)
-- and a new one opened, so past edits keep the status the editor had when they made them.
-- Each run snapshots every editor's most recent status.
select user_key, user_name, is_anonymous, is_bot
from (
    select
        user_key, user_name, is_anonymous, is_bot,
        row_number() over (partition by user_key order by event_time desc, edit_id desc) as rn
    from {{ ref('fct_edits') }}
    where user_name is not null
)
where rn = 1

{% endsnapshot %}
