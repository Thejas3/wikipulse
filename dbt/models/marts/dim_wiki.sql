-- One row per wiki seen in the data, with a readable language name from the seed where known.
with wikis as (
    select distinct wiki_key, wiki, language
    from {{ ref('fct_edits') }}
)

select
    w.wiki_key,
    w.wiki,
    w.language as language_code,
    coalesce(s.language_name, w.language) as language,
    'wikipedia' as project
from wikis w
left join {{ ref('wikis') }} s on s.language_code = w.language
