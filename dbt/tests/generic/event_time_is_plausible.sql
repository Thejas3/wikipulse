{% test event_time_is_plausible(model, column_name, processed_at, max_age_days=7, max_future_minutes=1) %}
-- PRD §8: event_time within the last 7 days and not in the future, both judged against
-- when the row was processed (so the test keeps passing on old data in the table).
select *
from {{ model }}
where {{ column_name }} > {{ processed_at }} + interval {{ max_future_minutes }} minute
   or {{ column_name }} < {{ processed_at }} - interval {{ max_age_days }} day
{% endtest %}
