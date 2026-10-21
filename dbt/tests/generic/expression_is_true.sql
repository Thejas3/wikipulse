{% test expression_is_true(model, expression, column_name=none) %}
-- Fails with the rows where the expression is false or null.
select * from {{ model }} where not coalesce(({{ expression }}), false)
{% endtest %}
