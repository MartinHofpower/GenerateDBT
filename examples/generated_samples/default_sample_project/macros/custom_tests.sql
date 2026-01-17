-- Medium complexity custom test macro
{% test custom_tests(model, column_name, threshold=0) %}

with validation as (
    select
        count(*) as total_rows,
        count({{ column_name }}) as non_null_rows
    from {{ model }}
)

select *
from validation
where (total_rows - non_null_rows) > {{ threshold }}

{% endtest %}
