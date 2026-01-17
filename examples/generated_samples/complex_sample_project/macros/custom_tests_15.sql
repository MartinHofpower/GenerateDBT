-- Complex custom test macro with detailed validation
{% test custom_tests_15(model, column_name, min_value=0, max_value=100) %}

with validation_stats as (
    select
        {{ column_name }},
        count(*) as row_count
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
),

invalid_values as (
    select
        {{ column_name }},
        row_count,
        case
            when {{ column_name }} < {{ min_value }} then 'below_minimum'
            when {{ column_name }} > {{ max_value }} then 'above_maximum'
            else 'valid'
        end as validation_status
    from validation_stats
)

select *
from invalid_values
where validation_status != 'valid'

{% endtest %}
