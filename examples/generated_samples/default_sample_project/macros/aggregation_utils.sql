-- Medium complexity aggregation macro
{% macro aggregation_utils(column_name, agg_type='count') %}
    {% if agg_type == 'count' %}
        count({{ column_name }})
    {% elif agg_type == 'sum' %}
        sum({{ column_name }})
    {% elif agg_type == 'avg' %}
        avg({{ column_name }})
    {% elif agg_type == 'distinct_count' %}
        count(distinct {{ column_name }})
    {% else %}
        count({{ column_name }})
    {% endif %}
{% endmacro %}
