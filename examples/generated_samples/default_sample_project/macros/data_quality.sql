-- Medium complexity data quality macro
{% macro data_quality(column_name, check_type='not_null') %}
    {% if check_type == 'not_null' %}
        case when {{ column_name }} is null then 0 else 1 end
    {% elif check_type == 'not_empty' %}
        case when {{ column_name }} is null or trim({{ column_name }}) = '' then 0 else 1 end
    {% elif check_type == 'positive' %}
        case when {{ column_name }} <= 0 then 0 else 1 end
    {% else %}
        1
    {% endif %}
{% endmacro %}
