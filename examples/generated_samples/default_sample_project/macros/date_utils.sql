-- Medium complexity date utility macro
{% macro date_utils(date_column, date_part='day') %}
    {% if date_part == 'day' %}
        date({{ date_column }})
    {% elif date_part == 'month' %}
        date_trunc('month', {{ date_column }})
    {% elif date_part == 'year' %}
        date_trunc('year', {{ date_column }})
    {% else %}
        {{ date_column }}
    {% endif %}
{% endmacro %}
