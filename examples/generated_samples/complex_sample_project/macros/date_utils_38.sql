-- Complex date utility macro with business logic
{% macro date_utils_38(date_column, operation='truncate', grain='day') %}
    {% if operation == 'truncate' %}
        {% if grain == 'day' %}
            date({{ date_column }})
        {% elif grain == 'week' %}
            date_trunc('week', {{ date_column }})
        {% elif grain == 'month' %}
            date_trunc('month', {{ date_column }})
        {% elif grain == 'quarter' %}
            date_trunc('quarter', {{ date_column }})
        {% elif grain == 'year' %}
            date_trunc('year', {{ date_column }})
        {% endif %}
    {% elif operation == 'extract' %}
        extract({{ grain }} from {{ date_column }})
    {% elif operation == 'age' %}
        datediff(day, {{ date_column }}, current_date())
    {% else %}
        {{ date_column }}
    {% endif %}
{% endmacro %}
