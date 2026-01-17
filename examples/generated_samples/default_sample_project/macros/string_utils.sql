-- Medium complexity string utility macro
{% macro string_utils(column_name, operation='upper') %}
    {% if operation == 'upper' %}
        upper({{ column_name }})
    {% elif operation == 'lower' %}
        lower({{ column_name }})
    {% elif operation == 'trim' %}
        trim({{ column_name }})
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}
