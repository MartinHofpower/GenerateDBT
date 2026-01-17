-- Complex string utility macro with multiple operations
{% macro string_utils_39(column_name, operations=['trim', 'upper']) %}
    {% set result = column_name %}
    {% for operation in operations %}
        {% if operation == 'upper' %}
            {% set result = 'upper(' ~ result ~ ')' %}
        {% elif operation == 'lower' %}
            {% set result = 'lower(' ~ result ~ ')' %}
        {% elif operation == 'trim' %}
            {% set result = 'trim(' ~ result ~ ')' %}
        {% elif operation == 'initcap' %}
            {% set result = 'initcap(' ~ result ~ ')' %}
        {% endif %}
    {% endfor %}
    {{ result }}
{% endmacro %}
