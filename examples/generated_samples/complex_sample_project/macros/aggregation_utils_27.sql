-- Complex aggregation macro with multiple metrics
{% macro aggregation_utils_27(column_name, metrics=['count', 'sum', 'avg']) %}
    {% for metric in metrics %}
        {% if metric == 'count' %}
            count({{ column_name }}) as {{ column_name }}_count
        {% elif metric == 'sum' %}
            sum({{ column_name }}) as {{ column_name }}_sum
        {% elif metric == 'avg' %}
            avg({{ column_name }}) as {{ column_name }}_avg
        {% elif metric == 'min' %}
            min({{ column_name }}) as {{ column_name }}_min
        {% elif metric == 'max' %}
            max({{ column_name }}) as {{ column_name }}_max
        {% elif metric == 'distinct_count' %}
            count(distinct {{ column_name }}) as {{ column_name }}_distinct_count
        {% elif metric == 'stddev' %}
            stddev({{ column_name }}) as {{ column_name }}_stddev
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
