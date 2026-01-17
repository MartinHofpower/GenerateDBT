-- Complex data quality scoring macro
{% macro data_quality_7(table_name, quality_checks) %}
    {% set check_clauses = [] %}
    
    {% for check in quality_checks %}
        {% if check.type == 'not_null' %}
            {% set clause = "sum(case when " ~ check.column ~ " is null then 0 else 1 end) * 1.0 / count(*) as " ~ check.column ~ "_not_null_score" %}
        {% elif check.type == 'unique' %}
            {% set clause = "count(distinct " ~ check.column ~ ") * 1.0 / count(*) as " ~ check.column ~ "_unique_score" %}
        {% elif check.type == 'completeness' %}
            {% set clause = "sum(case when " ~ check.column ~ " is not null and trim(" ~ check.column ~ ") != '' then 1 else 0 end) * 1.0 / count(*) as " ~ check.column ~ "_completeness_score" %}
        {% endif %}
        {% do check_clauses.append(clause) %}
    {% endfor %}
    
    select
        '{{ table_name }}' as table_name,
        count(*) as total_rows,
        {% for clause in check_clauses %}
            {{ clause }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref(table_name) }}
{% endmacro %}
