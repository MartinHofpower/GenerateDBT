"""Generate dbt macro files."""
from pathlib import Path
from typing import List
from .config import GeneratorConfig, ComplexityLevel


class MacroGenerator:
    """Generator for dbt macro files."""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
    
    def generate_macros(self, output_dir: Path) -> List[str]:
        """Generate all macro files.
        
        Args:
            output_dir: Root directory for the dbt project
            
        Returns:
            List of generated macro file paths
        """
        macros_dir = output_dir / "macros"
        macros_dir.mkdir(parents=True, exist_ok=True)
        
        generated_files = []
        
        # Generate various types of macros based on count
        macro_types = [
            ("string_utils", self._generate_string_utils_macro),
            ("date_utils", self._generate_date_utils_macro),
            ("aggregation_utils", self._generate_aggregation_macro),
            ("custom_tests", self._generate_custom_test_macro),
            ("data_quality", self._generate_data_quality_macro),
        ]
        
        for i in range(self.config.num_macros):
            macro_type, generator_func = macro_types[i % len(macro_types)]
            macro_name = f"{macro_type}_{(i // len(macro_types)) + 1}" if i >= len(macro_types) else macro_type
            
            content = generator_func(macro_name)
            file_path = macros_dir / f"{macro_name}.sql"
            
            with open(file_path, "w") as f:
                f.write(content)
            generated_files.append(str(file_path))
        
        return generated_files
    
    def _generate_string_utils_macro(self, macro_name: str) -> str:
        """Generate a string utilities macro."""
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return f"""-- Simple string utility macro
{{% macro {macro_name}(column_name) %}}
    upper({{{{ column_name }}}})
{{% endmacro %}}
"""
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return f"""-- Medium complexity string utility macro
{{% macro {macro_name}(column_name, operation='upper') %}}
    {{% if operation == 'upper' %}}
        upper({{{{ column_name }}}})
    {{% elif operation == 'lower' %}}
        lower({{{{ column_name }}}})
    {{% elif operation == 'trim' %}}
        trim({{{{ column_name }}}})
    {{% else %}}
        {{{{ column_name }}}}
    {{% endif %}}
{{% endmacro %}}
"""
        else:
            return f"""-- Complex string utility macro with multiple operations
{{% macro {macro_name}(column_name, operations=['trim', 'upper']) %}}
    {{% set result = column_name %}}
    {{% for operation in operations %}}
        {{% if operation == 'upper' %}}
            {{% set result = 'upper(' ~ result ~ ')' %}}
        {{% elif operation == 'lower' %}}
            {{% set result = 'lower(' ~ result ~ ')' %}}
        {{% elif operation == 'trim' %}}
            {{% set result = 'trim(' ~ result ~ ')' %}}
        {{% elif operation == 'initcap' %}}
            {{% set result = 'initcap(' ~ result ~ ')' %}}
        {{% endif %}}
    {{% endfor %}}
    {{{{ result }}}}
{{% endmacro %}}
"""
    
    def _generate_date_utils_macro(self, macro_name: str) -> str:
        """Generate a date utilities macro."""
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return f"""-- Simple date utility macro
{{% macro {macro_name}(date_column) %}}
    date({{{{ date_column }}}})
{{% endmacro %}}
"""
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return f"""-- Medium complexity date utility macro
{{% macro {macro_name}(date_column, date_part='day') %}}
    {{% if date_part == 'day' %}}
        date({{{{ date_column }}}})
    {{% elif date_part == 'month' %}}
        date_trunc('month', {{{{ date_column }}}})
    {{% elif date_part == 'year' %}}
        date_trunc('year', {{{{ date_column }}}})
    {{% else %}}
        {{{{ date_column }}}}
    {{% endif %}}
{{% endmacro %}}
"""
        else:
            return f"""-- Complex date utility macro with business logic
{{% macro {macro_name}(date_column, operation='truncate', grain='day') %}}
    {{% if operation == 'truncate' %}}
        {{% if grain == 'day' %}}
            date({{{{ date_column }}}})
        {{% elif grain == 'week' %}}
            date_trunc('week', {{{{ date_column }}}})
        {{% elif grain == 'month' %}}
            date_trunc('month', {{{{ date_column }}}})
        {{% elif grain == 'quarter' %}}
            date_trunc('quarter', {{{{ date_column }}}})
        {{% elif grain == 'year' %}}
            date_trunc('year', {{{{ date_column }}}})
        {{% endif %}}
    {{% elif operation == 'extract' %}}
        extract({{{{ grain }}}} from {{{{ date_column }}}})
    {{% elif operation == 'age' %}}
        datediff(day, {{{{ date_column }}}}, current_date())
    {{% else %}}
        {{{{ date_column }}}}
    {{% endif %}}
{{% endmacro %}}
"""
    
    def _generate_aggregation_macro(self, macro_name: str) -> str:
        """Generate an aggregation utilities macro."""
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return f"""-- Simple aggregation macro
{{% macro {macro_name}(column_name) %}}
    count({{{{ column_name }}}})
{{% endmacro %}}
"""
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return f"""-- Medium complexity aggregation macro
{{% macro {macro_name}(column_name, agg_type='count') %}}
    {{% if agg_type == 'count' %}}
        count({{{{ column_name }}}})
    {{% elif agg_type == 'sum' %}}
        sum({{{{ column_name }}}})
    {{% elif agg_type == 'avg' %}}
        avg({{{{ column_name }}}})
    {{% elif agg_type == 'distinct_count' %}}
        count(distinct {{{{ column_name }}}})
    {{% else %}}
        count({{{{ column_name }}}})
    {{% endif %}}
{{% endmacro %}}
"""
        else:
            return f"""-- Complex aggregation macro with multiple metrics
{{% macro {macro_name}(column_name, metrics=['count', 'sum', 'avg']) %}}
    {{% for metric in metrics %}}
        {{% if metric == 'count' %}}
            count({{{{ column_name }}}}) as {{{{ column_name }}}}_count
        {{% elif metric == 'sum' %}}
            sum({{{{ column_name }}}}) as {{{{ column_name }}}}_sum
        {{% elif metric == 'avg' %}}
            avg({{{{ column_name }}}}) as {{{{ column_name }}}}_avg
        {{% elif metric == 'min' %}}
            min({{{{ column_name }}}}) as {{{{ column_name }}}}_min
        {{% elif metric == 'max' %}}
            max({{{{ column_name }}}}) as {{{{ column_name }}}}_max
        {{% elif metric == 'distinct_count' %}}
            count(distinct {{{{ column_name }}}}) as {{{{ column_name }}}}_distinct_count
        {{% elif metric == 'stddev' %}}
            stddev({{{{ column_name }}}}) as {{{{ column_name }}}}_stddev
        {{% endif %}}
        {{% if not loop.last %}},{{% endif %}}
    {{% endfor %}}
{{% endmacro %}}
"""
    
    def _generate_custom_test_macro(self, macro_name: str) -> str:
        """Generate a custom test macro."""
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return f"""-- Simple custom test macro
{{% test {macro_name}(model, column_name) %}}

select *
from {{{{ model }}}}
where {{{{ column_name }}}} is null

{{% endtest %}}
"""
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return f"""-- Medium complexity custom test macro
{{% test {macro_name}(model, column_name, threshold=0) %}}

with validation as (
    select
        count(*) as total_rows,
        count({{{{ column_name }}}}) as non_null_rows
    from {{{{ model }}}}
)

select *
from validation
where (total_rows - non_null_rows) > {{{{ threshold }}}}

{{% endtest %}}
"""
        else:
            return f"""-- Complex custom test macro with detailed validation
{{% test {macro_name}(model, column_name, min_value=0, max_value=100) %}}

with validation_stats as (
    select
        {{{{ column_name }}}},
        count(*) as row_count
    from {{{{ model }}}}
    where {{{{ column_name }}}} is not null
    group by {{{{ column_name }}}}
),

invalid_values as (
    select
        {{{{ column_name }}}},
        row_count,
        case
            when {{{{ column_name }}}} < {{{{ min_value }}}} then 'below_minimum'
            when {{{{ column_name }}}} > {{{{ max_value }}}} then 'above_maximum'
            else 'valid'
        end as validation_status
    from validation_stats
)

select *
from invalid_values
where validation_status != 'valid'

{{% endtest %}}
"""
    
    def _generate_data_quality_macro(self, macro_name: str) -> str:
        """Generate a data quality macro."""
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return f"""-- Simple data quality check macro
{{% macro {macro_name}(column_name) %}}
    case when {{{{ column_name }}}} is null then 0 else 1 end
{{% endmacro %}}
"""
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return f"""-- Medium complexity data quality macro
{{% macro {macro_name}(column_name, check_type='not_null') %}}
    {{% if check_type == 'not_null' %}}
        case when {{{{ column_name }}}} is null then 0 else 1 end
    {{% elif check_type == 'not_empty' %}}
        case when {{{{ column_name }}}} is null or trim({{{{ column_name }}}}) = '' then 0 else 1 end
    {{% elif check_type == 'positive' %}}
        case when {{{{ column_name }}}} <= 0 then 0 else 1 end
    {{% else %}}
        1
    {{% endif %}}
{{% endmacro %}}
"""
        else:
            return f"""-- Complex data quality scoring macro
{{% macro {macro_name}(table_name, quality_checks) %}}
    {{% set check_clauses = [] %}}
    
    {{% for check in quality_checks %}}
        {{% if check.type == 'not_null' %}}
            {{% set clause = "sum(case when " ~ check.column ~ " is null then 0 else 1 end) * 1.0 / count(*) as " ~ check.column ~ "_not_null_score" %}}
        {{% elif check.type == 'unique' %}}
            {{% set clause = "count(distinct " ~ check.column ~ ") * 1.0 / count(*) as " ~ check.column ~ "_unique_score" %}}
        {{% elif check.type == 'completeness' %}}
            {{% set clause = "sum(case when " ~ check.column ~ " is not null and trim(" ~ check.column ~ ") != '' then 1 else 0 end) * 1.0 / count(*) as " ~ check.column ~ "_completeness_score" %}}
        {{% endif %}}
        {{% do check_clauses.append(clause) %}}
    {{% endfor %}}
    
    select
        '{{{{ table_name }}}}' as table_name,
        count(*) as total_rows,
        {{% for clause in check_clauses %}}
            {{{{ clause }}}}{{% if not loop.last %}},{{% endif %}}
        {{% endfor %}}
    from {{{{ ref(table_name) }}}}
{{% endmacro %}}
"""
