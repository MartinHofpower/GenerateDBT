"""Generate dbt model files."""
import random
from typing import List, Dict, Set
from pathlib import Path
from .config import GeneratorConfig, ComplexityLevel


class ModelGenerator:
    """Generator for dbt model files."""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.model_graph: Dict[str, List[str]] = {}
        self.model_types: Dict[str, str] = {}
        
    def generate_models(self, output_dir: Path) -> List[str]:
        """Generate all model files.
        
        Args:
            output_dir: Root directory for the dbt project
            
        Returns:
            List of generated model file paths
        """
        models_dir = output_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)
        
        generated_files = []
        
        # Generate seeds schema (for reference)
        if self.config.num_seeds > 0:
            seeds_schema = self._generate_seeds_schema()
            with open(models_dir / "seeds_schema.yml", "w") as f:
                f.write(seeds_schema)
            generated_files.append(str(models_dir / "seeds_schema.yml"))
        
        # Plan model structure
        models = self._plan_model_structure()
        
        # Generate staging models
        if self.config.include_staging:
            staging_dir = models_dir / "staging"
            staging_dir.mkdir(exist_ok=True)
            for model_name in models.get("staging", []):
                content = self._generate_staging_model(model_name)
                file_path = staging_dir / f"{model_name}.sql"
                with open(file_path, "w") as f:
                    f.write(content)
                generated_files.append(str(file_path))
        
        # Generate intermediate models
        if self.config.include_intermediate:
            intermediate_dir = models_dir / "intermediate"
            intermediate_dir.mkdir(exist_ok=True)
            for model_name in models.get("intermediate", []):
                content = self._generate_intermediate_model(model_name)
                file_path = intermediate_dir / f"{model_name}.sql"
                with open(file_path, "w") as f:
                    f.write(content)
                generated_files.append(str(file_path))
        
        # Generate mart models
        if self.config.include_marts:
            marts_dir = models_dir / "marts"
            marts_dir.mkdir(exist_ok=True)
            for model_name in models.get("marts", []):
                content = self._generate_mart_model(model_name)
                file_path = marts_dir / f"{model_name}.sql"
                with open(file_path, "w") as f:
                    f.write(content)
                generated_files.append(str(file_path))
        
        # Generate schema.yml for all models
        schema_content = self._generate_schema_yml(models)
        with open(models_dir / "schema.yml", "w") as f:
            f.write(schema_content)
        generated_files.append(str(models_dir / "schema.yml"))
        
        return generated_files
    
    def _plan_model_structure(self) -> Dict[str, List[str]]:
        """Plan the structure of models to generate.
        
        Returns:
            Dictionary mapping model type to list of model names
        """
        models = {
            "staging": [],
            "intermediate": [],
            "marts": []
        }
        
        num_staging = 0
        num_intermediate = 0
        num_marts = 0
        
        # Distribute models across types
        if self.config.include_staging:
            num_staging = max(self.config.num_seeds, self.config.num_models // 3)
        
        if self.config.include_intermediate:
            num_intermediate = self.config.num_models // 3
        
        if self.config.include_marts:
            num_marts = self.config.num_models - num_staging - num_intermediate
            if num_marts < 1:
                num_marts = 1
        
        # Generate staging model names
        for i in range(num_staging):
            model_name = f"stg_source_{i + 1}"
            models["staging"].append(model_name)
            self.model_types[model_name] = "staging"
            self.model_graph[model_name] = []
        
        # Generate intermediate model names
        for i in range(num_intermediate):
            model_name = f"int_transformed_{i + 1}"
            models["intermediate"].append(model_name)
            self.model_types[model_name] = "intermediate"
            # Intermediate models depend on staging
            dependencies = random.sample(
                models["staging"],
                min(self.config.max_dependencies, len(models["staging"]))
            ) if models["staging"] else []
            self.model_graph[model_name] = dependencies
        
        # Generate mart model names
        for i in range(num_marts):
            model_name = f"fct_business_event_{i + 1}" if i % 2 == 0 else f"dim_entity_{i + 1}"
            models["marts"].append(model_name)
            self.model_types[model_name] = "marts"
            # Marts depend on intermediate or staging
            available_deps = models["intermediate"] + models["staging"]
            dependencies = random.sample(
                available_deps,
                min(self.config.max_dependencies, len(available_deps))
            ) if available_deps else []
            self.model_graph[model_name] = dependencies
        
        return models
    
    def _generate_seeds_schema(self) -> str:
        """Generate schema documentation for seed files (reference only)."""
        lines = [
            "# This file documents the expected seed files",
            "# Seed files should be created as CSV files in the seeds/ directory",
            "",
            "version: 2",
            "",
            "seeds:"
        ]
        
        for i in range(self.config.num_seeds):
            seed_name = f"raw_data_{i + 1}"
            lines.extend([
                f"  - name: {seed_name}",
                f"    description: Raw data source {i + 1} for testing",
                "    columns:",
                "      - name: id",
                "        description: Primary key",
                "      - name: value",
                "        description: Sample value",
                "      - name: created_at",
                "        description: Timestamp",
                ""
            ])
        
        return "\n".join(lines)
    
    def _generate_staging_model(self, model_name: str) -> str:
        """Generate a staging model SQL file."""
        # Extract source number from model name
        source_num = model_name.split("_")[-1]
        seed_index = (int(source_num) - 1) % max(self.config.num_seeds, 1)
        seed_name = f"raw_data_{seed_index + 1}"
        
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return self._generate_simple_staging(model_name, seed_name)
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return self._generate_medium_staging(model_name, seed_name)
        else:
            return self._generate_complex_staging(model_name, seed_name)
    
    def _generate_simple_staging(self, model_name: str, seed_name: str) -> str:
        """Generate a simple staging model."""
        return f"""{{{{
    config(
        materialized='view'
    )
}}}}

-- Simple staging model for {model_name}
select
    id,
    value,
    created_at
from {{{{ ref('{seed_name}') }}}}
"""
    
    def _generate_medium_staging(self, model_name: str, seed_name: str) -> str:
        """Generate a medium complexity staging model."""
        return f"""{{{{
    config(
        materialized='view',
        tags=['staging']
    )
}}}}

-- Medium complexity staging model
with source as (
    select * from {{{{ ref('{seed_name}') }}}}
),

renamed as (
    select
        id as {model_name}_id,
        value as {model_name}_value,
        created_at as {model_name}_created_at,
        current_timestamp() as loaded_at
    from source
)

select * from renamed
"""
    
    def _generate_complex_staging(self, model_name: str, seed_name: str) -> str:
        """Generate a complex staging model."""
        return f"""{{{{
    config(
        materialized='view',
        tags=['staging', 'hourly']
    )
}}}}

-- Complex staging model with data quality checks
with source as (
    select * from {{{{ ref('{seed_name}') }}}}
),

cleaned as (
    select
        id,
        value,
        created_at,
        -- Data quality flags
        case
            when id is null then 'missing_id'
            when value is null then 'missing_value'
            else 'valid'
        end as data_quality_flag
    from source
),

renamed as (
    select
        id as {model_name}_id,
        value as {model_name}_value,
        created_at as {model_name}_created_at,
        data_quality_flag,
        current_timestamp() as loaded_at,
        '{{{{ run_started_at }}}}' as run_started_at
    from cleaned
    where data_quality_flag = 'valid'
)

select * from renamed
"""
    
    def _generate_intermediate_model(self, model_name: str) -> str:
        """Generate an intermediate model SQL file."""
        dependencies = self.model_graph.get(model_name, [])
        
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return self._generate_simple_intermediate(model_name, dependencies)
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return self._generate_medium_intermediate(model_name, dependencies)
        else:
            return self._generate_complex_intermediate(model_name, dependencies)
    
    def _generate_simple_intermediate(self, model_name: str, dependencies: List[str]) -> str:
        """Generate a simple intermediate model."""
        if not dependencies:
            ref_table = "stg_source_1"
        else:
            ref_table = dependencies[0]
            
        return f"""{{{{
    config(
        materialized='table'
    )
}}}}

-- Simple intermediate transformation
select
    *,
    1 as record_count
from {{{{ ref('{ref_table}') }}}}
"""
    
    def _generate_medium_intermediate(self, model_name: str, dependencies: List[str]) -> str:
        """Generate a medium complexity intermediate model."""
        if not dependencies:
            dependencies = ["stg_source_1"]
        
        ctes = []
        for dep in dependencies:
            ctes.append(f"{dep.replace('stg_', '').replace('int_', '')} as (\n    select * from {{{{ ref('{dep}') }}}}\n)")
        
        main_table = dependencies[0].replace('stg_', '').replace('int_', '')
        
        return f"""{{{{
    config(
        materialized='table',
        tags=['intermediate']
    )
}}}}

-- Medium complexity intermediate model with aggregations
with {',\n\n'.join(ctes)},

aggregated as (
    select
        count(*) as total_records,
        count(distinct {main_table}.{dependencies[0]}_id) as unique_ids,
        current_timestamp() as aggregated_at
    from {main_table}
)

select * from aggregated
"""
    
    def _generate_complex_intermediate(self, model_name: str, dependencies: List[str]) -> str:
        """Generate a complex intermediate model."""
        if not dependencies:
            dependencies = ["stg_source_1"]
        
        ctes = []
        for i, dep in enumerate(dependencies):
            alias = f"source_{i + 1}"
            ctes.append(f"{alias} as (\n    select * from {{{{ ref('{dep}') }}}}\n)")
        
        return f"""{{{{
    config(
        materialized='table',
        tags=['intermediate', 'daily']
    )
}}}}

-- Complex intermediate model with joins and window functions
with {',\n\n'.join(ctes)},

joined as (
    select
        source_1.*,
        row_number() over (partition by source_1.{dependencies[0]}_id order by source_1.{dependencies[0]}_created_at desc) as row_num
    from source_1
    {self._generate_join_clauses(dependencies[1:]) if len(dependencies) > 1 else ''}
),

final as (
    select
        *
    from joined
    where row_num = 1
)

select * from final
"""
    
    def _generate_join_clauses(self, dependencies: List[str]) -> str:
        """Generate JOIN clauses for complex models."""
        clauses = []
        # Get the first dependency's column name for join
        first_dep_col = f"{dependencies[0] if dependencies else 'stg_source_1'}_id"
        
        for i, dep in enumerate(dependencies):
            source_alias = f"source_{i + 2}"
            dep_col = f"{dep}_id"
            clauses.append(f"    left join {source_alias} on source_1.{first_dep_col} = {source_alias}.{dep_col}")
        return "\n" + "\n".join(clauses)
    
    def _generate_mart_model(self, model_name: str) -> str:
        """Generate a mart model SQL file."""
        dependencies = self.model_graph.get(model_name, [])
        
        if self.config.complexity == ComplexityLevel.SIMPLE:
            return self._generate_simple_mart(model_name, dependencies)
        elif self.config.complexity == ComplexityLevel.MEDIUM:
            return self._generate_medium_mart(model_name, dependencies)
        else:
            return self._generate_complex_mart(model_name, dependencies)
    
    def _generate_simple_mart(self, model_name: str, dependencies: List[str]) -> str:
        """Generate a simple mart model."""
        if not dependencies:
            ref_table = "stg_source_1"
        else:
            ref_table = dependencies[0]
            
        return f"""{{{{
    config(
        materialized='table'
    )
}}}}

-- Simple mart model
select * from {{{{ ref('{ref_table}') }}}}
"""
    
    def _generate_medium_mart(self, model_name: str, dependencies: List[str]) -> str:
        """Generate a medium complexity mart model."""
        if not dependencies:
            dependencies = ["int_transformed_1"]
        
        is_fact = "fct_" in model_name
        
        return f"""{{{{
    config(
        materialized='table',
        tags=['marts', '{'fact' if is_fact else 'dimension'}']
    )
}}}}

-- {'Fact' if is_fact else 'Dimension'} table for business analysis
with base as (
    select * from {{{{ ref('{dependencies[0]}') }}}}
),

final as (
    select
        *,
        '{'fact' if is_fact else 'dimension'}' as model_type
    from base
)

select * from final
"""
    
    def _generate_complex_mart(self, model_name: str, dependencies: List[str]) -> str:
        """Generate a complex mart model."""
        if not dependencies:
            dependencies = ["int_transformed_1"]
        
        is_fact = "fct_" in model_name
        
        ctes = []
        for i, dep in enumerate(dependencies):
            alias = f"dep_{i + 1}"
            ctes.append(f"{alias} as (\n    select * from {{{{ ref('{dep}') }}}}\n)")
        
        return f"""{{{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', '{'fact' if is_fact else 'dimension'}', 'incremental']
    )
}}}}

-- Complex {'fact' if is_fact else 'dimension'} table with incremental loading
with {',\n\n'.join(ctes)},

combined as (
    select
        {{{{ dbt_utils.generate_surrogate_key(['dep_1.{dependencies[0]}_id']) }}}} as surrogate_id,
        dep_1.*,
        current_timestamp() as inserted_at,
        current_timestamp() as updated_at
    from dep_1
    {self._generate_join_clauses(dependencies[1:]) if len(dependencies) > 1 else ''}
),

final as (
    select * from combined
    {{% if is_incremental() %}}
    where updated_at > (select max(updated_at) from {{{{ this }}}})
    {{% endif %}}
)

select * from final
"""
    
    def _generate_schema_yml(self, models: Dict[str, List[str]]) -> str:
        """Generate schema.yml file documenting all models."""
        lines = [
            "version: 2",
            "",
            "models:"
        ]
        
        for model_type in ["staging", "intermediate", "marts"]:
            if model_type not in models:
                continue
                
            for model_name in models[model_type]:
                lines.extend([
                    f"  - name: {model_name}",
                    f"    description: {model_type.capitalize()} model {model_name}",
                    "    columns:"
                ])
                
                # Determine appropriate column name based on model type and complexity
                if model_type == "staging" and self.config.complexity == ComplexityLevel.SIMPLE:
                    # Simple staging models use original column names
                    primary_col = "id"
                else:
                    # Medium and complex models use prefixed column names
                    primary_col = f"{model_name}_id"
                
                lines.extend([
                    f"      - name: {primary_col}",
                    "        description: Primary identifier",
                    f"        tests:",
                    f"          - unique",
                    f"          - not_null",
                    ""
                ])
        
        return "\n".join(lines)
