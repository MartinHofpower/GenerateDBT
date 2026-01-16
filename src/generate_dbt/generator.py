"""Main generator orchestrator for dbt projects."""
import yaml
from pathlib import Path
from typing import Dict, Any
from .config import GeneratorConfig
from .model_generator import ModelGenerator
from .macro_generator import MacroGenerator


class ProjectGenerator:
    """Main class for generating complete dbt projects."""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.model_gen = ModelGenerator(config)
        self.macro_gen = MacroGenerator(config)
    
    def generate(self) -> Dict[str, Any]:
        """Generate a complete dbt project.
        
        Returns:
            Dictionary with generation results and file paths
        """
        output_path = Path(self.config.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        results = {
            "project_name": self.config.project_name,
            "output_dir": str(output_path.absolute()),
            "files_generated": []
        }
        
        # Generate dbt_project.yml
        project_yml_path = self._generate_project_yml(output_path)
        results["files_generated"].append(project_yml_path)
        
        # Generate models
        model_files = self.model_gen.generate_models(output_path)
        results["files_generated"].extend(model_files)
        results["num_models"] = len([f for f in model_files if f.endswith(".sql")])
        
        # Generate macros
        macro_files = self.macro_gen.generate_macros(output_path)
        results["files_generated"].extend(macro_files)
        results["num_macros"] = len(macro_files)
        
        # Generate sample seeds directory and documentation
        seeds_info = self._generate_seeds_info(output_path)
        results["files_generated"].extend(seeds_info)
        
        # Generate README
        readme_path = self._generate_project_readme(output_path)
        results["files_generated"].append(readme_path)
        
        # Generate .gitkeep for tests directory
        tests_path = self._create_tests_directory(output_path)
        results["files_generated"].append(tests_path)
        
        return results
    
    def _generate_project_yml(self, output_dir: Path) -> str:
        """Generate dbt_project.yml file."""
        project_config = {
            "name": self.config.project_name,
            "version": "1.0.0",
            "config-version": 2,
            "profile": self.config.project_name,
            "model-paths": ["models"],
            "analysis-paths": ["analyses"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                self.config.project_name: {
                    "staging": {
                        "+materialized": "view",
                        "+tags": ["staging"]
                    },
                    "intermediate": {
                        "+materialized": "table",
                        "+tags": ["intermediate"]
                    },
                    "marts": {
                        "+materialized": "table",
                        "+tags": ["marts"]
                    }
                }
            },
            "seeds": {
                self.config.project_name: {
                    "+quote_columns": False
                }
            }
        }
        
        file_path = output_dir / "dbt_project.yml"
        with open(file_path, "w") as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)
        
        return str(file_path)
    
    def _generate_seeds_info(self, output_dir: Path) -> list:
        """Generate seed files and documentation."""
        seeds_dir = output_dir / "seeds"
        seeds_dir.mkdir(exist_ok=True)
        
        generated_files = []
        
        # Create sample CSV seed files
        for i in range(self.config.num_seeds):
            seed_name = f"raw_data_{i + 1}"
            csv_content = "id,value,created_at\n"
            csv_content += f"1,sample_value_{i + 1}_1,2024-01-01\n"
            csv_content += f"2,sample_value_{i + 1}_2,2024-01-02\n"
            csv_content += f"3,sample_value_{i + 1}_3,2024-01-03\n"
            
            file_path = seeds_dir / f"{seed_name}.csv"
            with open(file_path, "w") as f:
                f.write(csv_content)
            generated_files.append(str(file_path))
        
        # Create README for seeds
        readme_content = """# Seeds Directory

This directory contains CSV files that serve as seed data for the dbt project.
Seed files are loaded into your data warehouse using the `dbt seed` command.

## Available Seeds

"""
        for i in range(self.config.num_seeds):
            seed_name = f"raw_data_{i + 1}"
            readme_content += f"- **{seed_name}.csv**: Sample raw data source {i + 1}\n"
        
        readme_content += """
## Usage

To load these seeds into your database:

```bash
dbt seed
```

To load specific seeds:

```bash
dbt seed --select raw_data_1
```
"""
        
        readme_path = seeds_dir / "README.md"
        with open(readme_path, "w") as f:
            f.write(readme_content)
        generated_files.append(str(readme_path))
        
        return generated_files
    
    def _generate_project_readme(self, output_dir: Path) -> str:
        """Generate project README file."""
        readme_content = f"""# {self.config.project_name}

This is a generated dbt project for testing purposes. It contains sample models, macros, and seed data that can be used to test dbt functionality in various data platforms.

## Project Structure

```
{self.config.project_name}/
├── dbt_project.yml          # Project configuration
├── models/                   # Data transformation models
│   ├── staging/             # Staging models (raw data cleaning)
│   ├── intermediate/        # Intermediate transformations
│   └── marts/               # Business-level models (facts & dimensions)
├── macros/                  # Reusable SQL macros
├── seeds/                   # CSV files for seed data
└── tests/                   # Custom data tests
```

## Configuration

- **Models**: {self.config.num_models} models across staging, intermediate, and mart layers
- **Macros**: {self.config.num_macros} reusable macros
- **Complexity**: {self.config.complexity.value}
- **Seeds**: {self.config.num_seeds} seed data files

## Getting Started

### Prerequisites

- Python 3.7+
- dbt-core (install using `pip install dbt-core`)
- Database adapter for your platform (e.g., `dbt-snowflake`, `dbt-bigquery`, `dbt-postgres`, etc.)

### Setup

1. Install dbt and the appropriate adapter:
   ```bash
   pip install dbt-core dbt-<your-adapter>
   ```

2. Configure your `profiles.yml` file with connection details for your data warehouse.
   Place this file in `~/.dbt/profiles.yml`:
   
   ```yaml
   {self.config.project_name}:
     outputs:
       dev:
         type: <adapter_type>  # e.g., postgres, snowflake, bigquery
         # Add your connection details here
     target: dev
   ```

3. Test your connection:
   ```bash
   dbt debug
   ```

### Usage

Load seed data:
```bash
dbt seed
```

Run all models:
```bash
dbt run
```

Run specific model layers:
```bash
dbt run --select staging      # Run only staging models
dbt run --select intermediate # Run only intermediate models
dbt run --select marts        # Run only mart models
```

Test models:
```bash
dbt test
```

Generate documentation:
```bash
dbt docs generate
dbt docs serve
```

## Model Layers

### Staging Models
- Source: Seed data files
- Purpose: Light transformations and renaming
- Materialization: View

### Intermediate Models
- Source: Staging models
- Purpose: Business logic transformations, joins, aggregations
- Materialization: Table

### Mart Models
- Source: Intermediate models (and staging where needed)
- Purpose: Final business-level tables (facts and dimensions)
- Materialization: Table (some with incremental loading)

## Macros

The project includes several utility macros:
- String manipulation utilities
- Date/time utilities
- Aggregation helpers
- Custom test macros
- Data quality checks

## Customization

This project was generated with configurable parameters. To generate a different project structure, modify the generation configuration in the source generator.

## Testing on Different Platforms

This project is platform-agnostic and can be tested on:
- Snowflake
- BigQuery
- Redshift
- Postgres
- Microsoft Fabric
- Databricks
- And any other dbt-supported platform

Simply install the appropriate dbt adapter and configure your `profiles.yml` accordingly.

## Notes

- All models include basic tests (unique, not_null)
- Schema documentation is provided in `schema.yml` files
- The project follows dbt best practices for model organization
- Generated for testing and demonstration purposes
"""
        
        file_path = output_dir / "README.md"
        with open(file_path, "w") as f:
            f.write(readme_content)
        
        return str(file_path)
    
    def _create_tests_directory(self, output_dir: Path) -> str:
        """Create tests directory with .gitkeep."""
        tests_dir = output_dir / "tests"
        tests_dir.mkdir(exist_ok=True)
        
        gitkeep_path = tests_dir / ".gitkeep"
        gitkeep_path.touch()
        
        return str(gitkeep_path)
