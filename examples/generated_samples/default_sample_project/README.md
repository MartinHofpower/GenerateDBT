# default_sample_project

This is a generated dbt project for testing purposes. It contains sample models, macros, and seed data that can be used to test dbt functionality in various data platforms.

## Project Structure

```
default_sample_project/
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

- **Models**: 10 models across staging, intermediate, and mart layers
- **Macros**: 5 reusable macros
- **Complexity**: medium
- **Seeds**: 3 seed data files

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
   default_sample_project:
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
