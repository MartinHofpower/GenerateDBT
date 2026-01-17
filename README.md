# GenerateDBT

A Python tool for generating dbt (data build tool) projects with configurable scale and complexity for testing purposes.

Disclaimer: This is a community-driven project initially set up with the help of GitHub Copilot and is not officially affiliated with dbt Labs. The tool is designed to help users create dbt projects for testing and learning purposes. The project is tested mainly with dbt-fabric but should work with other dbt adapters as well.

## Overview

GenerateDBT creates complete dbt projects with models, macros, and seed data that can be used to test dbt functionality across different data platforms (Snowflake, BigQuery, Postgres, Microsoft Fabric, Databricks, etc.). The generated code is platform-agnostic and follows dbt best practices.

## Features

- 🎯 **Configurable Scale**: Choose the number of models and macros to generate
- 📊 **Complexity Levels**: Simple, medium, or complex model patterns
- 🏗️ **Model Layers**: Staging, intermediate, and mart models following dbt conventions
- 🔧 **Utility Macros**: String, date, aggregation, and data quality macros
- 📝 **Documentation**: Auto-generated schema.yml and README files
- 🌱 **Seed Data**: Sample CSV files for testing
- 🚀 **Platform-Agnostic**: Works with any dbt-supported database

## Installation

### Clone Repo

```bash
git clone https://github.com/MartinHofpower/GenerateDBT.git
```

### From Source

```bash
cd GenerateDBT
pip install -e .
```

## Quick Start

Generate a default dbt project:

```bash
generate-dbt
```

This creates a project with:
- 10 models (staging, intermediate, and marts)
- 5 macros
- 3 seed data files
- Medium complexity
- Output to `./generated_dbt_project`

## Usage

### Basic Examples

Generate a simple project:
```bash
generate-dbt --complexity simple
```

Generate a complex project with more models:
```bash
generate-dbt --num-models 20 --num-macros 10 --complexity complex
```

Generate to a specific directory:
```bash
generate-dbt --output-dir ./my_test_project --project-name my_dbt_test
```

### Advanced Options

```bash
generate-dbt \
  --num-models 15 \
  --num-macros 8 \
  --num-seeds 5 \
  --complexity complex \
  --output-dir ./test_project \
  --project-name fabric_test \
  --max-dependencies 4 \
  --no-intermediate
```

### Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--num-models` | 10 | Number of models to generate |
| `--num-macros` | 5 | Number of macros to generate |
| `--complexity` | medium | Complexity level (simple, medium, complex) |
| `--output-dir` | ./generated_dbt_project | Output directory |
| `--project-name` | test_dbt_project | Name of the dbt project |
| `--num-seeds` | 3 | Number of seed data files |
| `--max-dependencies` | 3 | Max dependencies per model |
| `--no-staging` | False | Skip staging models |
| `--no-intermediate` | False | Skip intermediate models |
| `--no-marts` | False | Skip mart models |

## Generated Project Structure

```
generated_dbt_project/
├── dbt_project.yml          # Project configuration
├── README.md                 # Generated project documentation
├── models/
│   ├── schema.yml           # Model documentation and tests
│   ├── staging/             # Staging models (light transformations)
│   │   └── stg_*.sql
│   ├── intermediate/        # Intermediate transformations
│   │   └── int_*.sql
│   └── marts/               # Business-level models
│       └── (fct_*.sql, dim_*.sql)
├── macros/                  # Reusable SQL macros
│   ├── string_utils.sql
│   ├── date_utils.sql
│   └── ...
├── seeds/                   # Sample CSV data
│   ├── raw_data_1.csv
│   └── ...
└── tests/                   # Custom test directory
```

## Complexity Levels

### Simple
- Basic SELECT statements
- Minimal transformations
- Simple macros with single operations

### Medium
- CTEs (Common Table Expressions)
- Basic joins and aggregations
- Macros with conditional logic
- Data quality checks

### Complex
- Multiple CTEs and complex joins
- Window functions
- Incremental materializations
- Advanced macros with loops
- Comprehensive data quality frameworks

## Using the Generated Project

1. **Navigate to the project:**
   ```bash
   cd generated_dbt_project
   ```

2. **Install dbt and adapter:**
   ```bash
   pip install dbt-core dbt-<your-adapter>
   ```
   Replace `<your-adapter>` with your platform (e.g., `dbt-snowflake`, `dbt-bigquery`, `dbt-fabric`, `dbt-postgres`)

3. **Configure profiles.yml:**
   Create or update `~/.dbt/profiles.yml` with your database credentials:
   ```yaml
   test_dbt_project:
     outputs:
       dev:
         type: <adapter_type>
         # Add your connection details
     target: dev
   ```

4. **Test connection:**
   ```bash
   dbt debug
   ```

5. **Load seed data:**
   ```bash
   dbt seed
   ```

6. **Run models:**
   ```bash
   dbt run
   ```

7. **Run tests:**
   ```bash
   dbt test
   ```

8. **Generate and view docs:**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Testing on Different Platforms

This tool generates platform-agnostic dbt code that works with any supported adapter:

- **Snowflake**: `pip install dbt-snowflake`
- **BigQuery**: `pip install dbt-bigquery`
- **Postgres**: `pip install dbt-postgres`
- **Redshift**: `pip install dbt-redshift`
- **Microsoft Fabric**: `pip install dbt-fabric`
- **Databricks**: `pip install dbt-databricks`

Simply install the appropriate adapter and configure your `profiles.yml` accordingly.

## Use Cases

- 🧪 **Testing dbt on new platforms**: Quickly generate test projects for Microsoft Fabric, Databricks, or other platforms
- 📚 **Learning dbt**: Study example projects with various patterns
- 🎓 **Training**: Create sample projects for teaching dbt concepts
- 🔬 **Performance testing**: Generate large projects to test performance
- 🐛 **Debugging**: Create reproducible test cases for dbt issues

## Development

### Requirements

- Python 3.7+
- PyYAML
- Click

### Setup Development Environment

```bash
git clone https://github.com/MartinHofpower/GenerateDBT.git
cd GenerateDBT
pip install -e .
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built for testing dbt projects across various data platforms
- Follows dbt best practices and conventions
- Inspired by the need for flexible, scalable dbt testing scenarios