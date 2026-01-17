# Examples

This directory contains example scripts and usage patterns for GenerateDBT.

## Python API Examples

Run the Python API examples:

```bash
cd examples
python python_api_examples.py
```

This will generate several example projects demonstrating different configurations:
- Basic project with default settings
- Staging-only project
- Complex project for Microsoft Fabric testing
- Custom configuration project

## CLI Examples

See [EXAMPLES.md](../EXAMPLES.md) in the root directory for CLI usage examples.

## Generated Projects

The example scripts will create several directories with generated dbt projects:
- `example_basic_project/`
- `example_staging_only/`
- `example_fabric_test/`
- `example_custom/`

Each generated project includes a README.md with instructions on how to use it.

# Pregenerated Projects

For better understanding without running the generator, you can explore the following pregenerated sample projects:
- [complex_sample_project](./generated_samples/complex_sample_project/): A complex dbt project with multiple models, macros, and seed data. Created by running:
  ```bash
  generate-dbt --num-macros 200 --num-models 500 --complexity complex --output-dir complex_sample_project --project-name complex_sample_project --num-seeds 100 --max-dependencies 50 
  ```
- [default_dbt_project](./generated_samples/default_sample_project/): A basic dbt project with the generator's default settings. Created by running:
  ```bash
  generate-dbt --output-dir default_sample_project --project-name default_sample_projec
  ```
