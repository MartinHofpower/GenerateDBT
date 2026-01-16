# Example Configuration Files

This directory contains example configurations for using GenerateDBT.

## Python API Usage

You can also use GenerateDBT as a Python library:

```python
from generate_dbt.config import GeneratorConfig, ComplexityLevel
from generate_dbt.generator import ProjectGenerator

# Create configuration
config = GeneratorConfig(
    num_models=15,
    num_macros=8,
    complexity=ComplexityLevel.COMPLEX,
    output_dir="./my_dbt_project",
    project_name="fabric_test",
    num_seeds=5,
    max_dependencies=4,
    include_staging=True,
    include_intermediate=True,
    include_marts=True
)

# Generate project
generator = ProjectGenerator(config)
results = generator.generate()

print(f"Generated {results['num_models']} models")
print(f"Generated {results['num_macros']} macros")
print(f"Project location: {results['output_dir']}")
```

## Common Scenarios

### Small Test Project
```bash
generate-dbt --num-models 5 --num-macros 2 --complexity simple
```

### Medium Test Project (Default)
```bash
generate-dbt --num-models 10 --num-macros 5 --complexity medium
```

### Large Test Project
```bash
generate-dbt --num-models 30 --num-macros 15 --complexity complex
```

### Microsoft Fabric Testing
```bash
generate-dbt \
  --num-models 20 \
  --num-macros 10 \
  --complexity complex \
  --output-dir ./fabric_test \
  --project-name fabric_dbt_test
```

### Only Staging and Marts (Skip Intermediate)
```bash
generate-dbt --no-intermediate
```
