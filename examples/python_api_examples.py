#!/usr/bin/env python3
"""
Example script demonstrating how to use GenerateDBT as a Python library.

This script shows various ways to programmatically generate dbt projects
with different configurations.
"""

from generate_dbt.config import GeneratorConfig, ComplexityLevel
from generate_dbt.generator import ProjectGenerator


def example_basic():
    """Generate a basic dbt project."""
    print("=" * 60)
    print("Example 1: Basic Project Generation")
    print("=" * 60)
    
    config = GeneratorConfig(
        num_models=10,
        num_macros=5,
        complexity=ComplexityLevel.MEDIUM,
        output_dir="./example_basic_project",
        project_name="basic_test"
    )
    
    generator = ProjectGenerator(config)
    results = generator.generate()
    
    print(f"✓ Generated {results['num_models']} models")
    print(f"✓ Generated {results['num_macros']} macros")
    print(f"✓ Output location: {results['output_dir']}")
    print(f"✓ Total files: {len(results['files_generated'])}")
    print()


def example_simple_staging_only():
    """Generate a simple project with only staging models."""
    print("=" * 60)
    print("Example 2: Staging-Only Project")
    print("=" * 60)
    
    config = GeneratorConfig(
        num_models=5,
        num_macros=2,
        complexity=ComplexityLevel.SIMPLE,
        output_dir="./example_staging_only",
        project_name="staging_test",
        include_staging=True,
        include_intermediate=False,
        include_marts=False
    )
    
    generator = ProjectGenerator(config)
    results = generator.generate()
    
    print(f"✓ Generated {results['num_models']} staging models")
    print(f"✓ Output location: {results['output_dir']}")
    print()


def example_complex_fabric():
    """Generate a complex project for Microsoft Fabric testing."""
    print("=" * 60)
    print("Example 3: Microsoft Fabric Test Project")
    print("=" * 60)
    
    config = GeneratorConfig(
        num_models=25,
        num_macros=12,
        complexity=ComplexityLevel.COMPLEX,
        output_dir="./example_fabric_test",
        project_name="fabric_test_project",
        num_seeds=5,
        max_dependencies=4
    )
    
    generator = ProjectGenerator(config)
    results = generator.generate()
    
    print(f"✓ Generated {results['num_models']} models")
    print(f"✓ Generated {results['num_macros']} macros")
    print(f"✓ Project name: {config.project_name}")
    print(f"✓ Output location: {results['output_dir']}")
    print()


def example_custom_configuration():
    """Generate a project with custom configuration."""
    print("=" * 60)
    print("Example 4: Custom Configuration")
    print("=" * 60)
    
    # You can customize every aspect
    config = GeneratorConfig(
        num_models=15,
        num_macros=7,
        complexity=ComplexityLevel.MEDIUM,
        output_dir="./example_custom",
        project_name="custom_dbt_project",
        include_staging=True,
        include_intermediate=True,
        include_marts=True,
        max_dependencies=2,
        num_seeds=4
    )
    
    generator = ProjectGenerator(config)
    results = generator.generate()
    
    print(f"✓ Project: {config.project_name}")
    print(f"✓ Models: {results['num_models']}")
    print(f"✓ Macros: {results['num_macros']}")
    print(f"✓ Complexity: {config.complexity.value}")
    print(f"✓ Location: {results['output_dir']}")
    
    # Print some details about generated files
    print(f"\n✓ Generated files:")
    sql_files = [f for f in results['files_generated'] if f.endswith('.sql')]
    yml_files = [f for f in results['files_generated'] if f.endswith('.yml') or f.endswith('.yaml')]
    csv_files = [f for f in results['files_generated'] if f.endswith('.csv')]
    
    print(f"  - SQL files: {len(sql_files)}")
    print(f"  - YAML files: {len(yml_files)}")
    print(f"  - CSV files: {len(csv_files)}")
    print()


def main():
    """Run all examples."""
    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + " " * 10 + "GenerateDBT Python API Examples" + " " * 16 + "║")
    print("╚" + "═" * 58 + "╝")
    print()
    
    try:
        example_basic()
        example_simple_staging_only()
        example_complex_fabric()
        example_custom_configuration()
        
        print("=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Navigate to any generated project directory")
        print("2. Configure your profiles.yml")
        print("3. Run: dbt seed && dbt run && dbt test")
        print()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
