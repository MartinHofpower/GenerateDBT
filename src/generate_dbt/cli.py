"""Command-line interface for GenerateDBT."""
import click
import sys
from pathlib import Path
from .config import GeneratorConfig, ComplexityLevel
from .generator import ProjectGenerator


@click.command()
@click.option(
    '--num-models',
    default=10,
    type=int,
    help='Number of models to generate (default: 10)'
)
@click.option(
    '--num-macros',
    default=5,
    type=int,
    help='Number of macros to generate (default: 5)'
)
@click.option(
    '--complexity',
    type=click.Choice(['simple', 'medium', 'complex'], case_sensitive=False),
    default='medium',
    help='Complexity level of generated code (default: medium)'
)
@click.option(
    '--output-dir',
    default='./generated_dbt_project',
    type=str,
    help='Output directory for generated project (default: ./generated_dbt_project)'
)
@click.option(
    '--project-name',
    default='test_dbt_project',
    type=str,
    help='Name of the dbt project (default: test_dbt_project)'
)
@click.option(
    '--num-seeds',
    default=3,
    type=int,
    help='Number of seed data files to generate (default: 3)'
)
@click.option(
    '--max-dependencies',
    default=3,
    type=int,
    help='Maximum number of dependencies per model (default: 3)'
)
@click.option(
    '--no-staging',
    is_flag=True,
    help='Do not generate staging models'
)
@click.option(
    '--no-intermediate',
    is_flag=True,
    help='Do not generate intermediate models'
)
@click.option(
    '--no-marts',
    is_flag=True,
    help='Do not generate mart models'
)
def main(num_models, num_macros, complexity, output_dir, project_name, 
         num_seeds, max_dependencies, no_staging, no_intermediate, no_marts):
    """Generate dbt models and macros for testing purposes.
    
    This tool creates a complete dbt project with configurable scale and complexity.
    The generated project is platform-agnostic and can be used to test dbt 
    functionality on any supported data platform (Snowflake, BigQuery, Postgres,
    Microsoft Fabric, etc.).
    
    Examples:
    
        # Generate a simple project with defaults
        generate-dbt
        
        # Generate a complex project with 20 models and 10 macros
        generate-dbt --num-models 20 --num-macros 10 --complexity complex
        
        # Generate only staging and mart models
        generate-dbt --no-intermediate
        
        # Generate to a specific directory with custom name
        generate-dbt --output-dir ./my_project --project-name my_test_project
    """
    click.echo("🚀 GenerateDBT - Creating dbt project for testing...\n")
    
    # Validate at least one model type is enabled
    if no_staging and no_intermediate and no_marts:
        click.echo("❌ Error: At least one model type must be enabled", err=True)
        sys.exit(1)
    
    # Create configuration
    try:
        config = GeneratorConfig(
            num_models=num_models,
            num_macros=num_macros,
            complexity=ComplexityLevel(complexity),
            output_dir=output_dir,
            project_name=project_name,
            num_seeds=num_seeds,
            max_dependencies=max_dependencies,
            include_staging=not no_staging,
            include_intermediate=not no_intermediate,
            include_marts=not no_marts
        )
    except ValueError as e:
        click.echo(f"❌ Configuration error: {e}", err=True)
        sys.exit(1)
    
    # Display configuration
    click.echo("📋 Configuration:")
    click.echo(f"  Project Name: {config.project_name}")
    click.echo(f"  Output Directory: {config.output_dir}")
    click.echo(f"  Number of Models: {config.num_models}")
    click.echo(f"  Number of Macros: {config.num_macros}")
    click.echo(f"  Number of Seeds: {config.num_seeds}")
    click.echo(f"  Complexity: {config.complexity.value}")
    click.echo(f"  Include Staging: {config.include_staging}")
    click.echo(f"  Include Intermediate: {config.include_intermediate}")
    click.echo(f"  Include Marts: {config.include_marts}")
    click.echo()
    
    # Generate project
    try:
        generator = ProjectGenerator(config)
        results = generator.generate()
        
        click.echo("✅ Project generated successfully!\n")
        click.echo("📊 Summary:")
        click.echo(f"  Total files created: {len(results['files_generated'])}")
        click.echo(f"  Models generated: {results['num_models']}")
        click.echo(f"  Macros generated: {results['num_macros']}")
        click.echo(f"  Location: {results['output_dir']}")
        click.echo()
        
        click.echo("🎯 Next Steps:")
        click.echo(f"  1. cd {output_dir}")
        click.echo("  2. Configure your profiles.yml with database connection details")
        click.echo("  3. Run: dbt debug (to test connection)")
        
        # Add dbt deps step for complex projects
        if config.complexity == ComplexityLevel.COMPLEX:
            click.echo("  4. Run: dbt deps (to install dependencies)")
            click.echo("  5. Run: dbt seed (to load seed data)")
            click.echo("  6. Run: dbt run (to build models)")
            click.echo("  7. Run: dbt test (to run tests)")
        else:
            click.echo("  4. Run: dbt seed (to load seed data)")
            click.echo("  5. Run: dbt run (to build models)")
            click.echo("  6. Run: dbt test (to run tests)")
        
        click.echo()
        click.echo("📖 For more information, see the README.md in the generated project")
        
    except Exception as e:
        click.echo(f"\n❌ Error during generation: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
