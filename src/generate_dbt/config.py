"""Configuration for dbt code generation."""
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ComplexityLevel(Enum):
    """Complexity levels for generated models."""
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


@dataclass
class GeneratorConfig:
    """Configuration for generating dbt code."""
    
    # Number of models to generate
    num_models: int = 10
    
    # Number of macros to generate
    num_macros: int = 5
    
    # Complexity level
    complexity: ComplexityLevel = ComplexityLevel.MEDIUM
    
    # Output directory
    output_dir: str = "./generated_dbt_project"
    
    # Project name
    project_name: str = "test_dbt_project"
    
    # Generate staging models
    include_staging: bool = True
    
    # Generate intermediate models
    include_intermediate: bool = True
    
    # Generate mart models
    include_marts: bool = True
    
    # Maximum number of dependencies per model
    max_dependencies: int = 3
    
    # Seed data tables to reference
    num_seeds: int = 3
    
    def __post_init__(self):
        """Validate configuration."""
        if self.num_models < 1:
            raise ValueError("num_models must be at least 1")
        if self.num_macros < 0:
            raise ValueError("num_macros must be non-negative")
        if isinstance(self.complexity, str):
            self.complexity = ComplexityLevel(self.complexity)
