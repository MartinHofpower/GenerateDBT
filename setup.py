"""Setup configuration for GenerateDBT package."""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="generate-dbt",
    version="0.1.0",
    author="GenerateDBT Contributors",
    description="Generate dbt models and macros for testing purposes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MartinHofpower/GenerateDBT",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pyyaml>=5.1",
        "click>=8.0",
    ],
    entry_points={
        "console_scripts": [
            "generate-dbt=generate_dbt.cli:main",
        ],
    },
)
