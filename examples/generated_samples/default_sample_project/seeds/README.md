# Seeds Directory

This directory contains CSV files that serve as seed data for the dbt project.
Seed files are loaded into your data warehouse using the `dbt seed` command.

## Available Seeds

- **raw_data_1.csv**: Sample raw data source 1
- **raw_data_2.csv**: Sample raw data source 2
- **raw_data_3.csv**: Sample raw data source 3

## Usage

To load these seeds into your database:

```bash
dbt seed
```

To load specific seeds:

```bash
dbt seed --select raw_data_1
```
