# How to Run dbt Models

This guide explains how to run dbt models in this project, which uses **three separate databases**:
- **source** - Raw/source data and staging models
- **transform** - Intermediate models
- **analytics** - Metrics/final models

## Prerequisites

1. **Load source data** (run these from project root):
   ```bash
   python extract/strava/load_strava_data.py
   python extract/Whoop/load_whoop_data.py
   ```

2. **Load seeds** into the source database:
   ```bash
   cd transform/dbt_model
   dbt seed --target source
   ```

## Running Models

All dbt commands should be run from the `transform/dbt_model/` directory.

### 1. Run Staging Models

Staging models read from source data and create views in the `source` database:

```bash
dbt run --select 'staging.*' --target source
```

This will create staging views that clean and standardize your source data.

### 2. Run Intermediate Models

Intermediate models read from staging models and create tables in the `transform` database:

```bash
dbt run --select 'intermediate.*' --target transform
```

**Note:** The `on-run-start` hook automatically attaches the `source` database, so intermediate models can reference staging models.

### 3. Run Metrics Models

Metrics models read from intermediate models and create tables in the `analytics` database:

```bash
dbt run --select 'metrics.*' --target analytics
```

**Note:** The `on-run-start` hook automatically attaches both `source` and `transform` databases, so metrics models can reference both staging and intermediate models.

### 4. Run All Models (Full Pipeline)

To run the entire pipeline in order:

```bash
# Step 1: Staging models
dbt run --select 'staging.*' --target source

# Step 2: Intermediate models
dbt run --select 'intermediate.*' --target transform

# Step 3: Metrics models
dbt run --select 'metrics.*' --target analytics
```

### 5. Run Specific Models

You can run specific models by name:

```bash
# Run a specific staging model
dbt run --select stg_strava_activities --target source

# Run a specific intermediate model
dbt run --select int_strava_activities --target transform

# Run a specific metrics model
dbt run --select fct_strava_activities --target analytics
```

### 6. Run Models with Dependencies

To run a model and all its dependencies:

```bash
# Run a model and all upstream dependencies
dbt run --select +fct_strava_activities --target analytics

# Run a model and all downstream dependencies
dbt run --select stg_strava_activities+ --target source
```

## Other Useful Commands

### Test Models

```bash
# Test all models
dbt test

# Test specific models
dbt test --select 'staging.*' --target source
dbt test --select 'intermediate.*' --target transform
dbt test --select 'metrics.*' --target analytics
```

### Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

### Compile Models (without running)

```bash
dbt compile
```

### List Models

```bash
# List all models
dbt list

# List models for a specific target
dbt list --target source
dbt list --target transform
dbt list --target analytics
```

### Clean Build Artifacts

```bash
dbt clean
```

## Database Structure

Schemas are organized by entity name across all databases:

```
source.duckdb (source database)
├── strava.strava_activities        # Source table
├── whoop.whoop_workouts           # Source table
├── whoop.whoop_sleeps             # Source table
├── whoop.whoop_physiological_cycles # Source table
├── whoop.whoop_recoveries         # Source table
├── seeds.activity_mapping          # Seed
├── seeds.mooncalender             # Seed
├── strava.stg_strava_*            # Staging views
├── whoop.stg_whoop_*              # Staging views
└── seeds.stg_dbt_seeds_*          # Staging views

transform.duckdb (transform database)
├── strava.int_strava_*            # Intermediate tables
└── whoop.int_whoop_*              # Intermediate tables

analytics.duckdb (analytics database)
├── strava.fct_strava_*            # Metrics/fact tables
├── whoop.fct_whoop_*              # Metrics/fact tables
├── whoop.dim_whoop_*              # Dimension tables
├── dates.dim_dates                # Dimension table
└── semantic.fct_activities        # Semantic/metrics tables
```

## Troubleshooting

### "Catalog 'source' does not exist" Error

This means the source database wasn't attached. The `on-run-start` hook should handle this automatically. If you see this error:

1. Make sure you're using the correct target (`--target transform` or `--target analytics`)
2. Check that the `attach_databases` macro is working
3. Verify the relative paths in `macros/attach_databases.sql` are correct

### "Table does not exist" Error

Make sure you've:
1. Loaded source data using the Python scripts
2. Run `dbt seed --target source` to load seeds
3. Run models in the correct order (staging → intermediate → metrics)

### Path Issues

All paths are relative to the `transform/dbt_model/` directory. If you get path errors:
- Make sure you're running dbt from the `transform/dbt_model/` directory
- Check that the database files exist in `../data/database/`

## Quick Reference

| Command | Target | Description |
|---------|--------|-------------|
| `dbt seed --target source` | source | Load CSV seeds into source database |
| `dbt run --select 'staging.*' --target source` | source | Build staging views |
| `dbt run --select 'intermediate.*' --target transform` | transform | Build intermediate tables |
| `dbt run --select 'metrics.*' --target analytics` | analytics | Build metrics tables |
| `dbt test` | all | Run all tests |
| `dbt docs generate` | all | Generate documentation |

