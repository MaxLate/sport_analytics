# Strava Analytics dbt Project

This dbt project transforms Strava and Whoop data using **three separate DuckDB databases**:
- **source** - Raw data and staging models
- **transform** - Intermediate models  
- **analytics** - Metrics and final models

## Quick Start

See [HOW_TO_RUN.md](HOW_TO_RUN.md) for detailed instructions on running dbt models.

### Quick Commands

```bash
# Load seeds
dbt seed --target source

# Run staging models
dbt run --select 'staging.*' --target source

# Run intermediate models
dbt run --select 'intermediate.*' --target transform

# Run metrics models
dbt run --select 'metrics.*' --target analytics
```

## Project Structure

- `models/staging/` - Staging models (views in source database)
- `models/intermediate/` - Intermediate models (tables in transform database)
- `models/metrics/` - Metrics models (tables in analytics database)
- `seeds/` - CSV seed files
- `macros/` - Custom macros including database attachment logic

## Resources

- [HOW_TO_RUN.md](HOW_TO_RUN.md) - Complete guide for running dbt commands
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
