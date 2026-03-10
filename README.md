# Sport Analytics - 2026 (Whoop & Strava)

**Important Note**: This project is still WORK IN PROGRESS and will be updated and enhanced most of the time. For more details, see ToDos below.
Since this project should also be used as a portfolio project for my career as Analytics Engineer, the focus lies on the end-to-end data pipeline. So for transparency, there are some parts which are vibecoded, especially all HTML and CSS parts.

**Intro**: An end-to-end data pipeline project that extracts, transforms, and analyzes fitness data from Strava and Whoop APIs. This project demonstrates a complete ELT (Extract, Load, Transform) workflow using modern data engineering tools, with interactive analytics dashboards and a simple MVP for natural language query capabilities.

![Text-to-SQL-Solution](Text-to-SQL.gif)

## 🎯 Overview

This repository serves as a learning and experimentation platform for data engineering, combining:
- **API Integration**: Automated data extraction from Strava and Whoop
- **Data Transformation**: Multi-stage dbt pipeline with staging, intermediate, and metrics layers
- **Analytics Applications**: Interactive dashboards and natural language query interfaces
- **Tech Stack**: DuckDB, dbt, FastAPI, and more


## ToDos:
- **Improve LLM with Whoop Data**: Add Whoop data like heart rate etc.
- **Alerting and Testing**: Set up a much better Testing and Alerting Setup via Slack. 
- **Orchestration**: Set up all python scripts and dbt runs by Airflow
- **Docker**: Dockerize the whole project


## 🏗️ Architecture

The project follows a three-stage ELT pipeline:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Extract   │ --> │    Load     │ --> │  Transform  │ --> │  Analytics  │
│  (APIs)     │     │  (DuckDB)   │     │    (dbt)    │     │  (FastAPI)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

**Note**: The pipeline uses incremental data ingestion - only new data is extracted and loaded, avoiding duplicate processing of existing records.

### Data Flow

1. **Extract** (`1_elt/0_extract/`): Python scripts fetch data from Strava and Whoop APIs
2. **Load** (`1_elt/1_load/`): Raw JSON data is loaded into DuckDB source database (incremental - only new data)
3. **Transform** (`1_elt/2_transform/`): dbt models transform data through staging → intermediate → metrics layers
4. **Analytics** (`2_analytics/`): FastAPI applications serve interactive dashboards and queries

### Database Architecture

The project uses **three separate DuckDB databases** for separation of concerns:

- **`source.duckdb`**: Raw source data and staging views
- **`transform.duckdb`**: Intermediate transformation tables
- **`analytics.duckdb`**: Final metrics and fact tables for analytics

## 📊 Data Sources

### Strava
- Activity data (runs, rides, swims, etc.)
- Social interactions (kudos, comments)
- Performance metrics (distance, time, elevation, etc.)

### Whoop
- Sleep data (duration, quality, efficiency)
- Recovery metrics
- Workout data

## 🛠️ Tech Stack

- **Database**: DuckDB (analytical database)
- **Transformation**: dbt (data build tool)
- **APIs**: FastAPI, uvicorn
- **Language**: Python 3.11+
- **Package Management**: uv
- **Code Quality**: black, sqlfluff, pre-commit
- **LLM Integration**: Ollama (for local natural language queries)

## 📁 Project Structure

```
sport_analytics/
├── 0_data/                    # Data storage
│   ├── database/              # DuckDB databases
│   └── raw/                   # Raw JSON files
│
├── 1_elt/                     # Extract, Load, Transform
│   ├── 0_extract/            # API extraction scripts
│   │   ├── strava/
│   │   └── whoop/
│   ├── 1_load/              # Data loading scripts
│   │   ├── strava/
│   │   └── whoop/
│   └── 2_transform/          # dbt transformation project
│       ├── models/
│       │   ├── staging/     # Staging models (views)
│       │   ├── intermediate/ # Intermediate models (tables)
│       │   └── metrics/     # Final metrics (tables)
│       ├── seeds/           # CSV seed files
│       └── macros/          # Custom dbt macros
│
├── 2_analytics/              # Analytics applications
│   ├── Chat-to-Data/        # Natural language query interface
│   └── sleep_analytics/     # Sleep data dashboard
│
├── config.yml                # Configuration (create from config_sample.yml)
├── config_sample.yml         # Sample configuration template
└── pyproject.toml            # Python dependencies
```

## 🚀 Getting Started

### Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- [dbt](https://www.getdbt.com/) (installed via uv)
- API credentials for Strava and/or Whoop

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd sport_analytics
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Set up configuration**
   ```bash
   cp config_sample.yml config.yml
   # Edit config.yml with your API credentials
   ```

4. **Set up dbt profiles** (if needed)
   ```bash
   cd 1_elt/2_transform
   # Edit profiles.yml with your database paths
   ```

### Configuration

Create `config.yml` from `config_sample.yml` and add your API credentials:

```yaml
strava:
  client_id: 'your_client_id'
  client_secret: 'your_client_secret'
  refresh_token: 'your_refresh_token'

whoop:
  client_id: 'your_client_id'
  client_secret: 'your_client_secret'
  access_token: 'your_access_token'
  refresh_token: 'your_refresh_token'
  redirect_url: 'http://localhost:8000/callback'

database:
  path: 0_data/database/source.duckdb
```

## 📖 Usage

### 1. Extract Data

Extract data from APIs:

```bash
# Extract Strava data
python 1_elt/0_extract/strava/extract_strava_data.py

# Extract Whoop data
python 1_elt/0_extract/whoop/extract_whoop_data.py
```

### 2. Load Data

Load raw JSON into DuckDB:

```bash
# Load Strava data
python 1_elt/1_load/strava/load_strava_data.py

# Load Whoop data
python 1_elt/1_load/whoop/load_whoop_data.py
```

### 3. Transform Data

Run dbt transformations:

```bash
cd 1_elt/2_transform

# Load seeds
dbt seed --target source

#  Please note that using duckdb currently makes execution and targeting rather cumbersome.  
# Run staging models
dbt run --select 'staging.*' --target source

# Run intermediate models
dbt run --select 'intermediate.*' --target transform

# Run metrics models
dbt run --select 'metrics.*' --target analytics
```

For detailed dbt instructions, see [`1_elt/2_transform/HOW_TO_RUN.md`](1_elt/2_transform/HOW_TO_RUN.md).

### 4. Run Analytics Applications

#### Chat-to-Data (Natural Language Queries)

Query your Strava activities using natural language:

```bash
# Start the API (from project root)
uv run uvicorn "2_analytics.Chat-to-Data.api:app" --reload

# In another terminal, start the HTML server
cd 2_analytics/Chat-to-Data
python3 -m http.server 5500

# Open http://127.0.0.1:5500 in your browser
```

**Prerequisites for Chat-to-Data:**
- Install Ollama: https://ollama.ai/download
- Pull a model: `ollama pull llama3.2`
- Optional: Set `OLLAMA_URL` and `OLLAMA_MODEL` environment variables

For more details, see [`2_analytics/Chat-to-Data/how_to_run_chat.txt`](2_analytics/Chat-to-Data/how_to_run_chat.txt).

#### Sleep Analytics Dashboard

Visualize your Whoop sleep data:

```bash
# Start the API (from project root)
uv run uvicorn "2_analytics.sleep_analytics.api:app" --reload

# In another terminal, start the HTML server
cd 2_analytics/sleep_analytics
python3 -m http.server 5500

# Open http://127.0.0.1:5500 in your browser
```

For more details, see [`2_analytics/sleep_analytics/how_to_run.txt`](2_analytics/sleep_analytics/how_to_run.txt).

## 🗄️ Database Schema

### Key Tables

**Staging Layer** (`source.duckdb`):
- `strava.stg_strava_activities`
- `whoop.stg_whoop_sleeps`
- `whoop.stg_whoop_workouts`

**Intermediate Layer** (`transform.duckdb`):
- `strava.int_strava_activities`
- `whoop.int_whoop_sleep`
- `whoop.int_whoop_workouts`

**Metrics Layer** (`analytics.duckdb`):
- `strava.fct_strava_activities`
- `strava.fct_strava_activities_socials`
- `whoop.fct_whoop_sleeps`
- `whoop.fct_whoop_sleep_quality`
- `whoop.fct_whoop_workouts`
- `semantic.fct_activities` (unified activities table)
- `dates.dim_dates` (date dimension table)

## 🧪 Testing

Run dbt tests:

```bash
cd 1_elt/2_transform

# Test all models
dbt test

# Test specific layer
dbt test --select 'staging.*' --target source
dbt test --select 'intermediate.*' --target transform
dbt test --select 'metrics.*' --target analytics
```

## 📝 Code Quality

The project uses pre-commit hooks for code quality:

- **black**: Python code formatting
- **sqlfluff**: SQL linting and formatting

Hooks run automatically on commit. To run manually:

```bash
pre-commit run --all-files
```

## 🔧 Development

### Adding New Data Sources

1. Create extraction script in `1_elt/0_extract/<source>/`
2. Create loading script in `1_elt/1_load/<source>/`
3. Add staging models in `1_elt/2_transform/models/staging/<source>/`
4. Build intermediate and metrics models as needed

### Adding New Analytics

1. Create new directory in `2_analytics/`
2. Add FastAPI application (`api.py`)
3. Add HTML frontend (`index.html`)
4. Document setup in `how_to_run.txt`

## 📚 Documentation

- [dbt Transformation Guide](1_elt/2_transform/HOW_TO_RUN.md)
- [Chat-to-Data Setup](2_analytics/Chat-to-Data/how_to_run_chat.txt)
- [Sleep Analytics Setup](2_analytics/sleep_analytics/how_to_run.txt)

## 🤝 Contributing

This is a personal learning project, but suggestions and improvements are welcome! Feel free to:
- Open issues for bugs or feature requests
- Submit pull requests with improvements
- Share ideas for new features or data sources

## 📄 License

This project is for personal learning and experimentation purposes.

## 🙏 Acknowledgments

- [Strava API](https://developers.strava.com/)
- [Whoop API](https://developer.whoop.com/)
- [dbt](https://www.getdbt.com/)
- [DuckDB](https://duckdb.org/)
- [FastAPI](https://fastapi.tiangolo.com/)

---

**Note**: This project is primarily for learning and experimentation with modern data engineering tools and practices.
