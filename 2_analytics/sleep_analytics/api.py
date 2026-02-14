# =====================================================================
# FastAPI backend for serving DuckDB
# =====================================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import duckdb
import pandas as pd

# Get the project root directory (go up 3 levels from 2_analytics/sleep_analytics/)
PROJECT_ROOT = Path(__file__).parent.parent.parent
DUCKDB_PATH = str(PROJECT_ROOT / "0_data" / "database" / "analytics.duckdb")
TABLE_NAME = "whoop.fct_whoop_sleeps"

app = FastAPI()

# Allow HTML frontend to access API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/data")
def get_data():
    """Returns the full sleep dataset as JSON."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute(
        f"""
        SELECT
        date_day,
        asleep_duration_minutes,
        light_sleep_duration_minutes,
        deep_sleep_duration_minutes,
        rem_duration_minutes,
        awake_duration_minutes,
        in_bed_duration_minutes,
        moon_phase

        FROM {TABLE_NAME}
        WHERE 
            asleep_duration_minutes IS NOT NULL
            AND moon_phase IS NOT NULL
    """
    ).fetchdf()
    con.close()

    return df.to_dict(orient="records")
