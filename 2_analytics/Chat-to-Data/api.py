# =====================================================================
# FastAPI backend for Text-to-SQL querying of fct_activities
# =====================================================================

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import duckdb
import pandas as pd
import yaml
import os
import re
from typing import Optional
from datetime import datetime, date

# Get the project root directory (go up 3 levels from 2_analytics/Chat-to-Data/api.py)
PROJECT_ROOT = Path(__file__).parent.parent.parent
DUCKDB_PATH = str(PROJECT_ROOT / "0_data" / "database" / "analytics.duckdb")
TABLE_NAME = "semantic.fct_activities"
YAML_SCHEMA_PATH = (
    PROJECT_ROOT
    / "1_elt"
    / "2_transform"
    / "models"
    / "metrics"
    / "semantic"
    / "fct_activities.yml"
)

app = FastAPI()

# Allow HTML frontend to access API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """Root endpoint with API information."""
    return {
        "message": "Strava Activities Query API",
        "version": "1.0.0",
        "endpoints": {
            "query": "/query (POST) - Submit a natural language question",
            "schema": "/schema (GET) - Get table schema information",
            "health": "/health (GET) - Check API health and Ollama status",
        },
    }


class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    sql: str
    result: list
    error: Optional[str] = None


def load_schema_context() -> str:
    """Load the YAML schema file and convert it to a context string for the LLM."""
    try:
        with open(YAML_SCHEMA_PATH, "r") as f:
            schema_data = yaml.safe_load(f)

        model_info = schema_data["models"][0]
        context = f"""Table: {TABLE_NAME}

Description:
{model_info.get('description', '')}

Columns:
"""
        # Track which columns we've added
        added_columns = set()

        for col in model_info.get("columns", []):
            col_name = col.get("name", "")
            col_desc = col.get("description", "").strip()

            # Map sport_type to activity_name if it exists in YAML
            if col_name == "sport_type":
                col_name = "activity_name"
                col_desc = col_desc.replace("sport_type", "activity_name")
                col_desc += " (Standardized sport type: 'Run', 'Ride', 'Swim', 'Yoga', 'WeightTraining', etc.)"

            # Clean up markdown formatting for better LLM understanding
            col_desc = re.sub(r"\*\*([^*]+)\*\*", r"\1", col_desc)  # Remove bold
            col_desc = re.sub(r"`([^`]+)`", r"\1", col_desc)  # Remove code formatting
            context += f"- {col_name}: {col_desc}\n"
            added_columns.add(col_name)

        # Add missing columns that exist in the actual table but might not be in YAML
        if "activity_name" not in added_columns:
            context += "- activity_name: Standardized sport type/activity name (e.g., 'Run', 'Ride', 'Swim', 'Yoga', 'WeightTraining'). Use this for filtering by sport type.\n"
        if "is_sport_exercise" not in added_columns:
            context += "- is_sport_exercise: Boolean flag (true/false) indicating if the activity is a sport/exercise (true) or general activity (false). Use this to filter for sport activities.\n"

        return context
    except Exception as e:
        return f"Table: {TABLE_NAME}\nColumns: strava_activity_id, strava_activity_id_of_day, date_day, name, activity_name, is_sport_exercise"


def get_sql_prompt(question: str, schema_context: str) -> str:
    """Generate the prompt for SQL conversion."""
    return f"""You are a SQL expert. Convert the following natural language question into a SQL query for DuckDB.

Database Schema:
{schema_context}

Rules:
1. Only use columns that exist in the schema above
2. Use proper SQL syntax for DuckDB
3. Return ONLY the SQL query, no explanations, no markdown formatting, no code blocks
4. Use the table name: {TABLE_NAME}
5. For date comparisons, use date_day column (format: 'YYYY-MM-DD')
6. For sport types, use the activity_name column with exact values like 'Run', 'Ride', 'Swim', 'Yoga', 'WeightTraining', etc.
7. The name column contains user-defined activity names (free-form text)
8. The activity_name column contains standardized sport types
9. Use is_sport_exercise (boolean) to filter for sport/exercise activities vs general activities
10. Be precise with column names and SQL syntax

Date Handling Examples:
- "in March 2025" → WHERE date_day >= '2025-03-01' AND date_day < '2025-04-01'
- "in 2025" → WHERE date_day >= '2025-01-01' AND date_day < '2026-01-01'
- "in March" (without year) → WHERE EXTRACT(MONTH FROM date_day) = 3 (searches across ALL years)
- "in January" (without year) → WHERE EXTRACT(MONTH FROM date_day) = 1 (searches across ALL years)
- "last 30 days" → WHERE date_day >= CURRENT_DATE - INTERVAL '30 days'
- "this month" → WHERE date_day >= DATE_TRUNC('month', CURRENT_DATE) AND date_day < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
- "on 2025-03-15" → WHERE date_day = '2025-03-15'
- "in March and April" → WHERE EXTRACT(MONTH FROM date_day) IN (3, 4)

Use EXTRACT(MONTH FROM date_day) when month is specified without year to search across all years.
Use EXTRACT(YEAR FROM date_day) when you need to filter by year.
For date ranges, use >= for start and < for end (exclusive end).

Question: {question}

SQL Query:"""


def clean_sql_response(sql: str) -> str:
    """Clean up SQL response from LLM."""
    # Remove markdown code blocks if present
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"^```\s*", "", sql, flags=re.MULTILINE)
    sql = sql.strip()
    # Remove any leading/trailing quotes if present
    sql = sql.strip('"').strip("'")
    return sql


def generate_sql_with_ollama(question: str, schema_context: str) -> str:
    """Generate SQL query using Ollama (local, free, no API key needed)."""
    try:
        import requests

        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        model = os.getenv(
            "OLLAMA_MODEL", "llama3.2"
        )  # Default to llama3.2, can use llama3, mistral, etc.

        prompt = get_sql_prompt(question, schema_context)

        response = requests.post(
            f"{ollama_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                },
            },
            timeout=30,
        )
        response.raise_for_status()

        sql = response.json().get("response", "").strip()
        return clean_sql_response(sql)
    except ImportError:
        raise ValueError(
            "requests library not installed. Install with: pip install requests"
        )
    except Exception as e:
        raise ValueError(
            f"Ollama error: {str(e)}. Make sure Ollama is running: ollama serve"
        )


def generate_sql_with_llm(question: str, schema_context: str) -> str:
    """Generate SQL using Ollama."""
    return generate_sql_with_ollama(question, schema_context)


def parse_date_from_question(question: str) -> Optional[str]:
    """Extract date information from question and return SQL WHERE clause for dates."""
    question_lower = question.lower()
    current_year = datetime.now().year

    # Pattern: "in March 2025" or "in March"
    month_year_match = re.search(r"in\s+(\w+)\s+(\d{4})", question_lower)
    if month_year_match:
        month_name = month_year_match.group(1)
        year = int(month_year_match.group(2))
        month_num = _month_name_to_number(month_name)
        if month_num:
            start_date = f"{year}-{month_num:02d}-01"
            # Calculate end date (first day of next month)
            if month_num == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{month_num + 1:02d}-01"
            return f"date_day >= '{start_date}' AND date_day < '{end_date}'"

    # Pattern: "in March" (without year, search across all years for that month)
    month_only_match = re.search(r"in\s+(\w+)(?:\s|$)", question_lower)
    if month_only_match and not month_year_match:
        month_name = month_only_match.group(1)
        month_num = _month_name_to_number(month_name)
        if month_num:
            # Use EXTRACT to filter by month across all years
            return f"EXTRACT(MONTH FROM date_day) = {month_num}"

    # Pattern: "in 2025" or "during 2025"
    year_match = re.search(r"(?:in|during)\s+(\d{4})", question_lower)
    if year_match:
        year = int(year_match.group(1))
        return f"date_day >= '{year}-01-01' AND date_day < '{year + 1}-01-01'"

    # Pattern: "last 30 days" or "past 30 days"
    days_match = re.search(r"(?:last|past)\s+(\d+)\s+days?", question_lower)
    if days_match:
        days = int(days_match.group(1))
        return f"date_day >= CURRENT_DATE - INTERVAL '{days} days'"

    # Pattern: "this month"
    if "this month" in question_lower:
        return f"date_day >= DATE_TRUNC('month', CURRENT_DATE) AND date_day < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"

    # Pattern: "this year"
    if "this year" in question_lower:
        return f"date_day >= DATE_TRUNC('year', CURRENT_DATE) AND date_day < DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'"

    return None


def _month_name_to_number(month_name: str) -> Optional[int]:
    """Convert month name to number (1-12)."""
    months = {
        "january": 1,
        "jan": 1,
        "february": 2,
        "feb": 2,
        "march": 3,
        "mar": 3,
        "april": 4,
        "apr": 4,
        "may": 5,
        "june": 6,
        "jun": 6,
        "july": 7,
        "jul": 7,
        "august": 8,
        "aug": 8,
        "september": 9,
        "sep": 9,
        "sept": 9,
        "october": 10,
        "oct": 10,
        "november": 11,
        "nov": 11,
        "december": 12,
        "dec": 12,
    }
    return months.get(month_name.lower())


def generate_sql_simple(question: str, schema_context: str) -> str:
    """Enhanced rule-based SQL generation as fallback with date parsing."""
    question_lower = question.lower()

    # Extract date filter if present
    date_filter = parse_date_from_question(question)
    where_clause = f"WHERE {date_filter}" if date_filter else ""

    # Extract sport type if present (use activity_name column)
    sport_filter = ""
    if "run" in question_lower or "running" in question_lower:
        sport_filter = "activity_name = 'Run'"
    elif (
        "ride" in question_lower
        or "cycling" in question_lower
        or "bike" in question_lower
    ):
        sport_filter = "activity_name = 'Ride'"
    elif "swim" in question_lower or "swimming" in question_lower:
        sport_filter = "activity_name = 'Swim'"
    elif "hike" in question_lower or "hiking" in question_lower:
        sport_filter = "activity_name = 'Hike'"
    elif "walk" in question_lower or "walking" in question_lower:
        sport_filter = "activity_name = 'Walk'"
    elif "yoga" in question_lower:
        sport_filter = "activity_name = 'Yoga'"
    elif (
        "weight" in question_lower
        or "weight training" in question_lower
        or "strength" in question_lower
    ):
        sport_filter = "activity_name = 'WeightTraining'"
    elif "sport" in question_lower or "exercise" in question_lower:
        # Filter for sport/exercise activities
        sport_filter = "is_sport_exercise = true"

    # Build WHERE clause
    conditions = []
    if date_filter:
        conditions.append(f"({date_filter})")
    if sport_filter:
        conditions.append(sport_filter)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Determine query type
    if "how many" in question_lower or "count" in question_lower:
        if "per day" in question_lower or "each day" in question_lower:
            if date_filter:
                return f"SELECT date_day, COUNT(*) as activity_count FROM {TABLE_NAME} {where_clause} GROUP BY date_day ORDER BY date_day"
            else:
                return f"SELECT date_day, COUNT(*) as activity_count FROM {TABLE_NAME} GROUP BY date_day ORDER BY date_day"
        else:
            return f"SELECT COUNT(*) as count FROM {TABLE_NAME} {where_clause}"

    elif (
        "list" in question_lower
        or "show" in question_lower
        or "what" in question_lower
        or "which" in question_lower
    ):
        return (
            f"SELECT * FROM {TABLE_NAME} {where_clause} ORDER BY date_day DESC LIMIT 50"
        )

    # Default fallback
    return f"SELECT * FROM {TABLE_NAME} {where_clause} ORDER BY date_day DESC LIMIT 10"


@app.get("/query")
def query_activities_get(question: str = "How many activities did I do?"):
    """GET endpoint for testing - accepts question as query parameter.

    Example: http://127.0.0.1:8000/query?question=How%20many%20activities%20did%20I%20do
    """
    try:
        # Use the same logic as POST endpoint
        schema_context = load_schema_context()

        # Generate SQL - try LLM first, fallback to simple if no LLM available
        try:
            sql = generate_sql_with_llm(question, schema_context)
        except (ValueError, Exception) as e:
            # Fallback to simple generation if LLM fails or not available
            sql = generate_sql_simple(question, schema_context)

        # Execute SQL
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            df = con.execute(sql).fetchdf()
            result = df.to_dict(orient="records")
        except Exception as e:
            con.close()
            return QueryResponse(
                sql=sql, result=[], error=f"SQL execution error: {str(e)}"
            )
        finally:
            con.close()

        return QueryResponse(sql=sql, result=result, error=None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query", response_model=QueryResponse)
def query_activities(request: QueryRequest):
    """Convert natural language question to SQL and execute it."""
    try:
        # Load schema context
        schema_context = load_schema_context()

        # Generate SQL - try LLM first, fallback to simple if no LLM available
        try:
            sql = generate_sql_with_llm(request.question, schema_context)
        except (ValueError, Exception) as e:
            # Fallback to simple generation if LLM fails or not available
            sql = generate_sql_simple(request.question, schema_context)

        # Execute SQL
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            df = con.execute(sql).fetchdf()
            result = df.to_dict(orient="records")
        except Exception as e:
            con.close()
            return QueryResponse(
                sql=sql, result=[], error=f"SQL execution error: {str(e)}"
            )
        finally:
            con.close()

        return QueryResponse(sql=sql, result=result, error=None)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schema")
def get_schema():
    """Get the schema information for the table."""
    try:
        schema_context = load_schema_context()
        return {"schema": schema_context}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data")
def get_data_info():
    """Data endpoint - returns information about available data."""
    try:
        con = duckdb.connect(DUCKDB_PATH)
        # Get row count from the main table
        result = con.execute(f"SELECT COUNT(*) as count FROM {TABLE_NAME}").fetchone()
        count = result[0] if result else 0
        con.close()

        return {
            "message": "Data endpoint",
            "table": TABLE_NAME,
            "row_count": count,
            "note": "Use /query endpoint to query the data with natural language",
        }
    except Exception as e:
        return {
            "message": "Data endpoint",
            "error": str(e),
            "note": "Database may not be initialized. Run dbt to build tables.",
        }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    llm_status = "Simple rule-based (fallback)"

    # Check if Ollama is available
    try:
        import requests

        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        # Quick check if Ollama is accessible
        try:
            requests.get(f"{ollama_url}/api/tags", timeout=2)
            llm_status = "Ollama (available)"
        except:
            llm_status = "Ollama (will try, may not be running)"
    except ImportError:
        llm_status = "Ollama (requests library not installed)"

    return {
        "status": "ok",
        "table": TABLE_NAME,
        "database": DUCKDB_PATH,
        "llm_provider": llm_status,
    }
