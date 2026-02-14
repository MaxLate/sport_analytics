#################################################################################
##### This script loads Whoop data from a JSON file into a DuckDB database. #####
##### 1. Finds the newest file with the given prefix and deletes older ones.#####
##### 2. Loads the data into a DuckDB database.                             #####
##### 3. Only inserts new rows that don't already exist.                    #####
#################################################################################

import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import re

# Get project root directory (4 levels up: whoop -> 1_load -> 1_elt -> project_root)
project_root = Path(__file__).parent.parent.parent.parent
data_dir = project_root / "0_data" / "raw" / "whoop"
db_dir = project_root / "0_data" / "database"
db_path = db_dir / "source.duckdb"


def get_newest_file_by_prefix(prefix: str) -> Path | None:
    """
    Finds the newest file with the given prefix and deletes older ones.
    Files are expected to be named like: prefix_YYYY-MM-DD.json

    Args:
        prefix: The file prefix (e.g., "workouts", "sleeps", "cycles", "recoveries")

    Returns:
        Path to the newest file, or None if no files found
    """
    if not data_dir.exists():
        return None

    # Find all files matching the prefix pattern: prefix_YYYY-MM-DD.json
    pattern = re.compile(rf"^{re.escape(prefix)}_(\d{{4}}-\d{{2}}-\d{{2}})\.json$")
    matching_files = []

    for file_path in data_dir.glob(f"{prefix}_*.json"):
        match = pattern.match(file_path.name)
        if match:
            try:
                date_str = match.group(1)
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                matching_files.append((file_date, file_path))
            except ValueError:
                # Skip files with invalid date format
                continue

    if not matching_files:
        return None

    # Sort by date (newest first)
    matching_files.sort(key=lambda x: x[0], reverse=True)

    # Keep the newest file
    newest_date, newest_file = matching_files[0]

    # Delete older files
    deleted_count = 0
    for date, file_path in matching_files[1:]:
        try:
            file_path.unlink()
            deleted_count += 1
            print(f"  🗑️  Deleted older file: {file_path.name}")
        except Exception as e:
            print(f"  ⚠️  Could not delete {file_path.name}: {e}")

    if deleted_count > 0:
        print(
            f"  ✅ Kept newest {prefix} file: {newest_file.name} (deleted {deleted_count} older file(s))"
        )
    else:
        print(f"  ✅ Using {prefix} file: {newest_file.name}")

    return newest_file


def get_files_in_directory():
    """
    Scans the data directory and finds the newest file for each category.
    Deletes older files with the same prefix.
    Returns a dictionary with categorized file paths.
    """
    if not data_dir.exists():
        print(f"⚠️  Directory not found: {data_dir}")
        return {"workouts": None, "sleeps": None, "cycles": None, "recoveries": None}

    print("📂 Scanning for WHOOP data files...")

    # Get newest file for each category (and delete older ones)
    categorized = {
        "workouts": get_newest_file_by_prefix("workouts"),
        "sleeps": get_newest_file_by_prefix("sleeps"),
        "cycles": get_newest_file_by_prefix("cycles"),
        "recoveries": get_newest_file_by_prefix("recoveries"),
    }

    return categorized


def load_to_duckdb(workouts, sleeps, cycles, recoveries):
    """
    Dynamically loads data into DuckDB tables, only inserting new rows that don't already exist.
    Uses unique identifiers to check for duplicates:
    - workouts: 'id'
    - sleeps: 'id'
    - cycles: 'cycle_id'
    - recoveries: 'cycle_id'
    """
    # Ensure database directory exists
    db_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS whoop;")

    # Mapping: data -> table name, display name, and unique ID field
    data_mapping = {
        "workouts": {
            "data": workouts,
            "table": "whoop.whoop_workouts",
            "display_name": "workouts",
            "id_field": "id",
        },
        "sleeps": {
            "data": sleeps,
            "table": "whoop.whoop_sleeps",
            "display_name": "sleeps",
            "id_field": "id",
        },
        "cycles": {
            "data": cycles,
            "table": "whoop.whoop_physiological_cycles",
            "display_name": "physiological cycles",
            "id_field": "cycle_id",
        },
        "recoveries": {
            "data": recoveries,
            "table": "whoop.whoop_recoveries",
            "display_name": "recoveries",
            "id_field": "cycle_id",
        },
    }

    # Process each data type
    for key, config in data_mapping.items():
        data = config["data"]
        table_name = config["table"]
        display_name = config["display_name"]
        id_field = config["id_field"]

        if data is None or len(data) == 0:
            print(f"⚠️  No {display_name} found")
            continue

        df = pd.DataFrame(data)
        temp_table = f"temp_{key}"

        # Check if table exists and get existing IDs
        table_exists = False
        existing_ids = set()

        try:
            # Try to query the table - if it exists, get existing IDs
            existing_df = con.execute(f"SELECT {id_field} FROM {table_name}").df()
            table_exists = True
            if not existing_df.empty:
                existing_ids = set(existing_df[id_field].tolist())
        except Exception:
            # Table doesn't exist - will create it
            table_exists = False

        # Filter out rows that already exist
        if table_exists and existing_ids:
            initial_count = len(df)
            df = df[~df[id_field].isin(existing_ids)]
            new_count = len(df)
            skipped_count = initial_count - new_count

            if skipped_count > 0:
                print(
                    f"  ℹ️  Skipped {skipped_count} existing {display_name} (already in database)"
                )

        if len(df) == 0:
            print(f"⚠️  No new {display_name} to insert (all already exist)")
            continue

        # Insert new rows
        con.register(temp_table, df)

        if table_exists:
            # Insert only new rows
            con.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")
            print(f"✅ Inserted {len(df)} new {display_name} into {table_name}")
        else:
            # Create table with new data
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_table}")
            print(f"✅ Created table {table_name} with {len(df)} {display_name}")

        con.unregister(temp_table)

    con.close()
    print(f"\n✅ Loading complete!")
    print(f"Data saved to: {db_path}")


# Main execution
if __name__ == "__main__":
    # Get categorized files (newest version of each)
    files = get_files_in_directory()

    # Read JSON files
    print("\n📖 Reading JSON files...")
    workouts_data = None
    sleeps_data = None
    cycles_data = None
    recoveries_data = None

    if files["workouts"]:
        try:
            with open(files["workouts"], "r") as f:
                workouts_data = json.load(f)
            print(
                f"  ✅ Loaded {len(workouts_data)} workouts from {files['workouts'].name}"
            )
        except Exception as e:
            print(f"  ⚠️  Failed to read workouts file: {e}")

    if files["sleeps"]:
        try:
            with open(files["sleeps"], "r") as f:
                sleeps_data = json.load(f)
            print(f"  ✅ Loaded {len(sleeps_data)} sleeps from {files['sleeps'].name}")
        except Exception as e:
            print(f"  ⚠️  Failed to read sleeps file: {e}")

    if files["cycles"]:
        try:
            with open(files["cycles"], "r") as f:
                cycles_data = json.load(f)
            print(f"  ✅ Loaded {len(cycles_data)} cycles from {files['cycles'].name}")
        except Exception as e:
            print(f"  ⚠️  Failed to read cycles file: {e}")

    if files["recoveries"]:
        try:
            with open(files["recoveries"], "r") as f:
                recoveries_data = json.load(f)
            print(
                f"  ✅ Loaded {len(recoveries_data)} recoveries from {files['recoveries'].name}"
            )
        except Exception as e:
            print(f"  ⚠️  Failed to read recoveries file: {e}")

    # Load to DuckDB
    print("\n💾 Loading data into DuckDB...")
    load_to_duckdb(workouts_data, sleeps_data, cycles_data, recoveries_data)
