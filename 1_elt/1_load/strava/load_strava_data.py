#################################################################################
##### This script loads Strava data from a JSON file into a DuckDB database.#####
##### 1. Finds the newest activities file and deletes older ones.           #####
##### 2. Loads the activities into a DuckDB database.                       #####
##### 3. Only inserts new rows that don't already exist.                    #####
#################################################################################


import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import re
import sys

# Add project root to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config_loader import Config


# Get project root directory (3 levels up from this script)
project_root = Path(__file__).parent.parent.parent
data_dir = project_root / "0_data" / "raw" / "strava"
db_dir = project_root / "0_data" / "database"
db_path = db_dir / "source.duckdb"


def get_newest_activities_file() -> Path | None:
    """
    Finds the newest activities file and deletes older ones.
    Files are expected to be named like: activities_YYYYMMDD_HHMMSS.json

    Returns:
        Path to the newest file, or None if no files found
    """
    if not data_dir.exists():
        return None

    # Find all files matching the pattern: activities_YYYYMMDD_HHMMSS.json
    pattern = re.compile(r"^activities_(\d{8}_\d{6})\.json$")
    matching_files = []

    for file_path in data_dir.glob("activities_*.json"):
        match = pattern.match(file_path.name)
        if match:
            try:
                timestamp_str = match.group(1)
                file_datetime = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                matching_files.append((file_datetime, file_path))
            except ValueError:
                # Skip files with invalid date format
                continue

    if not matching_files:
        return None

    # Sort by datetime (newest first)
    matching_files.sort(key=lambda x: x[0], reverse=True)

    # Keep the newest file
    newest_datetime, newest_file = matching_files[0]

    # Delete older files
    deleted_count = 0
    for datetime_obj, file_path in matching_files[1:]:
        try:
            file_path.unlink()
            deleted_count += 1
            print(f"  🗑️  Deleted older file: {file_path.name}")
        except Exception as e:
            print(f"  ⚠️  Could not delete {file_path.name}: {e}")

    if deleted_count > 0:
        print(
            f"  ✅ Kept newest activities file: {newest_file.name} (deleted {deleted_count} older file(s))"
        )
    else:
        print(f"  ✅ Using activities file: {newest_file.name}")

    return newest_file


def flatten_activity(activity):
    """Flatten nested activity data for easier analysis."""
    flat = {
        "activity_id": activity.get("id"),
        "name": activity.get("name"),
        "type": activity.get("type"),
        "sport_type": activity.get("sport_type"),
        "start_date": activity.get("start_date"),
        "start_date_local": activity.get("start_date_local"),
        "timezone": activity.get("timezone"),
        "distance": activity.get("distance"),
        "moving_time": activity.get("moving_time"),
        "elapsed_time": activity.get("elapsed_time"),
        "total_elevation_gain": activity.get("total_elevation_gain"),
        "average_speed": activity.get("average_speed"),
        "max_speed": activity.get("max_speed"),
        "average_heartrate": activity.get("average_heartrate"),
        "max_heartrate": activity.get("max_heartrate"),
        "average_watts": activity.get("average_watts"),
        "kilojoules": activity.get("kilojoules"),
        "average_cadence": activity.get("average_cadence"),
        "achievement_count": activity.get("achievement_count"),
        "kudos_count": activity.get("kudos_count"),
        "comment_count": activity.get("comment_count"),
        "athlete_count": activity.get("athlete_count"),
        "trainer": activity.get("trainer"),
        "commute": activity.get("commute"),
        "manual": activity.get("manual"),
        "private": activity.get("private"),
        "flagged": activity.get("flagged"),
        "gear_id": activity.get("gear_id"),
        "start_latitude": (
            activity.get("start_latlng", [None, None])[0]
            if activity.get("start_latlng")
            else None
        ),
        "start_longitude": (
            activity.get("start_latlng", [None, None])[1]
            if activity.get("start_latlng")
            else None
        ),
        "end_latitude": (
            activity.get("end_latlng", [None, None])[0]
            if activity.get("end_latlng")
            else None
        ),
        "end_longitude": (
            activity.get("end_latlng", [None, None])[1]
            if activity.get("end_latlng")
            else None
        ),
        "extracted_at": datetime.now().isoformat(),
    }
    return flat


def load_to_duckdb(activities, config):
    """
    Dynamically loads data into DuckDB table, only inserting new rows that don't already exist.
    Uses unique identifier 'activity_id' to check for duplicates.
    """
    # Ensure database directory exists
    # Use source.duckdb for the new multi-database structure
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / "0_data" / "database" / "source.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if activities is None or len(activities) == 0:
        print("⚠️  No activities found")
        return

    # Flatten activities
    flattened = [flatten_activity(a) for a in activities]
    df = pd.DataFrame(flattened)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS strava;")

    table_name = "strava.strava_activities"
    id_field = "activity_id"
    temp_table = "temp_activities"

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
                f"  ℹ️  Skipped {skipped_count} existing activities (already in database)"
            )

    if len(df) == 0:
        print("⚠️  No new activities to insert (all already exist)")
        con.close()
        return

    # Insert new rows
    con.register(temp_table, df)

    if table_exists:
        # Insert only new rows
        con.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")
        print(f"✅ Inserted {len(df)} new activities into {table_name}")
    else:
        # Create table with new data
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_table}")
        print(f"✅ Created table {table_name} with {len(df)} activities")

    con.unregister(temp_table)

    con.close()
    print(f"\n✅ Loading complete!")
    print(f"Data saved to: {db_path}")


# Main execution
if __name__ == "__main__":
    # Get newest activities file
    print("📂 Scanning for Strava data files...")
    activities_file = get_newest_activities_file()

    # Read JSON file
    print("\n📖 Reading JSON file...")
    activities_data = None

    if activities_file:
        try:
            with open(activities_file, "r") as f:
                activities_data = json.load(f)
            print(
                f"  ✅ Loaded {len(activities_data)} activities from {activities_file.name}"
            )
        except Exception as e:
            print(f"  ⚠️  Failed to read activities file: {e}")
    else:
        print("  ⚠️  No activities file found")

    # Load to DuckDB
    if activities_data:
        print("\n💾 Loading data into DuckDB...")
        config = Config()
        load_to_duckdb(activities_data, config)
    else:
        print("\n⚠️  No data to load")
