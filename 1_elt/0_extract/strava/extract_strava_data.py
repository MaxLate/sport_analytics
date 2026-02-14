#############################################################################################
##### This script extracts Strava data from the Strava API and saves it to a JSON file. #####
##### 1. Gets all activities from the Strava API.                                       #####
##### 2. Saves the activities to a JSON file.                                           #####
#############################################################################################

import json
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config_loader import Config
from strava.strava_client import StravaClient


def ensure_directories(config):
    """Create necessary directories if they don't exist."""
    db_path = Path(config.database_path)
    raw_path = Path(config.raw_data_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.mkdir(parents=True, exist_ok=True)


def save_raw_json(data, filename, config):
    """Save raw JSON data for backup."""
    # Save to strava subdirectory within raw data path
    filepath = Path(config.raw_data_path) / "strava" / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved raw data to {filepath}")


def main():
    print("🚀 Strava Activity Extraction")
    print("=" * 50)

    # Load config
    try:
        config = Config()
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return

    # Ensure directories exist
    ensure_directories(config)

    # Initialize Strava client
    try:
        client = StravaClient()
    except ValueError as e:
        print(f"❌ Error: {e}")
        return

    # Get athlete info
    athlete = client.get_athlete()
    if athlete:
        print(f"\n👤 Athlete: {athlete['firstname']} {athlete['lastname']}")
        athlete_id = athlete["id"]
    else:
        print("❌ Failed to get athlete info")
        return

    # Get all activities
    activities = client.get_all_activities()

    if not activities:
        print("❌ No activities found")
        return

    # Save raw JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_raw_json(activities, f"activities_{timestamp}.json", config)

    print("\n✅ Extraction complete!")


if __name__ == "__main__":
    main()
