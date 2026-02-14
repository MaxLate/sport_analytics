###########################################################################################
##### This script extracts Whoop data from the Whoop API and saves it to a JSON file. #####
##### I'm using the whoop package from the repo: https://github.com/hedgertronic/whoop  ###
###########################################################################################

import datetime
import json
import sys
from pathlib import Path

# Add project root to path (4 levels up: whoop -> 0_extract -> 1_elt -> project_root)
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))
from config_loader import Config
from whoop import WhoopClient

config = Config()
start_date = "2025-01-01 00:00:00.000000"

# Set database path in 0_data/raw/whoop folder
db_dir = project_root / "0_data" / "raw" / "whoop"
db_dir.mkdir(parents=True, exist_ok=True)


print("Fetching Whoop data...")
# Use access_token from config instead of username/password
# WHOOP no longer supports password-based authentication
workouts = []
sleeps = []
cycles = []
recoveries = []

with WhoopClient(authenticate=False) as client:
    print("  Getting workouts...")
    try:
        workouts = client.get_workout_collection(start_date=start_date)
        workout_file = (
            db_dir / f"workouts_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
        )
        with open(workout_file, "w") as f:
            json.dump(workouts, f, indent=2)
        print(f"    Found {len(workouts)} workouts")
        print(f"    Saved to: {workout_file}")
    except Exception as e:
        print(f"    ⚠️  Failed to get workouts: {e}")

    print("  Getting sleeps...")
    try:
        sleeps = client.get_sleep_collection(start_date=start_date)
        sleep_file = (
            db_dir / f"sleeps_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
        )
        with open(sleep_file, "w") as f:
            json.dump(sleeps, f, indent=2)
        print(f"    Found {len(sleeps)} sleeps")
        print(f"    Saved to: {sleep_file}")
    except Exception as e:
        print(f"    ⚠️  Failed to get sleeps: {e}")

    print("  Getting physiological cycles...")
    try:
        cycles = client.get_cycle_collection(start_date=start_date)
        cycle_file = (
            db_dir / f"cycles_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
        )
        with open(cycle_file, "w") as f:
            json.dump(cycles, f, indent=2)
        print(f"    Found {len(cycles)} physiological cycles")
        print(f"    Saved to: {cycle_file}")
    except Exception as e:
        print(f"    ⚠️  Failed to get physiological cycles: {e}")

    print("  Getting recovery data...")
    try:
        recoveries = client.get_recovery_collection(start_date=start_date)
        recovery_file = (
            db_dir / f"recoveries_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
        )
        with open(recovery_file, "w") as f:
            json.dump(recoveries, f, indent=2)
        print(f"    Found {len(recoveries)} recoveries")
        print(f"    Saved to: {recovery_file}")
    except Exception as e:
        print(f"    ⚠️  Failed to get recoveries: {e}")
