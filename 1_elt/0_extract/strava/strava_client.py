#######################################################################
##### This script is a client for interacting with the Strava API.#####
##### It handles authentication and API requests to Strava.       #####
#######################################################################

import requests
import time
from datetime import datetime
import sys
from pathlib import Path

# Add project root to path to import config_loader (4 levels up: strava -> 0_extract -> 1_elt -> project_root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from config_loader import Config


class StravaClient:
    """Client for interacting with Strava API."""

    def __init__(self):
        self.config = Config()
        self.client_id = self.config.strava_client_id
        self.client_secret = self.config.strava_client_secret
        self.refresh_token = self.config.strava_refresh_token
        self.access_token = None
        self.token_expires_at = 0

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError(
                "Missing Strava credentials in config.yml\n"
                "Please run 'python 1_elt/0_extract/strava/auth_strava.py' first to set up authentication."
            )

    def _refresh_access_token(self):
        """Refresh the access token using the refresh token."""
        print("🔄 Refreshing access token...")

        token_url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }

        response = requests.post(token_url, data=payload)

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data["access_token"]
            self.token_expires_at = token_data["expires_at"]
            print(
                f"✅ Token refreshed. Expires at: {datetime.fromtimestamp(self.token_expires_at)}"
            )
            return True
        else:
            print(f"❌ Error refreshing token: {response.status_code}")
            print(response.text)
            return False

    def _ensure_valid_token(self):
        """Ensure we have a valid access token."""
        current_time = time.time()

        # Refresh if token doesn't exist or is about to expire (5 min buffer)
        if not self.access_token or current_time >= (self.token_expires_at - 300):
            return self._refresh_access_token()

        return True

    def _make_request(self, endpoint, params=None):
        """Make an authenticated request to Strava API."""
        if not self._ensure_valid_token():
            raise Exception("Failed to get valid access token")

        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"https://www.strava.com/api/v3/{endpoint}"

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Error: {response.status_code}")
            print(response.text)
            return None

    def get_athlete(self):
        """Get the authenticated athlete's profile."""
        print("👤 Fetching athlete profile...")
        return self._make_request("athlete")

    def get_activities(self, per_page=30, page=1, after=None, before=None):
        """
        Get athlete's activities.

        Args:
            per_page: Number of activities per page (max 200)
            page: Page number
            after: Unix timestamp to fetch activities after
            before: Unix timestamp to fetch activities before
        """
        params = {"per_page": per_page, "page": page}

        if after:
            params["after"] = after
        if before:
            params["before"] = before

        print(f"📊 Fetching activities (page {page}, {per_page} per page)...")
        return self._make_request("athlete/activities", params=params)

    def get_all_activities(self, after=None, before=None):
        """
        Get all activities by paginating through results.

        Args:
            after: Unix timestamp to fetch activities after
            before: Unix timestamp to fetch activities before
        """
        all_activities = []
        page = 1
        per_page = 200  # Max allowed by Strava

        print("📥 Fetching all activities...")

        while True:
            activities = self.get_activities(
                per_page=per_page, page=page, after=after, before=before
            )

            if not activities or len(activities) == 0:
                break

            all_activities.extend(activities)
            print(
                f"   Retrieved {len(activities)} activities (total: {len(all_activities)})"
            )

            if len(activities) < per_page:
                break

            page += 1
            time.sleep(0.5)  # Be nice to the API

        print(f"✅ Total activities retrieved: {len(all_activities)}")
        return all_activities

    def get_activity_by_id(self, activity_id):
        """Get detailed information about a specific activity."""
        print(f"🔍 Fetching activity {activity_id}...")
        return self._make_request(f"activities/{activity_id}")

    def get_activity_streams(self, activity_id, keys=None):
        """
        Get activity streams (GPS, heart rate, power, etc.).

        Args:
            activity_id: Activity ID
            keys: List of stream types (e.g., ['time', 'latlng', 'heartrate', 'watts'])
        """
        if keys is None:
            keys = [
                "time",
                "latlng",
                "distance",
                "altitude",
                "heartrate",
                "watts",
                "cadence",
                "temp",
            ]

        keys_str = ",".join(keys)
        endpoint = f"activities/{activity_id}/streams"
        params = {"keys": keys_str, "key_by_type": True}

        print(f"📈 Fetching streams for activity {activity_id}...")
        return self._make_request(endpoint, params=params)

    def get_athlete_stats(self, athlete_id):
        """Get athlete statistics."""
        print(f"📊 Fetching athlete stats for {athlete_id}...")
        return self._make_request(f"athletes/{athlete_id}/stats")


if __name__ == "__main__":
    # Test the client
    try:
        client = StravaClient()

        # Get athlete info
        athlete = client.get_athlete()
        if athlete:
            print(f"\n👤 Athlete: {athlete['firstname']} {athlete['lastname']}")
            print(f"   ID: {athlete['id']}")

        # Get recent activities
        activities = client.get_activities(per_page=5)
        if activities:
            print(f"\n📊 Recent Activities ({len(activities)}):")
            for activity in activities:
                print(
                    f"   - {activity['name']} ({activity['type']}) - {activity['start_date']}"
                )
    except (FileNotFoundError, ValueError) as e:
        print(f"❌ {e}")
