#!/usr/bin/env python3
"""
Configuration Loader
Loads configuration from config.yml
"""

import yaml
from pathlib import Path


class Config:
    """Load and access configuration from config.yml"""

    def __init__(self, config_path="config.yml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self):
        """Load YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self.config_path}\n"
                f"Please copy config_sample.yml to config.yml and add your credentials."
            )

        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)

    def get(self, *keys, default=None):
        """
        Get nested config value.

        Example:
            config.get('strava', 'client_id')
            config.get('database', 'path')
        """
        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default

            if value is None:
                return default

        return value

    def update(self, *keys, value):
        """
        Update nested config value and save to file.

        Example:
            config.update('strava', 'refresh_token', value='new_token')
        """
        # Navigate to the nested location
        current = self.config
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # Set the value
        current[keys[-1]] = value

        # Save to file
        self._save_config()

    def _save_config(self):
        """Save configuration to YAML file."""
        with open(self.config_path, "w") as f:
            yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)

    @property
    def strava_client_id(self):
        return self.get("strava", "client_id")

    @property
    def strava_client_secret(self):
        return self.get("strava", "client_secret")

    @property
    def strava_refresh_token(self):
        return self.get("strava", "refresh_token")

    @property
    def database_path(self):
        return self.get("database", "path", default="0_data/database/source.duckdb")

    @property
    def raw_data_path(self):
        return self.get("paths", "raw_data", default="0_data/raw")

    @property
    def processed_data_path(self):
        return self.get("paths", "processed_data", default="0_data/processed")

    @property
    def whoop_client_id(self):
        return self.get("whoop", "client_id")

    @property
    def whoop_client_secret(self):
        return self.get("whoop", "client_secret")

    @property
    def whoop_refresh_token(self):
        return self.get("whoop", "refresh_token")

    @property
    def whoop_redirect_url(self):
        return self.get("whoop", "redirect_url")

    @property
    def whoop_access_token(self):
        return self.get("whoop", "access_token")


if __name__ == "__main__":
    # Test config loader
    try:
        config = Config()
        print("✅ Config loaded successfully!")
        print(f"   Client ID: {config.strava_client_id}")
        print(f"   Database Path: {config.database_path}")
    except FileNotFoundError as e:
        print(f"❌ {e}")
