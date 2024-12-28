import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Any, Dict
from .utils.logger_config import setup_logging


class CacheManager:
    def __init__(self, cache_dir: Optional[str] = None):
        """Initialize the cache manager with a specified cache directory."""
        self.logger = setup_logging(__name__)
        self.logger.info("Initializing CacheManager")

        if cache_dir is None:
            home_dir = str(Path.home())
            cache_dir = os.path.join(home_dir, "sofascore_cache")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, cache_type: str, key: str) -> Path:
        """Get the full path for a cache file"""
        try:
            # Handle special cases where key might contain slashes (like seasons "22/23")
            if cache_type == "match_dicts":
                # Split key into parts and handle each part separately
                parts = key.split("/")
                if len(parts) >= 3:  # league/year/page format
                    league = parts[0]
                    year = parts[1].replace(
                        "/", "_"
                    )  # Replace slashes in year with underscores
                    page = parts[2]
                    cache_dir = self.cache_dir / cache_type / league / year
                    cache_dir.mkdir(parents=True, exist_ok=True)
                    return cache_dir / f"{page}.json"
                else:
                    self.logger.warning(f"Unexpected key format for match_dicts: {key}")
                    # Fall through to default handling

            # For all other cases, including unexpected match_dicts format
            safe_key = key.replace("/", "_")
            cache_dir = self.cache_dir / cache_type
            cache_dir.mkdir(parents=True, exist_ok=True)
            return cache_dir / f"{safe_key}.json"
        except Exception as e:
            self.logger.error(
                f"Error creating cache path for type '{cache_type}' and key '{key}': {str(e)}"
            )
            raise

    def _is_cache_valid(
        self, cache_path: Path, cache_duration: Optional[timedelta] = None
    ) -> bool:
        """Check if cache exists and is valid based on duration"""
        if not cache_path.exists():
            return False

        if cache_duration is None:  # None means cache forever
            return True

        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - mtime < cache_duration

    def _get(
        self, cache_type: str, key: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Generic get method for cache data"""
        cache_path = self._get_cache_path(cache_type, key)

        if self._is_cache_valid(cache_path, cache_duration):
            try:
                self.logger.debug(f"Attempting to read cache file: {cache_path}")
                with open(cache_path, "r") as f:
                    cache_data = json.load(f)
                    self.logger.trace(
                        f"Cache data content: {json.dumps(cache_data, indent=2)}"
                    )

                # Handle old format where data was stored directly
                if not isinstance(cache_data, dict) or "data" not in cache_data:
                    self.logger.debug(f"Using legacy cache format for key: {key}")
                    return cache_data

                # Check if the cache has expired based on its own duration
                if cache_data.get("duration") is not None:
                    cache_time = datetime.fromtimestamp(cache_data["timestamp"])
                    if datetime.now() - cache_time > timedelta(
                        seconds=cache_data["duration"]
                    ):
                        self.logger.info(f"Cache expired for key: {key}")
                        return None

                self.logger.debug(
                    f"Successfully read data from cache: {cache_path}  with length: {len(cache_data['data'])}"
                )
                return cache_data.get("data")
            except (json.JSONDecodeError, IOError) as e:
                self.logger.error(f"Failed to read cache file {cache_path}: {str(e)}")
                return None
        self.logger.debug(f"Cache invalid or not found for key: {key}")
        return None

    def _save(
        self,
        cache_type: str,
        key: str,
        data: Any,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Generic save method for cache data

        Args:
            cache_type: Type of cache (e.g., 'match_dict', 'match_stats')
            key: Unique identifier for the cached data
            data: The data to cache
            cache_duration: Optional duration for how long to cache the data
            url: Optional original URL where the data was fetched from
        """
        cache_path = self._get_cache_path(cache_type, key)
        try:
            self.logger.debug(
                f"Attempting to save data to cache with key: {key} and cache_type: {cache_type}"
            )

            # Save data along with cache duration and original URL if specified
            cache_data = {
                "data": data,
                "timestamp": datetime.now().timestamp(),
                "duration": cache_duration.total_seconds() if cache_duration else None,
                "source_url": url,
            }

            with open(cache_path, "w") as f:
                json.dump(cache_data, f, indent=4, sort_keys=True)
            self.logger.debug(f"Successfully saved data to cache: {cache_path}")
        except IOError as e:
            self.logger.error(f"Failed to save cache file {cache_path}: {str(e)}")
            raise  # Propagate the error instead of silently failing

    # Match dictionary methods
    def get_match_dict(
        self, match_id: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get match data from cache"""
        return self._get("match_dict", match_id, cache_duration)

    def save_match_dict(
        self,
        match_id: str,
        match_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save match data to cache"""
        self._save("match_dict", match_id, match_data, cache_duration, url)

    # Match statistics methods
    def get_match_stats(
        self, match_id: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get match statistics from cache"""
        return self._get("match_stats", match_id, cache_duration)

    def save_match_stats(
        self,
        match_id: str,
        stats_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save match statistics to cache"""
        self._save("match_stats", match_id, stats_data, cache_duration, url)

    # Match odds methods
    def get_match_odds(
        self, match_id: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get match odds from cache"""
        return self._get("match_odds", match_id, cache_duration)

    def save_match_odds(
        self,
        match_id: str,
        odds_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save match odds to cache"""
        self._save("match_odds", match_id, odds_data, cache_duration, url)

    # Player IDs methods
    def get_player_ids(
        self, match_id: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get player IDs from cache"""
        return self._get("player_ids", match_id, cache_duration)

    def save_player_ids(
        self,
        match_id: str,
        player_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save player IDs to cache"""
        self._save("player_ids", match_id, player_data, cache_duration, url)

    # Positions methods
    def get_positions(
        self, key: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get positions from cache"""
        return self._get("positions", key, cache_duration)

    def save_positions(
        self,
        key: str,
        positions_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save positions to cache"""
        self._save("positions", key, positions_data, cache_duration, url)

    # Valid seasons methods
    def get_valid_seasons(
        self, league: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get valid seasons from cache"""
        return self._get("valid_seasons", league, cache_duration)

    def save_valid_seasons(
        self,
        league: str,
        seasons_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save valid seasons to cache"""
        self._save("valid_seasons", league, seasons_data, cache_duration, url)

    # Match dicts methods
    def get_match_dicts(
        self,
        league: str,
        year: str,
        page: int,
        cache_duration: Optional[timedelta] = None,
    ) -> Optional[Dict]:
        """Get match dicts from cache"""
        # Handle year format like "22/23" by replacing slashes with underscores in the key
        key = f"{league}/{year.replace('/', '_')}/{page}"
        return self._get("match_dicts", key, cache_duration)

    def save_match_dicts(
        self,
        league: str,
        year: str,
        page: int,
        matches_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save match dicts to cache"""
        # Handle year format like "22/23" by replacing slashes with underscores in the key
        key = f"{league}/{year.replace('/', '_')}/{page}"
        self._save("match_dicts", key, matches_data, cache_duration, url)

    def is_match_finished(self, match_id: str) -> bool:
        """
        Check if a match is finished based on its cached data
        """
        match_data = self.get_match_dict(match_id)
        if match_data:
            self.logger.debug(
                f"Checking if match {match_id} is finished: {len(match_data)}"
            )
            return match_data.get("status", {}).get("type") == "finished"
        self.logger.debug(f"No cached data found for match {match_id}")
        return False

    def get_league_movements(
        self, key: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get league movements (promoted/relegated teams) from cache

        Parameters
        ----------
        key : str
            Cache key in format "{league}_{season}_movements"
        cache_duration : Optional[timedelta], optional
            Maximum age of cached data, by default None (cache forever)

        Returns
        -------
        Optional[Dict]
            Cached league movements data or None if not found/invalid
        """
        return self._get("league_movements", key, cache_duration)

    def save_league_movements(
        self,
        key: str,
        movements_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save league movements (promoted/relegated teams) to cache

        Parameters
        ----------
        key : str
            Cache key in format "{league}_{season}_movements"
        movements_data : Dict
            League movements data to cache
        cache_duration : Optional[timedelta], optional
            Duration to cache the data, by default None (cache forever)
        url : Optional[str], optional
            Original URL where data was fetched from, by default None
        """
        self._save("league_movements", key, movements_data, cache_duration, url)

    def get_league_standings(
        self, key: str, cache_duration: Optional[timedelta] = None
    ) -> Optional[Dict]:
        """Get league standings from cache

        Parameters
        ----------
        key : str
            Cache key in format "{league}_{season}_standings"
        cache_duration : Optional[timedelta], optional
            Maximum age of cached data, by default None (cache forever)

        Returns
        -------
        Optional[Dict]
            Cached league standings data or None if not found/invalid
        """
        return self._get("league_standings", key, cache_duration)

    def save_league_standings(
        self,
        key: str,
        standings_data: Dict,
        cache_duration: Optional[timedelta] = None,
        url: Optional[str] = None,
    ) -> None:
        """Save league standings to cache

        Parameters
        ----------
        key : str
            Cache key in format "{league}_{season}_standings"
        standings_data : Dict
            League standings data to cache
        cache_duration : Optional[timedelta], optional
            Duration to cache the data, by default None (cache forever)
        url : Optional[str], optional
            Original URL where data was fetched from, by default None
        """
        self._save("league_standings", key, standings_data, cache_duration, url)
