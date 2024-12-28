import pandas as pd
from .scraperfc_exceptions import InvalidLeagueException, InvalidYearException
from .utils import botasaurus_get
import numpy as np
from typing import Union, Sequence, Optional
from datetime import timedelta
from .cache_manager import CacheManager
from .utils.logger_config import setup_logging

""" These are the status codes for Sofascore events. Found in event['status'] key.
{100: {'code': 100, 'description': 'Ended', 'type': 'finished'},
 120: {'code': 120, 'description': 'AP', 'type': 'finished'},
 110: {'code': 110, 'description': 'AET', 'type': 'finished'},
 70: {'code': 70, 'description': 'Canceled', 'type': 'canceled'},
 60: {'code': 60, 'description': 'Postponed', 'type': 'postponed'},
 93: {'code': 93, 'description': 'Removed', 'type': 'finished'},
 90: {'code': 90, 'description': 'Abandoned', 'type': 'canceled'},
 7: {'code': 7, 'description': '2nd half', 'type': 'inprogress'},
 6: {'code': 6, 'description': '1st half', 'type': 'inprogress'},
 0: {'code': 0, 'description': 'Not started', 'type': 'notstarted'}}
"""


API_PREFIX = 'https://api.sofascore.com/api/v1'

class Sofascore:
    
    # ==============================================================================================
    def __init__(self, log_file: str = "scraperfc.log") -> None:
        self.log_file = log_file
        self.logger = setup_logging(__name__)
        self.logger.debug("Initializing Sofascore scraper with log file: {}", log_file)
        self.cache_manager = CacheManager()
        self.league_stats_fields = [
            'goals', 'yellowCards', 'redCards', 'groundDuelsWon', 'groundDuelsWonPercentage',
            'aerialDuelsWon', 'aerialDuelsWonPercentage', 'successfulDribbles',
            'successfulDribblesPercentage', 'tackles', 'assists', 'accuratePassesPercentage',
            'totalDuelsWon', 'totalDuelsWonPercentage', 'minutesPlayed', 'wasFouled', 'fouls',
            'dispossessed', 'possesionLost', 'appearances', 'started', 'saves', 'cleanSheets',
            'savedShotsFromInsideTheBox', 'savedShotsFromOutsideTheBox',
            'goalsConcededInsideTheBox', 'goalsConcededOutsideTheBox', 'highClaims',
            'successfulRunsOut', 'punches', 'runsOut', 'accurateFinalThirdPasses',
            'bigChancesCreated', 'accuratePasses', 'keyPasses', 'accurateCrosses',
            'accurateCrossesPercentage', 'accurateLongBalls', 'accurateLongBallsPercentage',
            'interceptions', 'clearances', 'dribbledPast', 'bigChancesMissed', 'totalShots',
            'shotsOnTarget', 'blockedShots', 'goalConversionPercentage', 'hitWoodwork', 'offsides',
            'expectedGoals', 'errorLeadToGoal', 'errorLeadToShot', 'passToAssist'
        ]
        self.concatenated_fields = '%2C'.join(self.league_stats_fields)

    # ==============================================================================================
    def _check_and_convert_to_match_id(self, match: Union[str, int]) -> int:
        """ Helper function that will take a Sofascore match URL or match ID and return a match ID

        Parameters
        ----------
        match : str or int
            Strings will be interprated as URLs and ints will be interpreted as match IDs.

        Returns
        -------
        match_id : int
        """
        if not isinstance(match, int) and not isinstance(match, str):
            raise TypeError('`match` must a string or int')
        match_id = match if isinstance(match, int) else self.get_match_id_from_url(match)
        return match_id

    # ==============================================================================================
    def _is_match_finished(self, match_id: Union[str, int]) -> bool:
        """Helper function to check if a match is finished.
        
        This function first checks the cache to see if the match is finished.
        If the match is not in the cache or not finished, it returns False.
        
        Parameters
        ----------
        match_id : str or int
            Match ID to check
            
        Returns
        -------
        bool
            True if match is finished, False otherwise
        """
        match_id = str(match_id)  # Convert to string since cache manager expects string
        self.logger.debug(f"Checking if match {match_id} is finished")
        return self.cache_manager.is_match_finished(match_id)

    # ==============================================================================================
    def get_valid_seasons(self, league: str, use_cache: bool = True) -> dict:
        """ Returns the valid seasons and their IDs for the given league

        Parameters
        ----------
        league : str
            League to get valid seasons for. See comps ScraperFC.Sofascore for valid leagues.
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.
        
        Returns
        -------
        seasons : dict
            Available seasons for the league. {season string: season ID, ...}
        """
        if not isinstance(league, str):
            raise TypeError('`league` must be an str.')
            
        if use_cache:
            cached_data = self.cache_manager.get_valid_seasons(league, timedelta(days=30))  # 1 month cache
            if cached_data:
                return cached_data
            
        response = botasaurus_get(f'{API_PREFIX}/unique-tournament/{league}/seasons/')
        if response and response.status_code == 200:
            seasons = dict([(x['year'], x['id']) for x in response.json()['seasons']])
        else:
            raise ValueError(f"Failed to get valid seasons for {league}")
        
        if use_cache:
            self.cache_manager.save_valid_seasons(league, seasons, url=f'{API_PREFIX}/unique-tournament/{league}/seasons/')
            
        return seasons

    # ==============================================================================================
    def get_match_dicts(self, year: str, league: str, use_cache: bool = True) -> Sequence[dict]:
        """ Returns the matches from the Sofascore API for a given league season.

        Parameters
        ----------
        year : str
            See the :ref:`sofascore_year` `year` parameter docs for details.
        league : int
            League to get valid seasons for. See comps ScraperFC.Sofascore for valid leagues.
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.
        
        Returns
        -------
        matches : list of dict
            Each element being a single game of the competition
        """
        self.logger.info(f"Sofascore.get_match_dicts: Getting matches")
        if not isinstance(year, str):
            raise TypeError('`year` must be an str.')
            
        valid_seasons = self.get_valid_seasons(league)
        if year not in valid_seasons.keys():
            raise InvalidYearException(year, league, list(valid_seasons.keys()))

        matches = list()
        i = 0
        while 1:
            cached_data = None
            if use_cache:
                cached_data = self.cache_manager.get_match_dicts(league, year, i, timedelta(days=1))
            
            if cached_data is not None:
                if isinstance(cached_data, list) and len(cached_data) == 0:  # If we previously got a 404
                    self.logger.debug(f"Found empty cache for page {i}, stopping pagination")
                    break
                matches += cached_data
                i += 1
                continue

            response = botasaurus_get(
                f'{API_PREFIX}/unique-tournament/{league}/season/{valid_seasons[year]}/' +
                f'events/last/{i}'
            )
            
            if response.status_code == 404:
                self.logger.warning(f"Got 404 for page {i}, saving empty cache")
                if use_cache:
                    self.cache_manager.save_match_dicts(league, year, i, [], timedelta(days=1), 
                        url=f'{API_PREFIX}/unique-tournament/{league}/season/{valid_seasons[year]}/events/last/{i}')
                break
            elif response.status_code == 200:
                page_data = response.json()['events']
                matches += page_data
                if use_cache:
                    self.cache_manager.save_match_dicts(league, year, i, page_data, url=f'{API_PREFIX}/unique-tournament/{league}/season/{valid_seasons[year]}/events/last/{i}')
            else:
                self.logger.error(f"Got unexpected status code {response.status_code}")
                break
            i += 1

        return matches

    # ==============================================================================================
    def get_match_id_from_url(self, match_url: str) -> int:
        """ Get match id from a Sofascore match URL.
        
        This can also be found in the 'id' key of the dict returned from get_match_dict().

        Parameters
        ----------
        match_url : str
            Full link to a SofaScore match

        Returns
        -------
        : int
            Match id for a SofaScore match
        """
        if not isinstance(match_url, str):
            raise TypeError('`match_url` must be a string.')
        match_id = int(match_url.split('#id:')[-1])
        return match_id

    # ==============================================================================================
    def get_match_url_from_id(self, match_id: Union[str, int]) -> str:
        """ Get the Sofascore match URL for a given match ID

        Parameters
        ----------
        match_id : str or int
            Sofascore match ID

        Returns
        -------
        : str
            URL to the Sofascore match
        """
        match_dict = self.get_match_dict(match_id)
        return f"https://www.sofascore.com/{match_dict['homeTeam']['slug']}-" +\
            f"{match_dict['awayTeam']['slug']}/{match_dict['customId']}#id:{match_dict['id']}"

    # ==============================================================================================
    def get_match_dict(self, match: Union[str, int], use_cache: bool = True) -> dict:
        """ Get match data dict for a single match

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.

        Returns
        -------
        : dict
            Generic data about a match
        """
        match_id = self._check_and_convert_to_match_id(match)
        
        if use_cache:
            cached_data = self.cache_manager.get_match_dict(str(match_id))  # Forever cache
            if cached_data:
                return cached_data

        response = botasaurus_get(f'{API_PREFIX}/event/{match_id}')
        if response and response.status_code == 200:
            data = response.json()['event']
        else:
            raise ValueError(f"Failed to get match data for ID {match_id}")
        
        if use_cache:
            self.cache_manager.save_match_dict(str(match_id), data, url=f'{API_PREFIX}/event/{match_id}')
            
        return data

    # ==============================================================================================
    def get_team_names(self, match: Union[str, int]) -> tuple[str, str]:
        """ Get the team names for the home and away teams

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID

        Returns
        -------
        : tuple of str
            Name of home and away team.
        """
        data = self.get_match_dict(match)
        home_team = data['homeTeam']['name']
        away_team = data['awayTeam']['name']
        return home_team, away_team
    
    # ==============================================================================================
    def get_positions(self, selected_positions: Sequence[str], use_cache: bool = True) -> str:
        """ Returns a string for the parameter filters of the scrape_league_stats() request.

        Parameters
        ----------
        selected_positions : list of str
            List of the positions available to filter on the SofaScore UI
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.
        
        Returns
        -------
        : str
            Joined abbreviations for the chosen positions
        """
        if not isinstance(selected_positions, list):
            raise TypeError('`selected_positions` must be a list.')
        if not np.all([isinstance(x, str) for x in selected_positions]):
            raise TypeError('All items in `selected_positions` must be strings.')
            
        if use_cache:
            selected_positions_key = '_'.join(sorted(selected_positions))
            cached_data = self.cache_manager.get_positions(selected_positions_key, timedelta(days=90))  # 3 months cache
            if cached_data:
                return cached_data

        positions = {'Goalkeepers': 'G', 'Defenders': 'D', 'Midfielders': 'M', 'Forwards': 'F'}
        if not np.isin(selected_positions, list(positions.keys())).all():
            raise ValueError(f'All items in `selected_positions` must be in {positions.keys()}')
            
        abbreviations = [positions[position] for position in selected_positions]
        result = '~'.join(abbreviations)
        
        if use_cache:
            self.cache_manager.save_positions(selected_positions_key, result, url=None)  # No URL since this is computed locally
            
        return result

    # ==============================================================================================
    def get_player_ids(self, match: Union[str, int], use_cache: bool = True) -> dict:
        """ Get the player IDs for a match
        
        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.

        Returns
        -------
        : dict
            Name and ID of every player in the match, {name: id, ...}
        """
        match_id = self._check_and_convert_to_match_id(match)
        
        if use_cache:
            cached_data = self.cache_manager.get_player_ids(str(match_id))  # Forever cache
            if cached_data:
                return cached_data
                
        url = f"{API_PREFIX}/event/{match_id}/lineups"
        response = botasaurus_get(url)
        
        if response and response.status_code == 200:
            home_players = response.json()['home']['players']
            away_players = response.json()['away']['players']
            for p in home_players:
                p["teamId"] = response.json()['home']['teamId']
                p["teamName"] = response.json()['home']['name']
            for p in away_players:
                p["teamId"] = response.json()['away']['teamId']
                p["teamName"] = response.json()['away']['name']
                players = home_players + away_players
                
            temp = pd.DataFrame(players)
            data = dict(zip(temp['name'], temp['id']))
            
            if use_cache:
                self.cache_manager.save_player_ids(str(match_id), data, url=f"{API_PREFIX}/event/{match_id}/lineups")
                
            return data
        return {}

    # ==============================================================================================
    def scrape_player_league_stats(
            self, year: str, league: str, accumulation: str='total',
            selected_positions: Sequence[str]=[
                'Goalkeepers', 'Defenders', 'Midfielders', 'Forwards'
            ]
        ) -> pd.DataFrame:
        """ Get every player statistic that can be asked in league pages on Sofascore.

        Parameters
        ----------
        year : str
            See the :ref:`sofascore_year` `year` parameter docs for details.
        league : str
            League to get valid seasons for. See comps ScraperFC.Sofascore for valid leagues.
        accumulation : str, optional
            Value of the filter accumulation. Can be "per90", "perMatch", or "total". Defaults to
            "total".
        selected_positions : list of str, optional
            Value of the filter positions. Defaults to ["Goalkeepers", "Defenders", "Midfielders",
            "Forwards"].

        Returns
        -------
        : DataFrame
        """
        if not isinstance(year, str):
            raise TypeError('`year` must be a string.')
        valid_seasons = self.get_valid_seasons(league)
        if year not in valid_seasons.keys():
            raise InvalidYearException(year, league, list(valid_seasons.keys()))
        if not isinstance(accumulation, str):
            raise TypeError('`accumulation` must be a string.')
        valid_accumulations = ['total', 'per90', 'perMatch']
        if accumulation not in valid_accumulations:
            raise ValueError(f'`accumulation` must be one of {valid_accumulations}')
        
        positions = self.get_positions(selected_positions)
        season_id = valid_seasons[year]
        league_id = comps[league]
        
        # Get all player stats from Sofascore API
        offset = 0
        results = list()
        while 1:
            request_url = 'https://api.sofascore.com/api/v1' +\
                f'/unique-tournament/{league_id}/season/{season_id}/statistics' +\
                f'?limit=100&offset={offset}' +\
                f'&accumulation={accumulation}' +\
                f'&fields={self.concatenated_fields}' +\
                f'&filters=position.in.{positions}'
            response = botasaurus_get(request_url)
            if response and response.status_code == 200:
                results += response.json()['results']
            if (response.json()['page'] == response.json()['pages']) or\
                    (response.json()['pages'] == 0):
                break
            offset += 100

        # Convert the player dicts to a dataframe. Dataframe will be empty if there aren't any
        # player stats
        if len(results) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_dict(results)  # type: ignore
            df['player id'] = df['player'].apply(pd.Series)['id']
            df['player'] = df['player'].apply(pd.Series)['name']
            df['team id'] = df['team'].apply(pd.Series)['id']
            df['team'] = df['team'].apply(pd.Series)['name']
        
        return df

    # ==============================================================================================
    def scrape_match_momentum(self, match: Union[str, int]) -> pd.DataFrame:
        """Get the match momentum values

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID
        
        Returns
        --------
        : DataFrame
            Dataframe of match momentum values. Will be empty if the match does not have
            match momentum data.
        """
        match_id = self._check_and_convert_to_match_id(match)
        url = f'{API_PREFIX}/event/{match_id}/graph'
        response = botasaurus_get(url)
        if response and response.status_code == 200:
            match_momentum_df = pd.DataFrame(response.json()['graphPoints'])
        else:
            self.logger.warning(f"Returned {response.status_code} from {url}. Returning empty dataframe.")
            match_momentum_df = pd.DataFrame()

        return match_momentum_df

    # ==============================================================================================
    def scrape_team_match_stats(self, match: Union[str, int], use_cache: bool = True) -> pd.DataFrame:
        """ Scrape team stats for a match

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.
            For finished matches, data is cached forever.

        Returns
        -------
        : DataFrame
        """
        match_id = self._check_and_convert_to_match_id(match)
        self.get_match_dict(match_id)
        
        if use_cache:
            cached_data = self.cache_manager.get_match_stats(str(match_id))  # Forever cache
            if cached_data is not None:  # Check for None specifically as empty list is valid
                self.logger.debug(f"Processing statistics for match {match_id}")
                
                df = pd.DataFrame()
                stats = cached_data
                
                if isinstance(stats, dict) and 'statistics' in stats:
                    stats = stats['statistics']
                
                for period in stats:
                    period_name = period['period']
                    
                    for group in period['groups']:
                        group_name = group['groupName']
                        temp = pd.DataFrame.from_dict(group['statisticsItems'])
                        temp['period'] = period_name
                        temp['group'] = group_name
                        df = pd.concat([df, temp], ignore_index=True)
                
                self.logger.debug(f"Successfully processed statistics for match {match_id}")
                return df

        url = f'{API_PREFIX}/event/{match_id}/statistics'
        try:
            response = botasaurus_get(url)
            if response and response.status_code == 200:
                df = pd.DataFrame()
                data = response.json()['statistics']
                
                # Cache if the match is finished
                if use_cache:
                    if self._is_match_finished(match_id):
                        self.cache_manager.save_match_stats(str(match_id), data, url=f'{API_PREFIX}/event/{match_id}/statistics')
                
                for period in data:
                    period_name = period['period']
                    self.logger.debug(f"Processing period: {period_name}")
                    
                    for group in period['groups']:
                        group_name = group['groupName']
                        self.logger.debug(f"Processing group: {group_name}")
                        
                        if 'statisticsItems' in group:
                            temp = pd.DataFrame.from_dict(group['statisticsItems'])
                            temp['period'] = period_name
                            temp['group'] = group_name
                            df = pd.concat([df, temp], ignore_index=True)
                        else:
                            self.logger.warning(f"No statisticsItems found in group {group_name}")
                
                self.logger.debug(f"Successfully processed statistics for match {match_id}")
            elif response.status_code == 404:
                self.logger.error("HTTP 404 error from {}: Match {} not found", url, match_id)
                df = pd.DataFrame()
                self.cache_manager.save_match_stats(str(match_id), [], timedelta(days=1), url=f'{API_PREFIX}/event/{match_id}/statistics')
            else:
                self.logger.error("HTTP {} error from {}: Failed to retrieve match {} statistics", response.status_code, url, match_id)
                df = pd.DataFrame()
        except Exception as e:
            self.logger.error("Error processing match {}: {}", match_id, str(e))
            df = pd.DataFrame()
        
        return df

    # ==============================================================================================
    def scrape_player_match_stats(self, match: Union[str, int]) -> pd.DataFrame:
        """ Scrape player stats for a match

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID

        Returns
        -------
        : DataFrame
        """
        match_id = self._check_and_convert_to_match_id(match)
        match_dict = self.get_match_dict(match_id)  # used to get home and away team names and IDs
        url = f'{API_PREFIX}/event/{match_id}/lineups'
        response = botasaurus_get(url)
        
        if response and response.status_code == 200:
            home_players = response.json()['home']['players']
            away_players = response.json()['away']['players']
            for p in home_players:
                p["teamId"] = match_dict["homeTeam"]["id"]
                p["teamName"] = match_dict["homeTeam"]["name"]
            for p in away_players:
                p["teamId"] = match_dict["awayTeam"]["id"]
                p["teamName"] = match_dict["awayTeam"]["name"]
                players = home_players + away_players
                
            temp = pd.DataFrame(players)
            columns = list()
            for c in temp.columns:
                if isinstance(temp.loc[0, c], dict):
                    # Break dicts into series
                    columns.append(temp[c].apply(pd.Series, dtype=object))
                else:
                    # Else they're already series
                    columns.append(temp[c])  # type: ignore
            df = pd.concat(columns, axis=1)
        else:
            self.logger.warning(f"Returned {response.status_code} from {url}. Returning empty dataframe.")
            df = pd.DataFrame()
        
        return df

    # ==============================================================================================
    def scrape_player_average_positions(self, match: Union[str, int]) -> pd.DataFrame:
        """ Return player averages positions for each team

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID

        Returns
        -------
        : DataFrame
            Each row is a player and columns averageX and averageY denote their average position on
            the match.
        """
        match_id = self._check_and_convert_to_match_id(match)
        home_name, away_name = self.get_team_names(match)
        url = f'{API_PREFIX}/event/{match_id}/average-positions'
        response = botasaurus_get(url)
        if response and response.status_code == 200:
            df = pd.DataFrame()
            for key, name in [('home', home_name), ('away', away_name)]:
                temp = pd.DataFrame(response.json()[key])
                temp['team'] = [name,] * temp.shape[0]
                temp = pd.concat(
                    [temp['player'].apply(pd.Series), temp.drop(columns=['player'])],
                    axis=1
                )
                df = pd.concat([df, temp], axis=0, ignore_index=True)
        else:
            self.logger.warning(f"Returned {response.status_code} from {url}. Returning empty dataframe.")
            df = pd.DataFrame()
        return df
    
    # ==============================================================================================
    def scrape_heatmaps(self, match: Union[str, int]) -> dict:
        """ Get the x-y coordinates to create a player heatmap for all players in the match.

        Players who didn't play will have an empty list of coordinates.

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID
        
        Returns
        -------
        : dict
            Dict of players, their IDs and their heatmap coordinates, {player name: {'id':
            player_id, 'heatmap': heatmap}, ...}
        """
        match_id = self._check_and_convert_to_match_id(match)
        players = self.get_player_ids(match)
        for player in players:
            player_id = players[player]
            url = f'{API_PREFIX}/event/{match_id}/player/{player_id}/heatmap'
            response = botasaurus_get(url)
            if response and response.status_code == 200:
                heatmap = [(z['x'], z['y']) for z in response.json()['heatmap']]
            else:
                # Players that didn't play have empty heatmaps. Don't print warning because there
                # would be a lot of them.
                heatmap = list()
            players[player] = {'id': player_id, 'heatmap': heatmap}
        return players
    
    # ==============================================================================================
    def scrape_match_shots(self, match: Union[str, int]) -> pd.DataFrame:
        """ Scrape shots for a match

        Parameters
        ----------
        match : str or int
            Sofascore match URL or match ID
        
        Returns
        -------
        : DataFrame
        """
        match_id = self._check_and_convert_to_match_id(match)
        url = f"{API_PREFIX}/event/{match_id}/shotmap"
        response = botasaurus_get(url)
        if response and response.status_code == 200:
            df = pd.DataFrame.from_dict(response.json()["shotmap"])
        else:
            self.logger.warning(f"Returned {response.status_code} from {url}. Returning empty dataframe.")
            df = pd.DataFrame()
        return df

    # ==============================================================================================
    def scrape_match_odds(self, match: Union[str, int], use_cache: bool = True) -> pd.DataFrame:
        """Get odds data for a specific match from Sofascore API.

        Parameters
        ----------
        match : str or int
            Either a Sofascore match URL or match ID.
        use_cache : bool, optional
            Whether to use cached data if available. Defaults to True.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the odds data with columns:
            - marketId: ID of the betting market
            - marketName: Name of the betting market (e.g., 'Full time', 'Double chance')
            - choiceGroup: Group for the choice if applicable (e.g., '2.5' for Over/Under)
            - name: Name of the betting selection
            - initialOdds: Initial odds value
            - initialFractionalValue: Initial fractional odds value
            - currentOdds: Current odds value
            - winning: Boolean indicating if the selection won
        """
        match_id = self._check_and_convert_to_match_id(match)
        
        if use_cache:
            # First check if match is finished
            is_finished = self._is_match_finished(match_id)
            
            # For finished matches, check cache without duration
            if is_finished:
                cached_data = self.cache_manager.get_match_odds(str(match_id))
                if cached_data is not None:
                    # Extract the markets from the nested data structure
                    if isinstance(cached_data, dict) and 'data' in cached_data:
                        cached_data = cached_data['data']
                    return self._process_odds_data(cached_data)
            else:
                # For unfinished matches, check cache with 1-day duration
                cached_data = self.cache_manager.get_match_odds(str(match_id), timedelta(days=1))
                if cached_data is not None:
                    # Extract the markets from the nested data structure
                    if isinstance(cached_data, dict) and 'data' in cached_data:
                        cached_data = cached_data['data']
                    return self._process_odds_data(cached_data)
        
        self.logger.info(f"Fetching odds data for match {match_id}")
        url = f'{API_PREFIX}/event/{match_id}/odds/1/all'
        response = botasaurus_get(url)
        
        if not response or response.status_code >= 300:
            status_code = response.status_code if response else 'No response'
            self.logger.warning(f"Failed to get odds data for match ID {match_id} with status {status_code}")
            if use_cache:
                if is_finished:
                    self.cache_manager.save_match_odds(str(match_id), {"markets": []}, url=url)  # Cache forever for finished matches
                else:
                    self.cache_manager.save_match_odds(str(match_id), {"markets": []}, timedelta(days=1), url=url)
            return pd.DataFrame()
            
        data = response.json()

        # Cache the raw data
        if use_cache:
            if is_finished:
                self.cache_manager.save_match_odds(str(match_id), data, url=url)  # Cache forever for finished matches
            else:
                self.cache_manager.save_match_odds(str(match_id), data, timedelta(days=1), url=url)
        
        return self._process_odds_data(data)

    def _process_odds_data(self, data: dict) -> pd.DataFrame:
        """Process the odds data into a DataFrame.
        
        Parameters
        ----------
        data : dict
            Raw odds data from the API or cache
            
        Returns
        -------
        pd.DataFrame
            Basic DataFrame with odds data
        """
        odds_data = []
        
        # Handle case where data is a list or None
        if not data or isinstance(data, list) or not isinstance(data, dict):
            self.logger.warning("Received invalid odds data format")
            return pd.DataFrame()
            
        # Process each market
        for market in data.get('markets', []):
            market_id = market.get('marketId')
            market_name = market.get('marketName')
            choice_group = market.get('choiceGroup')  # Get choiceGroup from market level
            
            for choice in market.get('choices', []):
                choice_data = {
                    'marketId': market_id,
                    'marketName': market_name,
                    'choiceGroup': choice_group,  # Use market-level choiceGroup
                    'name': choice.get('name'),
                    'initialOdds': choice.get('initialOdds'),
                    'initialFractionalValue': choice.get('initialFractionalValue'),
                    'currentOdds': choice.get('currentOdds', choice.get('fractionalValue')),  # Use fractionalValue as fallback
                    'winning': choice.get('winning')
                }
                odds_data.append(choice_data)
        
        return pd.DataFrame(odds_data)
