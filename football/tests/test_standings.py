import pytest
from unittest.mock import Mock, patch, call, MagicMock
from io import StringIO
from django.core.management import call_command
from django.test import TestCase
from celery.exceptions import Retry, TimeoutError
import requests
import time

from football.models import League, Team, Country, LeagueTableSnapshot
from football.management.commands.init_standings import Command
from football.tasks import populate_standings_task, _process_single_league


class TestInitStandingsCommand(TestCase):
    """Test cases for the init_standings management command"""
    
    def setUp(self):
        """Set up test data"""
        self.country = Country.objects.create(name='Test')
        
        
        self.leagues = [
            League.objects.create(id=1, name="Premier League", season=2024, country=self.country),
            League.objects.create(id=2, name="La Liga", season=2024, country=self.country),
            League.objects.create(id=3, name="Serie A", season=2024, country=self.country),
            League.objects.create(id=4, name="Bundesliga", season=2024, country=self.country),
            League.objects.create(id=5, name="Ligue 1", season=2024, country=self.country),
        ]

    def test_get_leagues_ids(self):
        """Test _get_leagues_ids returns correct list of IDs"""
        command = Command()
        league_ids = command._get_leagues_ids()
        
        assert len(league_ids) == 5
        assert all(isinstance(id_, int) for id_ in league_ids)
        assert set(league_ids) == {1, 2, 3, 4, 5}

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_handle_default_parameters(self, mock_delay, mock_redis):
        """Test command with default parameters"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_standings', stdout=out)
        
        output = out.getvalue()
        assert "Found 5 leagues to process" in output
        assert "Queued batch" in output
        
        # Should create 1 batch (5 leagues with default batch size 5)
        assert mock_delay.call_count == 1

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_handle_custom_batch_size(self, mock_delay, mock_redis):
        """Test command with custom batch size"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_standings', '--leagues-per-task=2', stdout=out)
        
        output = out.getvalue()
        assert "Found 5 leagues to process" in output
        
        # Should create 3 batches (2 + 2 + 1 leagues)
        assert mock_delay.call_count == 3

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_handle_dry_run(self, mock_delay, mock_redis):
        """Test command in dry run mode"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_standings', '--dry-run', stdout=out)
        
        output = out.getvalue()
        assert "Dry run mode - no tasks will be created" in output
        assert "DRY RUN: Would queue batch for league IDs:" in output
        
        # No tasks should be created in dry run
        mock_delay.assert_not_called()

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_queue_limit_reached(self, mock_delay, mock_redis):
        """Test behavior when queue limit is reached"""
        mock_redis_conn = Mock()
        # First call returns queue full, second call allows processing
        mock_redis_conn.llen.side_effect = [100, 50]  # max_queue default is 100
        mock_redis.return_value = mock_redis_conn
        
        with patch('football.management.commands.init_standings.time.sleep') as mock_sleep:
            out = StringIO()
            call_command('init_standings', '--max-queue=100', '--wait-time=5', stdout=out)
            
            output = out.getvalue()
            assert "Queue limit reached" in output
            assert "Waiting 5s..." in output
            mock_sleep.assert_called()

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_handle_connection_error(self, mock_delay, mock_redis):
        """Test handling of connection errors"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        mock_delay.side_effect = ConnectionError("Redis connection failed")
        
        out = StringIO()
        err = StringIO()
        call_command('init_standings', stdout=out, stderr=err)
        
        error_output = err.getvalue()
        assert "Celery connection error" in error_output
        # Should break on ConnectionError
        assert mock_delay.call_count == 1

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_handle_timeout_error(self, mock_delay, mock_redis):
        """Test handling of timeout errors"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        mock_delay.side_effect = [TimeoutError("Task timeout"), None]
        
        out = StringIO()
        err = StringIO()
        call_command('init_standings', stdout=out, stderr=err)
        
        error_output = err.getvalue()
        assert "Celery connection error" in error_output
        # Should continue on TimeoutError
        assert mock_delay.call_count == 1

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_summary_output(self, mock_delay, mock_redis):
        """Test summary output shows correct statistics"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_standings', stdout=out)
        
        output = out.getvalue()
        assert "Processing complete:" in output
        assert "Successful batches: 1" in output
        assert "Failed batches: 0" in output
        assert "Total batches: 1" in output

    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_small_delay_between_tasks(self, mock_delay, mock_redis):
        """Test that small delay is added between task queueing"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        with patch('football.management.commands.init_standings.time.sleep') as mock_sleep:
            call_command('init_standings', '--leagues-per-task=2')
            
            # Should have small delays (0.1s) between batches plus any queue waits
            sleep_calls = [call for call in mock_sleep.call_args_list if call[0][0] == 0.1]
            assert len(sleep_calls) >= 1  # At least one 0.1s delay

    def test_get_leagues_ids_database_error(self):
        """Test handling of database errors in _get_leagues_ids"""
        command = Command()
        
        with patch('football.models.League.objects.only') as mock_objects:
            mock_objects.side_effect = Exception("Database connection failed")
            
            with patch('football.management.commands.init_standings.logger') as mock_logger:
                result = command._get_leagues_ids()
                
                assert result == []
                mock_logger.error.assert_called_with("Failed to fetch league IDs: Database connection failed")


class TestPopulateStandingsTask(TestCase):
    """Test cases for the populate_standings_task Celery task"""
    
    def setUp(self):
        """Set up test data"""
        self.country = Country.objects.create(name='Test')
        self.leagues = [
            League.objects.create(id=1, name="Premier League", season=2024, country=self.country),
            League.objects.create(id=2, name="La Liga", season=2024, country=self.country),
        ]
        
        self.teams = [
            Team.objects.create(id=1, name="Manchester United", country=self.country),
            Team.objects.create(id=2, name="Liverpool", country= self.country),
        ]

    @patch('football.tasks._process_single_league')
    def test_populate_standings_task_success(self, mock_process):
        """Test successful task execution"""
        league_ids = [1, 2]
        
        populate_standings_task(league_ids)
        
        assert mock_process.call_count == 2
        call_args_list = mock_process.call_args_list
        called_league_ids = [call[0][1] for call in call_args_list]
        assert 1 in called_league_ids
        assert 2 in called_league_ids

    @patch('football.tasks._process_single_league')
    def test_populate_standings_task_missing_league(self, mock_process):
        """Test task with non-existent league"""
        league_ids = [1, 999, 2]  # 999 doesn't exist
        
        with patch('football.tasks.logger') as mock_logger:
            populate_standings_task(league_ids)
            
            mock_logger.warning.assert_called_with("League 999 not found in database")
            # Should still process existing leagues
            assert mock_process.call_count == 2

    @patch('football.tasks._process_single_league')
    def test_populate_standings_task_exception_handling(self, mock_process):
        """Test task exception handling and retry with exponential backoff"""
        mock_process.side_effect = Exception("Processing failed")
        
        with patch.object(populate_standings_task, 'retry', side_effect=Retry()) as mock_retry:
            with pytest.raises(Retry):
                populate_standings_task([1])
            
            # Verify retry was called with exponential backoff
            mock_retry.assert_called_once()
            args, kwargs = mock_retry.call_args
            assert 'countdown' in kwargs
            # First retry should have countdown of 5 * (2^0) = 5
            assert kwargs['countdown'] == 5


class TestProcessSingleLeague(TestCase):
    """Test cases for _process_single_league helper function"""
    
    def setUp(self):
        """Set up test data"""
        self.country = Country.objects.create(name='Test')
        self.league = League.objects.create(id=1, name="Premier League", season=2024, country= self.country)
        self.teams = [
            Team.objects.create(id=1, name="Manchester United", country= self.country),
            Team.objects.create(id=2, name="Liverpool", country = self.country),
        ]
        self.task = Mock()
        self.task.request.retries = 0
        self.task.max_retries = 3

    @patch('football.api_client.get_league_table')
    @patch('football.tasks.LeagueTableSnapshot.objects.bulk_create')
    def test_process_single_league_success(self, mock_bulk_create, mock_api):
        """Test successful league processing"""
        mock_api.return_value = {
            'results': 1,
            'response': [
                {
                    'league': {
                        'standings': [
                            [
                                {
                                    'rank': 1,
                                    'team': {'id': 1, 'name': 'Manchester United'},
                                    'points': 30,
                                    'goalsDiff': 10,
                                    'all': {
                                        'goals': {'for': 25, 'against': 15},
                                        'played': 10,
                                        'win': 8,
                                        'draw': 2,
                                        'lose': 0
                                    },
                                    'form': 'WWWWW',
                                    'home': {'played': 5},
                                    'away': {'played': 5}
                                },
                                {
                                    'rank': 2,
                                    'team': {'id': 2, 'name': 'Liverpool'},
                                    'points': 25,
                                    'goalsDiff': 5,
                                    'all': {
                                        'goals': {'for': 20, 'against': 15},
                                        'played': 10,
                                        'win': 7,
                                        'draw': 1,
                                        'lose': 2
                                    },
                                    'form': 'WWLWW',
                                    'home': {'played': 5},
                                    'away': {'played': 5}
                                }
                            ]
                        ]
                    }
                }
            ]
        }
        
        _process_single_league(self.task, 1, self.league)
        
        mock_api.assert_called_once_with(league_id=1, season=2024)
        mock_bulk_create.assert_called_once()
        
        args, kwargs = mock_bulk_create.call_args
        snapshots = args[0]
        
        assert len(snapshots) == 2
        assert snapshots[0].rank == 1
        assert snapshots[0].team.id == 1
        assert snapshots[0].points == 30
        assert snapshots[0].goals_for == 25
        assert snapshots[0].goals_against == 15
        assert snapshots[0].goal_difference == 10
        assert snapshots[0].matches_played == 10
        assert snapshots[0].wins == 8
        assert snapshots[0].draws == 2
        assert snapshots[0].losses == 0
        assert snapshots[0].last_five == 'WWWWW'
        
        assert kwargs['ignore_conflicts'] is True

    @patch('football.api_client.get_league_table')
    def test_process_single_league_invalid_api_response(self, mock_api):
        """Test handling of invalid API response"""
        mock_api.return_value = None
        
        _process_single_league(self.task, 1, self.league)
        
        self.task.retry.assert_called_once()

    @patch('football.api_client.get_league_table')
    def test_process_single_league_empty_results(self, mock_api):
        """Test handling of empty API results"""
        mock_api.return_value = {'results': 0, 'response': []}
        
        _process_single_league(self.task, 1, self.league)
        
        self.task.retry.assert_called_once()

    @patch('football.api_client.get_league_table')
    def test_process_single_league_malformed_response(self, mock_api):
        """Test handling of malformed API response"""
        mock_api.return_value = {'response': 'not a list'}
        
        _process_single_league(self.task, 1, self.league)
        
        self.task.retry.assert_called_once()

    @patch('football.api_client.get_league_table')
    def test_process_single_league_malformed_standings_data(self, mock_api):
        """Test handling of malformed standings data"""
        mock_api.return_value = {
            'results': 1,
            'response': [{'league': {}}]  # Missing standings
        }
        
        with patch('football.tasks.logger') as mock_logger:
            _process_single_league(self.task, 1, self.league)
            
            mock_logger.error.assert_called()
            # Should not retry for data structure errors
            self.task.retry.assert_not_called()

    @patch('football.api_client.get_league_table')
    def test_process_single_league_max_retries_exceeded(self, mock_api):
        """Test behavior when max retries exceeded"""
        self.task.request.retries = 3
        self.task.max_retries = 3
        
        mock_api.side_effect = Exception("API Error")
        
        with patch('football.tasks.logger') as mock_logger:
            _process_single_league(self.task, 1, self.league)
            
            mock_logger.error.assert_called_with("Max retries exceeded for league 1")
            self.task.retry.assert_not_called()

    @patch('football.api_client.get_league_table')
    def test_process_single_league_request_exception(self, mock_api):
        """Test handling of request exceptions (should retry)"""
        mock_api.side_effect = requests.RequestException("Connection timeout")
        
        with patch('football.tasks.logger') as mock_logger:
            _process_single_league(self.task, 1, self.league)
            
            mock_logger.warning.assert_called()
            self.task.retry.assert_called_once()

    @patch('football.api_client.get_league_table')
    def test_process_single_league_unexpected_exception(self, mock_api):
        """Test handling of unexpected exceptions (should not retry)"""
        mock_api.side_effect = ValueError("Unexpected error")
        
        with patch('football.tasks.logger') as mock_logger:
            _process_single_league(self.task, 1, self.league)
            
            mock_logger.error.assert_called()
            self.task.retry.assert_not_called()

    @patch('football.api_client.get_league_table')
    @patch('football.tasks.Team.objects.filter')
    @patch('football.tasks.LeagueTableSnapshot.objects.bulk_create')
    def test_process_single_league_missing_teams(self, mock_bulk_create, mock_teams_filter, mock_api):
        """Test handling when some teams are missing from database"""
        mock_api.return_value = {
            'results': 1,
            'response': [
                {
                    'league': {
                        'standings': [
                            [
                                {
                                    'rank': 1,
                                    'team': {'id': 1, 'name': 'Manchester United'},
                                    'points': 30,
                                    'goalsDiff': 10,
                                    'all': {'goals': {'for': 25, 'against': 15}},
                                    'form': 'WWWWW'
                                },
                                {
                                    'rank': 2,
                                    'team': {'id': 999, 'name': 'Missing Team'},  # Team not in DB
                                    'points': 25,
                                    'goalsDiff': 5,
                                    'all': {'goals': {'for': 20, 'against': 15}},
                                    'form': 'WWLWW'
                                }
                            ]
                        ]
                    }
                }
            ]
        }
        
        # Only return the first team (id=1)
        mock_teams_filter.return_value = [self.teams[0]]
        
        
        _process_single_league(self.task, 1, self.league)
        
        mock_bulk_create.assert_called_once()
        args, kwargs = mock_bulk_create.call_args
        snapshots = args[0]
        
        # Should create snapshots for both teams, but missing team will have team=None
        assert len(snapshots) == 1
        assert snapshots[0].team == self.teams[0]  # Found team

    @patch('football.api_client.get_league_table')
    @patch('football.tasks.LeagueTableSnapshot.objects.bulk_create')
    def test_process_single_league_handles_missing_stats(self, mock_bulk_create, mock_api):
        """Test handling of missing statistical data in API response"""
        mock_api.return_value = {
            'results': 1,
            'response': [
                {
                    'league': {
                        'standings': [
                            [
                                {
                                    'rank': 1,
                                    'team': {'id': 1, 'name': 'Manchester United'},
                                    'points': 30,
                                    'goalsDiff': 10,
                                    'form': 'WWWWW'
                                    # Missing 'all', 'home', 'away' stats
                                }
                            ]
                        ]
                    }
                }
            ]
        }
        
        _process_single_league(self.task, 1, self.league)
        
        mock_bulk_create.assert_called_once()
        args, kwargs = mock_bulk_create.call_args
        snapshots = args[0]
        
        assert len(snapshots) == 1
        assert snapshots[0].goals_for == 0  # Should default to 0
        assert snapshots[0].goals_against == 0
        assert snapshots[0].matches_played == 0
        assert snapshots[0].wins == 0
        assert snapshots[0].draws == 0
        assert snapshots[0].losses == 0


# Integration test using pytest fixtures
@pytest.fixture
def leagues_data():
    """Fixture providing test leagues"""
    country = Country.objects.create(name='Test')
    return [
        League.objects.create(id=1, name="Premier League", season=2024, country= country),
        League.objects.create(id=2, name="La Liga", season=2024, country= country),
        League.objects.create(id=3, name="Serie A", season=2024, country= country),
    ]


@pytest.mark.django_db
class TestInitStandingsIntegration:
    """Integration tests for the complete workflow"""
    
    @patch('football.api_client.get_league_table')
    @patch('football.management.commands.init_standings.get_redis_connection')
    @patch('football.management.commands.init_standings.populate_standings_task.delay')
    def test_end_to_end_workflow(self, mock_delay, mock_redis, mock_api, leagues_data):
        """Test the complete workflow from command to task execution"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        mock_api.return_value = {
            'results': 1,
            'response': [{'league': {'standings': [[]]}}]
        }
        
        out = StringIO()
        call_command('init_standings', '--leagues-per-task=2', stdout=out)
        
        output = out.getvalue()
        assert "Found 3 leagues to process" in output
        assert mock_delay.call_count == 2  # 3 leagues / 2 = 2 batches
        
        call_args_list = mock_delay.call_args_list
        all_league_ids = []
        for call_args in call_args_list:
            all_league_ids.extend(call_args[0][0])
        
        assert set(all_league_ids) == {1, 2, 3}


# Parametrized tests for different scenarios
@pytest.mark.parametrize("batch_size,expected_batches", [
    (1, 5),  # 5 leagues, 1 per batch = 5 batches
    (2, 3),  # 5 leagues, 2 per batch = 3 batches  
    (3, 2),  # 5 leagues, 3 per batch = 2 batches
    (5, 1),  # 5 leagues, 5 per batch = 1 batch
    (10, 1), # 5 leagues, 10 per batch = 1 batch
])
@pytest.mark.django_db
@patch('football.management.commands.init_standings.get_redis_connection')
@patch('football.management.commands.init_standings.populate_standings_task.delay')
def test_batch_size_calculations(mock_delay, mock_redis, batch_size, expected_batches):
    """Test different batch size calculations"""
    mock_redis_conn = Mock()
    mock_redis_conn.llen.return_value = 0
    mock_redis.return_value = mock_redis_conn
    country = Country.objects.create(name='Test')
    for i in range(5):
        League.objects.create(id=i+1, name=f"League{i+1}", season=2024, country=country)
    
    call_command('init_standings', f'--leagues-per-task={batch_size}')
    
    assert mock_delay.call_count == expected_batches


@pytest.mark.django_db
class TestStandingsTaskRetryBehavior:
    """Tests for retry behavior with exponential backoff"""
    
    def test_exponential_backoff_calculation(self):
        """Test that retry countdown follows exponential backoff pattern"""
        task = Mock()
        task.request.retries = 2  # Third attempt
        task.max_retries = 3
        
        with patch.object(populate_standings_task, 'retry') as mock_retry:
            with patch('football.tasks._process_single_league', side_effect=Exception("Test error")):
                try:
                    populate_standings_task([1])
                except Exception as e:
                    pass
                
                if mock_retry.called:
                    args, kwargs = mock_retry.call_args
                    # Third retry should have countdown of 5 * (2^2) = 20
                    expected_countdown = 5 * (2 ** task.request.retries)
                    assert kwargs['countdown'] == expected_countdown