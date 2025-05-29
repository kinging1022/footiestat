import pytest
from unittest.mock import Mock, patch, call
from io import StringIO
from django.core.management import call_command
from django.test import TestCase
from celery.exceptions import Retry, TimeoutError

from football.models import Country
from football.management.commands.init_teams import Command
from football.tasks import populate_teams_task, _process_single_country


class TestInitTeamsCommand(TestCase):
    """Test cases for the init_teams management command"""
    
    def setUp(self):
        """Set up test data"""
        self.countries = [
            Country.objects.create(id=1, name="England"),
            Country.objects.create(id=2, name="Spain"),
            Country.objects.create(id=3, name="Germany"),
            Country.objects.create(id=4, name="France"),
            Country.objects.create(id=5, name="Italy"),
            Country.objects.create(id=6, name="Brazil"),
            Country.objects.create(id=7, name="Argentina"),
        ]
        
    def test_get_countries_names(self):
        """Test _get_countries_names returns correct list"""
        command = Command()
        countries_names = command._get_countries_names()
        
        assert len(countries_names) == 7
        assert "England" in countries_names
        assert "Argentina" in countries_names
        assert isinstance(countries_names, list)

    def test_get_countries_names_with_limits(self):
        """Test _get_countries_names with start and end limits"""
        command = Command()
        
        # Test start limit only
        countries_names = command._get_countries_names(start_limit=2)
        assert len(countries_names) == 5  # Should get last 5 countries
        
        # Test end limit only  
        countries_names = command._get_countries_names(end_limit=3)
        assert len(countries_names) == 3  # Should get first 3 countries
        
        # Test both limits
        countries_names = command._get_countries_names(start_limit=1, end_limit=4)
        assert len(countries_names) == 3  # Should get countries at index 1,2,3

    def test_get_countries_names_with_zero_start_limit(self):
        """Test that start_limit=0 works correctly"""
        command = Command()
        countries_names = command._get_countries_names(start_limit=0, end_limit=2)
        assert len(countries_names) == 2  # Should get first 2 countries

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_default_parameters(self, mock_delay, mock_redis):
        """Test command with default parameters"""
        # Mock Redis queue size
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_teams', stdout=out)
        
        output = out.getvalue()
        assert "Found 7 countries to process" in output
        assert "Queued batch" in output
        
        # Should create 2 batches (5 + 2 countries)
        assert mock_delay.call_count == 2

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_custom_batch_size(self, mock_delay, mock_redis):
        """Test command with custom batch size"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_teams', '--countries-per-task=3', stdout=out)
        
        output = out.getvalue()
        assert "Found 7 countries to process" in output
        
        # Should create 3 batches (3 + 3 + 1 countries)
        assert mock_delay.call_count == 3

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_dry_run(self, mock_delay, mock_redis):
        """Test command in dry run mode"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_teams', '--dry-run', stdout=out)
        
        output = out.getvalue()
        assert "Dry run mode - no tasks will be created" in output
        assert "DRY RUN: Would queue batch for countries:" in output
        
        # No tasks should be created in dry run
        mock_delay.assert_not_called()

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_queue_limit_reached(self, mock_delay, mock_redis):
        """Test behavior when queue limit is reached"""
        mock_redis_conn = Mock()
        # First call returns queue full, second call allows processing
        mock_redis_conn.llen.side_effect = [100, 50]  # max_queue default is 100
        mock_redis.return_value = mock_redis_conn
        
        with patch('football.management.commands.init_teams.time.sleep') as mock_sleep:
            out = StringIO()
            call_command('init_teams', '--max-queue=100', '--wait-time=5', stdout=out)
            
            output = out.getvalue()
            assert "Queue limit reached" in output
            assert "Waiting 5s..." in output
            mock_sleep.assert_called_with(5)

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_connection_error(self, mock_delay, mock_redis):
        """Test handling of connection errors"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        mock_delay.side_effect = ConnectionError("Redis connection failed")
        
        out = StringIO()
        err = StringIO()
        call_command('init_teams', stdout=out, stderr=err)
        
        error_output = err.getvalue()
        assert "Celery connection error" in error_output
        # Should break on ConnectionError
        assert mock_delay.call_count == 1

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_timeout_error(self, mock_delay, mock_redis):
        """Test handling of timeout errors"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        mock_delay.side_effect = [TimeoutError("Task timeout"), None]  # First fails, second succeeds
        
        out = StringIO()
        err = StringIO()
        call_command('init_teams', stdout=out, stderr=err)
        
        error_output = err.getvalue()
        assert "Celery connection error" in error_output
        # Should continue on TimeoutError
        assert mock_delay.call_count == 2

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_generic_exception(self, mock_delay, mock_redis):
        """Test handling of generic exceptions"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        mock_delay.side_effect = [Exception("Generic error"), None]  # First fails, second succeeds
        
        out = StringIO()
        err = StringIO()
        call_command('init_teams', stdout=out, stderr=err)
        
        error_output = err.getvalue()
        assert "Failed to queue batch" in error_output
        # Should continue on generic exception
        assert mock_delay.call_count == 2

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_summary_output(self, mock_delay, mock_redis):
        """Test summary output shows correct statistics"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_teams', stdout=out)
        
        output = out.getvalue()
        assert "Processing complete:" in output
        assert "Successful batches: 2" in output
        assert "Failed batches: 0" in output
        assert "Total batches: 2" in output

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_summary_with_failures(self, mock_delay, mock_redis):
        """Test summary output with some failures"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        mock_delay.side_effect = [Exception("Error"), None]  # First fails, second succeeds
        
        out = StringIO()
        err = StringIO()
        call_command('init_teams', stdout=out, stderr=err)
        
        output = out.getvalue()
        assert "Processing complete:" in output
        assert "Successful batches: 1" in output
        assert "Failed batches: 1" in output
        assert "Total batches: 2" in output

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_queue_size_display(self, mock_delay, mock_redis):
        """Test that queue size is displayed correctly"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 25
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_teams', stdout=out)
        
        output = out.getvalue()
        assert "(Queue size: ~26)" in output  # Should show current + 1

    @patch('football.management.commands.init_teams.tqdm')
    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_progress_bar_integration(self, mock_delay, mock_redis, mock_tqdm):
        """Test progress bar is properly integrated"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        mock_progress = Mock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress
        
        call_command('init_teams', '--countries-per-task=2')
        
        # Should update progress for each batch
        expected_calls = [call(1)] * 4  # 7 countries / 2 = 4 batches
        mock_progress.update.assert_has_calls(expected_calls)

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_with_start_and_end_limits(self, mock_delay, mock_redis):
        """Test command with start and end limits"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        out = StringIO()
        call_command('init_teams', '--start-limit=1', '--end-limit=4', stdout=out)
        
        output = out.getvalue()
        assert "Found 3 countries to process" in output


class TestPopulateTeamsTask(TestCase):
    """Test cases for the populate_teams_task Celery task (unchanged from original)"""
    
    def setUp(self):
        """Set up test data"""
        self.countries = [
            Country.objects.create(id=1, name="England"),
            Country.objects.create(id=2, name="Spain"),
        ]
        
    @patch('football.tasks._process_single_country')
    def test_populate_teams_task_success(self, mock_process):
        """Test successful task execution"""
        country_names = ["England", "Spain"]
        
        populate_teams_task(country_names)
        
        assert mock_process.call_count == 2
        call_args_list = mock_process.call_args_list
        called_countries = [call[0][1] for call in call_args_list]
        assert "England" in called_countries
        assert "Spain" in called_countries
        
    @patch('football.tasks._process_single_country')
    def test_populate_teams_task_missing_country(self, mock_process):
        """Test task with non-existent country"""
        country_names = ["England", "NonExistent", "Spain"]
        
        with patch('football.tasks.logger') as mock_logger:
            populate_teams_task(country_names)
            
            mock_logger.warning.assert_called_with("Country NonExistent not found in database")
            assert mock_process.call_count == 2
            
    @patch('football.tasks._process_single_country')
    def test_populate_teams_task_exception_handling(self, mock_process):
        """Test task exception handling and retry"""
        mock_process.side_effect = Exception("Processing failed")
        
        with patch.object(populate_teams_task, 'retry', side_effect=Retry()) as mock_retry:
            with pytest.raises(Retry):
                populate_teams_task(["England"])
            
            mock_retry.assert_called_once()


class TestProcessSingleCountry(TestCase):
    """Test cases for _process_single_country helper function (unchanged from original)"""
    
    def setUp(self):
        """Set up test data"""
        self.country = Country.objects.create(id=1, name="England")
        self.task = Mock()
        self.task.request.retries = 0
        self.task.max_retries = 3
        
    @patch('football.api_client.get_team_details')
    @patch('football.tasks.Team.objects.bulk_create')
    def test_process_single_country_success(self, mock_bulk_create, mock_api):
        """Test successful country processing"""
        mock_api.return_value = {
            'response': [
                {
                    'team': {
                        'id': 1,
                        'name': 'Manchester United',
                        'code': 'MUN',
                        'logo': 'https://example.com/logo.png',
                        'national': False
                    }
                },
                {
                    'team': {
                        'id': 2,
                        'name': 'Liverpool',
                        'code': 'LIV',
                        'logo': 'https://example.com/logo2.png',
                        'national': False
                    }
                }
            ]
        }
        
        _process_single_country(self.task, "England", self.country)
        
        mock_api.assert_called_once_with(country="England")
        mock_bulk_create.assert_called_once()
        args, kwargs = mock_bulk_create.call_args
        teams = args[0]
        
        assert len(teams) == 2
        assert teams[0].id == 1
        assert teams[0].name == 'Manchester United'
        assert teams[0].short_name == 'MUN'
        assert teams[0].country == self.country
        assert kwargs['batch_size'] == 500
        assert kwargs['ignore_conflicts'] is True


# Integration test using pytest fixtures
@pytest.fixture
def countries_data():
    """Fixture providing test countries"""
    return [
        Country.objects.create(id=1, name="England"),
        Country.objects.create(id=2, name="Spain"),
        Country.objects.create(id=3, name="Germany"),
    ]


@pytest.mark.django_db
class TestInitTeamsIntegration:
    """Integration tests for the complete workflow"""
    
    @patch('football.api_client.get_team_details')
    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_end_to_end_workflow(self, mock_delay, mock_redis, mock_api, countries_data):
        """Test the complete workflow from command to task execution"""
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        mock_api.return_value = {
            'response': [
                {'team': {'id': 1, 'name': 'Test Team', 'code': 'TEST'}}
            ]
        }
        
        out = StringIO()
        call_command('init_teams', '--countries-per-task=2', stdout=out)
        
        output = out.getvalue()
        assert "Found 3 countries to process" in output
        assert mock_delay.call_count == 2
        
        call_args_list = mock_delay.call_args_list
        all_countries = []
        for call_args in call_args_list:
            all_countries.extend(call_args[0][0])
        
        assert set(all_countries) == {"England", "Spain", "Germany"}


# Parametrized tests for different scenarios
@pytest.mark.parametrize("batch_size,expected_batches", [
    (1, 7),  # 7 countries, 1 per batch = 7 batches
    (3, 3),  # 7 countries, 3 per batch = 3 batches  
    (5, 2),  # 7 countries, 5 per batch = 2 batches
    (10, 1), # 7 countries, 10 per batch = 1 batch
])
@pytest.mark.django_db
@patch('football.management.commands.init_teams.get_redis_connection')
@patch('football.management.commands.init_teams.populate_teams_task.delay')
def test_batch_size_calculations(mock_delay, mock_redis, batch_size, expected_batches):
    """Test different batch size calculations"""
    mock_redis_conn = Mock()
    mock_redis_conn.llen.return_value = 0
    mock_redis.return_value = mock_redis_conn
    
    for i in range(7):
        Country.objects.create(id=i+1, name=f"Country{i+1}")
    
    call_command('init_teams', f'--countries-per-task={batch_size}')
    
    assert mock_delay.call_count == expected_batches


# New tests specific to the modified command features
@pytest.mark.django_db
class TestNewCommandFeatures:
    """Tests for new features in the modified command"""
    
    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_redis_queue_monitoring(self, mock_delay, mock_redis):
        """Test Redis queue size monitoring"""
        # Create test countries
        for i in range(3):
            Country.objects.create(id=i+1, name=f"Country{i+1}")
            
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 50
        mock_redis.return_value = mock_redis_conn
        
        call_command('init_teams')
        
        # Should check queue size for each batch
        assert mock_redis_conn.llen.call_count >= 1
        assert mock_redis_conn.llen.call_args[0][0] == "celery"

    @patch('football.management.commands.init_teams.get_redis_connection')
    def test_get_countries_names_database_error(self, mock_redis):
        """Test handling of database errors in _get_countries_names"""
        command = Command()
        
        # Mock database error
        with patch('football.models.Country.objects.only') as mock_objects:
            mock_objects.side_effect = Exception("Database connection failed")
            
            with patch('football.management.commands.init_teams.logger') as mock_logger:
                result = command._get_countries_names()
                
                assert result == []
                mock_logger.error.assert_called_with("Failed to fetch country names: Database connection failed")

    @patch('football.management.commands.init_teams.get_redis_connection')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_empty_batch_handling(self, mock_delay, mock_redis):
        """Test handling of empty batches"""
        # Create only 2 countries but use batch size of 5
        for i in range(2):
            Country.objects.create(id=i+1, name=f"Country{i+1}")
            
        mock_redis_conn = Mock()
        mock_redis_conn.llen.return_value = 0
        mock_redis.return_value = mock_redis_conn
        
        call_command('init_teams', '--countries-per-task=5')
        
        # Should only create 1 batch
        assert mock_delay.call_count == 1