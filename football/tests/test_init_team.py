import pytest
from unittest.mock import Mock, patch, call
from io import StringIO
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from celery.exceptions import Retry

from football.models import Country, Team
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
        
    def test_get_countries_map(self):
        """Test _get_countries_map returns correct mapping"""
        command = Command()
        countries_map = command._get_countries_map()
        
        assert len(countries_map) == 7
        assert "England" in countries_map
        assert "Argentina" in countries_map
        assert isinstance(countries_map["England"], Country)
        assert countries_map["England"].id == 1

    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_default_parameters(self, mock_delay):
        """Test command with default parameters"""
        out = StringIO()
        call_command('init_teams', stdout=out)
        
        output = out.getvalue()
        assert "Found 7 countries to process" in output
        assert "Queued batch" in output
        
        # Should create 2 batches (5 + 2 countries)
        assert mock_delay.call_count == 2
        
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_custom_batch_size(self, mock_delay):
        """Test command with custom batch size"""
        out = StringIO()
        call_command('init_teams', '--countries-per-task=3', stdout=out)
        
        output = out.getvalue()
        assert "Found 7 countries to process" in output
        
        # Should create 3 batches (3 + 3 + 1 countries)
        assert mock_delay.call_count == 3
        
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_dry_run(self, mock_delay):
        """Test command in dry run mode"""
        out = StringIO()
        call_command('init_teams', '--dry-run', stdout=out)
        
        output = out.getvalue()
        assert "Dry run mode - no tasks will be actually created" in output
        assert "Would queue batch for:" in output
        
        # No tasks should be created in dry run
        mock_delay.assert_not_called()
        
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_handle_task_creation_failure(self, mock_delay):
        """Test handling of task creation failures"""
        mock_delay.side_effect = Exception("Celery connection failed")
        
        out = StringIO()
        err = StringIO()
        call_command('init_teams', stdout=out, stderr=err)
        
        error_output = err.getvalue()
        assert "Failed to queue batch" in error_output
        assert "Celery connection failed" in error_output

    @patch('football.management.commands.init_teams.tqdm')
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_progress_bar_integration(self, mock_delay, mock_tqdm):
        """Test progress bar is properly integrated"""
        mock_progress = Mock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress
        
        call_command('init_teams', '--countries-per-task=2')
        
        # Should update progress for each batch
        expected_calls = [call(1)] * 4  # 7 countries / 2 = 4 batches
        mock_progress.update.assert_has_calls(expected_calls)


class TestPopulateTeamsTask(TestCase):
    """Test cases for the populate_teams_task Celery task"""
    
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
        
        # Call the task function directly (it will use the real database)
        populate_teams_task(country_names)
        
        # Should process each country - checking the call was made
        assert mock_process.call_count == 2
        # Check that the calls include the country names we expect
        call_args_list = mock_process.call_args_list
        called_countries = [call[0][1] for call in call_args_list]  # Extract country names
        assert "England" in called_countries
        assert "Spain" in called_countries
        
    @patch('football.tasks._process_single_country')
    def test_populate_teams_task_missing_country(self, mock_process):
        """Test task with non-existent country"""
        country_names = ["England", "NonExistent", "Spain"]
        
        with patch('football.tasks.logger') as mock_logger:
            populate_teams_task(country_names)
            
            # Should log warning for missing country
            mock_logger.warning.assert_called_with("Country NonExistent not found in database")
            
            # Should still process existing countries
            assert mock_process.call_count == 2
            
    @patch('football.tasks._process_single_country')
    def test_populate_teams_task_exception_handling(self, mock_process):
        """Test task exception handling and retry"""
        mock_process.side_effect = Exception("Processing failed")
        
        # Mock the retry mechanism by patching the task's retry method
        with patch.object(populate_teams_task, 'retry', side_effect=Retry()) as mock_retry:
            with pytest.raises(Retry):
                populate_teams_task(["England"])
            
            # Verify retry was called
            mock_retry.assert_called_once()


class TestProcessSingleCountry(TestCase):
    """Test cases for _process_single_country helper function"""
    
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
        # Mock API response
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
        
        # Should call API with correct country
        mock_api.assert_called_once_with(country="England")
        
        # Should create teams via bulk_create
        mock_bulk_create.assert_called_once()
        args, kwargs = mock_bulk_create.call_args
        teams = args[0]
        
        assert len(teams) == 2
        assert teams[0].id == 1
        assert teams[0].name == 'Manchester United'
        assert teams[0].short_name == 'MUN'
        assert teams[0].country == self.country
        assert teams[1].id == 2
        assert teams[1].name == 'Liverpool'
        assert teams[1].country == self.country
        
        # Check bulk_create parameters
        assert kwargs['batch_size'] == 500
        assert kwargs['ignore_conflicts'] is True
        
    @patch('football.api_client.get_team_details')
    def test_process_single_country_invalid_api_response(self, mock_api):
        """Test handling of invalid API response"""
        mock_api.return_value = None
        
        _process_single_country(self.task, "England", self.country)
        
        # Should retry on invalid response
        self.task.retry.assert_called_once()
        
    @patch('football.api_client.get_team_details')  
    def test_process_single_country_malformed_response(self, mock_api):
        """Test handling of malformed API response"""
        mock_api.return_value = {'response': 'not a list'}
        
        _process_single_country(self.task, "England", self.country)
        
        # Should retry on malformed response
        self.task.retry.assert_called_once()
        
    @patch('football.api_client.get_team_details')
    @patch('football.tasks.Team.objects.bulk_create')
    def test_process_single_country_skips_invalid_teams(self, mock_bulk_create, mock_api):
        """Test that teams without ID are skipped"""
        mock_api.return_value = {
            'response': [
                {'team': {'id': 1, 'name': 'Valid Team'}},
                {'team': {'name': 'Invalid Team'}},  # No ID
                {'team': {'id': 2, 'name': 'Another Valid Team'}}
            ]
        }
        
        _process_single_country(self.task, "England", self.country)
        
        # Should only create teams with valid ID
        mock_bulk_create.assert_called_once()
        args, kwargs = mock_bulk_create.call_args
        teams = args[0]
        assert len(teams) == 2
        assert all(team.id for team in teams)
        
    @patch('football.api_client.get_team_details')
    def test_process_single_country_max_retries_exceeded(self, mock_api):
        """Test behavior when max retries exceeded"""
        self.task.request.retries = 3
        self.task.max_retries = 3
        
        mock_api.side_effect = Exception("API Error")
        
        with patch('football.tasks.logger') as mock_logger:
            _process_single_country(self.task, "England", self.country)
            
            # Should log error and not retry
            mock_logger.error.assert_called_with("Max retries exceeded for England")
            self.task.retry.assert_not_called()
            
    @patch('football.api_client.get_team_details')
    def test_process_single_country_api_exception(self, mock_api):
        """Test handling of API exceptions"""
        mock_api.side_effect = Exception("Connection timeout")
        
        _process_single_country(self.task, "England", self.country)
        
        # Should retry on API exception
        self.task.retry.assert_called_once()


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
    @patch('football.management.commands.init_teams.populate_teams_task.delay')
    def test_end_to_end_workflow(self, mock_delay, mock_api, countries_data):
        """Test the complete workflow from command to task execution"""
        # Setup API mock
        mock_api.return_value = {
            'response': [
                {'team': {'id': 1, 'name': 'Test Team', 'code': 'TEST'}}
            ]
        }
        
        # Run command
        out = StringIO()
        call_command('init_teams', '--countries-per-task=2', stdout=out)
        
        # Verify command executed
        output = out.getvalue()
        assert "Found 3 countries to process" in output
        assert mock_delay.call_count == 2  # 3 countries / 2 = 2 batches
        
        # Verify task arguments
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
@patch('football.management.commands.init_teams.populate_teams_task.delay')
def test_batch_size_calculations(mock_delay, batch_size, expected_batches):
    """Test different batch size calculations"""
    # Create test countries
    for i in range(7):
        Country.objects.create(id=i+1, name=f"Country{i+1}")
    
    call_command('init_teams', f'--countries-per-task={batch_size}')
    
    assert mock_delay.call_count == expected_batches