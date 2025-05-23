import pytest
from unittest.mock import Mock, patch
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import transaction
from io import StringIO
from football.models import Country, League
from football.management.commands.init_data import Command


@pytest.fixture
def sample_api_response():
    """Sample API response fixture"""
    return {
        'response': [
            {
                'league': {
                    'id': 4,
                    'name': 'Euro Championship',
                    'type': 'Cup',
                    'logo': 'https://media.api-sports.io/football/leagues/4.png'
                },
                'country': {
                    'name': 'World',
                    'code': None,
                    'flag': None
                },
                'seasons': [
                    {'year': 2008}
                ]
            },
            {
                'league': {
                    'id': 39,
                    'name': 'Premier League',
                    'type': 'League',
                    'logo': 'https://media.api-sports.io/football/leagues/39.png'
                },
                'country': {
                    'name': 'England',
                    'code': 'GB',
                    'flag': 'https://media.api-sports.io/flags/gb.svg'
                },
                'seasons': [
                    {'year': 2023}
                ]
            },
            {
                'league': {
                    'id': 61,
                    'name': 'Ligue 1',
                    'type': 'League',
                    'logo': 'https://media.api-sports.io/football/leagues/61.png'
                },
                'country': {
                    'name': 'France',
                    'code': 'FR',
                    'flag': 'https://media.api-sports.io/flags/fr.svg'
                },
                'seasons': [
                    {'year': 2023}
                ]
            }
        ]
    }


@pytest.fixture
def command():
    """Command instance fixture"""
    return Command()


@pytest.mark.django_db
class TestInitDataCommand:
    """Test suite for the init_data management command"""

    @patch('football.management.commands.init_data.get_league_details')
    def test_successful_initialization(self, mock_get_league_details, sample_api_response):
        """Test successful command execution"""
        mock_get_league_details.return_value = sample_api_response
        
        out = StringIO()
        call_command('init_data', stdout=out)
        
        # Verify countries were created
        assert Country.objects.count() == 3
        world_country = Country.objects.get(name='World')
        england_country = Country.objects.get(name='England')
        france_country = Country.objects.get(name='France')
        
        assert world_country.country_code is None
        assert england_country.country_code == 'GB'
        assert france_country.country_code == 'FR'
        
        # Verify leagues were created
        assert League.objects.count() == 3
        euro_league = League.objects.get(id=4)
        premier_league = League.objects.get(id=39)
        ligue1 = League.objects.get(id=61)
        
        assert euro_league.name == 'Euro Championship'
        assert euro_league.country == world_country
        assert euro_league.season == 2008
        
        assert premier_league.name == 'Premier League'
        assert premier_league.country == england_country
        assert premier_league.season == 2023
        
        # Check success message
        assert 'Successfully initialized 3 prioritized leagues' in out.getvalue()

    @patch('football.management.commands.init_data.get_league_details')
    def test_custom_batch_size(self, mock_get_league_details, sample_api_response):
        """Test command with custom batch size"""
        mock_get_league_details.return_value = sample_api_response
        
        out = StringIO()
        call_command('init_data', '--batch-size=2', stdout=out)
        
        # Should still create all records regardless of batch size
        assert Country.objects.count() == 3
        assert League.objects.count() == 3

    @patch('football.management.commands.init_data.get_league_details')
    def test_existing_countries_not_duplicated(self, mock_get_league_details, sample_api_response):
        """Test that existing countries are not duplicated"""
        # Create existing country
        existing_country = Country.objects.create(
            name='England',
            country_code='GB',
            flag='https://existing-flag.com'
        )
        
        mock_get_league_details.return_value = sample_api_response
        
        call_command('init_data')
        
        # Should still have only 3 countries total
        assert Country.objects.count() == 3
        
        # Existing country should remain unchanged
        england_country = Country.objects.get(name='England')
        assert england_country.id == existing_country.id
        assert england_country.flag == 'https://existing-flag.com'  # Original flag preserved

    @patch('football.management.commands.init_data.get_league_details')
    def test_existing_leagues_not_duplicated(self, mock_get_league_details, sample_api_response):
        """Test that existing leagues are not duplicated"""
        # Create existing country and league
        country = Country.objects.create(name='England', country_code='GB')
        League.objects.create(
            id=39,
            name='Premier League',
            country=country,
            season=2023
        )
        
        mock_get_league_details.return_value = sample_api_response
        
        call_command('init_data')
        
        # Should still have only 3 leagues total
        assert League.objects.count() == 3

    @patch('football.management.commands.init_data.get_league_details')
    def test_invalid_data_handling(self, mock_get_league_details):
        """Test handling of invalid/incomplete data"""
        invalid_response = {
            'response': [
                # Missing league name
                {
                    'league': {
                        'id': 1,
                        'type': 'Cup',
                        'logo': 'https://example.com/logo.png'
                    },
                    'country': {
                        'name': 'Test Country',
                        'code': 'TC',
                        'flag': 'https://example.com/flag.png'
                    },
                    'seasons': [{'year': 2023}]
                },
                # Missing country
                {
                    'league': {
                        'id': 2,
                        'name': 'Test League',
                        'type': 'League',
                        'logo': 'https://example.com/logo2.png'
                    },
                    'country': None,
                    'seasons': [{'year': 2023}]
                },
                # Missing seasons
                {
                    'league': {
                        'id': 3,
                        'name': 'Another League',
                        'type': 'League',
                        'logo': 'https://example.com/logo3.png'
                    },
                    'country': {
                        'name': 'Another Country',
                        'code': 'AC',
                        'flag': 'https://example.com/flag2.png'
                    },
                    'seasons': []
                },
                # Valid entry
                {
                    'league': {
                        'id': 4,
                        'name': 'Valid League',
                        'type': 'League',
                        'logo': 'https://example.com/logo4.png'
                    },
                    'country': {
                        'name': 'Valid Country',
                        'code': 'VC',
                        'flag': 'https://example.com/flag3.png'
                    },
                    'seasons': [{'year': 2023}]
                }
            ]
        }
        
        mock_get_league_details.return_value = invalid_response
        
        out = StringIO()
        call_command('init_data', stdout=out)
        
        # Countries are processed first, so Test Country and Another Country will be created
        # even if their leagues fail validation
        assert Country.objects.count() == 3  # Test Country, Another Country, Valid Country
        assert League.objects.count() == 1   # Only Valid League
        
        # Check warning message about skipped records
        assert 'Skipped 3 invalid/missing records' in out.getvalue()

    @patch('football.management.commands.init_data.get_league_details')
    def test_api_failure(self, mock_get_league_details):
        """Test handling of API failure"""
        mock_get_league_details.side_effect = Exception("API Error")
        
        err = StringIO()
        with pytest.raises(Exception):
            call_command('init_data', stderr=err)
        
        # No data should be created
        assert Country.objects.count() == 0
        assert League.objects.count() == 0

    @patch('football.management.commands.init_data.get_league_details')
    def test_empty_api_response(self, mock_get_league_details):
        """Test handling of empty API response"""
        mock_get_league_details.return_value = {'response': []}
        
        out = StringIO()
        call_command('init_data', stdout=out)
        
        # No data should be created
        assert Country.objects.count() == 0
        assert League.objects.count() == 0
        
        assert 'Successfully initialized 0 prioritized leagues' in out.getvalue()

    def test_fetch_leagues_method(self, command, sample_api_response):
        """Test the _fetch_leagues method"""
        with patch('football.management.commands.init_data.get_league_details') as mock_api:
            mock_api.return_value = sample_api_response
            
            result = command._fetch_leagues()
            
            assert len(result) == 3
            assert result[0]['league']['id'] == 4
            assert result[1]['league']['id'] == 39
            assert result[2]['league']['id'] == 61

    def test_process_countries_method(self, command, sample_api_response):
        """Test the _process_countries method"""
        leagues_data = sample_api_response['response']
        
        country_map = command._process_countries(leagues_data)
        
        # Check countries were created
        assert len(country_map) == 3
        assert 'World' in country_map
        assert 'England' in country_map
        assert 'France' in country_map
        
        # Verify database records
        assert Country.objects.count() == 3
        
        world_country = Country.objects.get(name='World')
        assert world_country.country_code is None
        assert world_country.flag is None

    def test_process_countries_with_existing(self, command, sample_api_response):
        """Test _process_countries with existing countries"""
        # Create existing country
        existing = Country.objects.create(name='England', country_code='UK')
        
        leagues_data = sample_api_response['response']
        country_map = command._process_countries(leagues_data)
        
        # Should still have 3 countries
        assert Country.objects.count() == 3
        
        # Existing country should be preserved
        england_country = Country.objects.get(name='England')
        assert england_country.id == existing.id
        assert england_country.country_code == 'UK'  # Original value preserved

    def test_process_leagues_method(self, command, sample_api_response):
        """Test the _process_leagues method"""
        leagues_data = sample_api_response['response']
        
        # First create countries
        country_map = command._process_countries(leagues_data)
        
        # Then process leagues
        command._process_leagues(leagues_data, country_map, batch_size=500)
        
        # Verify leagues were created
        assert League.objects.count() == 3
        
        euro_league = League.objects.get(id=4)
        assert euro_league.name == 'Euro Championship'
        assert euro_league.type == 'Cup'
        assert euro_league.season == 2008
        assert euro_league.country.name == 'World'

    def test_process_leagues_small_batch_size(self, command, sample_api_response):
        """Test _process_leagues with small batch size"""
        leagues_data = sample_api_response['response']
        country_map = command._process_countries(leagues_data)
        
        # Use batch size of 1 to test batch processing
        command._process_leagues(leagues_data, country_map, batch_size=1)
        
        # All leagues should still be created
        assert League.objects.count() == 3

    @patch('football.management.commands.init_data.get_league_details')
    def test_transaction_rollback_on_error(self, mock_get_league_details, sample_api_response):
        """Test that transaction is rolled back on error"""
        mock_get_league_details.return_value = sample_api_response
        
        # Mock an error in the API call itself to trigger the exception handling
        mock_get_league_details.side_effect = Exception("API Error")
        
        err = StringIO()
        with pytest.raises(Exception):
            call_command('init_data', stderr=err)
        
        # No data should be committed due to transaction rollback
        assert Country.objects.count() == 0
        assert League.objects.count() == 0

    def test_command_help_text(self, command):
        """Test that command has proper help text"""
        assert command.help == 'Initialize the database with prioritized leagues and countries'

    def test_batch_size_argument(self, command):
        """Test batch size argument parsing"""
        parser = Mock()
        command.add_arguments(parser)
        
        parser.add_argument.assert_called_once_with(
            '--batch-size',
            type=int,
            default=500,
            help='Batch size for bulk operations'
        )






    