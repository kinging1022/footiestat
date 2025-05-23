from django.core.management.base import BaseCommand
from django.db import transaction
from football.models import Country, League
from football.api_client import get_league_details
from tqdm import tqdm  

class Command(BaseCommand):
    help = 'Initialize the database with prioritized leagues and countries'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=500,
            help='Batch size for bulk operations'
        )

    @transaction.atomic
    def handle(self, *args, **kwargs):
        batch_size = kwargs['batch_size']
        
        try:
           

            # 1. Fetch and filter leagues
            leagues_data = self._fetch_leagues()
            
            # 2. Process countries
            country_map = self._process_countries(leagues_data)
            
            # 3. Process leagues in batches
            self._process_leagues(leagues_data, country_map, batch_size)

            self.stdout.write(
                self.style.SUCCESS(f"Successfully initialized {len(leagues_data)} prioritized leagues")
            )

        except Exception as e:
            self.stderr.write(
                self.style.ERROR(f"Failed: {str(e)}")
            )
            raise



    def _fetch_leagues(self):
        """Fetch leagues and filter by priority"""
        raw_data = get_league_details()
        return raw_data['response']  
       

    def _process_countries(self, leagues_data):
        """Process and bulk create countries"""
        countries = {}
        for item in leagues_data:
            country = item['country']
            if not country:
                continue
            countries[country['name']] = country

        # Get existing countries
        existing = Country.objects.filter(name__in=countries.keys())
        country_map = {c.name: c for c in existing}

        # Prepare new countries
        new_countries = [
            Country(
                name=name,
                country_code=data.get('code'),
                flag=data.get('flag')
            )
            for name, data in countries.items()
            if name not in country_map
        ]

        # Bulk create with ignore_conflicts
        Country.objects.bulk_create(
            new_countries,
            ignore_conflicts=True
        )
        
        # Refresh mapping
        return {c.name: c for c in 
                Country.objects.filter(name__in=countries.keys())}

   



    def _process_leagues(self, leagues_data, country_map, batch_size):
        """Process leagues in batches with validation"""
        leagues = []
        skipped = 0
        
        for item in tqdm(leagues_data, desc="Processing leagues"):
            try:
                league_data = item['league']
                country_name = item['country']['name']
                season_year = item['seasons'][0]['year']
                
                # Validation
                if not all([league_data.get('name'), country_name, season_year]):
                    skipped += 1
                    continue
                    
                country = country_map.get(country_name)
                if not country:
                    skipped += 1
                    continue
                
                leagues.append(
                    League(
                        id=league_data['id'],
                        name=league_data['name'],
                        logo=league_data.get('logo'),
                        type=league_data.get('type'),
                        country=country,
                        season=season_year
                    )
                )

                # Batch processing
                if len(leagues) >= batch_size:
                    League.objects.bulk_create(
                        leagues,
                        ignore_conflicts=True,
                        batch_size=batch_size
                    )
                    leagues = []
                    
            except Exception as e:
                self.stderr.write(f"Error processing league {item.get('id')}: {str(e)}")
                skipped += 1

        # Final batch
        if leagues:
            League.objects.bulk_create(
                leagues,
                ignore_conflicts=True,
                batch_size=batch_size
            )
        
        if skipped:
            self.stdout.write(self.style.WARNING(f"Skipped {skipped} invalid/missing records"))