# football/management/commands/bootstrap_fixtures.py
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime, timedelta
import time



class Command(BaseCommand):
    help = 'Bootstrap initial 5 days of fixtures (run once on first deployment)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=7,
            help='Number of days to bootstrap (default: 7)'
        )

    def handle(self, *args, **options):
        days = options['days']
        
        self.stdout.write(self.style.WARNING(
            f'🚀 Starting bootstrap for {days} days of fixtures...\n'
        ))
        
        start_date = datetime.now()
        total_fixtures = 0
        
        for i in range(days):
            current_date = start_date + timedelta(days=i)
            date_str = current_date.strftime('%Y-%m-%d')
            
            self.stdout.write(f'\n📅 Processing {date_str} (day {i+1}/{days})...')
            
            try:
                # Fetch and process this day
                from football.tasks import fetch_and_process_day

                result = fetch_and_process_day(date_str)
                
                fixtures_count = result['created'] + result['updated']
                total_fixtures += fixtures_count
                
                self.stdout.write(self.style.SUCCESS(
                    f'✅ {date_str}: {result["created"]} created, '
                    f'{result["updated"]} updated, {result["skipped"]} skipped'
                ))
                
                # Rate limit protection (0.5s between days)
                if i < days - 1:
                    time.sleep(0.5)
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(
                    f'❌ Error processing {date_str}: {e}'
                ))
                continue
        
        self.stdout.write(self.style.SUCCESS(
            f'\n\n✅ Bootstrap complete! Total fixtures: {total_fixtures}\n'
        ))
        
        self.stdout.write(
            'Next steps:\n'
            '1. Data processing will happen automatically via Celery Beat\n'
            '2. Check processing status: GET /api/admin/processing-status/\n'
            '3. Daily ingestion runs at 2 AM automatically\n'
        )