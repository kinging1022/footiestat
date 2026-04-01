from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('football', '0021_alter_league_type_alter_team_short_name'),
    ]

    operations = [
        # Add has_data flag — True = real stats, False = sentinel (no data from API)
        migrations.AddField(
            model_name='fixturestatistics',
            name='has_data',
            field=models.BooleanField(default=True),
        ),
        # Make team identity fields nullable so sentinel rows can be created
        # without dummy team names/ids when the API returns nothing
        migrations.AlterField(
            model_name='fixturestatistics',
            name='home_team_id',
            field=models.PositiveIntegerField(null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='fixturestatistics',
            name='home_team_name',
            field=models.CharField(max_length=100, null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='fixturestatistics',
            name='away_team_id',
            field=models.PositiveIntegerField(null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='fixturestatistics',
            name='away_team_name',
            field=models.CharField(max_length=100, null=True, blank=True),
        ),
    ]
