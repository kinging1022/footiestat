from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('football', '0018_alter_fixturestatistics_fixture_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='leaguetablesnapshot',
            name='group_name',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
