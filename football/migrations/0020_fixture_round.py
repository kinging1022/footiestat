from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('football', '0019_leaguetablesnapshot_group_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='fixture',
            name='round',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
