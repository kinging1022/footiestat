# Generated by Django 5.2 on 2025-05-21 22:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('football', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='team',
            name='national',
            field=models.BooleanField(default=False),
        ),
    ]
