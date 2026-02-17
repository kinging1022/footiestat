from django.contrib import admin
from .models import Country, League, LeagueTableSnapshot,Team, TeamFormSnapshot, Fixture , HeadToHeadMatch, FixtureIngestion, FixtureAdvancedStats
# Register your models here.
admin.site.register(Country)
admin.site.register(League)
admin.site.register(LeagueTableSnapshot)
admin.site.register(Team)
admin.site.register(TeamFormSnapshot)
admin.site.register(Fixture)
admin.site.register(HeadToHeadMatch)
admin.site.register(FixtureIngestion)
admin.site.register(FixtureAdvancedStats)