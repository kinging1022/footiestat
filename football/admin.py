from django.contrib import admin
from .models import Country, League, LeagueTableSnapshot,Team, TeamFormSnapshot, Fixture
# Register your models here.
admin.site.register(Country)
admin.site.register(League)
admin.site.register(LeagueTableSnapshot)
admin.site.register(Team)
admin.site.register(TeamFormSnapshot)
admin.site.register(Fixture)
