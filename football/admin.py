from django.contrib import admin
from .models import Country, League, LeagueTableSnapshot,Team, TeamFormSnapshot, Fixture , HeadToHeadMatch, FixtureIngestion, FixtureAdvancedStats,FixtureStatistics
# Register your models here.
admin.site.register(Country)
admin.site.register(LeagueTableSnapshot)
admin.site.register(Team)
admin.site.register(TeamFormSnapshot)
admin.site.register(Fixture)
admin.site.register(HeadToHeadMatch)
admin.site.register(FixtureIngestion)
admin.site.register(FixtureAdvancedStats)
admin.site.register(FixtureStatistics)





@admin.register(League)
class LeagueAdmin(admin.ModelAdmin):
    list_display = ['name', 'country', 'season', 'priority', 'type']
    list_filter = ['season', 'country', 'type']
    search_fields = ['name', 'country__name']
    list_editable = ['priority']  
    ordering = ['priority', 'name']