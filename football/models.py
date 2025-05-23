from django.db import models


class Country(models.Model):
    name = models.CharField(max_length=100, verbose_name="Country Name", unique=True)
    country_code = models.CharField(max_length=3, blank=True, null=True, verbose_name="Country Code")
    flag = models.URLField(verbose_name="Country Flag URL", null=True, blank=True)
   
    class Meta:
        verbose_name = "Country"
        verbose_name_plural = "Countries"
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]
        

    def __str__(self):
        return f"{self.name} ({self.country_code})"


class League(models.Model): 
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100, verbose_name="League Name")
    logo = models.URLField(verbose_name="League Logo URL",blank=True,null=True)
    type = models.CharField(max_length=50, blank=True)
    country = models.ForeignKey(
        Country, 
        on_delete=models.CASCADE,
        related_name='leagues'
    )
    season = models.IntegerField()

    class Meta:
        verbose_name = "League"
        verbose_name_plural = "Leagues"
        constraints = [
            models.UniqueConstraint(fields=['name', 'season', 'country'], name='unique_league_season')
        ]

    def __str__(self):
        return f"{self.country.name} - {self.name} - {self.season} Season"




class Team(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100, db_index=True, verbose_name="Team Name")
    short_name = models.CharField(max_length=20, blank=True)
    logo = models.URLField(verbose_name="Team Logo URL", blank=True, null=True)
    country = models.ForeignKey(Country,on_delete=models.PROTECT,related_name='teams')
    national = models.BooleanField(default=False)
    class Meta:
        verbose_name = "Team"
        verbose_name_plural = "Teams"
        ordering = ['name']

    def __str__(self):
        return self.name
    

class TeamFormSnapshot(models.Model):
    team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="form_snapshots")
    season = models.PositiveSmallIntegerField()
    
    fixture_id = models.BigIntegerField(unique=True)  
    match_date = models.DateField(db_index=True)
    is_home = models.BooleanField()  
    
    opponent_id = models.ForeignKey(Team, on_delete=models.PROTECT, related_name="opponent_snapshots")
    goals_for = models.PositiveSmallIntegerField()
    goals_against = models.PositiveSmallIntegerField()
    result = models.CharField(max_length=1, choices=[('W', 'Win'), ('L', 'Loss'), ('D', 'Draw')],blank=True, null=True)
    
    league = models.ForeignKey(League, on_delete=models.PROTECT)
    matchday = models.PositiveSmallIntegerField()  
    
   
    
    class Meta:
        indexes = [
            models.Index(fields=['team', 'season']),
            models.Index(fields=['match_date']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['team', 'fixture_id'], name='unique_team_form_snapshot')
        ]


class LeagueTableSnapshot(models.Model):
    league = models.ForeignKey(League, on_delete=models.CASCADE,related_name='tables')
    season = models.IntegerField()
    round_name = models.CharField(max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now_add=True, db_index=True)
    rank = models.PositiveSmallIntegerField()
    team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name='league_table_snapshots')
    points = models.PositiveSmallIntegerField()
    goals_for = models.PositiveSmallIntegerField()
    goals_against = models.PositiveSmallIntegerField()
    goal_difference = models.IntegerField()
    matches_played = models.PositiveSmallIntegerField()
    wins = models.PositiveSmallIntegerField()
    draws = models.PositiveSmallIntegerField()
    losses = models.PositiveSmallIntegerField()
    last_five = models.CharField(max_length=50, null=True, blank=True)
    home_stat = models.JSONField(null=True, blank=True, default=dict)
    away_stat = models.JSONField(null=True, blank=True, default=dict)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['league', 'season', 'team'], name='unique_league_table_snapshot'),
        ]
        indexes = [
            models.Index(fields=['league']),
            models.Index(fields=['season']),
        ]
        verbose_name = "League Table Snapshot"
        verbose_name_plural = "League Table Snapshots"

    def __str__(self):
        round_display = self.round_name if self.round_name else "Unknown Round"
        return f"{self.league.name} - {self.season} ({round_display})"


class Fixture(models.Model): 
    STATUS_TBD = "TBD"
    STATUS_NS = "NS"
    STATUS_1H = "1H"
    STATUS_HT = "HT"
    STATUS_2H = "2H"
    STATUS_ET = "ET"
    STATUS_P = "P"
    STATUS_FT = "FT"
    STATUS_AET = "AET"
    STATUS_PEN = "PEN"
    STATUS_SUSP = "SUSP"
    STATUS_INT = "INT"
    STATUS_PST = "PST"
    STATUS_CANC = "CANC"
    STATUS_ABD = "ABD"
    STATUS_AWD = "AWD"
    STATUS_WO = "WO"
    STATUS_LIVE = "LIVE"

    STATUS_CHOICES = [
        (STATUS_TBD, "Time to be defined"),
        (STATUS_NS, "Not started"),
        (STATUS_1H, "First half"),
        (STATUS_HT, "Halftime"),
        (STATUS_2H, "Second half"),
        (STATUS_ET, "Extra time"),
        (STATUS_P, "Penalty in progress"),
        (STATUS_FT, "Full time"),
        (STATUS_AET, "After extra time"),
        (STATUS_PEN, "Penalty shootout"),
        (STATUS_SUSP, "Suspended"),
        (STATUS_INT, "Interrupted"),
        (STATUS_PST, "Postponed"),
        (STATUS_CANC, "Cancelled"),
        (STATUS_ABD, "Abandoned"),
        (STATUS_AWD, "Awarded"),
        (STATUS_WO, "WalkOver"),
        (STATUS_LIVE, "Live"),
    ]

    id = models.IntegerField(primary_key=True)
    date = models.DateTimeField(db_index=True)
    league = models.ForeignKey(
        League,
        on_delete=models.CASCADE,
        related_name='fixtures'
    )
    home_team = models.ForeignKey(
        Team,
        on_delete=models.CASCADE,
        related_name='home_fixtures'
    )
    away_team = models.ForeignKey(
        Team,
        on_delete=models.CASCADE,
        related_name='away_fixtures'
    )
    referee = models.CharField(max_length=100, null=True, blank=True)
    venue = models.CharField(max_length=100, null=True, blank=True)
    status = models.CharField(max_length=50, default=STATUS_NS, choices=STATUS_CHOICES)
    head_to_head_snapshot = models.JSONField(null=True, blank=True, default=dict)
    snapshot_processed = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Fixture"
        verbose_name_plural = "Fixtures"
        ordering = ['date']
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['home_team', 'away_team']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return (
            f"{self.home_team} vs {self.away_team} | "
            f"{self.date.strftime('%Y-%m-%d %H:%M')} | "
            f"{self.get_status_display()}"
        )