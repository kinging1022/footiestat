from django.db import models
from django.shortcuts import render
from django.utils import timezone
from django.utils.text import slugify

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
    logo = models.URLField(verbose_name="League Logo URL", blank=True, null=True)
    type = models.CharField(max_length=50, blank=True, null=True)
    country = models.ForeignKey(
        Country, 
        on_delete=models.CASCADE,
        related_name='leagues'
    )
    season = models.IntegerField()
    
    # ✅ NEW: Priority for homepage display
    priority = models.PositiveSmallIntegerField(
        default=999,
        help_text="Lower number = higher priority. 1-10 for top leagues, 999 for others"
    )
    
    class Meta:
        verbose_name = "League"
        verbose_name_plural = "Leagues"
        constraints = [
            models.UniqueConstraint(fields=['name', 'season', 'country'], name='unique_league_season')
        ]
        ordering = ['priority', 'name']  
    
    def __str__(self):
        return f"{self.country.name} - {self.name} - {self.season} Season"
    
    @property
    def is_priority(self):
        """Check if this is a priority league"""
        return self.priority <= 20







class Team(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100, db_index=True, verbose_name="Team Name")
    short_name = models.CharField(max_length=20, blank=True, null=True)
    logo = models.URLField(verbose_name="Team Logo URL", blank=True, null=True)
    country = models.ForeignKey(Country,on_delete=models.PROTECT,related_name='teams')
    national = models.BooleanField(default=False)
    class Meta:
        verbose_name = "Team"
        verbose_name_plural = "Teams"
        ordering = ['name']

    def __str__(self):
        return f'{self.name}-{self.country.name}'



    

class TeamFormSnapshot(models.Model):
    team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="form_snapshots")
    league_name = models.CharField(max_length=100, null=True, blank=True)
    league_id = models.PositiveIntegerField(null=True, blank=True)
    season = models.PositiveSmallIntegerField()
    fixture_id = models.BigIntegerField()  
    match_date = models.DateTimeField(db_index=True)
    is_home = models.BooleanField()  
    opponent = models.ForeignKey(Team, on_delete=models.PROTECT, related_name="opponent_snapshots")
    home_fulltime_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_fulltime_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_half_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_half_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_extra_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_extra_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_penalty_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_penalty_goals = models.PositiveSmallIntegerField(null=True, blank=True) 
    result = models.CharField(max_length=5, choices=[('W', 'Win'), ('L', 'Loss'), ('D', 'Draw')],blank=True, null=True)
    round_name = models.CharField(max_length=50, null=True, blank=True) 

    
    
   
    
    class Meta:
        indexes = [
            models.Index(fields=['team', 'season']),
            models.Index(fields=['match_date']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['team', 'fixture_id'], name='unique_team_form_snapshot')
        ]

        ordering = ['team', '-match_date']

    def __str__(self):
        return self.team.name + " vs " + self.opponent.name + " on " + self.match_date.strftime('%Y-%m-%d') + " (" + self.result + ")"





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
    group_name = models.CharField(max_length=100, null=True, blank=True)
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
    round = models.CharField(max_length=100, null=True, blank=True)

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
    
    @property
    def slug(self):
        home = slugify(self.home_team.name)
        away = slugify(self.away_team.name)
        date = self.date.strftime('%Y%m%d')
        return f"{home}-vs-{away}-{date}"


class HeadToHeadMatch(models.Model):
    fixture = models.ForeignKey(
        'Fixture',
        on_delete=models.CASCADE,
        related_name='h2h_matches'
    )
    past_fixture_id = models.IntegerField()
    league_name = models.CharField(max_length=100)
    date = models.DateTimeField()
    home_name = models.CharField(max_length=100)
    away_name = models.CharField(max_length=100)
    home_fulltime_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_fulltime_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_half_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_half_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_extra_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_extra_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_penalty_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_penalty_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    

    class Meta:
        verbose_name = "Head to Head Match"
        verbose_name_plural = "Head to Head Matches"
        ordering = ['fixture', '-date']

    def __str__(self):
        return f"{self.home_name} {self.home_fulltime_goals} - {self.away_fulltime_goals} {self.away_name} on {self.date.date()}"
    



class FixtureIngestion(models.Model):
    fixture = models.OneToOneField(Fixture, related_name='ingestion', on_delete= models.CASCADE, primary_key=True)

    #ingestion flags
    needs_h2h = models.BooleanField(default=True)
    needs_form = models.BooleanField(default=True)
    needs_standings = models.BooleanField(default=True)
    needs_advanced_stats = models.BooleanField(default=True)
    needs_detailed_stats = models.BooleanField(default=True)  


    # Processing timestamps
    h2h_processed_at = models.DateTimeField(null=True, blank=True)
    form_processed_at = models.DateTimeField(null=True, blank=True)
    standings_processed_at = models.DateTimeField(null=True, blank=True)
    advanced_stats_processed_at = models.DateTimeField(null=True, blank=True)
    detailed_stats_processed_at = models.DateTimeField(null=True, blank=True)  
    
    # Retry tracking
    h2h_retry_count = models.IntegerField(default=0)
    form_retry_count = models.IntegerField(default=0)
    standings_retry_count = models.IntegerField(default=0)
    advanced_stats_retry_count = models.IntegerField(default=0)
    detailed_stats_retry_count = models.IntegerField(default=0)
    
    # Error tracking
    last_error = models.TextField(null=True, blank=True)
    
    # CRITICAL: Mark when fixture is ready for display
    is_fully_processed = models.BooleanField(default=False, db_index=True)
    fully_processed_at = models.DateTimeField(null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)




    class Meta:
        ordering = ['fixture__date', 'fixture__id'] 
        indexes = [
            models.Index(fields=['needs_h2h', 'h2h_retry_count']),
            models.Index(fields=['needs_form', 'form_retry_count']),
            models.Index(fields=['needs_standings', 'standings_retry_count']),
            models.Index(fields=['needs_advanced_stats']),
            models.Index(fields=['is_fully_processed']),  # For quick user queries
        ]

    def check_and_mark_complete(self):
             if not any([
                self.needs_h2h,
                self.needs_form,
                self.needs_standings,
                self.needs_advanced_stats,
                self.needs_detailed_stats
             ]):
                self.is_fully_processed = True
                self.fully_processed_at = timezone.now()
                self.save(update_fields=['is_fully_processed', 'fully_processed_at','updated_at'])
    
        
        

    @property
    def processing_percentage(self):
        """Get completion percentage"""
        total = 5   
        completed = sum([
            not self.needs_h2h,
            not self.needs_form,
            not self.needs_standings,
            not self.needs_advanced_stats,
            not self.needs_detailed_stats
        ])
        return (completed / total) * 100




class FixtureAdvancedStats(models.Model):
   
    fixture = models.OneToOneField(
        Fixture, 
        on_delete=models.CASCADE, 
        primary_key=True,
        related_name='advanced_stats'
    )
    
    # === HOME TEAM STATS ===
    
    # Last 5 matches overall (any home/away)
    home_last_5_form = models.JSONField(
        default=list, 
        help_text="Last 5 matches overall"
    )
    
    # Last 5 home matches (is_home=True)
    home_last_5_home_form = models.JSONField(
        default=list, 
        help_text="Last 5 home matches only"
    )
    
    # Last 5 vs teams in similar standing position
    home_last_5_vs_similar_rank = models.JSONField(
        default=list, 
        help_text="Last 5 vs teams near opponent's rank (±3 positions)"
    )
    
    # Aggregated stats - Overall (last 5 any matches)
    home_wins_last_5 = models.PositiveSmallIntegerField(default=0)
    home_draws_last_5 = models.PositiveSmallIntegerField(default=0)
    home_losses_last_5 = models.PositiveSmallIntegerField(default=0)
    home_goals_scored_last_5 = models.PositiveSmallIntegerField(default=0)
    home_goals_conceded_last_5 = models.PositiveSmallIntegerField(default=0)
    
    # Aggregated stats - Home only (last 5 home matches)
    home_home_wins_last_5 = models.PositiveSmallIntegerField(default=0)
    home_home_draws_last_5 = models.PositiveSmallIntegerField(default=0)
    home_home_losses_last_5 = models.PositiveSmallIntegerField(default=0)
    
    # === AWAY TEAM STATS ===
    
    # Last 5 matches overall (any home/away)
    away_last_5_form = models.JSONField(
        default=list,
        help_text="Last 5 matches overall"
    )
    
    # Last 5 away matches (is_home=False)
    away_last_5_away_form = models.JSONField(
        default=list, 
        help_text="Last 5 away matches only"
    )
    
    # Last 5 vs teams in similar standing position
    away_last_5_vs_similar_rank = models.JSONField(
        default=list,
        help_text="Last 5 vs teams near opponent's rank (±3 positions)"
    )
    
    # Aggregated stats - Overall (last 5 any matches)
    away_wins_last_5 = models.PositiveSmallIntegerField(default=0)
    away_draws_last_5 = models.PositiveSmallIntegerField(default=0)
    away_losses_last_5 = models.PositiveSmallIntegerField(default=0)
    away_goals_scored_last_5 = models.PositiveSmallIntegerField(default=0)
    away_goals_conceded_last_5 = models.PositiveSmallIntegerField(default=0)
    
    # Aggregated stats - Away only (last 5 away matches)
    away_away_wins_last_5 = models.PositiveSmallIntegerField(default=0)
    away_away_draws_last_5 = models.PositiveSmallIntegerField(default=0)
    away_away_losses_last_5 = models.PositiveSmallIntegerField(default=0)
    
    # === META ===
    computed_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Fixture Advanced Stats"
        verbose_name_plural = "Fixture Advanced Stats"
    
    def __str__(self):
        return f"Advanced stats for {self.fixture}"
    


class FixtureStatistics(models.Model):
    # match_id is the past match — this is the true unique key
    match_id = models.BigIntegerField(primary_key=True)

    # parent_fixture groups past matches back to the upcoming fixture
    fixture = models.ForeignKey(
        'Fixture',
        on_delete=models.CASCADE,
        related_name='detailed_stats',
        db_index=True
    )

    date = models.DateField(null=True, blank=True)

    home_fulltime_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_fulltime_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_half_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_half_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_extra_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_extra_time_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    home_penalty_goals = models.PositiveSmallIntegerField(null=True, blank=True)
    away_penalty_goals = models.PositiveSmallIntegerField(null=True, blank=True)

    # HOME TEAM STATISTICS
    home_team_id = models.PositiveIntegerField()
    home_team_name = models.CharField(max_length=100)

    home_shots_on_goal = models.PositiveSmallIntegerField(null=True, blank=True)
    home_shots_off_goal = models.PositiveSmallIntegerField(null=True, blank=True)
    home_total_shots = models.PositiveSmallIntegerField(null=True, blank=True)
    home_blocked_shots = models.PositiveSmallIntegerField(null=True, blank=True)
    home_shots_insidebox = models.PositiveSmallIntegerField(null=True, blank=True)
    home_shots_outsidebox = models.PositiveSmallIntegerField(null=True, blank=True)

    home_fouls = models.PositiveSmallIntegerField(null=True, blank=True)
    home_corner_kicks = models.PositiveSmallIntegerField(null=True, blank=True)
    home_offsides = models.PositiveSmallIntegerField(null=True, blank=True)
    home_ball_possession = models.PositiveSmallIntegerField(null=True, blank=True)

    home_yellow_cards = models.PositiveSmallIntegerField(null=True, blank=True)
    home_red_cards = models.PositiveSmallIntegerField(null=True, blank=True)

    home_goalkeeper_saves = models.PositiveSmallIntegerField(null=True, blank=True)
    home_total_passes = models.PositiveIntegerField(null=True, blank=True)
    home_passes_accurate = models.PositiveIntegerField(null=True, blank=True)
    home_passes_percentage = models.PositiveSmallIntegerField(null=True, blank=True)

    home_expected_goals = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    home_goals_prevented = models.SmallIntegerField(null=True, blank=True)

    # AWAY TEAM STATISTICS
    away_team_id = models.PositiveIntegerField()
    away_team_name = models.CharField(max_length=100)

    away_shots_on_goal = models.PositiveSmallIntegerField(null=True, blank=True)
    away_shots_off_goal = models.PositiveSmallIntegerField(null=True, blank=True)
    away_total_shots = models.PositiveSmallIntegerField(null=True, blank=True)
    away_blocked_shots = models.PositiveSmallIntegerField(null=True, blank=True)
    away_shots_insidebox = models.PositiveSmallIntegerField(null=True, blank=True)
    away_shots_outsidebox = models.PositiveSmallIntegerField(null=True, blank=True)

    away_fouls = models.PositiveSmallIntegerField(null=True, blank=True)
    away_corner_kicks = models.PositiveSmallIntegerField(null=True, blank=True)
    away_offsides = models.PositiveSmallIntegerField(null=True, blank=True)
    away_ball_possession = models.PositiveSmallIntegerField(null=True, blank=True)

    away_yellow_cards = models.PositiveSmallIntegerField(null=True, blank=True)
    away_red_cards = models.PositiveSmallIntegerField(null=True, blank=True)

    away_goalkeeper_saves = models.PositiveSmallIntegerField(null=True, blank=True)
    away_total_passes = models.PositiveIntegerField(null=True, blank=True)
    away_passes_accurate = models.PositiveIntegerField(null=True, blank=True)
    away_passes_percentage = models.PositiveSmallIntegerField(null=True, blank=True)

    away_expected_goals = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    away_goals_prevented = models.SmallIntegerField(null=True, blank=True)

    processed_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['fixture']),
            models.Index(fields=['processed_at']),
        ]

    def __str__(self):
        return f"Stats: match {self.match_id} (parent fixture {self.fixture_id})"