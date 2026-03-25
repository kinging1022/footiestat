from django.urls import path
from .views import fixture_stats, home, matches, fixture_deep_stats

urlpatterns = [
    path('', home, name='home'),
    path('matches/', matches, name='matches'),
    path('match/<int:match_id>/deep-stats/', fixture_deep_stats, name='fixture_deep_stats'),
    path('match/<int:fixture_id>/<slug:slug>/stats/', fixture_stats, name='fixture_stats'),
    


               
]