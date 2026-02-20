from django.urls import path
from .views import home, matches

urlpatterns = [
    path('', home, name='home'),
    path('matches/', matches, name='matches')
               
]