
from django.urls import path
from . import views

app_name = 'blog'

urlpatterns = [
    path('blog/', views.blog_home, name='home'),
    path('create/', views.create_article, name='create_article'),
    path('<slug:slug>/', views.article_detail, name='article_detail'),
    path('<slug:slug>/edit/', views.edit_article, name='edit_article'),
]