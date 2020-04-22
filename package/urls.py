from django.urls import path
from . import views
from django.views.generic.base import TemplateView

app_name = 'package'

urlpatterns = [
    path('track/', views.track_package, name='track-package'),

]