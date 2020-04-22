from django.contrib import admin
from django.urls import path,include
from django.contrib.auth import views as auth_views
from django.views.generic.base import TemplateView
from . import views
from package import views as package_views

urlpatterns = [
    path('login/', auth_views.LoginView.as_view(template_name = 'user/login.html'), name = 'login'),
    path('logout/', auth_views.LogoutView.as_view(template_name = 'user/logout.html'), name = 'logout'),
    path('register/', views.register, name = 'register'),
    path('profile/', views.profile, name = 'profile'),
    path('create/', views.create_package, name = 'create'),
    path('detail/<package_id>/', views.detail, name = 'detail'),
    path('redirect/<package_id>', package_views.redirect_package, name = 'redirect'),
]