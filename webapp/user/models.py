from django.db import models
from django.contrib.auth.models import User
import django.utils

# Create your models here.
# class Customer(models.Model):
#     user_id = user_id = models.IntegerField(default = 0, primary_key = True)
#     username = models.CharField(max_length=200, default = '')
#     email = models.CharField(max_length=200, default = '')
#     password = models.CharField(max_length=200, default = '')


class Customer(models.Model):
    user = models.OneToOneField(User, on_delete = models.CASCADE)
    owner_id = models.IntegerField(default = 0)
