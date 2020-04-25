from django.db import models
from user.models import Customer

# Create your models here.
class Package(models.Model):
    package_id = models.IntegerField()

    dst_x = models.IntegerField(blank = True, null = True)
    dst_y = models.IntegerField(blank = True, null = True)
    cur_x = models.IntegerField(blank = True, null = True)
    cur_y = models.IntegerField(blank = True, null = True)

    status = models.CharField(max_length=200, default = 'created')
    truck_id = models.IntegerField(blank = True, null = True)

    item = models.CharField(max_length = 200, default = '', blank = True)
    # owner = models.ForeignKey(Customer, on_delete=models.SET_NULL, blank=True, null=True)
    owner_id = models.IntegerField(blank = False, null = False, default = 0)
