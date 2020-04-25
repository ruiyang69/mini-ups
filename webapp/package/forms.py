from django import forms
from django.contrib.auth.models import User
from django.contrib.auth.forms import UserCreationForm
from .models import Package

class TrackForm(forms.ModelForm):
     class Meta:
          model = Package
          fields = ['package_id']

class CreatePackageForm(forms.ModelForm):
    class Meta:
          model = Package
          fields = ['package_id', 'dst_x', 'dst_y', 'item', 'owner_id']

          widget = {
            'package_id': forms.TextInput(attrs={ 'class': 'form-control',}),
            'dst_x': forms.TextInput(attrs={ 'class': 'form-control',}),
            'dst_y': forms.TextInput(attrs={ 'class': 'form-control',}),
            # 'cur_x': forms.TextInput(attrs={ 'class': 'form-control',}),
            # 'cur_y': forms.TextInput(attrs={ 'class': 'form-control',}),
            # 'status': forms.TextInput(attrs={ 'class': 'form-control',}),
            # 'truck_id': forms.TextInput(attrs={ 'class': 'form-control',}),
            'item': forms.TextInput(attrs={ 'class': 'form-control',}),
            'owner_id' :forms.TextInput(attrs={ 'class': 'form-control',}),
          }


class UpdateForm(forms.ModelForm):
     class Meta:
          model = Package
          fields = ['cur_x', 'cur_y']

          widget = {
            'cur_x': forms.TextInput(attrs={
                'class': 'form-control',
            }),
            'cur_y': forms.TextInput(attrs={
                'class': 'form-control',
            }),
          }


class RedirectForm(forms.ModelForm):
     class Meta:
          model = Package
          fields = ['dst_x', 'dst_y']

          widget = {
            'dst_x': forms.TextInput(attrs={
                'class': 'form-control',
            }),
            'dst_y': forms.TextInput(attrs={
                'class': 'form-control',
            }),
          }