from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from . import views

from .forms import TrackForm, UpdateForm, CreatePackageForm, RedirectForm
from .models import Package



# Create your views here.
def track_package(request):
     if request.method == 'POST':
          form = TrackForm(request.POST)
          if form.is_valid():
               form.save(commit = False)
               package = Package.objects.filter(package_id=form.cleaned_data['package_id']).first()
               if(package):
                    return render(request, 'user/detail.html', {'package': package})
               else:
                    messages.error(request, 'Cannot find a package with this id')
                    form = TrackForm()
                    return render(request, 'package/track.html', {'form': form})
     else:
          form = TrackForm()
     return render(request, 'package/track.html', {'form': form})


def redirect_package(request, package_id):
     if request.method == 'POST':
          if 'redirect_to' in request.POST:
               form = RedirectForm(request.POST)
               if form.is_valid():
                    new_dst_x = form.cleaned_data['dst_x']
                    new_dst_y = form.cleaned_data['dst_y']
                    package = Package.objects.filter(package_id = package_id).first()
                    if(package and package.status is not 'delievered'):
                         package.dst_x = new_dst_x
                         package.dst_y = new_dst_y
                         package.save()
                         return render(request, 'user/detail.html', {'package': package})
                    else:
                         messages.error(request, 'Cannot find a package with this id')
                         form = RedirectForm()
                         return render(request, 'package/redirect.html', {'form': form})
     else: 
          form = RedirectForm()
     return render(request, 'package/redirect.html', {'form': form})

