from django.shortcuts import render, redirect
from django.contrib.auth.models import User
from .forms import UserRegisterForm
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from package.forms import CreatePackageForm
from package.models import Package

# Create your views here.
def home(request):
     return render(request, 'index.html')


def register(request):
    if request.method == 'POST':
        form = UserRegisterForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, f'You are now able to login!')
            return redirect('login')
    else:
        form = UserRegisterForm()
    return render(request, 'user/register.html', {'form': form})


@login_required
def profile(request):
    user = request.user
    package = Package.objects.filter(owner = request.user.username)
    return render(request, 'user/profile.html', {'package': package})



# testing purpose
@login_required
def create_package(request):
    if request.method == 'POST':
        form = CreatePackageForm(request.POST)
        if form.is_valid():
            form.save()
            package = Package.objects.filter(owner=request.user.username)
            print(package)
            return render(request, 'user/profile.html', {'package': package})
    else:
        form = CreatePackageForm()
    return render(request, 'package/create_package.html', {'form': form})



def detail(request, package_id):
    package = Package.objects.filter(package_id = package_id).first()
    return render(request, 'user/detail.html', {'package': package})