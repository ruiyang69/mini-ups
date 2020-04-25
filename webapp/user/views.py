from django.shortcuts import render, redirect
from django.contrib.auth.models import User
from .forms import UserRegisterForm
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from package.forms import CreatePackageForm
from package.models import Package
from .models import Customer
from .forms import UpdateOwneridForm

# Create your views here.
def home(request):
     return render(request, 'index.html')


def register(request):
    if request.method == 'POST':
        form = UserRegisterForm(request.POST)
        if form.is_valid():
            user = form.save()
            customer = Customer()
            customer.user = user
            customer.owner_id  = 0
            customer.save()
            c = Customer.objects.filter(owner_id = 0)
            print(c)
            messages.success(request, f'You are now able to login!')
            return redirect('login')
    else:
        form = UserRegisterForm()
    return render(request, 'user/register.html', {'form': form})


@login_required
def profile(request):
    customer = Customer.objects.filter(user = request.user).first()
    package = Package.objects.filter(owner_id = customer.owner_id)
    return render(request, 'user/profile.html', {'package': package})



# testing purpose
@login_required
def create_package(request):
    if request.method == 'POST':
        form = CreatePackageForm(request.POST)
        if form.is_valid():
            package = form.save(commit = False)
            package.owner_id = request.user.id
            form.save()
            package = Package.objects.filter(owner_id = request.user.id)
            return render(request, 'user/profile.html', {'package': package})
    else:
        form = CreatePackageForm()
    return render(request, 'package/create_package.html', {'form': form})



def detail(request, package_id):
    package = Package.objects.filter(package_id = package_id).first()
    return render(request, 'user/detail.html', {'package': package})


def update_ownerid(request):
    form = UpdateOwneridForm()
    if request.method == 'POST':
        form = UpdateOwneridForm(request.POST)
        if form.is_valid():
            new_id = form.cleaned_data['owner_id']
            cur_customer = Customer.objects.filter(user = request.user).first()
            cur_customer.owner_id = new_id
            cur_customer.save()
            messages.success(request, f'you have updated your owner id')
            return render(request, 'index.html')

    else:
        return render(request, 'user/update_ownerid.html', {'form': form})