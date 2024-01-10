from django.urls import path
from . import views

urlpatterns = [
    path("", views.main, name="main"),
    path("result/<str:unit_name>/<str:DOW>/", views.result, name="result"),
]