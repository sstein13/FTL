from django.urls import path
from . import views

app_name = "volume_forecaster"
urlpatterns = [
    path("", views.main, name="main"),
    path("result/<str:unit_name>/<str:formattedDate>/<int:weeks_to_forecast>/", views.result, name="result"),
]