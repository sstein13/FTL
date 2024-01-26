from django.db import models

class Unit(models.Model):
    Sales = "Sales"
    Solutions = "Solutions"
    Midvale = "Midvale"
    Commercial = "Commercial"
    unit_name = [(Sales, "Sales"), (Solutions, "Solutions"), (Midvale, "Midvale"), (Commercial, "Commercial")]
