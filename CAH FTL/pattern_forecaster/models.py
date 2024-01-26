from django.db import models

class Unit(models.Model):
    Sales = "Sales"
    Client_Service = "Client Service"
    Agency = "Agency"
    Experts = "Experts"
    unit_name = [(Sales, "Sales"), (Client_Service, "Client Service"), (Agency, "Agency"), (Experts, "Experts")]

class DOW(models.Model):
    Sunday = "Sunday"
    Monday = "Monday"
    Tuesday = "Tuesday"
    Wednesday = "Wednesday"
    Thursday = "Thursday"
    Friday = "Friday"
    Saturday = "Saturday"
