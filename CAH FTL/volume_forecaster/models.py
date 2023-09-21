from django.db import models

class Unit(models.Model):
    Sales = "Sales"
    Client_Service = "Client Service"
    Agency = "Agency"
    Experts = "Experts"
    unit_name = [(Sales, "Sales"), (Client_Service, "Client Service"), (Agency, "Agency"), (Experts, "Experts")]
