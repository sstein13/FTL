from django.shortcuts import render
from django.http import HttpResponse, request, HttpResponseRedirect
from django.template import loader
from django.http import request
from django.urls import reverse

forecast = None

def main(request):
    template = loader.get_template("pattern_forecaster/main.html")
    context = {}
    if request.method == 'POST' and 'forecast_button' in request.POST:
        unit = request.POST['forecast_button'].split('/')[0]
        unit_name = unit
        if "_" in unit_name:
            unit_name = unit_name.replace("_", " ")
        DOW = request.POST['forecast_button'].split('/')[1]
        # import function to run
        from pattern_forecaster.forecaster import pattern_forecast
        # call function
        global forecast
        forecast = pattern_forecast(unit_name, DOW)
        # return user to required page
        return HttpResponseRedirect("result/{}/".format(unit + "/" + DOW))
    return HttpResponse(template.render(context,request))

def result(request, unit_name, DOW):
    template = loader.get_template("pattern_forecaster/result.html")

    forecast_html = '<table class="table"><thead><tr><th scope="col">Interval</th><th scope="col">Forecast</th></tr></thead>'
    for interval in forecast:
        forecast_html = forecast_html + '<tr><th scope="row">' + interval + "</th><td>" + str(forecast[interval]) + "</td></tr>"
    forecast_html = forecast_html + "</tbody></table>"


    context = {"unit":unit_name,
               "DOW":DOW,
               "forecast":forecast,
               "forecast_html":forecast_html
              }
    return HttpResponse(template.render(context,request))