from django.shortcuts import render
from django.http import HttpResponse, request, HttpResponseRedirect
from django.template import loader
from django.http import request
from django.urls import reverse

forecast = None

def main(request):
    template = loader.get_template("volume_forecaster/main.html")
    context = {}
    if request.method == 'POST' and 'forecast_button' in request.POST:
        unit = request.POST['forecast_button']
        unit_name = unit
        if "_" in unit_name:
            unit_name = unit_name.replace("_", " ")
        # import function to run
        from volume_forecaster.forecaster import tactical_volume_forecast_v2
        # call function
        global forecast
        forecast = tactical_volume_forecast_v2(unit_name)
        # return user to required page
        return HttpResponseRedirect("result/{}/".format(unit))
    return HttpResponse(template.render(context,request))

def result(request, unit_name):
    from volume_forecaster.forecaster import object_to_str, start_of_week, today
    template = loader.get_template("volume_forecaster/result.html")
    unit = forecast["unit"]
    week1 = start_of_week(object_to_str(today), 1)
    week2 = start_of_week(object_to_str(today), 2)
    week3 = start_of_week(object_to_str(today), 3)
    week1_forecast = forecast[week1]
    week2_forecast = forecast[week2]
    week3_forecast = forecast[week3]

    week1_forecast_html = '<table class="table"><thead><tr><th scope="col">Day of Week</th><th scope="col">Volume Forecast</th></tr></thead>'
    for dow in week1_forecast:
        week1_forecast_html = week1_forecast_html + '<tr><th scope="row">' + dow + "</th><td>" + str(week1_forecast[dow]) + "</td></tr>"
    week1_forecast_html = week1_forecast_html + "</tbody></table>"

    week2_forecast_html = '<table class="table"><thead><tr><th scope="col">Day of Week</th><th scope="col">Volume Forecast</th></tr></thead>'
    for dow in week2_forecast:
        week2_forecast_html = week2_forecast_html + '<tr><th scope="row">' + dow + "</th><td>" + str(week2_forecast[dow]) + "</td></tr>"
    week2_forecast_html = week2_forecast_html + "</tbody></table>"

    week3_forecast_html = '<table class="table"><thead><tr><th scope="col">Day of Week</th><th scope="col">Volume Forecast</th></tr></thead>'
    for dow in week3_forecast:
        week3_forecast_html = week3_forecast_html + '<tr><th scope="row">' + dow + "</th><td>" + str(week3_forecast[dow]) + "</td></tr>"
    week3_forecast_html = week3_forecast_html + "</tbody></table>"

    context = {"unit":unit,
               "week1":week1,
               "week2":week2,
               "week3":week3,
               "week1_forecast_html":week1_forecast_html,
               "week2_forecast_html":week2_forecast_html,
               "week3_forecast_html":week3_forecast_html,}
    return HttpResponse(template.render(context,request))


