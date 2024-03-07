from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from volume_forecaster.forecaster import tactical_volume_forecast
from datetime import datetime, timedelta

forecast = None

def main(request):
    template = loader.get_template("volume_forecaster/main.html")
    context = {}
    if request.method == 'POST' and 'forecast_button' in request.POST:
        unit = request.POST['forecast_button']
        unit_name = unit.replace("_", " ") if "_" in unit else unit
        formattedDate = request.POST.get('selectedDate', '')
        weeks_to_forecast = int(request.POST.get('weeks_to_forecast', 3))  # Default to 3 weeks if not specified
        weeks_to_forecast = min(weeks_to_forecast, 8)  # Limit to a maximum of 8 weeks
        # Call the forecasting function with the unit name, formatted date, and weeks to forecast
        global forecast
        forecast = tactical_volume_forecast(unit_name, weeks_to_forecast, formattedDate)
        # Redirect to the result page with both unit and formatted date
        return HttpResponseRedirect("result/{}/{}/{}/".format(unit, formattedDate.replace('/', '-'), weeks_to_forecast))
    return HttpResponse(template.render(context, request))

def result(request, unit_name, formattedDate, weeks_to_forecast):
    formattedDate = formattedDate.replace('-','/')
    template = loader.get_template("volume_forecaster/result.html")
    unit = forecast["unit"]
    # Convert the formattedDate string to a datetime object
    start_date = datetime.strptime(formattedDate, '%m/%d/%Y')
    # Generate forecasts for the specified number of weeks
    forecasts_html = ""
    for week_number in range(1, int(weeks_to_forecast) + 1):
        current_week = start_date + timedelta(days=(week_number - 1) * 7)
        current_week_str = current_week.strftime('%m/%d/%Y')
        week_forecast = forecast.get(current_week_str, {})

        # Generate HTML for the current week's forecast
        forecasts_html += generate_week_forecast_html(week_number, week_forecast, current_week_str)

    context = {
        "unit": unit,
        "forecasts_html": forecasts_html,
    }
    return HttpResponse(template.render(context, request))

def generate_week_forecast_html(week_number, week_forecast, current_week_str):
    week_forecast_html = '''<h3>Forecast for week starting on ''' + str(current_week_str) + '''</h3><table class="table"><thead><tr><th scope="col">Day of Week</th><th scope="col">Volume Forecast</th><th>Adjustment
<button type="button" id="adjustment-type-number-week''' + str(week_number) + '''" class="adjustment-type-btn active" onclick="setAdjustmentType('number', ''' + str(week_number) + ''')" checked>#</button>
<button type="button" id="adjustment-type-percent-week''' + str(week_number) + '''" class="adjustment-type-btn" onclick="setAdjustmentType('percent', ''' + str(week_number) + ''')">%</button></th><th>Adjusted Forecast</th></tr></thead>'''
    for dow in week_forecast:
        week_forecast_html = week_forecast_html + '<tr><th scope="row">' + dow + '</th><td id="forecast-week' + str(week_number) + '-' + dow +'">' + str(week_forecast[dow]) + '</td><td><input type="number" id="adjustment-week' + str(week_number) + '-' + dow +'" oninput="applyAdjustment(\'week' + str(week_number) + '-' + dow + '\')"></td><td id="adjusted-forecast-week' + str(week_number) + '-' + dow +'">' + str(week_forecast[dow]) + '</td></tr>'
    week_forecast_html = week_forecast_html + "</tbody></table>"
    return week_forecast_html


