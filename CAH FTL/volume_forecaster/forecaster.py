# Imports
import pandas as pd
import datetime as dt
import numpy as np
import pyodbc
import holidays
from sklearn.linear_model import ElasticNet, ElasticNetCV
from sklearn.linear_model import LinearRegression

# Connect to DB
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
cursor = conn.cursor()


# Global variable definitions
today = dt.date.today()
current_year = str(dt.date.today())[:4]
weekday_dict = {0:"Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday", 5:"Saturday", 6:"Sunday"}
moving_holidays = ["New Year's Day", "Juneteenth National Independence Day", "Independence Day", "Veterans Day", "Christmas Day"]
static_holidays= ["Martin Luther King Jr. Day", "Washington's Birthday", "Memorial Day", "Labor Day", "Columbus Day", "Thanksgiving"]

# Creates the dataframe from the Actual Volume excel file and cleans data
CAH_volume_df = pd.read_excel("volume_forecaster/Actual Volume.xlsx")
CAH_volume_df.fillna(0, inplace = True)
CAH_volume_df.rename(columns={"# Service Level Calls Offered":"Date", "Unnamed: 1":"Start of Week", "Unnamed: 2":"Day of Week", "Department":"Advisor", "Unnamed: 4":"Agency", "Unnamed: 5":"Agency Helpline", "Unnamed: 6":"ASU", "Unnamed: 7":"ASU Set", "Unnamed: 8":"Claims Back Office", "Unnamed: 9":"Claims Hertz", "Unnamed: 10":"Claims Lead Line", "Unnamed: 11":"Claims Material Damage", "Unnamed: 12":"Claims Service Center", "Unnamed: 13":"Claims Team Lead", "Unnamed: 14":"Client Service", "Unnamed: 15":"Client Service Experts", "Unnamed: 16":"Client Service Set", "Unnamed: 17":"Sales", "Unnamed: 18":"Sales Experts", "Unnamed: 19":"Service Desk", "Unnamed: 20":"Underwriting", "Unnamed: 21":"Unite", "Unnamed: 22":"Unspecified", "Unnamed: 23":"Workforce"}, inplace=True)
CAH_volume_df.drop(index=0, inplace=True)
     # Deletes columns for units that no longer exist or that we do not forecast for
CAH_volume_df.drop(["Advisor", "Agency Helpline", "ASU", "ASU Set", "Claims Back Office", "Claims Hertz", "Claims Lead Line", "Claims Material Damage", "Claims Service Center", "Claims Team Lead", "Client Service Set", "Service Desk", "Underwriting", "Unite", "Unspecified", "Workforce"], axis = 1, inplace = True)
     # Combines Sales and Service Experts, since they are forecasted as one unit
CAH_volume_df["Experts"] = CAH_volume_df["Sales Experts"] + CAH_volume_df["Client Service Experts"]
CAH_volume_df.drop(["Sales Experts", "Client Service Experts"], axis = 1, inplace = True)
    # Changes the Date column to objects instead of strings and adds a Year column
CAH_volume_df["Date"] = pd.to_datetime(CAH_volume_df.Date)
CAH_volume_df["Year"] = CAH_volume_df["Date"].dt.strftime('%Y')

# Creates the dataframe from the Define Matching Weeks excel file and cleans data
define_matching_weeks_df = pd.read_excel("volume_forecaster/Define Matching Weeks.xlsx")
define_matching_weeks_df.fillna("none", inplace = True)

# Defines SQL queries
cisco_all_sql = """
Select cast(cti.DateTime as date) as 'Date', dpq.Dept_Name, sum(CallsOfferedRouted + CallsRequeried) as CallsOffered
From OperationalDataStore.AcqCiscoAW.Call_Type_SG_Interval cti
left join OperationalDataStore.ArcCiscoAW.V_CallDataCisco_Dim_Precision_Queue dpq
	on REPLACE(dpq.PrecisionQueueID, '~', '') = concat(cti.PrecisionQueueID, ODSDataSourceID)
Where cast(cti.DateTime as time) > '07:00:01'
And cast(cti.Datetime as time) < '21:59:59'
And dpq.Dept_Name in ('Sales Experts','Client Service Experts','Client Service','Sales','Agency')
Group by cast(cti.DateTime as date), dpq.Dept_Name
Order by cast(cti.DateTime as date)
"""

# Creates a dataframe of all Cisco volume in the database
cursor.execute(cisco_all_sql)
results = cursor.fetchall()
ODS_volume_df = pd.DataFrame.from_records(results, columns=[col[0] for col in cursor.description])
ODS_volume_df["Year"] = pd.to_datetime(ODS_volume_df["Date"]).dt.year

# Defines helper functions
def str_to_object(str_date):
    """
    Returns the date as a datetime object when entered in mm/dd/yyyy format.
    """
    month = str_date[:2]
    day = str_date[3:5]
    year = str_date[-4:]
    
    iso_date = year + '-' + month + '-' + day
    
    output = dt.date.fromisoformat(iso_date)
    return output


def object_to_str(object_date):
    """
    Returns the date in mm/dd/yyyy format when entered as a datetime object.
    """
    iso_date = dt.date.isoformat(object_date)
    
    month = iso_date[5:7]
    day = iso_date[-2:]
    year = iso_date[:4]
    
    output = month + "/" + day + "/" + year
    return output


def to_iso(date):
    """
    Returns the date in iso format when entered in mm/dd/yyyy format.
    """
    month = date[:2]
    day = date[3:5]
    year = date[-4:]
    
    iso_date = year + '-' + month + '-' + day
    
    return iso_date


def start_of_week(date, week_offset=0):
    """
    Returns the Start of Week for the date when entered in mm/dd/yyyy format. If a Sunday is entered, returns the Sunday \
    prior. Week offset is the number of weeks in the future (positive number) or past (negative number).
    """
    if type(date) == str:
        dt_date = str_to_object(date)
    else:
        dt_date = date
        
    sow_week_prior = 0
    
    if dt.date.weekday(dt_date) == 0:
        sow_week_prior = dt_date + dt.timedelta(days=-1 + week_offset * 7)
    elif dt.date.weekday(dt_date) == 1:
        sow_week_prior = dt_date + dt.timedelta(days=-2 + week_offset * 7)
    elif dt.date.weekday(dt_date) == 2:
        sow_week_prior = dt_date + dt.timedelta(days=-3 + week_offset * 7)
    elif dt.date.weekday(dt_date) == 3:
        sow_week_prior = dt_date + dt.timedelta(days=-4 + week_offset * 7)
    elif dt.date.weekday(dt_date) == 4:
        sow_week_prior = dt_date + dt.timedelta(days=-5 + week_offset * 7)
    elif dt.date.weekday(dt_date) == 5:
        sow_week_prior = dt_date + dt.timedelta(days=-6 + week_offset * 7)
    elif dt.date.weekday(dt_date) == 6:
        sow_week_prior = dt_date + dt.timedelta(days=-7 + week_offset * 7)
        
    output = object_to_str(sow_week_prior)
    return output


def day_volume(date, unit):
    """Retrieves a single day's volume for a unit. Date must be in mm/dd/yyyy format."""
    
    date = str_to_object(date)

    global cursor

        
    if date >= dt.date(2023, 1, 1):
        if unit == "Experts":
            return day_volume(object_to_str(date), "Sales Experts") + day_volume(object_to_str(date), "Client Service Experts")
        temp_df2 = ODS_volume_df[ODS_volume_df["Date"] == to_iso(object_to_str(date))]
        try:
            output = temp_df2.loc[temp_df2['Dept_Name'] == unit, 'CallsOffered'].values[0]
        except:
            output = 0
        return output
    else:
        temp_df1 = CAH_volume_df.loc[CAH_volume_df["Date"] == to_iso(object_to_str(date))]
        output = temp_df1[unit].sum()
        return output

    

def week_volume(start_of_week, unit):
    """Given a Sunday in mm/dd/yyyy format, retreives volume for that week in dictionary format."""
    start_of_week = str_to_object(start_of_week)
    volume_dict = {}
    for dow in range(7):
        volume = day_volume(object_to_str(start_of_week + pd.Timedelta(days=1 + dow)), unit)
        if volume == 0:
            continue
        volume_dict[object_to_str(start_of_week + pd.Timedelta(days=1 + dow))] = volume
    return volume_dict


def holiday_id(start_of_week):
    """If there is a holiday in the week following a Sunday given in mm/dd/yyyy format, returns the Name, Date, Type, and DOW of the holiday"""
    start_object = str_to_object(start_of_week)
    holiday = False
    for dow in range(7):
        current_date = start_object + pd.Timedelta(days=1 + dow)
        year = object_to_str(current_date)[-4:]
        for date, name in sorted(holidays.US(years=int(year)).items()):
            if date == current_date:
                holiday_name = name
                holiday_date = object_to_str(date)
                holiday_dow = weekday_dict[date.weekday()]
                holiday = True
    if holiday == False:
        return {"Type":"No Holiday"}
    if holiday == True:
        if holiday_name in moving_holidays:
            return {"Name":holiday_name,"Date":holiday_date,"Type":"Moving Holiday","DOW":holiday_dow}
        else:
            return {"Name":holiday_name,"Date":holiday_date,"Type":"Static Holiday","DOW":holiday_dow}
    



#Defines forecasting functions

def week_total_volume_forecast(unit, start_date=start_of_week((object_to_str(today)),1)):
    """Forecasts the total volume for a week given a Sunday start date entered in mm/dd/yyyy format"""

    if str_to_object(start_date) > str_to_object(start_of_week((object_to_str(today)),1)):
        comparison_week = start_of_week((object_to_str(today)),1)
    else:
        comparison_week = start_date

    # Creates a list of prior years' matching weeks for the week being forecasted
    forecast_week_matching_weeks_df = define_matching_weeks_df.copy()
    forecast_week_matching_weeks_list = []
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        forecast_week_matching_weeks_df.pop(forecast_week_matching_weeks_df.columns.values[0])
    current_row = forecast_week_matching_weeks_df.index[forecast_week_matching_weeks_df[start_date[-4:] + " Date"] == start_date][0]
    forecast_week_matching_weeks_list = forecast_week_matching_weeks_df.values[current_row].tolist()

    # Creates a list of prior years' matching weeks for the comparison week (different from the forecast week when forecasting more than 1 week out)
    comparison_week_matching_weeks_df = define_matching_weeks_df.copy()
    comparison_week_matching_weeks_list = []
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        comparison_week_matching_weeks_df.pop(comparison_week_matching_weeks_df.columns.values[0])
    current_row = comparison_week_matching_weeks_df.index[comparison_week_matching_weeks_df[comparison_week[-4:] + " Date"] == comparison_week][0]
    comparison_week_matching_weeks_list = comparison_week_matching_weeks_df.values[current_row].tolist()

    # Creates a dictionary of total year volume for prior years
    yearly_volume_dict = {}
    ODS_yearly_volume_df = ODS_volume_df.groupby(["Dept_Name","Year"]).sum(numeric_only=True).reset_index()
    if unit == "Experts":
        ODS_yearly_volume_df = ODS_yearly_volume_df.loc[(ODS_yearly_volume_df["Dept_Name"] == "Client Service Experts") | (ODS_yearly_volume_df["Dept_Name"] == "Sales Experts")]
    else:
        ODS_yearly_volume_df = ODS_yearly_volume_df.loc[ODS_yearly_volume_df["Dept_Name"] == unit]
    ODS_yearly_volume_df.drop(columns=["Dept_Name"],inplace=True)
    ODS_yearly_volume_df = ODS_volume_df.groupby(["Year"]).sum(numeric_only=True).reset_index()
    ODS_yearly_volume_dict = ODS_yearly_volume_df.set_index("Year").to_dict('dict')
    CAH_yearly_volume_df = CAH_volume_df[["Year", unit]]
    CAH_yearly_volume_df = CAH_yearly_volume_df.groupby(["Year"]).sum()
    CAH_yearly_volume_df = CAH_yearly_volume_df[CAH_yearly_volume_df.index.astype(int)<=2018]
    CAH_yearly_volume_dict = CAH_yearly_volume_df.to_dict('dict')
    for year in CAH_yearly_volume_dict[unit]:
        yearly_volume_dict[year]=CAH_yearly_volume_dict[unit][year]
    for year in ODS_yearly_volume_dict["CallsOffered"]:
        if int(year)>2018:
            yearly_volume_dict[str(year)]=ODS_yearly_volume_dict["CallsOffered"][year]

    # Identifies if the week being forecasted is a moving holiday and, if so, skips to a different function
    holiday = holiday_id(start_date)
    if holiday["Type"] == "Moving Holiday":
        return MH_week_total_volume_forecast(unit, start_date, holiday, yearly_volume_dict, comparison_week, comparison_week_matching_weeks_list)

    # Creates a dictionary of the percent of total year volume for each matching forecast week in prior years
    per_matching_weeks = {}
    for start in forecast_week_matching_weeks_list:
        if start[-4:] not in yearly_volume_dict or start == start_date or yearly_volume_dict[start[-4:]] == 0:
            continue
        per_matching_weeks[start] = sum(week_volume(start,unit).values())/yearly_volume_dict[start[-4:]]

    # Creates a dictionary of the percent of total year volume for the 4 weeks prior to each matching comparison week in prior years
    last_4_percent_of_year = {}
    for start in comparison_week_matching_weeks_list:
        if start[-4:] not in yearly_volume_dict or start == comparison_week or yearly_volume_dict[start[-4:]] == 0:
            continue
        year = start[-4:]
        last_4_percent_of_year[year] = sum(week_volume(start_of_week(start,-1),unit).values())/yearly_volume_dict[year] + sum(week_volume(start_of_week(start,-2),unit).values())/yearly_volume_dict[year] +\
                                           sum(week_volume(start_of_week(start,-3),unit).values())/yearly_volume_dict[year] + sum(week_volume(start_of_week(start,-4),unit).values())/yearly_volume_dict[year]

    # Eliminates outliers in per_matching_weeks and calculates average
    count = 0
    total = 0
    values = []
    for start in per_matching_weeks:
        values.append(list(per_matching_weeks.values()))
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    upper = Q3 + 1.5 * IQR
    lower = Q1 - 1.5 * IQR
    for start in list(per_matching_weeks):
        if per_matching_weeks[start] >= upper:
            del per_matching_weeks[start]
        elif per_matching_weeks[start] <= lower:
            del per_matching_weeks[start]
        else:
            count += 1
            total += per_matching_weeks[start]
    avg_per_matching_weeks = total/count

    # Eliminates outliers in last_4_percent_of_year and calculates average
    count = 0
    total = 0
    values = []
    for year in last_4_percent_of_year:
        values.append(list(last_4_percent_of_year.values()))
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    upper = Q3 + 1.5 * IQR
    lower = Q1 - 1.5 * IQR
    for year in list(last_4_percent_of_year):
        if last_4_percent_of_year[year] >= upper:
            del last_4_percent_of_year[year]
        elif last_4_percent_of_year[year] <= lower:
            del last_4_percent_of_year[year]
        else:
            count += 1
            total += last_4_percent_of_year[year]
    avg_last_4_percent_of_year = total/count

    # Forecasts the current year's total volume
    year_forecast = round((1 / avg_last_4_percent_of_year) * (sum(week_volume(start_of_week(comparison_week,-1),unit).values()) + sum(week_volume(start_of_week(comparison_week,-2),unit).values()) +\
                                           sum(week_volume(start_of_week(comparison_week,-3),unit).values()) + sum(week_volume(start_of_week(comparison_week,-4),unit).values())))

    # Forecasts week volume using method 1 (average % of year for historical matching weeks)
    week_forecast_m1 = year_forecast * avg_per_matching_weeks

    # Creates a dictionary of the historical change from the comparison week prior to the forecast matching week
    week_over_week_change = {}
    for forecast_start,comparison_start in zip(forecast_week_matching_weeks_list, comparison_week_matching_weeks_list):
        if forecast_start[-4:] not in yearly_volume_dict or forecast_start == start_date or comparison_start[-4:] > comparison_week[-4:] or sum(week_volume(comparison_start,unit).values()) == 0:
            continue
        week_over_week_change[forecast_start[-4:]] = sum(week_volume(start_of_week(forecast_start,-1),unit).values()) / sum(week_volume(comparison_start,unit).values())

    # Eliminates outliers in week_over_week_change and calculates average
    count = 0
    total = 0
    values = []
    for year in week_over_week_change:
        values.append(list(week_over_week_change.values()))
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    upper = Q3 + 1.5 * IQR
    lower = Q1 - 1.5 * IQR
    for year in list(week_over_week_change):
        if week_over_week_change[year] >= upper:
            del week_over_week_change[year]
        elif week_over_week_change[year] <= lower:
            del week_over_week_change[year]
        else:
            count += 1
            total += week_over_week_change[year]
    avg_week_over_week_change = total/count

    # Forecasts week volume using method 2 (average week over week change from prior years)
    week_forecast_m2 = sum(week_volume(start_of_week(comparison_week,-1),unit).values()) * avg_week_over_week_change

    # Creates a dictionary of the historical change from the last 4 comparison weeks to the matching forecast week
    four_week_over_week_change = {}
    for forecast_start,comparison_start in zip(forecast_week_matching_weeks_list, comparison_week_matching_weeks_list):
        if forecast_start[-4:] not in yearly_volume_dict or forecast_start == start_date or comparison_start[-4:] > comparison_week[-4:] or sum(week_volume(comparison_start,unit).values()) == 0:
            continue
        four_week_over_week_change[forecast_start[-4:]] = (sum(week_volume(start_of_week(forecast_start,-1),unit).values()) + sum(week_volume(start_of_week(forecast_start,-2),unit).values())\
             + sum(week_volume(start_of_week(forecast_start,-3),unit).values()) + sum(week_volume(start_of_week(forecast_start,-4),unit).values())) / 4 / sum(week_volume(comparison_start,unit).values())
        
    # Eliminates outliers in four_week_over_week_change and calculates average
    count = 0
    total = 0
    values = []
    for year in four_week_over_week_change:
        values.append(list(four_week_over_week_change.values()))
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    upper = Q3 + 1.5 * IQR
    lower = Q1 - 1.5 * IQR
    for year in list(four_week_over_week_change):
        if four_week_over_week_change[year] >= upper:
            del four_week_over_week_change[year]
        elif four_week_over_week_change[year] <= lower:
            del four_week_over_week_change[year]
        else:
            count += 1
            total += four_week_over_week_change[year]
    avg_four_week_over_week_change = total/count

    # Forecasts week volume using method 3 (average 4 week over week change from prior years)
    week_forecast_m3 = ((sum(week_volume(start_of_week(comparison_week,-1),unit).values()) + sum(week_volume(start_of_week(comparison_week,-2),unit).values()) +\
                        sum(week_volume(start_of_week(comparison_week,-3),unit).values()) + sum(week_volume(start_of_week(comparison_week,-4),unit).values())) / 4) * avg_four_week_over_week_change
    
    output = {1: week_forecast_m1, 2: week_forecast_m2, 3: week_forecast_m3}

    return output


def MH_week_total_volume_forecast(unit, start_date, holiday, yearly_volume_dict, comparison_week, comparison_week_matching_weeks_list):
    """Forecasts the total volume for a week when it is a moving holiday by averaging the week of year volume % for each year where the holiday fell on the same dow"""
    total_matching_week_volume = {}
    for year in range(2012,int(holiday["Date"][-4:])):
        if str(year) not in yearly_volume_dict:
            continue
        for date, name in sorted(holidays.US(years=int(year)).items()):
            if name == holiday["Name"]:
                dow = weekday_dict[date.weekday()]
                if dow == holiday["DOW"]:
                    total_matching_week_volume[start_of_week(object_to_str(date))]=sum(week_volume(start_of_week(object_to_str(date)),unit).values())
    matching_weeks_per_dict = {}
    for start in total_matching_week_volume:
        matching_weeks_per_dict[start[-4:]]=total_matching_week_volume[start]/yearly_volume_dict[start[-4:]]
    total = 0
    count = 0
    for year in matching_weeks_per_dict:
        total += matching_weeks_per_dict[year]
        count += 1
    avg_week_of_year_per = total/count
    #Rest of the below is copied from method 1
    # Creates a dictionary of the percent of total year volume for the 4 weeks prior to each matching comparison week in prior years
    last_4_percent_of_year = {}
    for start in comparison_week_matching_weeks_list:
        if start[-4:] not in yearly_volume_dict or start == comparison_week:
            continue
        year = start[-4:]
        last_4_percent_of_year[year] = sum(week_volume(start_of_week(start,-1),unit).values())/yearly_volume_dict[year] + sum(week_volume(start_of_week(start,-2),unit).values())/yearly_volume_dict[year] +\
                                           sum(week_volume(start_of_week(start,-3),unit).values())/yearly_volume_dict[year] + sum(week_volume(start_of_week(start,-4),unit).values())/yearly_volume_dict[year]

    # Eliminates outliers in last_4_percent_of_year and calculates average
    count = 0
    total = 0
    values = []
    for year in last_4_percent_of_year:
        values.append(list(last_4_percent_of_year.values()))
    Q1 = np.percentile(values, 25)
    Q3 = np.percentile(values, 75)
    IQR = Q3 - Q1
    upper = Q3 + 1.5 * IQR
    lower = Q1 - 1.5 * IQR
    for year in list(last_4_percent_of_year):
        if last_4_percent_of_year[year] >= upper:
            del last_4_percent_of_year[year]
        elif last_4_percent_of_year[year] <= lower:
            del last_4_percent_of_year[year]
        else:
            count += 1
            total += last_4_percent_of_year[year]
    avg_last_4_percent_of_year = total/count

    # Forecasts the current year's total volume
    year_forecast = round((1 / avg_last_4_percent_of_year) * (sum(week_volume(start_of_week(comparison_week,-1),unit).values()) + sum(week_volume(start_of_week(comparison_week,-2),unit).values()) +\
                                           sum(week_volume(start_of_week(comparison_week,-3),unit).values()) + sum(week_volume(start_of_week(comparison_week,-4),unit).values())))

    # Forecasts week volume using method 1 (average % of year for historical matching weeks)
    forecast = year_forecast * avg_week_of_year_per
    return forecast


    

def tactical_volume_forecast(unit, start_date=start_of_week((object_to_str(today)),1)):
    """Forecasts the weekly volume for the next 3 weeks"""
    accuracy_dict = {}
    num_training_weeks = 1
    holiday_avoidance_offset = 0
    while num_training_weeks <= 5:
        current_week = object_to_str(str_to_object(start_date) - dt.timedelta(weeks=num_training_weeks+holiday_avoidance_offset+1))
        if holiday_id(current_week)["Type"] != "No Holiday":
            holiday_avoidance_offset += 1
            continue
        accuracy_dict[current_week]=week_total_volume_forecast(unit, current_week)
        accuracy_dict[current_week]["Actual"]=sum(week_volume(current_week,unit).values())
        num_training_weeks += 1
    df=pd.DataFrame(accuracy_dict)
    df=df.transpose()
    X = df[[1,2,3]]
    y = df['Actual']
    elastic = ElasticNetCV()
    elastic.fit(X,y)
    forecast = {}
    predict = {}
    num_forecasting_weeks = 0
    while num_forecasting_weeks < 3:
        current_week = object_to_str(str_to_object(start_date) + dt.timedelta(weeks=num_forecasting_weeks))
        forecast = week_total_volume_forecast(unit, current_week)
        if holiday_id(current_week)["Type"] == "Moving Holiday":
            predict[current_week] = forecast
        else:
            predict[current_week] = int(elastic.predict([[forecast[1],forecast[2],forecast[3]]]).item())
        num_forecasting_weeks +=1
    final_answer={"unit":unit}
    for start in predict:
        dow_forecast = dow_per_volume_forecast(unit,start)
        final_answer[start]={}
        for dow in dow_forecast:
            final_answer[start][dow] = round(dow_forecast[dow] * predict[start])
    return final_answer


def dow_per_volume_forecast(unit, start_date=start_of_week((object_to_str(today)),1)):
    """Forecasts the day of week percents using the average of recent prior weeks for a week given a Sunday start date entered in mm/dd/yyyy format"""
    if str_to_object(start_date) > str_to_object(start_of_week((object_to_str(today)),1)):
        comparison_week = start_of_week((object_to_str(today)),1)
    else:
        comparison_week = start_date
    if holiday_id(start_date)["Type"] != "No Holiday":
        return holiday_dow_per_volume_forecast(unit, start_date)
    dow_per_dict = {}
    num_training_weeks = 1
    holiday_avoidance_offset = 0
    while num_training_weeks <= 4:
        current_week = object_to_str(str_to_object(comparison_week) - dt.timedelta(weeks=num_training_weeks+holiday_avoidance_offset+1))
        if holiday_id(current_week)["Type"] != "No Holiday":
            holiday_avoidance_offset += 1
            continue
        dow_per_dict[current_week]={}
        for dow in range (0,7):
            dow_per_dict[current_week][dow]=day_volume(object_to_str(str_to_object(current_week) + dt.timedelta(days=dow+1)),unit)/sum(week_volume(current_week, unit).values())
        num_training_weeks += 1
    avg_dow_per = {}
    for dow in range (0,7):
        total = 0
        count = 0
        for start in dow_per_dict:
            total += dow_per_dict[start][dow]
            count += 1
        avg_dow_per[weekday_dict[dow]]=total/count
    return avg_dow_per

def holiday_dow_per_volume_forecast(unit, start_date):
    """Forecasts the day of week percents using the average of prior years where the holiday fell on the same dow"""
    holiday_info = holiday_id(start_date)
    total_matching_week_volume = {}
    for year in range(2012,int(holiday_info["Date"][-4:])):
        for date, name in sorted(holidays.US(years=int(year)).items()):
            if name == holiday_info["Name"]:
                dow = weekday_dict[date.weekday()]
                if dow == holiday_info["DOW"]:
                    if sum(week_volume(start_of_week(object_to_str(date)),unit).values()) == 0:
                        continue
                    total_matching_week_volume[start_of_week(object_to_str(date))]=sum(week_volume(start_of_week(object_to_str(date)),unit).values())
    dow_per_dict = {}
    for start in total_matching_week_volume:
        dow_per_dict[start]={}
        for dow in range (0,7):
            dow_per_dict[start][dow]=day_volume(object_to_str(str_to_object(start) + dt.timedelta(days=dow+1)),unit)/total_matching_week_volume[start]
    avg_dow_per = {}
    for dow in range (0,7):
        total = 0
        count = 0
        for start in dow_per_dict:
            total += dow_per_dict[start][dow]
            count += 1
        avg_dow_per[weekday_dict[dow]]=total/count
    return avg_dow_per

