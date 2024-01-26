
# Imports
import pandas as pd
import datetime as dt
import numpy as np
import pyodbc
import copy
import holidays
from sklearn.linear_model import ElasticNet, ElasticNetCV
from sklearn.linear_model import LinearRegression
from sklearn.datasets import make_regression

# Connect to DB
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
cursor = conn.cursor()


# Global variable definitions
today = dt.date.today()
current_year = str(dt.date.today())[:4]
weekday_dict = {0:"Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday", 5:"Saturday", 6:"Sunday"}
moving_holidays = ["New Year's Day", "Juneteenth National Independence Day", "Independence Day", "Veterans Day", "Christmas Day"]
static_holidays= ["Martin Luther King Jr. Day", "Memorial Day", "Labor Day", "Columbus Day", "Thanksgiving"]
data_dict = None
data_date = None
data_ = None
data_unit = None
yearly_volume_dict = {}
master_volume_dict = {}
queries_saved = 0

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
cisco_sql = """
Select cti.DateTime, cti.PrecisionQueueID, cti.ODSDataSourceID, dpq.EnterpriseName, dpq.Dept_Name, CallsOfferedRouted + CallsRequeried as CallsOffered
From AcqCiscoAW.Call_Type_SG_Interval cti
left join ArcCiscoAW.V_CallDataCisco_Dim_Precision_Queue dpq
	on REPLACE(dpq.PrecisionQueueID, '~', '') = concat(cti.PrecisionQueueID, ODSDataSourceID)
Where cast(cti.DateTime as time) > '07:00:01'
And cast(cti.Datetime as time) < '21:59:59'
And cti.DateTime > ?
And cti.DateTime < ?
"""

cisco_year_sql = '''Select sum(CallsOfferedRouted + CallsRequeried) as CallsOffered
  From AcqCiscoAW.Call_Type_SG_Interval cti
  left join ArcCiscoAW.V_CallDataCisco_Dim_Precision_Queue dpq
  on REPLACE(dpq.PrecisionQueueID, '~', '') = concat(cti.PrecisionQueueID, ODSDataSourceID)
  Where cast(cti.DateTime as date) > '2018-02-28'
  And cast(cti.DateTime as time) > '07:00:01'
  And cast(cti.Datetime as time) < '21:59:59'
  and datepart(year, cast(cti.Datetime as date)) = ?
  And dpq.Dept_Name = ?
  '''

cisco_by_date_sql ="""
Select cti.DateTime as Date, CallsOfferedRouted + CallsRequeried as CallsOffered
From AcqCiscoAW.Call_Type_SG_Interval cti
left join ArcCiscoAW.V_CallData Cisco_Dim_Precision_Queue dpq
	on REPLACE(dpq.PrecisionQueueID, '~', '') = concat(cti.PrecisionQueueID, ODSDataSourceID)
Where cast(cti.DateTime as time) > '07:00:01'
And cast(cti.Datetime as time) < '21:59:59'
And cti.DateTime > '2018-02-28'
And dpq.Dept_Name = ?
"""

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
print("Fetching data from ODS database...")
cursor.execute(cisco_all_sql)
results = cursor.fetchall()
ODS_volume_df = pd.DataFrame.from_records(results, columns=[col[0] for col in cursor.description])

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

    global data_unit
    global master_volume_dict
    global queries_saved
    global cursor

    if unit != data_unit:
        data_unit = unit
        master_volume_dict = {}
    else:
        if date in master_volume_dict:
            queries_saved += 1
            return master_volume_dict[date]

        
    if date >= dt.date(2023, 1, 1):
        if unit == "Experts":
            return day_volume(object_to_str(date), "Sales Experts") + day_volume(object_to_str(date), "Client Service Experts")
        temp_df2 = ODS_volume_df[ODS_volume_df["Date"] == to_iso(object_to_str(date))]
        try:
            output = temp_df2.loc[temp_df2['Dept_Name'] == unit, 'CallsOffered'].values[0]
        except:
            output = 0
        master_volume_dict[date] = output
        return output
    else:
        temp_df1 = CAH_volume_df.loc[CAH_volume_df["Date"] == to_iso(object_to_str(date))]
        output = temp_df1[unit].sum()
        master_volume_dict[date] = output
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


def dataframe(unit):
    """Returns a dataframe with all daily volume data for a unit."""
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
    cursor = conn.cursor()
    cursor.execute(cisco_by_date_sql, unit)
    term_list = cursor.fetchall()
    df = pd.DataFrame.from_records(term_list, columns=[col[0] for col in cursor.description])
    df["Date"] = df["Date"].dt.date
    df = df.groupby(["Date"], as_index=False).sum()
    df.rename(columns={"Date": "ds", "CallsOffered": "y"}, inplace=True)
    return df


def total_volume_in_range(start_date, end_date, unit):
    """Given start and end dates in mm/dd/yyyy format, returns total volume in that range. (end date excluded) (currently only works for dates after 3/1/2018)"""
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
    cursor = conn.cursor()
    if unit == "Experts":
        return total_volume_in_range(start_date, end_date, "Sales Experts") + total_volume_in_range(start_date, end_date, "Client Service Experts")
    
    start_date = to_iso(start_date)
    end_date = to_iso(end_date)
    cursor.execute(cisco_sql, start_date + ' 00:00:00', end_date + ' 00:00:00')
    term_list = cursor.fetchall()
    df = pd.DataFrame.from_records(term_list, columns=[col[0] for col in cursor.description])
    return int(df.loc[df["Dept_Name"] == unit, "CallsOffered"].sum())



def yearly_volume(year, unit):
    """Given a year entered as an interval, returns the total volume for that year."""
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
    cursor = conn.cursor()

    if year <= 2023:
        temp_df1 = CAH_volume_df.loc[CAH_volume_df["Year"] == str(year)]
        return temp_df1[unit].sum()
    else:
        cursor.execute(cisco_year_sql, str(year), unit)
        term_list = cursor.fetchall()
        df = pd.DataFrame.from_records(term_list, columns=[col[0] for col in cursor.description])
        return df.CallsOffered.sum()


def gather_data(unit, start_date=start_of_week((object_to_str(today)),1)):
    """Gathers relevant historical data to complete methods 1-4"""

    global data_date
    global data_dict
    global data_unit
    global yearly_volume_dict
    global master_volume_dict

    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
    cursor = conn.cursor()

    if data_dict == None or data_date != start_date:

        print ("Gathering relevant historical data...")

        data_dict = {}
        data_date = start_date
        
        
        data_dict["yearly_volume_dict"] = {} #Dictionary of each year's total volume
        data_dict["working_matching_weeks_list"] = []  #Lists the start_date and the start of all matching weeks for prior years
        data_dict["current_matching_weeks_list"] = [] #Lists the start of last week and all matching weeks for prior years (used for year_output when start_date is in the future)
        data_dict["matching_weeks_total_dict"] = {} #Dictionary of the total volume for the matching weeks of each year
        data_dict["matching_weeks_per_dict"] = {} #Dictionary of the percent of total year volume for the matching weeks of each year
        data_dict["dow_per_dict"] = {} #Dictionary of the dow % for each day of week for the matching weeks of each year
        data_dict["last_4_matching_weeks_total_dict"] = {} #Dictionary of the total volume for the prior 4 matching weeks of each year
        data_dict["last_4_matching_weeks_per_dict"] = {} #Dictionary of the percentage of total year volume for the prior 4 matching weeks of each year
        data_dict["week_to_compare_historical_volume"] = {} #Dictionary of total volume for each prior year matching the comparison week.
        data_dict["working_week_historical_volume"] = {} #Dictionary of total volume for each prior year matching the forecast week.
        data_dict["wow_list"] = [] #List of % change between prior years' comparison and working weeks.
        data_dict["week_prior_volume"] = {} #Dictionary of volume by dow for the comparison week.
        data_dict["week_prior_total_volume"] = 0 #Sum of prior week's total volume.
        data_dict["per_last_4_matching_volume"] = {} #Dictionary of the percentage of volume received in the last 4 comparison weeks of prior years.
        data_dict["per_last_4_matching_volume"] = {} #Dictionary of the percentage of volume received in the forecast week of prior years.
        data_dict["past_years_holiday_dow"] = {} #When there is a moving holiday, dictionary of the dow that holiday fell on each year
        data_dict["holiday_name"] = None #The name of the weeks' moving holiday, if there is one
        data_dict["working_year"] = start_date[-4:]
        data_dict["last_4_current_year_total"] = 0 #Total # calls received in the last 4 weeks.

        if start_date < start_of_week((object_to_str(today)),1):
            week_to_compare = start_of_week(start_date, -1)
        else:
            week_to_compare = start_of_week(object_to_str(today), -1)

        comparison_year = week_to_compare[-4:]

        data_dict["last_4_current_year_volume"] = sum(week_volume(week_to_compare, unit).values()) + sum(week_volume(start_of_week(week_to_compare), unit).values())\
        + sum(week_volume(start_of_week(week_to_compare, -1), unit).values()) + sum(week_volume(start_of_week(week_to_compare, -2), unit).values())

        # Filling in working_matching_weeks_list
        working_week_matching_weeks_df = define_matching_weeks_df.copy()
        for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
            working_week_matching_weeks_df.pop(working_week_matching_weeks_df.columns.values[0])
        working_row = working_week_matching_weeks_df.index[working_week_matching_weeks_df[data_dict["working_year"] + " Date"] == start_date][0]
        data_dict["working_matching_weeks_list"] = working_week_matching_weeks_df.values[working_row].tolist()

        # Filling in current_matching_weeks_list
        current_week_matching_weeks_df = define_matching_weeks_df.copy()
        for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
            current_week_matching_weeks_df.pop(current_week_matching_weeks_df.columns.values[0])
        current_row = current_week_matching_weeks_df.index[current_week_matching_weeks_df[current_year + " Date"] == start_of_week((object_to_str(today)),1)][0]
        data_dict["current_matching_weeks_list"] = current_week_matching_weeks_df.values[current_row].tolist()

        # Filling in comparison_weeks (intermediary)
        comparison_week_matching_weeks_df = define_matching_weeks_df.copy()
        for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
            comparison_week_matching_weeks_df.pop(comparison_week_matching_weeks_df.columns.values[0])
        comparison_row = comparison_week_matching_weeks_df.index[comparison_week_matching_weeks_df[comparison_year + " Date"] == week_to_compare][0]
        comparison_weeks = comparison_week_matching_weeks_df.values[comparison_row].tolist()
        for week in list(comparison_weeks):
            if int(week[-4:]) >= int(comparison_year):
                comparison_weeks.remove(week)

        # Filling in yearly_volume_dict
        if yearly_volume_dict == {} or data_unit != unit:
            for year in range(2012, int(object_to_str(today)[-4:])+1):
                data_dict["yearly_volume_dict"][year] = yearly_volume(year, unit)
                yearly_volume_dict = data_dict["yearly_volume_dict"]
        else:
            data_dict["yearly_volume_dict"] = yearly_volume_dict

        # Filling in last_4_matching_weeks_total_dict 
        if start_date < start_of_week(object_to_str(today)):
            for start in data_dict["working_matching_weeks_list"]:
                year_current = start[-4:]
                data_dict["last_4_matching_weeks_total_dict"][year_current] = {start_of_week(start,-1): sum(week_volume(start_of_week(start,-1), unit).values()),
                start_of_week(start,-2): sum(week_volume(start_of_week(start,-2), unit).values()), 
                start_of_week(start,-3): sum(week_volume(start_of_week(start,-3), unit).values()),
                start_of_week(start,-4): sum(week_volume(start_of_week(start,-4), unit).values())}
        else:
            for start in data_dict["current_matching_weeks_list"]:
                year_current = start[-4:]
                data_dict["last_4_matching_weeks_total_dict"][year_current] = {start_of_week(start,-1): sum(week_volume(start_of_week(start,-1), unit).values()),
                start_of_week(start,-2): sum(week_volume(start_of_week(start,-2), unit).values()), 
                start_of_week(start,-3): sum(week_volume(start_of_week(start,-3), unit).values()),
                start_of_week(start,-4): sum(week_volume(start_of_week(start,-4), unit).values())}

        # Filling in last_4_matching_weeks_per_dict
        for year in data_dict["last_4_matching_weeks_total_dict"]:
            try:
                year_int = int(year)
                if data_dict["yearly_volume_dict"][year_int] == 0:
                    continue
                data_dict["last_4_matching_weeks_per_dict"][year] = sum(data_dict["last_4_matching_weeks_total_dict"][year].values()) / data_dict["yearly_volume_dict"][year_int]
            except:
                continue

        # Filling in data_dict["matching_weeks_total_dict"]
        for start in data_dict["working_matching_weeks_list"]:
            year_working = start[-4:]
            data_dict["matching_weeks_total_dict"][year_working] = sum(week_volume(start_of_week(start, 1), unit).values())

        # Filling in data_dict["matching_weeks_per_dict"]
        for year in data_dict["matching_weeks_total_dict"]:
            if int(year) not in data_dict["yearly_volume_dict"]:
                continue
            elif data_dict["yearly_volume_dict"][int(year)] == 0:
                continue
            year_int = int(year)
            data_dict["matching_weeks_per_dict"][year] = data_dict["matching_weeks_total_dict"][year] / data_dict["yearly_volume_dict"][year_int]

        # Filling in data_dict["dow_per_dict"]
        for start in data_dict["working_matching_weeks_list"]:
            year_current = start[-4:]
            if int(year_current) >= int(data_dict["working_year"]):
                continue
            if year_current not in data_dict["matching_weeks_total_dict"] or data_dict["matching_weeks_total_dict"][year_current] == 0:
                continue
            if year_current not in data_dict["dow_per_dict"]:
                data_dict["dow_per_dict"][year_current] = {}
            for dow in range(0, 7):
                date_current = object_to_str(str_to_object(start) + dt.timedelta(days=dow + 1))
                dow_current = weekday_dict[dow]
                date_current_volume = day_volume(date_current, unit)
                data_dict["dow_per_dict"][year_current][dow_current] = date_current_volume / data_dict["matching_weeks_total_dict"][year_current]


        # Filling in data_dict["week_to_compare_historical_volume"]
        for week in comparison_weeks:
            data_dict["week_to_compare_historical_volume"][week] = {}
            current_week = week_volume(week, unit)
            for day in current_week:
                for dow in weekday_dict.keys():
                    if str_to_object(day).weekday() == dow:
                        data_dict["week_to_compare_historical_volume"][week][weekday_dict[dow]] = current_week[day]

        # Filling in data_dict["working_week_historical_volume"]
        for week in data_dict["working_matching_weeks_list"]:
            data_dict["working_week_historical_volume"][week] = {}
            current_week = week_volume(week, unit)
            for day in current_week:
                for dow in weekday_dict.keys():
                    if str_to_object(day).weekday() == dow:
                        data_dict["working_week_historical_volume"][week][weekday_dict[dow]] = current_week[day]

        # Filling in data_dict["wow_list"]
        for start in data_dict["working_week_historical_volume"]:
            temp_working_year = start[-4:]
            for start2 in data_dict["week_to_compare_historical_volume"]:
                temp_comparison_year = start2[-4:]
                if temp_comparison_year == temp_working_year:
                    data_dict["wow_list"].append(sum(data_dict["working_week_historical_volume"][start].values()) / sum(data_dict["week_to_compare_historical_volume"][start2].values()))

        # Filling in data_dict["week_prior_volume"]
        data_dict["week_prior_volume"] = week_volume(week_to_compare, unit)

        # Filling in data_dict["week_prior_total_volume"]
        data_dict["week_prior_total_volume"] = sum(data_dict["week_prior_volume"].values())

        # Filling in data_dict["per_last_4_matching_volume"]
        for week in comparison_weeks:
            year = week[-4:]
            if int(year) not in data_dict["yearly_volume_dict"]:
                continue
            data_dict["per_last_4_matching_volume"][week[-4:]] = (sum(week_volume(week, unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])]) + (sum(week_volume(start_of_week(week), unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])])\
            + (sum(week_volume(start_of_week(week, -1), unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])]) + (sum(week_volume(start_of_week(week, -2), unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])])

        # Filling in data_dict["per_last_4_matching_volume"]
        for week in data_dict["working_matching_weeks_list"]:
            year = week[-4:]
            if int(year) not in data_dict["yearly_volume_dict"]:
                continue
            data_dict["per_last_4_matching_volume"][week[-4:]] = (sum(week_volume(week, unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])]) + (sum(week_volume(start_of_week(week), unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])])\
            + (sum(week_volume(start_of_week(week, -1), unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])]) + (sum(week_volume(start_of_week(week, -2), unit).values()) / data_dict["yearly_volume_dict"][int(week[-4:])])
            
        # Filling in data_dict["past_years_holiday_dow"]
        data_dict["holiday_name"] = None
        week_df = define_matching_weeks_df[define_matching_weeks_df.isin([start_date]).any(axis=1)]
        if week_df['Holiday'].isin(moving_holidays).any():
            data_dict["holiday"] = "Moving holiday"
        elif week_df['Holiday'].isin(static_holidays).any():
            data_dict["holiday"] = "Static holiday"
        else:
            data_dict["holiday"] = "No holiday"
        
        if data_dict["holiday"] == "Moving holiday":
            week_list = [str_to_object(start_date)+dt.timedelta(days=1), str_to_object(start_date)+dt.timedelta(days=2), str_to_object(start_date)+dt.timedelta(days=3),\
                     str_to_object(start_date)+dt.timedelta(days=4), str_to_object(start_date)+dt.timedelta(days=5), str_to_object(start_date)+dt.timedelta(days=6),\
                     str_to_object(start_date)+dt.timedelta(days=7)]
            data_dict["holiday_name"] = week_df['Holiday'].item()
            for object_day in week_list:
                for date, name in sorted(holidays.US(years=int(object_to_str(object_day)[-4:])).items()):
                    if name == data_dict["holiday_name"]:
                        holiday_date = date
                    else:
                        continue
            for year in range(2012,int(current_year)):
                for date, name in sorted(holidays.US(years=year).items()):
                    if name == data_dict["holiday_name"]:
                        data_dict["past_years_holiday_dow"][start_of_week(object_to_str(date))[-4:]] = date.weekday()
                    else:
                        continue
            for year in data_dict["past_years_holiday_dow"]:
                if year not in data_dict["matching_weeks_per_dict"]:
                    continue
                else:
                    if data_dict["past_years_holiday_dow"][year] != holiday_date.weekday():
                        del data_dict["matching_weeks_per_dict"][year]

        # Filling in data_dict["last_4_current_year_total"]
        week_offset = -4
        while week_offset < 0:
            data_dict["last_4_current_year_total"] += sum(week_volume(start_of_week(start_date,week_offset), unit).values())
            week_offset += 1

        return data_dict
    
    else:
        return data_dict
    



def method_1(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 1, forecasting yearly volume using the last 4 weeks method.
    """

    data = gather_data(unit, start_date)

    print("Running Method 1 for week of " + start_date)

    ### DELETING KEYS IN DICTS WHERE THE VALUE IS 0 ###

    for year in list(data["yearly_volume_dict"]):
        if data["yearly_volume_dict"][year] == 0:
            del data["yearly_volume_dict"][year]
            
    for year in list(data["matching_weeks_total_dict"]):
        if data["matching_weeks_total_dict"][year] == 0:
            del data["matching_weeks_total_dict"][year]


      
    ### REMOVING OUTLIERS AND CALCULATING END VALUES ###
    outliers_excluded = 0

    count_last_4_per = 0
    total_last_4_per = 0
    year_per_values = []
    for year in data["last_4_matching_weeks_per_dict"]:
        year_per_values.append(list(data["last_4_matching_weeks_per_dict"].values()))
    year_per_Q1 = np.percentile(year_per_values, 25)
    year_per_Q3 = np.percentile(year_per_values, 75)
    year_per_IQR = year_per_Q3 - year_per_Q1
    year_per_upper = year_per_Q3 + 1.5 * year_per_IQR
    year_per_lower = year_per_Q1 - 1.5 * year_per_IQR
    for year in list(data["last_4_matching_weeks_per_dict"]):
        if data["last_4_matching_weeks_per_dict"][year] >= year_per_upper:
            del data["last_4_matching_weeks_per_dict"][year]
            outliers_excluded += 1
        elif data["last_4_matching_weeks_per_dict"][year] <= year_per_lower:
            del data["last_4_matching_weeks_per_dict"][year]
            outliers_excluded += 1
    for year in data["last_4_matching_weeks_per_dict"]:
        if int(year) >= int(data["working_year"]):
            continue
        count_last_4_per += 1
        total_last_4_per += data["last_4_matching_weeks_per_dict"][year]
    avg_per_last_4 = total_last_4_per / count_last_4_per
    year_output = round((1 / avg_per_last_4) * data["last_4_current_year_total"])
    
    if data["holiday"] != "Moving holiday":
        woy_values = []
        for year in data["matching_weeks_per_dict"]:
            woy_values.append(list(data["matching_weeks_per_dict"].values()))
        woy_Q1 = np.percentile(woy_values, 25)
        woy_Q3 = np.percentile(woy_values, 75)
        woy_IQR = woy_Q3 - woy_Q1
        woy_upper = woy_Q3 + 1.5 * woy_IQR
        woy_lower = woy_Q1 - 1.5 * woy_IQR
        for year in list(data["matching_weeks_per_dict"]):
            if data["matching_weeks_per_dict"][year] >= woy_upper:
                del data["matching_weeks_per_dict"][year]
                outliers_excluded += 1
            elif data["matching_weeks_per_dict"][year] <= woy_lower:
                del data["matching_weeks_per_dict"][year]
                outliers_excluded += 1
    else:
        holiday_date = None
        for dow in range(0, 7):
                date_testing = object_to_str(str_to_object(start_date) + dt.timedelta(days=dow + 1))
                year_testing = date_testing[-4:]
                for date, name in sorted(holidays.US(years=int(year_testing[-4:])).items()):
                    if date == str_to_object(date_testing):
                        holiday_date = date_testing
                    else:
                        continue
        for year in data["past_years_holiday_dow"]:
            if year not in data["matching_weeks_per_dict"]:
                continue
            elif data["past_years_holiday_dow"][year] != str_to_object(holiday_date).weekday():
                del data["matching_weeks_per_dict"][year]
        for year in list(data["matching_weeks_per_dict"]):
            if int(year) >= int(start_date[-4:]):
                del data["matching_weeks_per_dict"][year]
    
    total_woy_per = 0
    count_woy_per = 0
    for year in data["matching_weeks_per_dict"]:
            if int(year) >= int(data["working_year"]):
                continue
            else:
                total_woy_per += data["matching_weeks_per_dict"][year]
                count_woy_per += 1
    avg_woy_per = total_woy_per / count_woy_per
    woy_output = round(avg_woy_per * year_output)



    
    if data["holiday"] != "Moving holiday":
        dow_buckets = {}
        for year in data["dow_per_dict"]:
            for dow in data["dow_per_dict"][year]:
                if dow not in dow_buckets:
                    dow_buckets[dow] = {}
                dow_buckets[dow][year] = data["dow_per_dict"][year][dow]


        for dow in dow_buckets:
            dow_values = []
            for year in dow_buckets[dow]:
                dow_values.append(list(dow_buckets[dow].values()))

            dow_Q1 = np.percentile(dow_values, 25)
            dow_Q3 = np.percentile(dow_values, 75)
            dow_IQR = dow_Q3 - dow_Q1
            dow_upper = dow_Q3 + 1.5 * dow_IQR
            dow_lower = dow_Q1 - 1.5 * dow_IQR

            for year in list(data["dow_per_dict"]):
                for dow2 in list(data["dow_per_dict"][year]):
                    if dow2 == dow and data["dow_per_dict"][year][dow] >= dow_upper:
                        del data["dow_per_dict"][year][dow]
                        outliers_excluded += 1
                    elif dow2 == dow and data["dow_per_dict"][year][dow] <= dow_lower:
                        del data["dow_per_dict"][year][dow]
                        outliers_excluded += 1
    else:
        holiday_date = None
        for dow in range(0, 7):
                date_testing = object_to_str(str_to_object(start_date) + dt.timedelta(days=dow + 1))
                year_testing = date_testing[-4:]
                for date, name in sorted(holidays.US(years=int(year_testing[-4:])).items()):
                    if date == str_to_object(date_testing):
                        holiday_date = date_testing
                    else:
                        continue
        for year in data["past_years_holiday_dow"]:
            if year not in data["dow_per_dict"]:
                continue
            elif data["past_years_holiday_dow"][year] != str_to_object(holiday_date).weekday():
                del data["dow_per_dict"][year]
        for year in list(data["dow_per_dict"]):
            if int(year) >= int(start_date[-4:]):
                del data["dow_per_dict"][year]
                    
    
    avg_dow_per = {"Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0, "Friday": 0, "Saturday": 0, "Sunday": 0}
    
    for dow in avg_dow_per.keys():
        count_dow_per = 0
        total_dow_per = 0
        for year in data["dow_per_dict"]:
            for dow2 in data["dow_per_dict"][year]:
                if dow2 == dow:
                    total_dow_per += data["dow_per_dict"][year][dow]
                    count_dow_per += 1
        if count_dow_per == 0:
            avg_dow_per[dow] = 0
        else:
            avg_dow_per[dow] = total_dow_per / count_dow_per
            
    dow_output = {}

    
    for dow in avg_dow_per.keys():
        if avg_dow_per[dow] == 0:
            continue
        else:
            dow_output[dow] = round(woy_output * avg_dow_per[dow])
            

    return {"start_date": start_date, "unit": unit, "forecast": dow_output, "outliers_excluded": outliers_excluded}

def method_2(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 2.
    """
    data = gather_data(unit, start_date)

    print("Running Method 2 for week of " + start_date)

        ### REMOVING OUTLIERS AND CALCULATING END VALUES ###
    outliers_excluded = 0

    if data["holiday"] != "Moving Holiday":
        wow_Q1 = np.percentile(data["wow_list"], 25)
        wow_Q3 = np.percentile(data["wow_list"], 75)
        wow_IQR = wow_Q3 - wow_Q1
        wow_upper = wow_Q3 + 1.5 * wow_IQR
        wow_lower = wow_Q1 - 1.5 * wow_IQR

        for wow in list(data["wow_list"]):
            if wow > wow_upper:
                data["wow_list"].remove(wow)
                outliers_excluded += 1
            elif wow < wow_lower:
                data["wow_list"].remove(wow)
                outliers_excluded += 1
                      
    wow_output = sum(data["wow_list"]) / len(data["wow_list"])     
       
    if data["holiday"] != "Moving Holiday":
        dow_buckets = {}

        for start in data["dow_per_dict"]:
            for dow in data["dow_per_dict"][start]:
                if dow not in dow_buckets:
                    dow_buckets[dow] = {}
                dow_buckets[dow][start[-4:]] = data["dow_per_dict"][start][dow]

        for dow in dow_buckets:
            dow_values = []
            for year in dow_buckets[dow]:
                dow_values.append(list(dow_buckets[dow].values()))

            dow_Q1 = np.percentile(dow_values, 25)
            dow_Q3 = np.percentile(dow_values, 75)
            dow_IQR = dow_Q3 - dow_Q1
            dow_upper = dow_Q3 + 1.5 * dow_IQR
            dow_lower = dow_Q1 - 1.5 * dow_IQR

            for start in list(data["dow_per_dict"]):
                for dow2 in list(data["dow_per_dict"][start]):
                    if dow2 == dow and data["dow_per_dict"][start][dow] >= dow_upper:
                        del data["dow_per_dict"][start][dow]
                        outliers_excluded += 1
                    elif dow2 == dow and data["dow_per_dict"][start][dow] <= dow_lower:
                        del data["dow_per_dict"][start][dow]
                        outliers_excluded += 1
    
    avg_dow_per = {"Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0, "Friday": 0, "Saturday": 0, "Sunday": 0}
    for dow in avg_dow_per.keys():
        count_dow_per = 0
        total_dow_per = 0
        for week in data["dow_per_dict"]:
            for dow2 in data["dow_per_dict"][week]:
                if dow2 == dow:
                    total_dow_per += data["dow_per_dict"][week][dow]
                    count_dow_per += 1
        if count_dow_per == 0:
            avg_dow_per[dow] = 0
        else:
            avg_dow_per[dow] = total_dow_per / count_dow_per
            
    dow_output = {}
    
    for dow in avg_dow_per:
        if avg_dow_per[dow] == 0:
            continue
        else:
            dow_output[dow] = round(data_dict["week_prior_total_volume"] * wow_output * avg_dow_per[dow])
            
    return {"start_date": start_date, "unit": unit, "forecast": dow_output, "outliers_excluded": outliers_excluded}

def method_3(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 3.
    """
    data = gather_data(unit, start_date)

    print("Running Method 3 for week of " + start_date)

     ### DELETING KEYS IN DICTS WHERE THE VALUE IS 0 ###               
    for dow in list(data["week_prior_volume"]):
        if data["week_prior_volume"][dow] == 0:
            del data["week_prior_volume"][dow]
            
    for week in list(data["week_to_compare_historical_volume"]):
        for dow in list(data["week_to_compare_historical_volume"][week]):
            if data["week_to_compare_historical_volume"][week][dow] == 0:
                del data["week_to_compare_historical_volume"][week][dow]
                
    for week in list(data["working_week_historical_volume"]):
        for dow in list(data["working_week_historical_volume"][week]):
            if data["working_week_historical_volume"][week][dow] == 0:
                del data["working_week_historical_volume"][week][dow]


    ### REMOVING OUTLIERS AND CALCULATING END VALUES ###            
    dow_buckets_week_to_compare_historical_volume = {}
    for start in data["week_to_compare_historical_volume"]:
        for dow in data["week_to_compare_historical_volume"][start]:
            if dow not in dow_buckets_week_to_compare_historical_volume:
                dow_buckets_week_to_compare_historical_volume[dow] = {}
            dow_buckets_week_to_compare_historical_volume[dow][start[-4:]] = data["week_to_compare_historical_volume"][start][dow]

            
    dow_buckets_working_week_historical_volume = {}
    for start in data["working_week_historical_volume"]:
        for dow in data["working_week_historical_volume"][start]:
            if dow not in dow_buckets_working_week_historical_volume:
                dow_buckets_working_week_historical_volume[dow] = {}
            dow_buckets_working_week_historical_volume[dow][start[-4:]] = data["working_week_historical_volume"][start][dow]
            
    per_change = {dow: {year: dow_buckets_working_week_historical_volume[dow][year]/dow_buckets_week_to_compare_historical_volume[dow][year] for year in dow_buckets_working_week_historical_volume[dow].keys() & dow_buckets_week_to_compare_historical_volume[dow]} for dow in dow_buckets_working_week_historical_volume.keys() & dow_buckets_week_to_compare_historical_volume}
    for dow in list(per_change):
        check_empty = not bool(per_change[dow])
        if check_empty == True:
            del per_change[dow]
            

    outliers_excluded = 0
    
    if data["holiday"] != "Moving Holiday":

        for dow in per_change:
            dod_values = []
            for year in dow_buckets_working_week_historical_volume[dow]:
                dod_values.append(list(per_change[dow].values()))

            dod_Q1 = np.percentile(dod_values, 25)
            dod_Q3 = np.percentile(dod_values, 75)
            dod_IQR = dod_Q3 - dod_Q1
            dod_upper = dod_Q3 + 1.5 * dod_IQR
            dod_lower = dod_Q1 - 1.5 * dod_IQR

            for year in list(per_change[dow]):
                if per_change[dow][year] > dod_upper:
                    del per_change[dow][year]
                    outliers_excluded += 1
                elif per_change[dow][year] < dod_lower:
                    del per_change[dow][year]
                    outliers_excluded += 1

    dod_output = {}
    for dow in list(per_change):
        total = 0
        count = 0
        if len(per_change[dow]) == 0:
            del per_change[dow]
        else:
            for year in per_change[dow]:
                total += per_change[dow][year]
                count += 1
            dod_output[dow] = total / count

        
    forecast = {}
    for date in data["week_prior_volume"]:
        dow = weekday_dict[dt.date.weekday(str_to_object(date))]
        if dow not in dod_output:
            continue
        forecast[dow] = round(data["week_prior_volume"][date] * dod_output[dow])
    
    return {"start_date": start_date, "unit": unit, "forecast": forecast, "outliers_excluded": outliers_excluded}

def method_4(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 4.
    """
    data = gather_data(unit, start_date)

    print("Running Method 4 for week of " + start_date)

    ### REMOVING OUTLIERS AND CALCULATING END VALUES ### 
    outliers_excluded = 0

    for year in list(data["per_last_4_matching_volume"]):
        year_values = list(data["per_last_4_matching_volume"].values())

        year_Q1 = np.percentile(year_values, 25)
        year_Q3 = np.percentile(year_values, 75)
        year_IQR = year_Q3 - year_Q1
        year_upper = year_Q3 + 1.5 * year_IQR
        year_lower = year_Q1 - 1.5 * year_IQR

        for year in list(data["per_last_4_matching_volume"]):
            if data["per_last_4_matching_volume"][year] > year_upper:
                del data["per_last_4_matching_volume"][year]
                outliers_excluded += 1
            elif data["per_last_4_matching_volume"][year] < year_lower:
                del data["per_last_4_matching_volume"][year]
                outliers_excluded += 1
    
    avg_per_last_4 = sum(data["per_last_4_matching_volume"].values()) / len(data["per_last_4_matching_volume"]) / 4
        

    if data["holiday"] != "Moving Holiday":
        
        for year in list(data["last_4_matching_weeks_per_dict"]):
            year_values = list(data["last_4_matching_weeks_per_dict"].values())

            year_Q1 = np.percentile(year_values, 25)
            year_Q3 = np.percentile(year_values, 75)
            year_IQR = year_Q3 - year_Q1
            year_upper = year_Q3 + 1.5 * year_IQR
            year_lower = year_Q1 - 1.5 * year_IQR

            for year in list(data["last_4_matching_weeks_per_dict"]):
                if data["last_4_matching_weeks_per_dict"][year] > year_upper:
                    del data["last_4_matching_weeks_per_dict"][year]
                    outliers_excluded += 1
                elif data["last_4_matching_weeks_per_dict"][year] < year_lower:
                    del data["last_4_matching_weeks_per_dict"][year]
                    outliers_excluded += 1
                
    avg_per_working = sum(data["last_4_matching_weeks_per_dict"].values()) / len(data["last_4_matching_weeks_per_dict"])
    
    per_change = (avg_per_working - avg_per_last_4)
    
    total_week_forecast = data["last_4_current_year_volume"] * (1 + per_change) / 4

        
    dow_buckets = {}
    
    for week in data["dow_per_dict"]:
        for dow in data["dow_per_dict"][week]:
            if dow not in dow_buckets:
                dow_buckets[dow] = []
            dow_buckets[dow].append(data["dow_per_dict"][week][dow])
    
    if data["holiday"] != "Moving Holiday":

        for dow in dow_buckets:
            dow_Q1 = np.percentile(dow_buckets[dow], 25)
            dow_Q3 = np.percentile(dow_buckets[dow], 75)
            dow_IQR = dow_Q3 - dow_Q1
            dow_upper = dow_Q3 + 1.5 * dow_IQR
            dow_lower = dow_Q1 - 1.5 * dow_IQR

            for value in list(dow_buckets[dow]):
                if value > dow_upper:
                    dow_buckets[dow].remove(value)
                    outliers_excluded += 1
                elif value < dow_lower:
                    dow_buckets[dow].remove(value)
                    outliers_excluded += 1
                
    dow_average = {}
    
    for dow in dow_buckets:
        dow_average[dow] = sum(dow_buckets[dow]) / len(dow_buckets[dow])
        
    dow_output = {}
    
    for dow in dow_average:
        if dow_average[dow] == 0:
            continue
        dow_output[dow] = round(dow_average[dow] * total_week_forecast)

    return {"start_date": start_date, "unit": unit, "forecast": dow_output, "outliers_excluded": outliers_excluded}

def tactical_volume_forecast_v2(unit):
    """
    Returns the tactical (next 3 weeks) volume forecast using an elastic net model.
    """
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
    cursor = conn.cursor()
    global queries_saved
    
    forecast_week = object_to_str(today)
    
    accuracy_dict = {}
    
    print("Training model...")
    
    current_week = -4
    
    while current_week < 0:
        
        if current_week == -4:
            accuracy_dict[1] = {}
            accuracy_dict[2] = {}
            accuracy_dict[3] = {}
            accuracy_dict[4] = {}
        
        current_method = 4
    
        while current_method > 0:
            

            if current_method == 1:
                temp_forecast_dict = method_1(unit, start_of_week(forecast_week, current_week))
            elif current_method == 2:
                temp_forecast_dict = method_2(unit, start_of_week(forecast_week, current_week))
            elif current_method == 3:
                temp_forecast_dict = method_3(unit, start_of_week(forecast_week, current_week))
            elif current_method == 4:
                temp_forecast_dict = method_4(unit, start_of_week(forecast_week, current_week))

            for dow in temp_forecast_dict["forecast"]:
                if dow == "Monday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=1))] = int(temp_forecast_dict["forecast"]["Monday"])
                elif dow == "Tuesday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=2))] = int(temp_forecast_dict["forecast"]["Tuesday"])
                elif dow == "Wednesday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=3))] = int(temp_forecast_dict["forecast"]["Wednesday"])
                elif dow == "Thursday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=4))] = int(temp_forecast_dict["forecast"]["Thursday"])
                elif dow == "Friday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=5))] = int(temp_forecast_dict["forecast"]["Friday"])
                elif dow == "Saturday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=6))] = int(temp_forecast_dict["forecast"]["Saturday"])
                elif dow == "Saturday":
                    accuracy_dict[current_method][object_to_str(str_to_object(temp_forecast_dict["start_date"]) + dt.timedelta(days=7))] = int(temp_forecast_dict["forecast"]["Sunday"])

            current_method = current_method - 1
        
        current_week = current_week + 1
    
    df = pd.DataFrame(accuracy_dict)
    df.dropna(inplace=True)
    
    actuals_dict = {}
    
    for date in accuracy_dict[1]:
        actuals_dict[date] = day_volume(date, unit)
        
    df["Actual"] = pd.Series(actuals_dict)
    
    X = df[[1,2,3,4]]
    y = df['Actual']
    
    elastic = ElasticNetCV()
    elastic.fit(X,y)
    
    regr = LinearRegression()
    regr.fit(X, y)

    print("Forecasting...")
    
    week1method1 = method_1(unit, start_of_week(forecast_week, 1))
    week1method2 = method_2(unit, start_of_week(forecast_week, 1))
    week1method3 = method_3(unit, start_of_week(forecast_week, 1))
    week1method4 = method_4(unit, start_of_week(forecast_week, 1))
    week2method1 = method_1(unit, start_of_week(forecast_week, 2))
    week2method2 = method_2(unit, start_of_week(forecast_week, 2))
    week2method3 = method_3(unit, start_of_week(forecast_week, 2))
    week2method4 = method_4(unit, start_of_week(forecast_week, 2))
    week3method1 = method_1(unit, start_of_week(forecast_week, 3))
    week3method2 = method_2(unit, start_of_week(forecast_week, 3))
    week3method3 = method_3(unit, start_of_week(forecast_week, 3))
    week3method4 = method_4(unit, start_of_week(forecast_week, 3))
    
    for day in week1method1["forecast"]:
        if day not in week1method3:
            week1method3["forecast"][day] = week1method2["forecast"][day]
    for day in week2method1["forecast"]:
        if day not in week2method3:
            week2method3["forecast"][day] = week2method2["forecast"][day]
    for day in week3method1["forecast"]:
        if day not in week3method3:
            week3method3["forecast"][day] = week3method2["forecast"][day]
    


    final_answer = {"unit": unit, start_of_week(forecast_week, 1):{}, start_of_week(forecast_week, 2):{}, start_of_week(forecast_week, 3):{}}

    #WEEK 1 FINAL ANSWER
    week_df = define_matching_weeks_df[define_matching_weeks_df.isin([start_of_week(forecast_week,1)]).any(axis=1)]
    if week_df['Holiday'].isin(moving_holidays).any():
        holiday = "Moving Holiday"
    elif week_df['Holiday'].isin(static_holidays).any():
        holiday = "Static Holiday"
    else:
        holiday = "No Holiday"
    print(holiday)
    for day in week1method1["forecast"]:
        predict = elastic.predict([[week1method1["forecast"][day],week1method2["forecast"][day],week1method3["forecast"][day],week1method4["forecast"][day]]])
        final_answer[start_of_week(forecast_week, 1)][day] = int(predict.item())

    #WEEK 2 FINAL ANSWER
    week_df = define_matching_weeks_df[define_matching_weeks_df.isin([start_of_week(forecast_week,2)]).any(axis=1)]
    if week_df['Holiday'].isin(moving_holidays).any():
        holiday = "Moving Holiday"
    elif week_df['Holiday'].isin(static_holidays).any():
        holiday = "Static Holiday"
    else:
        holiday = "No Holiday"
    print(holiday)
    for day in week2method1["forecast"]:
        predict = elastic.predict([[week2method1["forecast"][day],week2method2["forecast"][day],week2method3["forecast"][day],week2method4["forecast"][day]]])  
        final_answer[start_of_week(forecast_week, 2)][day] = int(predict.item())

    #WEEK 3 FINAL ANSWER
    week_df = define_matching_weeks_df[define_matching_weeks_df.isin([start_of_week(forecast_week,3)]).any(axis=1)]
    if week_df['Holiday'].isin(moving_holidays).any():
        holiday = "Moving Holiday"
    elif week_df['Holiday'].isin(static_holidays).any():
        holiday = "Static Holiday"
    else:
        holiday = "No Holiday"
    print(holiday)
    if week3method1 == week3method2 == week3method3 == week3method4:
        for day in week3method1["forecast"]:
            predict = regr.predict([[week3method1["forecast"][day],week3method2["forecast"][day],week3method3["forecast"][day],week3method4["forecast"][day]]])
            final_answer[start_of_week(forecast_week, 3)][day] = int(predict.item())
    else:
        for day in week3method1["forecast"]:
            predict = elastic.predict([[week3method1["forecast"][day],week3method2["forecast"][day],week3method3["forecast"][day],week3method4["forecast"][day]]])
            final_answer[start_of_week(forecast_week, 3)][day] = int(predict.item())

    print(queries_saved)
    print(final_answer)
    conn.close()
    return final_answer



