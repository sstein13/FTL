import pandas as pd
import datetime as dt
import numpy as np
import pyodbc
import copy

# Connect to EchoPass Database
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=camrptsql01;DATABASE=EchoPass;Trusted_Connection=yes')
cursor = conn.cursor()

# Global variable definitions
today = dt.date.today()
current_year = str(dt.date.today())[:4]
weekday_dict = {0:"Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday", 5:"Saturday", 6:"Sunday"}

# Defines SQL Queries
genysis_sql="""
Select cast(EchoPassDate as Date) as Date, sum(Offered) as Offered, Mdivqueue as Queue
From [EchoPass].[dbo].[GIM_Data]
Where cast(EchoPassDate as Date) >= ?
and cast(EchoPassDate as Date) < ?
Group By cast(EchoPassDate as Date), Mdivqueue
"""

genysis_year_sql="""
Select datepart(year,cast(EchoPassDate as date)) as Date, sum(Offered) as Offered, Mdivqueue as Queue
From [EchoPass].[dbo].[GIM_Data]
Where datepart(year, cast(EchoPassDate as date)) = ?
Group By datepart(year,cast(EchoPassDate as date)), Mdivqueue
"""

department_mapping_sql = """
select [Queue], [System], [Group], Subgroup from dbo.DepartmentMapping
"""

# Maps departments
cursor.execute(department_mapping_sql)
sql_output = cursor.fetchall()
queues_df = pd.DataFrame.from_records(sql_output, columns=[col[0] for col in cursor.description])

# Creates a dataframe from the Define Matching Weeks excel file and cleans data
define_matching_weeks_df = pd.read_excel("HS FTL\Define Matching Weeks.xlsx")
define_matching_weeks_df.fillna("none", inplace = True)

def define_dept(df):
    """
    Takes a dataframe and returns the same dataframe but with a column specifying which department each queue belongs to.
    """
    return df.merge(queues_df, how="left", on="Queue")


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
    next_date = object_to_str(str_to_object(date) + dt.timedelta(days=1))
    
    cursor.execute(genysis_sql, to_iso(date), to_iso(next_date))
    sql_output = cursor.fetchall()
    df = pd.DataFrame.from_records(sql_output, columns=[col[0] for col in cursor.description])
    df = define_dept(df)
    return int(df.loc[df["Group"] == unit, "Offered"].sum())


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


def yearly_volume(year, unit):
    """Given a year entered as an interval, returns the total volume for that year."""
    cursor.execute(genysis_sql, "01/01/"+str(year), "01/01/"+str(year+1))
    sql_output = cursor.fetchall()
    df = pd.DataFrame.from_records(sql_output, columns=[col[0] for col in cursor.description])
    df = define_dept(df)
    total = int(df.loc[df["Group"] == unit, "Offered"].sum())
    return total


def method_1(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 1, forecasting yearly volume using the last 4 weeks method.
    """
    print("Running Method 1 for week of " + start_date)

    yearly_volume_dict = {} #Dictionary of each year's total volume
    working_matching_weeks_list = []  #Lists the start_date and the start of all matching weeks for prior years
    current_matching_weeks_list = [] #Lists the start of last week and all matching weeks for prior years (used for year_output when start_date is in the future)
    matching_weeks_total_dict = {} #Dictionary of the total volume for the matching weeks of each year
    matching_weeks_per_dict = {} #Dictionary of the percent of total year volume for the matching weeks of each year
    dow_per_dict = {} #Dictionary of the dow % for each day of week for the matching weeks of each year
    last_4_matching_weeks_total_dict = {} #Dictionary of the total volume for the prior 4 matching weeks of each year
    last_4_matching_weeks_per_dict = {} #Dictionary of the percent of total year volume in the prior 4 matching weeks of each year

    working_year = start_date[-4:]


    ### FILLING OUT LISTS AND DICTS ###
    for year in range(2012, int(object_to_str(today)[-4:])+1):
            yearly_volume_dict[year] = yearly_volume(year, "Sales")

    working_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        working_week_matching_weeks_df.pop(working_week_matching_weeks_df.columns.values[0])
    working_row = working_week_matching_weeks_df.index[working_week_matching_weeks_df[working_year + " Date"] == start_date][0]
    working_matching_weeks_list = working_week_matching_weeks_df.values[working_row].tolist()

    current_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        current_week_matching_weeks_df.pop(current_week_matching_weeks_df.columns.values[0])
    current_row = current_week_matching_weeks_df.index[current_week_matching_weeks_df[current_year + " Date"] == start_of_week((object_to_str(today)),1)][0]
    current_matching_weeks_list = current_week_matching_weeks_df.values[current_row].tolist()

    if start_date < start_of_week(object_to_str(today)):
        for start in working_matching_weeks_list:
            year_current = start[-4:]
            last_4_matching_weeks_total_dict[year_current] = {start_of_week(start,-1): sum(week_volume(start_of_week(start,-1), unit).values()),
            start_of_week(start,-2): sum(week_volume(start_of_week(start,-2), unit).values()), 
            start_of_week(start,-3): sum(week_volume(start_of_week(start,-3), unit).values()),
            start_of_week(start,-4): sum(week_volume(start_of_week(start,-4), unit).values())}
    else:
        for start in current_matching_weeks_list:
            year_current = start[-4:]
            last_4_matching_weeks_total_dict[year_current] = {start_of_week(start,-1): sum(week_volume(start_of_week(start,-1), unit).values()),
            start_of_week(start,-2): sum(week_volume(start_of_week(start,-2), unit).values()), 
            start_of_week(start,-3): sum(week_volume(start_of_week(start,-3), unit).values()),
            start_of_week(start,-4): sum(week_volume(start_of_week(start,-4), unit).values())}

    for year in last_4_matching_weeks_total_dict:
        year_int = int(year)
        if yearly_volume_dict[year_int] == 0:
            continue
        last_4_matching_weeks_per_dict[year] = sum(last_4_matching_weeks_total_dict[year].values()) / yearly_volume_dict[year_int]

    for start in working_matching_weeks_list:
        year_working = start[-4:]
        matching_weeks_total_dict[year_working] = sum(week_volume(start_of_week(start, 1), unit).values())

    for year in matching_weeks_total_dict:
        if int(year) not in yearly_volume_dict:
            continue
        elif yearly_volume_dict[int(year)] == 0:
            continue
        year_int = int(year)
        matching_weeks_per_dict[year] = matching_weeks_total_dict[year] / yearly_volume_dict[year_int]

    for start in working_matching_weeks_list:
        year_current = start[-4:]
        if int(year_current) >= int(working_year):
            continue
        if year_current not in matching_weeks_total_dict or matching_weeks_total_dict[year_current] == 0:
            continue
        if year_current not in dow_per_dict:
            dow_per_dict[year_current] = {}
        for dow in range(0, 7):
            date_current = object_to_str(str_to_object(start) + dt.timedelta(days=dow + 1))
            dow_current = weekday_dict[dow]
            dow_per_dict[year_current][dow_current] = day_volume(date_current, unit) / matching_weeks_total_dict[year_current]

       
    ### DELETING KEYS IN DICTS WHERE THE VALUE IS 0 ###

    for year in list(yearly_volume_dict):
        if yearly_volume_dict[year] == 0:
            del yearly_volume_dict[year]
            
    for year in list(matching_weeks_total_dict):
        if matching_weeks_total_dict[year] == 0:
            del matching_weeks_total_dict[year]

      
    ### REMOVING OUTLIERS AND CALCULATING END VALUES ###
    outliers_excluded = 0

    count_last_4_per = 0
    total_last_4_per = 0
    year_per_values = []
    for year in last_4_matching_weeks_per_dict:
        year_per_values.append(list(last_4_matching_weeks_per_dict.values()))
    year_per_Q1 = np.percentile(year_per_values, 25)
    year_per_Q3 = np.percentile(year_per_values, 75)
    year_per_IQR = year_per_Q3 - year_per_Q1
    year_per_upper = year_per_Q3 + 1.5 * year_per_IQR
    year_per_lower = year_per_Q1 - 1.5 * year_per_IQR
    for year in list(last_4_matching_weeks_per_dict):
        if last_4_matching_weeks_per_dict[year] >= year_per_upper:
            del last_4_matching_weeks_per_dict[year]
            outliers_excluded += 1
        elif last_4_matching_weeks_per_dict[year] <= year_per_lower:
            del last_4_matching_weeks_per_dict[year]
            outliers_excluded += 1
    for year in last_4_matching_weeks_per_dict:
        if int(year) >= int(working_year):
            continue
        count_last_4_per += 1
        total_last_4_per += last_4_matching_weeks_per_dict[year]
    avg_per_last_4 = total_last_4_per / count_last_4_per
    last_4_current_year_total = 0
    for year in last_4_matching_weeks_total_dict:
        if year == working_year:
            for start in last_4_matching_weeks_total_dict[year]:
                last_4_current_year_total += last_4_matching_weeks_total_dict[year][start]
    year_output = round((1 / avg_per_last_4) * last_4_current_year_total)
    
    
    total_woy_per = 0
    count_woy_per = 0
    woy_values = []
    for year in matching_weeks_per_dict:
        woy_values.append(list(matching_weeks_per_dict.values()))
    woy_Q1 = np.percentile(woy_values, 25)
    woy_Q3 = np.percentile(woy_values, 75)
    woy_IQR = woy_Q3 - woy_Q1
    woy_upper = woy_Q3 + 1.5 * woy_IQR
    woy_lower = woy_Q1 - 1.5 * woy_IQR
    for year in list(matching_weeks_per_dict):
        if matching_weeks_per_dict[year] >= woy_upper:
            del matching_weeks_per_dict[year]
            outliers_excluded += 1
        elif matching_weeks_per_dict[year] <= woy_lower:
            del matching_weeks_per_dict[year]
            outliers_excluded += 1
    for year in matching_weeks_per_dict:
        if int(year) >= int(working_year):
            continue
        else:
            total_woy_per += matching_weeks_per_dict[year]
            count_woy_per += 1
    avg_woy_per = total_woy_per / count_woy_per
    woy_output = round(avg_woy_per * year_output)
    

    dow_buckets = {}
    for year in dow_per_dict:
        for dow in dow_per_dict[year]:
            if dow not in dow_buckets:
                dow_buckets[dow] = {}
            dow_buckets[dow][year] = dow_per_dict[year][dow]


    for dow in dow_buckets:
        dow_values = []
        for year in dow_buckets[dow]:
            dow_values.append(list(dow_buckets[dow].values()))

        dow_Q1 = np.percentile(dow_values, 25)
        dow_Q3 = np.percentile(dow_values, 75)
        dow_IQR = dow_Q3 - dow_Q1
        dow_upper = dow_Q3 + 1.5 * dow_IQR
        dow_lower = dow_Q1 - 1.5 * dow_IQR

        for year in list(dow_per_dict):
            for dow2 in list(dow_per_dict[year]):
                if dow2 == dow and dow_per_dict[year][dow] >= dow_upper:
                    del dow_per_dict[year][dow]
                    outliers_excluded += 1
                elif dow2 == dow and dow_per_dict[year][dow] <= dow_lower:
                    del dow_per_dict[year][dow]
                    outliers_excluded += 1
    
    avg_dow_per = {"Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0, "Friday": 0, "Saturday": 0, "Sunday": 0}
    
    for dow in avg_dow_per.keys():
        count_dow_per = 0
        total_dow_per = 0
        for year in dow_per_dict:
            for dow2 in dow_per_dict[year]:
                if dow2 == dow:
                    total_dow_per += dow_per_dict[year][dow]
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
    print("Running Method 2 for week of " + start_date)

    week_to_compare_historical_volume = {} #Dictionary of total volume for each prior year matching the comparison week.
    working_week_historical_volume = {} #Dictionary of total volume for each prior year matching the forecast week.
    dow_per_dict = {} #Dictionary of the dow % for each day of week for the matching weeks of each year.
    wow_list = [] #List of % change between prior years' comparison and working weeks.

    
    working_year = start_date[-4:]
    
    
    if start_date < start_of_week((object_to_str(today)),1):
        week_to_compare = start_of_week(start_date, -1)
    else:
        week_to_compare = start_of_week(object_to_str(today), -1)
        
    comparison_year = week_to_compare[-4:]
          
    comparison_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        comparison_week_matching_weeks_df.pop(comparison_week_matching_weeks_df.columns.values[0])
    comparison_row = comparison_week_matching_weeks_df.index[comparison_week_matching_weeks_df[comparison_year + " Date"] == week_to_compare][0]
    comparison_weeks = comparison_week_matching_weeks_df.values[comparison_row].tolist()
    for week in list(comparison_weeks):
        if int(week[-4:]) >= int(comparison_year):
            comparison_weeks.remove(week)
            
    working_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        working_week_matching_weeks_df.pop(working_week_matching_weeks_df.columns.values[0])
    working_row = working_week_matching_weeks_df.index[working_week_matching_weeks_df[working_year + " Date"] == start_date][0]
    working_weeks = working_week_matching_weeks_df.values[working_row].tolist()
    for week in list(working_weeks):
        if int(week[-4:]) >= int(working_year):
            working_weeks.remove(week)
   

   ### FILLING OUT LISTS AND DICTS ###  
    for week in comparison_weeks:
        week_to_compare_historical_volume[week] = sum(week_volume(week, unit).values())
    
    for week in working_weeks:
        working_week_historical_volume[week] = sum(week_volume(week, unit).values()) 

    for start in working_weeks:
        year_current = start[-4:]
        if int(year_current) >= int(working_year):
            continue
        if start not in working_week_historical_volume or working_week_historical_volume[start] == 0:
            continue
        if year_current not in dow_per_dict:
            dow_per_dict[year_current] = {}
        for dow in range(0, 7):
            date_current = object_to_str(str_to_object(start) + dt.timedelta(days=dow + 1))
            dow_current = weekday_dict[dow]
            dow_per_dict[year_current][dow_current] = day_volume(date_current, unit) / working_week_historical_volume[start]  
    
    week_prior_total_volume = sum(week_volume(week_to_compare, unit).values())
                        
    for start in list(week_to_compare_historical_volume):
        if week_to_compare_historical_volume[start] == 0:
            del week_to_compare_historical_volume[start]

    for start in working_week_historical_volume:
        temp_working_year = start[-4:]
        for start2 in week_to_compare_historical_volume:
            temp_comparison_year = start2[-4:]
            if temp_comparison_year == temp_working_year:
                wow_list.append(working_week_historical_volume[start] / week_to_compare_historical_volume[start2])
    

    ### REMOVING OUTLIERS AND CALCULATING END VALUES ###
    outliers_excluded = 0

    wow_Q1 = np.percentile(wow_list, 25)
    wow_Q3 = np.percentile(wow_list, 75)
    wow_IQR = wow_Q3 - wow_Q1
    wow_upper = wow_Q3 + 1.5 * wow_IQR
    wow_lower = wow_Q1 - 1.5 * wow_IQR

    for wow in list(wow_list):
        if wow > wow_upper:
            wow_list.remove(wow)
            outliers_excluded += 1
        elif wow < wow_lower:
            wow_list.remove(wow)
            outliers_excluded += 1
                
    wow_output = sum(wow_list) / len(wow_list)     
            
    dow_buckets = {}

    for start in dow_per_dict:
        for dow in dow_per_dict[start]:
            if dow not in dow_buckets:
                dow_buckets[dow] = {}
            dow_buckets[dow][start[-4:]] = dow_per_dict[start][dow]

    for dow in dow_buckets:
        dow_values = []
        for year in dow_buckets[dow]:
            dow_values.append(list(dow_buckets[dow].values()))

        dow_Q1 = np.percentile(dow_values, 25)
        dow_Q3 = np.percentile(dow_values, 75)
        dow_IQR = dow_Q3 - dow_Q1
        dow_upper = dow_Q3 + 1.5 * dow_IQR
        dow_lower = dow_Q1 - 1.5 * dow_IQR

        for start in list(dow_per_dict):
            for dow2 in list(dow_per_dict[start]):
                if dow2 == dow and dow_per_dict[start][dow] >= dow_upper:
                    del dow_per_dict[start][dow]
                    outliers_excluded += 1
                elif dow2 == dow and dow_per_dict[start][dow] <= dow_lower:
                    del dow_per_dict[start][dow]
                    outliers_excluded += 1
    
    avg_dow_per = {"Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0, "Friday": 0, "Saturday": 0, "Sunday": 0}
    for dow in avg_dow_per.keys():
        count_dow_per = 0
        total_dow_per = 0
        for week in dow_per_dict:
            for dow2 in dow_per_dict[week]:
                if dow2 == dow:
                    total_dow_per += dow_per_dict[week][dow]
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
            dow_output[dow] = round(week_prior_total_volume * wow_output * avg_dow_per[dow])
    
    return {"start_date": start_date, "unit": unit, "forecast": dow_output, "outliers_excluded": outliers_excluded}

def method_3(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 3.
    """
    print("Running Method 3 for week of " + start_date)

    week_to_compare_historical_volume = {} #Dictionary of volume by dow for each prior year matching the comparison week.
    working_week_historical_volume = {} #Dictionary of volume by dow for each prior year matching the forecast week.
    week_prior_total_volume = {} #Dictionary of volume by dow for the comparison week.
    
    working_year = start_date[-4:]
    
    
    if start_date < start_of_week((object_to_str(today)),1):
        week_to_compare = start_of_week(start_date, -1)
    else:
        week_to_compare = start_of_week(object_to_str(today), -1)
        
    comparison_year = week_to_compare[-4:]
        
        
    comparison_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        comparison_week_matching_weeks_df.pop(comparison_week_matching_weeks_df.columns.values[0])
    comparison_row = comparison_week_matching_weeks_df.index[comparison_week_matching_weeks_df[comparison_year + " Date"] == week_to_compare][0]
    comparison_weeks = comparison_week_matching_weeks_df.values[comparison_row].tolist()
    for week in list(comparison_weeks):
        if int(week[-4:]) >= int(comparison_year):
            comparison_weeks.remove(week)
            
    working_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        working_week_matching_weeks_df.pop(working_week_matching_weeks_df.columns.values[0])
    working_row = working_week_matching_weeks_df.index[working_week_matching_weeks_df[working_year + " Date"] == start_date][0]
    working_weeks = working_week_matching_weeks_df.values[working_row].tolist()
    for week in list(working_weeks):
        if int(week[-4:]) >= int(working_year):
            working_weeks.remove(week)
    
    
    ### FILLING OUT LISTS AND DICTS ### 
    for week in comparison_weeks:
        week_to_compare_historical_volume[week] = {}
        current_week = week_volume(week, unit)
        for day in current_week:
            for dow in weekday_dict.keys():
                if str_to_object(day).weekday() == dow:
                    week_to_compare_historical_volume[week][weekday_dict[dow]] = current_week[day]

    for week in working_weeks:
        working_week_historical_volume[week] = {}
        current_week = week_volume(week, unit)
        for day in current_week:
            for dow in weekday_dict.keys():
                if str_to_object(day).weekday() == dow:
                    working_week_historical_volume[week][weekday_dict[dow]] = current_week[day]

    prior_week = week_volume(week_to_compare, unit)
    for day in prior_week:
        for dow in weekday_dict.keys():
            if str_to_object(day).weekday() == dow:
                week_prior_total_volume[weekday_dict[dow]] = prior_week[day]
                    
                    
     ### DELETING KEYS IN DICTS WHERE THE VALUE IS 0 ###               
    for dow in list(week_prior_total_volume):
        if week_prior_total_volume[dow] == 0:
            del week_prior_total_volume[dow]
            
    for week in list(week_to_compare_historical_volume):
        for dow in list(week_to_compare_historical_volume[week]):
            if week_to_compare_historical_volume[week][dow] == 0:
                del week_to_compare_historical_volume[week][dow]
                
    for week in list(working_week_historical_volume):
        for dow in list(working_week_historical_volume[week]):
            if working_week_historical_volume[week][dow] == 0:
                del working_week_historical_volume[week][dow]


    ### REMOVING OUTLIERS AND CALCULATING END VALUES ###            
    dow_buckets_week_to_compare_historical_volume = {}
    for start in week_to_compare_historical_volume:
        for dow in week_to_compare_historical_volume[start]:
            if dow not in dow_buckets_week_to_compare_historical_volume:
                dow_buckets_week_to_compare_historical_volume[dow] = {}
            dow_buckets_week_to_compare_historical_volume[dow][start[-4:]] = week_to_compare_historical_volume[start][dow]
            
    dow_buckets_working_week_historical_volume = {}
    for start in working_week_historical_volume:
        for dow in working_week_historical_volume[start]:
            if dow not in dow_buckets_working_week_historical_volume:
                dow_buckets_working_week_historical_volume[dow] = {}
            dow_buckets_working_week_historical_volume[dow][start[-4:]] = working_week_historical_volume[start][dow]
            
    per_change = {dow: {year: dow_buckets_working_week_historical_volume[dow][year]/dow_buckets_week_to_compare_historical_volume[dow][year] for year in dow_buckets_working_week_historical_volume[dow].keys() & dow_buckets_week_to_compare_historical_volume[dow]} for dow in dow_buckets_working_week_historical_volume.keys() & dow_buckets_week_to_compare_historical_volume}
    for dow in list(per_change):
        check_empty = not bool(per_change[dow])
        if check_empty == True:
            del per_change[dow]
    
    
    outliers_excluded = 0

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
    for dow in per_change:
        total = 0
        count = 0
        for year in per_change[dow]:
            total += per_change[dow][year]
            count += 1
        dod_output[dow] = total / count
        
    forecast = {}
    for dow in week_prior_total_volume:
        if dow not in dod_output:
            continue
        forecast[dow] = round(week_prior_total_volume[dow] * dod_output[dow])
    
    
    return {"start_date": start_date, "unit": unit, "forecast": forecast, "outliers_excluded": outliers_excluded}

def method_4(unit, start_date=start_of_week((object_to_str(today)),1)):
    """
    Returns the total volume forecasted for the given unit for next week using method 4.
    """
    print("Running Method 4 for week of " + start_date)

    yearly_volume_dict = {} #Dictionary of each year's total volume.
    per_last_4_matching_volume = {} #Dictionary of the percentage of volume received in the last 4 comparison weeks of prior years.
    per_working_matching_volume = {} #Dictionary of the percentage of volume received in the forecast week of prior years.
    dow_per_dict = {} #Dictionary of the dow % for each day of week for the matching weeks of each year.

    
    working_year = start_date[-4:]
    
    if start_date < start_of_week((object_to_str(today)),1):
        week_to_compare = start_of_week(start_date, -1)
    else:
        week_to_compare = start_of_week(object_to_str(today), -1)
        
    comparison_year = week_to_compare[-4:]
    
    last_4_current_year_volume = sum(week_volume(week_to_compare, unit).values()) + sum(week_volume(start_of_week(week_to_compare), unit).values())\
    + sum(week_volume(start_of_week(week_to_compare, -1), unit).values()) + sum(week_volume(start_of_week(week_to_compare, -2), unit).values())
    
    

    comparison_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        comparison_week_matching_weeks_df.pop(comparison_week_matching_weeks_df.columns.values[0])
    comparison_row = comparison_week_matching_weeks_df.index[comparison_week_matching_weeks_df[comparison_year + " Date"] == week_to_compare][0]
    comparison_weeks = comparison_week_matching_weeks_df.values[comparison_row].tolist()
    for week in list(comparison_weeks):
        if int(week[-4:]) >= int(comparison_year):
            comparison_weeks.remove(week)
            
    working_week_matching_weeks_df = define_matching_weeks_df.copy()
    for i in range(int((len(define_matching_weeks_df.columns) + 1)/2)):
        working_week_matching_weeks_df.pop(working_week_matching_weeks_df.columns.values[0])
    working_row = working_week_matching_weeks_df.index[working_week_matching_weeks_df[working_year + " Date"] == start_date][0]
    working_weeks = working_week_matching_weeks_df.values[working_row].tolist()
    for week in list(working_weeks):
        if int(week[-4:]) >= int(working_year):
            working_weeks.remove(week)
            

    ### FILLING OUT LISTS AND DICTS ### 
    for year in range(2012, int(object_to_str(today)[-4:])+1):
        yearly_volume_dict[year] = yearly_volume(year, unit)

    for year in list(yearly_volume_dict):
        if yearly_volume_dict[year] == 0:
            del yearly_volume_dict[year]
    
    for week in comparison_weeks:
        year = week[-4:]
        if int(year) not in yearly_volume_dict:
            continue
        per_last_4_matching_volume[week[-4:]] = (sum(week_volume(week, unit).values()) / yearly_volume_dict[int(week[-4:])]) + (sum(week_volume(start_of_week(week), unit).values()) / yearly_volume_dict[int(week[-4:])])\
        + (sum(week_volume(start_of_week(week, -1), unit).values()) / yearly_volume_dict[int(week[-4:])]) + (sum(week_volume(start_of_week(week, -2), unit).values()) / yearly_volume_dict[int(week[-4:])])

    for week in working_weeks:
        year = week[-4:]
        if int(year) not in yearly_volume_dict:
            continue
        per_working_matching_volume[week[-4:]] = (sum(week_volume(week, unit).values()) / yearly_volume_dict[int(week[-4:])]) + (sum(week_volume(start_of_week(week), unit).values()) / yearly_volume_dict[int(week[-4:])])\
        + (sum(week_volume(start_of_week(week, -1), unit).values()) / yearly_volume_dict[int(week[-4:])]) + (sum(week_volume(start_of_week(week, -2), unit).values()) / yearly_volume_dict[int(week[-4:])])
    
    for start in working_weeks:
        year_current = start[-4:]
        if int(year_current) >= int(working_year):
            continue
        if int(year_current) not in yearly_volume_dict or yearly_volume_dict[int(year_current)] == 0:
            continue
        if year_current not in dow_per_dict:
            dow_per_dict[year_current] = {}
        for dow in range(0, 7):
            date_current = object_to_str(str_to_object(start) + dt.timedelta(days=dow + 1))
            dow_current = weekday_dict[dow]
            dow_per_dict[year_current][dow_current] = day_volume(date_current, unit) / sum(week_volume(start, unit).values())

    
    ### REMOVING OUTLIERS AND CALCULATING END VALUES ### 
    outliers_excluded = 0

    for year in list(per_last_4_matching_volume):
        year_values = list(per_last_4_matching_volume.values())

        year_Q1 = np.percentile(year_values, 25)
        year_Q3 = np.percentile(year_values, 75)
        year_IQR = year_Q3 - year_Q1
        year_upper = year_Q3 + 1.5 * year_IQR
        year_lower = year_Q1 - 1.5 * year_IQR

        for year in list(per_last_4_matching_volume):
            if per_last_4_matching_volume[year] > year_upper:
                del per_last_4_matching_volume[year]
                outliers_excluded += 1
            elif per_last_4_matching_volume[year] < year_lower:
                del per_last_4_matching_volume[year]
                outliers_excluded += 1
    
    avg_per_last_4 = sum(per_last_4_matching_volume.values()) / len(per_last_4_matching_volume) / 4
        

    for year in list(per_working_matching_volume):
        year_values = list(per_working_matching_volume.values())

        year_Q1 = np.percentile(year_values, 25)
        year_Q3 = np.percentile(year_values, 75)
        year_IQR = year_Q3 - year_Q1
        year_upper = year_Q3 + 1.5 * year_IQR
        year_lower = year_Q1 - 1.5 * year_IQR

        for year in list(per_working_matching_volume):
            if per_working_matching_volume[year] > year_upper:
                del per_working_matching_volume[year]
                outliers_excluded += 1
            elif per_working_matching_volume[year] < year_lower:
                del per_working_matching_volume[year]
                outliers_excluded += 1
                
    avg_per_working = sum(per_working_matching_volume.values()) / len(per_working_matching_volume)
    
    per_change = (avg_per_working - avg_per_last_4)
    
    total_week_forecast = last_4_current_year_volume * (1 + per_change) / 4

            
    dow_buckets = {}
    
    for week in dow_per_dict:
        for dow in dow_per_dict[week]:
            if dow not in dow_buckets:
                dow_buckets[dow] = []
            dow_buckets[dow].append(dow_per_dict[week][dow])
            
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

def accuracy(function):
    """
    Returns the accuracy of a forecast.
    """
    unit = function["unit"]
    start_date = function["start_date"]
    forecast = function["forecast"]
    actual_volume = {}
    
    for dow in forecast:
        if dow == "Monday":
            actual_volume["Monday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=1)), unit)
        elif dow == "Tuesday":
            actual_volume["Tuesday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=2)), unit)
        elif dow == "Wednesday":
            actual_volume["Wednesday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=3)), unit)
        elif dow == "Thursday":
            actual_volume["Thursday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=4)), unit)
        elif dow == "Friday":
            actual_volume["Friday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=5)), unit)
        elif dow == "Saturday":
            actual_volume["Saturday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=6)), unit)
        elif dow == "Sunday":
            actual_volume["Sunday"] = day_volume(object_to_str(str_to_object(start_date) + dt.timedelta(days=7)), unit)
        
                
    accuracy_by_day = copy.deepcopy(actual_volume)
    
    for dow in accuracy_by_day:
        if forecast[dow] == 0:
            continue
        accuracy_by_day[dow] = abs((actual_volume[dow] - forecast[dow]) / forecast[dow])
        
    week_accuracy = sum(accuracy_by_day.values()) / len(accuracy_by_day)
    
    return {"week_accuracy": week_accuracy, "accuracy_by_day": accuracy_by_day}

def most_accurate(unit):
    """
    Returns the most accurate forecasting method over the past 3 weeks.
    """
    method_1_accuracy = (accuracy(method_1(unit, start_of_week(object_to_str(today), -3)))["week_accuracy"] + \
                         accuracy(method_1(unit, start_of_week(object_to_str(today), -2)))["week_accuracy"] + \
                         accuracy(method_1(unit, start_of_week(object_to_str(today), -1)))["week_accuracy"]) / 3

    method_2_accuracy = (accuracy(method_2(unit, start_of_week(object_to_str(today), -3)))["week_accuracy"] + \
                         accuracy(method_2(unit, start_of_week(object_to_str(today), -2)))["week_accuracy"] + \
                         accuracy(method_2(unit, start_of_week(object_to_str(today), -1)))["week_accuracy"]) / 3
    
    method_3_accuracy = (accuracy(method_3(unit, start_of_week(object_to_str(today), -3)))["week_accuracy"] + \
                         accuracy(method_3(unit, start_of_week(object_to_str(today), -2)))["week_accuracy"] + \
                         accuracy(method_3(unit, start_of_week(object_to_str(today), -1)))["week_accuracy"]) / 3
    
    method_4_accuracy = (accuracy(method_4(unit, start_of_week(object_to_str(today), -3)))["week_accuracy"] + \
                         accuracy(method_4(unit, start_of_week(object_to_str(today), -2)))["week_accuracy"] + \
                         accuracy(method_4(unit, start_of_week(object_to_str(today), -1)))["week_accuracy"]) / 3
    
    if method_1_accuracy < method_2_accuracy and method_1_accuracy < method_3_accuracy and method_1_accuracy < method_4_accuracy:
        best_accuracy = method_1_accuracy
        most_accurate_method = 1
    elif method_2_accuracy <= method_1_accuracy and method_2_accuracy < method_3_accuracy and method_2_accuracy < method_4_accuracy:
        best_accuracy = method_2_accuracy
        most_accurate_method = 2
    elif method_3_accuracy <= method_1_accuracy and method_3_accuracy <= method_2_accuracy and method_3_accuracy < method_4_accuracy:
        best_accuracy = method_3_accuracy
        most_accurate_method = 3
    elif method_4_accuracy <= method_1_accuracy and method_4_accuracy <= method_2_accuracy and method_4_accuracy <= method_3_accuracy:
        best_accuracy = method_4_accuracy
        most_accurate_method = 4
    else:
        return "something's gone horribly wrong"
        
    best_accuracy = round((1-best_accuracy) * 100, 2)
    
    return {"most_accurate_method": most_accurate_method, "accuracy": best_accuracy}

def tactical_volume_forecast(unit):
    """
    Returns the tactical (next 3 weeks) volume forecast using the method that has been the most accurate recently.
    """
    
    final_answer = most_accurate(unit)
    method = final_answer["most_accurate_method"]
    accuracy = final_answer["accuracy"]
    
    if method == 1:
        print("\n*** Method 1 has been " + str(accuracy) + "% accurate over the past three weeks. ***\n")
        return {"unit":unit,"method":method, "accuracy":accuracy, start_of_week(object_to_str(today), 1):method_1(unit, start_of_week(object_to_str(today), 1)), start_of_week(object_to_str(today), 2):method_1(unit, start_of_week(object_to_str(today), 2)), start_of_week(object_to_str(today), 3):method_1(unit, start_of_week(object_to_str(today), 3))}
    
    elif method == 2:
        print("\n*** Method 2 has been " + str(accuracy) + "% accurate over the past three weeks. ***\n")
        return {"unit":unit,"method":method, "accuracy":accuracy, start_of_week(object_to_str(today), 1):method_2(unit, start_of_week(object_to_str(today), 1)), start_of_week(object_to_str(today), 2):method_2(unit, start_of_week(object_to_str(today), 2)), start_of_week(object_to_str(today), 3):method_2(unit, start_of_week(object_to_str(today), 3))}
    
    elif method == 3:
        print("\n*** Method 3 has been " + str(accuracy) + "% accurate over the past three weeks. ***\n")
        return {"unit":unit,"method":method, "accuracy":accuracy, start_of_week(object_to_str(today), 1):method_3(unit, start_of_week(object_to_str(today), 1)), start_of_week(object_to_str(today), 2):method_3(unit, start_of_week(object_to_str(today), 2)), start_of_week(object_to_str(today), 3):method_3(unit, start_of_week(object_to_str(today), 3))}
    
    elif method == 4:
        print("\n*** Method 4 has been " + str(accuracy) + "% accurate over the past three weeks. ***\n")
        return {"unit":unit,"method":method, "accuracy":accuracy, start_of_week(object_to_str(today), 1):method_4(unit, start_of_week(object_to_str(today), 1)), start_of_week(object_to_str(today), 2):method_4(unit, start_of_week(object_to_str(today), 2)), start_of_week(object_to_str(today), 3):method_4(unit, start_of_week(object_to_str(today), 3))}
    
    else:
        return "something's gone horribly wrong"

#RUN FUNCTIONS HERE
print(method_4("Midvale Home & Auto"))


 