#Imports
import pandas as pd
import datetime as dt
import pyodbc

# Global variable definitions
today = dt.date.today()

# Helper function definitions
def day_to_interval(str_day):
    """
    Takes a day of week written out fully and converts it to an interval, with Sunday being 1.
    """
    int_day = 1
    if str_day == "Monday":
        int_day = 2
    elif str_day == "Tuesday":
        int_day = 3
    elif str_day == "Wednesday":
        int_day = 4
    elif str_day == "Thursday":
        int_day = 5
    elif str_day == "Friday":
        int_day = 6
    elif str_day == "Saturday":
        int_day = 7

    return int_day

# Defines SQL Queries
vol_by_interval_sql = """
Select term.DateTime, cast(term.DateTime as date) as 'Date', dateadd(minute, floor(datepart(minute,term.DateTime)/15)*15,dateadd(hour,datepart(hour,DateTime),convert(datetime,cast(term.DateTime as date)))) as 'Date_Interval', cast(dateadd(minute, floor(datepart(minute,term.DateTime)/15)*15,dateadd(hour,datepart(hour,DateTime),convert(datetime,cast(term.DateTime as date)))) as time) as 'Interval', dpq.Dept_Name 
from OperationalDataStore.AcqCiscoAW.Termination_Call_Detail as term
left join OperationalDataStore.ArcCiscoAW.V_CallDataCisco_Dim_Precision_Queue dpq
	on REPLACE(dpq.PrecisionQueueID, '~', '') = concat(term.PrecisionQueueID, ODSDataSourceID)
Where term.DateTime > getdate()-30
And dpq.Dept_Name = ?
And datepart(weekday,term.DateTime) = ?
"""

# Main function(s)
def pattern_forecast(unit, DOW):
    """
    Returns the average % of total day volume for each interval for the unit and day of week specified over the last 30 days.
    """
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=aahssdbods.amfam.com;DATABASE=OperationalDataStore;Trusted_Connection=yes')
    cursor = conn.cursor()
    if unit == "Experts":
        df1 = pattern_forecast('Sales Experts', DOW)
        df2 = pattern_forecast('Client Service Experts', DOW)
        df_concat = pd.concat((df1, df2))
        by_row_index = df_concat.groupby(df_concat['Interval'])
        df_means = by_row_index.mean().reset_index(names='Interval')
        average_dict = dict(zip(df_means.Interval, df_means.Volume_Pattern))
        return average_dict
    DOW = day_to_interval(DOW)
    cursor.execute(vol_by_interval_sql, unit, DOW)
    term_list = cursor.fetchall()
    df = pd.DataFrame.from_records(term_list, columns=[col[0] for col in cursor.description])
    df = df.groupby(['Date_Interval', 'Date', 'Dept_Name', 'Interval'],as_index=False).count()
    df = df.rename(columns={'DateTime': 'Volume'})
    num_dates = len(pd.unique(df['Date']))
    total_volume_by_date = df.groupby('Date')['Volume'].sum().reset_index(name='TotalVolume')
    df = df.merge(total_volume_by_date, on='Date')
    df['Volume_Pattern'] = (df['Volume'] / df['TotalVolume'])
    average_df = df.groupby('Interval').sum().reset_index(names='Interval')
    average_df['Interval'] = pd.to_datetime(average_df['Interval']).dt.strftime('%H:%M')
    average_df['Volume_Pattern'] = (average_df['Volume_Pattern']/num_dates)*10000
    if unit == "Sales Experts" or unit == "Client Service Experts":
        return average_df
    average_dict = dict(zip(average_df.Interval, average_df.Volume_Pattern))
    conn.close()
    return average_dict
