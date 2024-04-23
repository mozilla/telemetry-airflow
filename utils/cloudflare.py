""" FUNCTIONS FOR INTERACTING WITH CLOUDFARE """
#Load libraries
from datetime import datetime, timedelta
import pandas as pd

#### PART 1 - FUNCTIONS FOR GENERATING URL TO HIT 
def generate_device_type_timeseries_api_call(strt_dt, end_dt, agg_int, location):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    if location == 'ALL':
        device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, strt_dt, end_dt, agg_int)
    else:
        device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, strt_dt, end_dt, location, agg_int)
    return device_usage_api_url


def generate_os_timeseries_api_call(strt_dt, end_dt, agg_int, location, device_type):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    if location == 'ALL' and device_type == 'ALL':
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, agg_int)
    elif location != 'ALL' and device_type == 'ALL':
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, agg_int)
    elif location == 'ALL' and device_type != 'ALL':
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&deviceType=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, device_type, agg_int)
    else:
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&deviceType=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, device_type, agg_int)
    return os_usage_api_url


def generate_browser_api_call(strt_dt, end_dt, device_type, location, op_system, user_typ):
    """ Generates the API URL"""
    #USER TYPE
    if user_typ == 'ALL':
        user_type_string = ''
    else:
        user_type_string = '&botClass=%s' % user_typ
    #LOCATION
    if location == 'ALL':
        location_string = ''
    else:
        location_string = '&location=%s' % location
    #OP SYSTEM
    if op_system == 'ALL':
        op_system_string = ''
    else:
        op_system_string = '&os=%s' % op_system
    
    #Device type
    if device_type == 'ALL':
        device_type_string = ''
    else:
        device_type_string = '&deviceType=%s' % device_type

    browser_api_url = "https://api.cloudflare.com/client/v4/radar/http/top/browsers?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z%s%s%s%s&format=json" % (strt_dt, end_dt, device_type_string, location_string, op_system_string, user_type_string)
    return browser_api_url


#### PART 2 - FUNCTIONS FOR PARSING RESPONSE JSON
def parse_device_type_timeseries_response_human(result):
    """ Takes the response JSON and returns parsed information"""
    ### HUMAN
    human_timestamps = result['human']['timestamps']
    human_desktop = result['human']['desktop']
    human_mobile = result['human']['mobile']
    human_other = result['human']['other']
    return human_timestamps, human_desktop, human_mobile, human_other


def parse_device_type_timeseries_response_bot(result):
    """ Takes the response JSON and returns parsed information"""
    ### BOT 
    bot_timestamps = result['bot']['timestamps']
    bot_desktop = result['bot']['desktop']
    bot_mobile = result['bot']['mobile']
    bot_other = result['bot']['other']
    return bot_timestamps, bot_desktop, bot_mobile, bot_other


def parse_timeseries_response_metadata(result):
    """ Takes the response JSON and returns parsed information"""
    ### METADATA
    confidence_level = result['meta']['confidenceInfo']['level']
    aggregation_interval = result['meta']['aggInterval']
    normalization = result['meta']['normalization']
    last_updated = result['meta']['lastUpdated']
    return confidence_level, aggregation_interval, normalization, last_updated


def parse_response_metadata(result): 
    """ Takes the response JSON and returns parsed information"""
    ### METADATA
    confidence_level = result['meta']['confidenceInfo']['level']
    normalization = result['meta']['normalization']
    last_updated = result['meta']['lastUpdated']
    return confidence_level, normalization, last_updated


def parse_browser_usg_start_and_end_time(result):
    """ HELLO """
    strt_time = result['meta']['dateRange'][0]['startTime']
    end_time = result['meta']['dateRange'][0]['endTime']
    return strt_time, end_time


#Generate the result dataframe
def make_device_usage_result_df(user_type, desktop, mobile, other, timestamps, last_upd, norm, conf, agg_interval, location):
    """ User type = string, desktop, mobile, other, timestamps, arrays, """
    return pd.DataFrame({'Timestamp': timestamps,
                'UserType': [user_type] * len(timestamps),
                'Location': [location] * len(timestamps),
                'DesktopUsagePct': desktop,
                'MobileUsagePct': mobile,
                'OtherUsagePct': other,
                'ConfLevel': [conf]* len(timestamps),
                'AggInterval': [agg_interval] * len(timestamps),
                'NormalizationType': [norm] * len(timestamps),
                'LastUpdated': [last_upd] * len(timestamps)
                })

#### PART 3 - OTHER
###NOTE - this function should be used in device & OS but not browser
def get_timeseries_api_call_date_ranges(start_date, end_date, max_days_interval): 
    """ Input start date, end date as string in YYYY-MM-DD format, max days interval as int, and returns a dataframe of intervals to use"""

    #Initialize arrays we will later use to make the dataframe
    sd_array = []
    ed_array = []

    #Convert the start date and end date to date objects
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    #Calculate # of days between start date and end date
    days_between_start_dt_and_end_dt = (end_dt - start_dt).days

    #Divide the # of days by how the max # of days you want in each API request
    nbr_iterations = int(days_between_start_dt_and_end_dt/max_days_interval) - 1

    #Initialize the current start date with the start date
    current_start_dt = start_dt

    #For each iteration
    for i in range(nbr_iterations):        
        #Append current start dt 
        sd_array.append(str(current_start_dt))
        #Calculate end date
        current_end_date = current_start_dt + timedelta(days=max_days_interval)
        if current_end_date <= end_dt:
            ed_array.append(str(current_end_date))
        else: 
            ed_array.append(str(end_dt))

        #Update the current start date to be 1 day after the last end date
        current_start_dt = current_end_date + timedelta(days = 1)

    dates_df = pd.DataFrame({"Start_Date": sd_array,
                             "End_Date": ed_array})
    return dates_df

def get_non_timeseries_api_call_date_ranges(start_date, end_date):
    """ Input start date, end date as string in YYYY-MM-DD format, max days interval as int, and returns a dataframe of intervals to use"""

    #Initialize arrays we will later use to make the dataframe
    sd_array = []
    ed_array = []

    #Convert the start date and end date to date objects
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    #Calculate # of days between start date and end date
    days_between_start_dt_and_end_dt = (end_dt - start_dt).days

    #Divide the # of days by how the max # of days you want in each API request
    nbr_iterations = int(days_between_start_dt_and_end_dt) - 1

    #Initialize the current start date with the start date
    current_start_dt = start_dt

    #For each iteration
    for i in range(nbr_iterations+1):
        #Append current start dt
        sd_array.append(str(current_start_dt))
        #Calculate end date
        current_end_date = current_start_dt + timedelta(days=1)
        if current_end_date <= end_dt:
            ed_array.append(str(current_end_date))
        else: 
            ed_array.append(str(end_dt))

        #Update the current start date to be 1 day after the last end date
        current_start_dt = current_end_date

    dates_df = pd.DataFrame({"Start_Date": sd_array,
                             "End_Date": ed_array})
    return dates_df