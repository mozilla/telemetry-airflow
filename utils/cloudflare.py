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