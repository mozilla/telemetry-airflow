""" FUNCTIONS FOR INTERACTING WITH CLOUDFARE """
#Load libraries
import pandas as pd

#### PART 1 - FUNCTIONS FOR GENERATING URL TO HIT 
def generate_device_type_timeseries_api_call(strt_dt, end_dt, agg_int):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, strt_dt, end_dt, agg_int)
    return device_usage_api_url


def generate_os_timeseries_api_call(strt_dt, end_dt, agg_int):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, agg_int)
    return os_usage_api_url

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


#Generate the result dataframe
def make_result_df(user_type, desktop, mobile, other, timestamps, last_upd, norm, conf, agg_interval):
    """ User type = string, desktop, mobile, other, timestamps, arrays, """
    return pd.DataFrame({'Timestamp': timestamps,
                'UserType': [user_type] * len(timestamps),
                'DesktopUsagePct': desktop,
                'MobileUsagePct': mobile,
                'OtherUsagePct': other,
                'ConfLevel': [conf]* len(timestamps),
                'AggInterval': [agg_interval] * len(timestamps),
                'NormalizationType': [norm] * len(timestamps),
                'LastUpdated': [last_upd] * len(timestamps)
                })

