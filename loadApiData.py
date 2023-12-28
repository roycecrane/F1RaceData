import sys

import fastf1 as ff1
from fastf1 import ergast as eg
import pandas as pd
import os


def load_data_api(year, race_number):
    # enable cache
    # directory: load_ff1_data/cache is created if it does not exist
    enable_cache('cache')

    df = get_ergast_data(year, race_number)
    # df now has columns: driver_name, lap_number,
    #                     total_time, driver_number,
    #                     driver_code, grid, laps,
    #                     podium, did_pit, lap_time

    # if year is greater than 2017 load ff1 specific data
    if year >= 2018:
        ff1_data = get_ff1_data(year, race_number)
        # ff1_data has columns: driver_code, lap_number,
        #                       sector_time, sector_number,
        #                       compound, track_status

        # merge the ergast data with ff1_data
        df = pd.merge(df, ff1_data, how='outer', on=['driver_code', 'lap_number'])
        df = calculate_flap(df)

    # get total_time column
    df = calc_total_time(df)
    # get_position_data function must be called after calc_total_time()
    df = get_position_data(df)

    return df


def get_ergast_data(year, race_number):
    max_value = 1000000
    ergast = eg.Ergast()
    # get all needed data from ff1
    lap_data = ergast.get_lap_times(season=year, round=race_number, limit=max_value).content[0]
    driver_data = ergast.get_driver_info(season=year, round=race_number)
    session_data = ergast.get_race_results(season=year, round=race_number).content[0]
    pit_data = ergast.get_pit_stops(season=year, round=race_number, limit=max_value).content[0]
    # filter out unneeded columns
    session_data = session_data.filter(['grid',
                                        'laps',
                                        'driverId',
                                        'position'])

    driver_data = driver_data.filter(['driverId',
                                      'driverNumber',
                                      'driverCode'])
    lap_data = lap_data.filter(['driverId',
                                'number',
                                'time'])

    # filter pit stops
    # pit_data df only contains laps where drivers pitted
    pits = pit_data.filter(['driverId', 'lap'])
    pits['did_pit'] = 'P'
    # rename 'lap' column in the pits df to number, so it can be merged with the other data
    pits = pits.rename(columns={'lap': 'number'})

    # merge all the data in to one dataframe
    # out_df is the df returned by this function
    out_df = pd.merge(driver_data, session_data, how='outer', on=['driverId'])
    out_df = pd.merge(lap_data, out_df, how='outer', on=['driverId'])
    out_df = pd.merge(out_df, pits, how='outer', on=['driverId', 'number'])
    # convert 'time' column from datetime to number of seconds
    out_df['LapTime'] = out_df.apply(lambda x: x['time'].total_seconds(), axis=1)
    # rename columns
    out_df = out_df.rename(columns={'driverId': 'driver_name',
                                    'number': 'lap_number',
                                    'driverNumber': 'driver_number',
                                    'driverCode': 'driver_code',
                                    'position': 'podium',
                                    'time': 'total_time',
                                    'LapTime': 'lap_time'})

    return out_df


def get_ff1_data(year, race_number):
    # get ff1 data from api

    session = ff1.get_session(year, race_number, 'R')
    session.load(telemetry=False, weather=False, messages = False)
    # messages = False, livedata = False
    sector_data = session.laps
    # load sector data
    sector_data = sector_data.filter(['Driver',
                                      'LapNumber',
                                      'LapTime',
                                      'Sector1Time',
                                      'Sector2Time',
                                      'Sector3Time',
                                      'Compound',
                                      'TrackStatus'])
    # convert datetimes to total seconds
    sector_data['LapTime'] = sector_data.apply(lambda x: x['LapTime'].total_seconds(), axis=1)
    sector_data['Sector2Time'] = sector_data.apply(lambda x: x['Sector2Time'].total_seconds(), axis=1)
    sector_data['Sector3Time'] = sector_data.apply(lambda x: x['Sector3Time'].total_seconds(), axis=1)
    # calculate sector 1 from laptime and sector 2 and 3
    sector_data['Sector1Time'] = sector_data.apply(lambda x: x['LapTime'] - x['Sector3Time'] - x['Sector2Time'], axis=1)

    # move sector time columns to new rows in sector_time column
    # before ...LapNumber, Sector1Time, Sector2Time, Sector3Time...
    #            1         25.765         37.42       21.053
    # now: ...LapNumber, sector_names,   sector_time...
    #          1          'Sector1Time'   25.765
    #          1          'Sector2Time,   37.42
    #          1          'Sector3Time'   21.053
    # sector_time make new columns sector_names and
    out_df = pd.melt(sector_data,
                     id_vars=['Driver',
                              'LapNumber'],
                     value_vars=['Sector1Time',
                                 'Sector2Time',
                                 'Sector3Time'],
                     var_name='sector_names',
                     value_name='sector_time')

    # make a new column called sector_numbers
    # the apply function here gets the result of a lambda that
    # takes the sector number X at index 6 of the 'SectorXTime' string in the new sector_names column
    # the whole column is then cast as type int from string with astype('int')
    out_df['sector_number'] = out_df['sector_names'].apply(lambda x: x[6]).astype('int')
    # the out_df is merged with sector data
    out_df = pd.merge(out_df, sector_data, how='outer', on=['Driver', 'LapNumber'])
    # drop unnecessary columns
    # the ergast df provides the lap times
    out_df = out_df.drop(['Sector1Time', 'Sector2Time', 'Sector3Time', 'LapTime', 'sector_names'], axis=1)

    # rename columns
    out_df = out_df.rename(columns={'Driver': 'driver_code',
                                    'LapNumber': 'lap_number',
                                    'TrackStatus': 'track_status'})

    # the status dictionary is the codes mapped to the abbreviation of their meaning
    # C clear - (beginning of session ot to indicate the end of another status)
    # YF yellow flag
    # unknown - (??? Never seen so far, does not exist?)
    # SC safety car
    # RF red flag
    # VSC  - Virtual Safety Car deployed
    # Virtual Safety Car end - (As indicated on the drivers steering wheel,
    # on tv and so on; status ‘1’ will mark the actual end)
    status = {'1': 'C ', '2': 'YF ', '3': 'unknown ', '4': 'SC ', '5': 'RF ', '6': 'VSC ', '7': 'VSC end '}
    # the lambda  in the map funtion takes the status value from out_df eg '56'
    # then assigns that value to the meaning of the number using the satus dict
    # so '56' becomes 'RF VSC'

    out_df['track_status'] = \
        out_df['track_status'].map(lambda status_numbers:
                                   ''.join([status[i] for i in str(status_numbers)
                                            if i in status.keys()]))

    return out_df


def get_position_data(df):
    # sort by total time so df is in order of position at any given sector or lap
    df = df.sort_values(['total_time'])
    # true if race year > 2017 and df has ff1 data
    if 'sector_number' in df:
        # group by ['lap_number', 'sector_number'] and do cumulative count to
        # find out what position a driver is in at each sector
        df['position'] = df.groupby(['lap_number', 'sector_number']).cumcount() + 1

    else:
        # for race_year < 2018. same as before without sectors
        df['position'] = df.groupby(['lap_number']).cumcount() + 1
    # calc position gained
    df['pos_gained'] = df.apply(lambda row: row['grid'] - row['position'], axis=1)
    return df


def calc_total_time(df):
    # depending on race year cumulative sum will be on lap_time or sector_time
    if 'sector_time' in df:
        column_name = 'sector_time'
    else:
        column_name = 'lap_time'
    # loop through all the driver codes
    for driver_code in df['driver_code'].unique():
        # at each matching driver code make a new column
        # total_time that is the cumulative sum the lap_times or sector_times columns
        # this works because the dataframe comes sorted by driver, lap_num, sector_num
        df.loc[df['driver_code'] == driver_code, 'total_time'] = df.loc[
            df['driver_code'] == driver_code, column_name].cumsum()

    return df


def enable_cache(cache_file):
    # check if cache dir is in project directory
    if not os.path.isdir(cache_file):
        try:
            # if no cache dir make cache directory
            os.mkdir(cache_file)
        except OSError as error:
            # cannot make cache directory print error
            print(f'could not create cache file!\n{error}')
    # tell ff1 to enable cache
    ff1.Cache.enable_cache('cache')


def calculate_flap(df):
    if 'sector_number' in df:
        df['flap'] = df.apply(lambda x:
                              round(float(x['lap_number']) - 1.0 + float(x['sector_number']) / 3.0, 2),
                              axis=1)
    return df


def calculate_gap(df, comparison_df):
    if 'lap_number' and 'sector_number' and 'total_time' in df:
        column_values = ['lap_number', 'sector_number', 'total_time']
    elif 'lap_number' and 'sector_number' in df:
        column_values = ['lap_number', 'total_time']
    else:
        print('did not calculate gap no matching columns')
        return df

    comparison_df = comparison_df.filter(column_values)
    if 'comparison' in df:
        df = df.drop(['comparison'], axis=1)
    comparison_df = comparison_df.rename(columns={'total_time': 'comparison'})
    df = pd.merge(df, comparison_df, how='outer', on=column_values[:len(column_values) - 1])
    df['gap'] = df.apply(lambda row: row['total_time'] - row['comparison'], axis=1)

    return df
