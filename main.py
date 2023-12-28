from loadLocalData import *
from loadApiData import load_data_api,calculate_gap
from plotData import *
import pandas as pd
# pip install pandas openpyxl

def main():
    year = 2018
    race_number = 11
    file_type = 'xlsx'

    if race_number < 10:
        file_name = f'{year}_0{race_number}'
    else:
        file_name = f'{year}_{race_number}'
    df = load_data_api(year, race_number)
    # df = df.sort_values(by=['grid', 'lap_number', 'sector_number'])
    save_data(df, file_name, file_type)

    df_leader = df.loc[df['position'].isin([1])]

    df_winner = df.loc[df['podium'].isin([1])]

    # df_leader = df.loc[df['driver_code'] == 'LEC']

    df = calculate_gap(df, df_leader)
    plot_data(df)
    df = calculate_gap(df, df_winner)
    plot_data(df)
    show()


if __name__ == '__main__':
    main()

