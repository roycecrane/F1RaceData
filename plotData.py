import pandas
import matplotlib.pyplot as plt

def plot_data(df):
    data = df
    grouped_data = data.groupby('driver_code')

    plt.figure()
    if 'sector_number' in df:
        for driver, group in grouped_data:
            plt.plot(group['flap'], group['gap'], label=group['driver_code'].values[0])

        plt.xlabel('total_time')
        plt.ylabel('gap')
        plt.title('event_name')
        plt.legend(title='driver_code')
        plt.legend(loc='upper left')

    else:
        for driver, group in grouped_data:
            plt.plot(group['lap_number'], group['gap'], label=group['driver_code'].values[0])

        plt.xlabel('lap')
        plt.ylabel('gap')
        plt.title('event_name')
        plt.legend(title='driver_code')
        plt.legend(loc='upper left')

def show():
    plt.show()