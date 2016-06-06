import pandas as pd
import numpy as np
import sys


def go(read_fn, write_fn):
    #take the Date and Time strings and convert into one datetime column
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
    chunksize = 10000
    dfc = pd.read_csv(read_fn, chunksize = chunksize, header=3, parse_dates={'datetime': ['Date', 'Start Time']}, \
                      date_parser = dateparse, index_col=0)

    df = pd.concat([chunk for chunk in dfc])

    #### Remove readings from 'BLANK' meter and offline (eg. '(R0)') meters
    df = df[~df.Meter.str.contains("BLANK")]

    #### Strip number (eg. '(B1)', '(U1)') from meter name to aggregate by building
    df.Meter = df.Meter.map(lambda x: " ".join(x.split()[:-1]))

    hourly_temp = df.resample("H", label = "right", closed = "right").apply({"Temperature": "mean"})

    #### New data frame with each building as a column
    df_final = pd.DataFrame(columns = df.Meter.unique(), index = hourly_temp.index)
    df_final['Temperature'] = hourly_temp.Temperature

    #### Sample building data hourly and add to new dataframe
    for meter in df.Meter.unique():
        building = df[df.Meter == meter]
        agg_data = building.resample("H", label = 'right', closed = 'right').apply(\
                                        {'Usage': sum})
        df_final[building.Meter[1]] = agg_data


    #### Resample into average hourly use by days of the week
    days = {0:"Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday", 5:"Saturday", 6:"Sunday"}
    df_final["Day"] = df_final.index.weekday
    df_final["Day"] = df_final.Day.map(days)

    #### Set index to time only
    df_final.index = df_final.index.time

    #### Create data frame with data aggregated by weekday
    average_by_weekday = pd.DataFrame()
    for day in days.values():
        day_frame = df_final[df_final.Day == day]
        day_frame = day_frame.groupby(day_frame.index).mean()
        day_frame["Day"] == day
        average_by_weekday = pd.concat([average_by_weekday, day_frame])

    #### Loop through buildings to create line with hourly average by weekday
    day_labels = ["M", "T", "W", "Th", "F", "Sa", "Su"]
    for building, val in average_by_weekday.iteritems():
        end = len(val)
        # Create labels for six hour intervals
        labels = [val.index.levels[1][i] for i in range(0, 24, 6)] * 7
        x = range(end)

        fig, ax1 = plt.subplots()
        # First axis with hourly data
        ax1.plot(x, val)
        ax1.set_xlim(0,168)
        ax1.set_xticks(range(0, 168, 6))
        ax1.set_xticklabels(labels, rotation = 75)
        ax1.set_ylabel('Average Energy Usage [kWh]', fontsize = 12)
        ax1.set_xlabel('Time', fontsize = 12)

        # Second axis with weekday on top of graph
        ax2 = ax1.twiny()
        ax2.set_xlim(ax1.get_xlim())
        halfday = end/14
        ax2.set_xticks(np.arange(halfday, end + halfday, end/7))
        for i in ticks:
            ax2.axvline(i, color='k', linestyle='--')
        ax2.set_xticklabels(day_labels)

        fig.tight_layout()
        fig.set_figwidth(25)
        fig.set_figheight(15)

        # Set title and save graphs
        plt.title('UChicago - %s - Hourly Average by Weekdays' % building, y = 1.05, fontsize = 16)
        plt.savefig('./weekplots/averageWeek%s.png' % building.replace(' ', '_').replace('/', '_'))
        plt.close()
