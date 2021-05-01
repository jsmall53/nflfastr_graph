import pandas as pd
import pyreadr

from datetime import date
from multiprocessing import Pool
import os


def update_current_pbp_data():
    # TODO: improve this to fetch the newest data available (not necessarily the current year)
    #       probably need to make a wrapper to get the 'season year'
    year = data.today().year
    download_pbp_data(year)


def download_all_pbp_data():
    # this downloads all available play-by-play data from 1999-present
    years = get_all_years_iter()
    print("Updating play-by-play data...")
    download_parallel(download_pbp_data, years)
    print("done")


def download_all_schedule_data():
    years = get_all_years_iter()
    print("Updating schedule data...")
    download_parallel(download_schedule_data, years)
    print("done")


def download_pbp_data(year):
    try:
        data = pd.read_csv(
            'https://github.com/nflverse/nflfastR-data/blob/master/data/play_by_play_' \
            + str(year) + '.csv.gz?raw=True',compression='gzip', low_memory=False
            )
        data.to_csv(f'data/pbp_{year}.csv.gz', compression='gzip')
    except:
        print(f"couldnt read play-by-play data for {year}")


def download_schedule_data(year):
    try:
        rds_file = pyreadr.download_file(f'https://github.com/nflverse/nflfastR-data/blob/master/schedules/sched_{str(year)}.rds?raw=True',
                                        f'data/sched_{str(year)}.rds')
        rds_df = pyreadr.read_r(rds_file)
        df = rds_df[None]
        df.to_csv(f'data/sched_{year}.csv.gz', compression='gzip')
        # delete the rds file
        os.remove(rds_file)
    except:
        print(f"couldnt read schedule data for {year}")


def download_parallel(function, iterable):
    with Pool() as p:
        p.map(function, iterable)


def get_all_years_iter():
    i = date.today().year
    years = []
    while i >= 1999:
        years.append(i)
        i -= 1
    return years