import pandas as pd

def get_pbp_data(year):
    data = pd.read_csv(f'data/pbp_{year}.csv.gz')
    return data