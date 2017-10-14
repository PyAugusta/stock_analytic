import pandas as pd
import numpy as np
from analytic.assets import nyse_open_utc, nyse_close_utc


def to_nyse_open(dt64):
    return dt64 + np.timedelta64(nyse_open_utc['hour'], 'h') + np.timedelta64(nyse_open_utc['minute'], 'm')


def to_nyse_close(dt64):
    return dt64 + np.timedelta64(nyse_close_utc['hour'], 'h') + np.timedelta64(nyse_close_utc['minute'], 'm')


def yahoo_to_close_open_list(yahoo_df):
    # get the dates from yahoo_df
    dates = yahoo_df.Date.unique()
    first_close = to_nyse_close(dates[0] - np.timedelta64(1, 'D'))
    first_open = to_nyse_open(dates[0])
    first_day = dates[0]
    close2open = [(first_day, pd.Timestamp(first_close), pd.Timestamp(first_open))]
    for date in dates[1:]:
        close = to_nyse_close(date)
        open = to_nyse_open(date + np.timedelta64(1, 'D'))
        close2open.append((date, pd.Timestamp(close), pd.Timestamp(open)))
    return close2open


def join_nyt_yahoo(nyt_df, yahoo_df, target_column='snippet'):
    close_to_open = yahoo_to_close_open_list(yahoo_df)
    join_dfs = []
    for day, close, open in close_to_open:
        nyt_chunk = nyt_df[(nyt_df['pub_date'] > close) & (nyt_df['pub_date'] < open)]
        docs = nyt_chunk[target_column].unique()
        df = yahoo_df[yahoo_df['Date'] == day]
        df['docs'] = [docs]*len(df)
        join_dfs.append(df)
    joined = pd.concat(join_dfs)
    return joined

