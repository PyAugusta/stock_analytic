from . import consolidate
from analytic import assets
from analytic.assets import utils
from datetime import datetime as dt
import pandas as pd
import pandas_datareader.data as web
import requests
import os


def gather_sp500_list(refresh=False):
    """Gets a list of tickers for companies on the S&P 500."""
    if refresh or not os.path.exists(assets.sp500_list_pickle):
        resp = requests.get(assets.sp500_list_url)
        sp500 = assets.parse_sp500_list_resp(resp)
    else:
        sp500 = utils.unpickle_it(assets.sp500_list_pickle)
    return sp500


def gather_yahoo_data(begin_date, end_date, refresh_tickers=False, save_dfs=True):
    """Gets Yahoo Finance API results"""
    tickers = gather_sp500_list(refresh=refresh_tickers)
    dfs = []
    print('getting yahoo finance data for {} tickers'.format(len(tickers)))
    count = 1
    for ticker in tickers:
        print('{}/{}: '.format(count, len(tickers)), ticker)
        try:
            print('')
            df = web.DataReader(ticker, "yahoo", begin_date, end_date)
            df['Ticker'] = ticker
            df.reset_index(inplace=True)
            dfs.append(df)
            if save_dfs:
                fn = os.path.join(assets._yahoo, '{}.pickle'.format(ticker))
                utils.pickle_it(df, fn)
        except:
            continue
    data = pd.concat(dfs)
    data.reset_index(inplace=True, drop=True)
    return data


def gather_yahoo_historical(years_back=1, refresh=False):
    """Gets the Yahoo Finance API results for the previous years_back until now"""
    end_date = dt.utcnow()
    begin_date = dt(end_date.year - years_back, end_date.month, end_date.day)
    if refresh or not os.path.exists(assets.yahoo_historical_pickle):
        data = gather_yahoo_data(begin_date, end_date, refresh_tickers=refresh)
        utils.pickle_it(data, assets.yahoo_historical_pickle)
    else:
        data = utils.unpickle_it(assets.yahoo_historical_pickle)
    return data


def gather_nyt_data(begin_date, end_date=dt.utcnow(), date_column='pub_date', **params):
    """Gets articles from the New York Times using data/filters
    defined in assets"""
    # normalize dates
    begin_date = dt.strftime(begin_date, assets.nyt_dt_fmt)
    end_date = dt.strftime(end_date, assets.nyt_dt_fmt)

    query = assets.nyt_query.copy()
    query['begin_date'] = begin_date
    query['end_date'] = end_date

    if params:
        query.update(params)
    resp = requests.get(assets.nyt_search_url, params=query)
    data_json = assets.parse_nyt_resp(resp)
    data = pd.DataFrame(data_json)
    if len(data) == 0:
        return pd.DataFrame()
    data[date_column] = pd.to_datetime(data[date_column])
    data.reset_index(inplace=True, drop=True)
    return data


def gather_nyt_historical(years_back=1, refresh=False, **params):
    """Gets the New York Times articles for the previous years_back until now"""
    end_date = dt.utcnow()
    begin_date = dt(end_date.year - years_back, end_date.month, end_date.day)
    if refresh or not os.path.exists(assets.nyt_historical_pickle):
        data = gather_nyt_data(begin_date, end_date, **params)
        utils.pickle_it(data, assets.nyt_historical_pickle)
    else:
        data = utils.unpickle_it(assets.nyt_historical_pickle)
    return data


def gather_nyt_docs_since_close(close_date):
    close_date = dt(close_date.year, close_date.month, close_date.day)
    data = gather_nyt_data(close_date)
    if len(data) == 0:
        return pd.DataFrame()
    close_datetime = dt(
        close_date.year, close_date.month, close_date.day,
        hour=assets.nyse_close_utc['hour'],
        minute=assets.nyse_close_utc['minute']
    )
    data_filtered = data[data['pub_date'] >= pd.Timestamp(close_datetime)]
    docs = data_filtered['snippet'].unique()
    df = pd.DataFrame(columns=['docs'], index=[pd.Timestamp(close_date)])
    df['docs'][0] = docs
    return df


def gather_nyt_yahoo(begin_date, end_date=dt.utcnow()):
    nyt = gather_nyt_data(begin_date, end_date)
    yahoo = gather_yahoo_data(begin_date, end_date)
    data = consolidate.join_nyt_yahoo(nyt, yahoo)
    return data


def gather_nyt_yahoo_historical(years_back=1, refresh=False):
    nyt = gather_nyt_historical(years_back=years_back, refresh=refresh)
    yahoo = gather_yahoo_historical(years_back=years_back, refresh=refresh)
    data = consolidate.join_nyt_yahoo(nyt, yahoo)
    utils.pickle_it(data, assets.nyt_yahoo_historical_pickle)
    return data
