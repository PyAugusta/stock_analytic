from analytic import assets
from analytic.assets import utils
from analytic.learn import score_docs
from analytic.gather import gather_nyt_docs_since_close
import pandas as pd


def get_analyzed_data(correlation_pickle):
    data = utils.unpickle_it(correlation_pickle)
    return data


def find_promising_correlations(correlation_matrix,
                                delim_column='Ticker',
                                target_column='growth',
                                column_a='positive',
                                column_b='negative',
                                compare_func=lambda a, b: a < 0 < b):
    grouped = correlation_matrix.groupby(delim_column)
    promising = []
    for delim, group in grouped:
        a = group[target_column][group.index == column_a].unique()[0]
        b = group[target_column][group.index == column_b].unique()[0]
        if compare_func(a, b):
            group[delim_column] = delim
            promising.append(group)
    return pd.concat(promising)


def predict_growth(last_close_date, method='pearson', compare_func=lambda a, b: a < 0 < b):
    p = assets.analyzed_historical_pickle.format(method)
    correlation_matrix = get_analyzed_data(p)
    pos_growth = find_promising_correlations(
        correlation_matrix,
        column_a='positive',
        column_b='negative',
        compare_func=compare_func
    )
    neg_growth = find_promising_correlations(
        correlation_matrix,
        column_a='negative',
        column_b='positive',
        compare_func=compare_func
    )
    nyt_since_close = gather_nyt_docs_since_close(last_close_date)
    scored = score_docs(nyt_since_close)
    pos_count = scored['positive'][0]
    neg_count = scored['negative'][0]
    if pos_count > neg_count:
        tickers = pos_growth['Ticker'].unique()
        print(
            'the news has been mostly positive, which means the '
            'following tickers may see growth in the market: '
        )
        print(tickers)
        return tickers
    elif neg_count > pos_count:
        tickers = neg_growth['Ticker'].unique()
        print(
            'the news has been mostly negative, which means the '
            'following tickers may see growth in the market: '
        )
        print(tickers)
        return tickers
    else:
        print('the news has been both positive and negative')
        return []
