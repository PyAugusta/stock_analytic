from . import text_classifier
from analytic.assets import utils
from analytic import assets
from analytic import gather
import pandas as pd


def get_data(refresh=False):
    data = gather.gather_nyt_yahoo_historical(refresh=refresh)
    return data


def calculate_growth(data):
    data['growth'] = data['Close'] - data['Open']
    return data


def score_docs(data, docs_column='docs'):
    classifier = text_classifier.get_classifier()

    def get_pos_neg(doc_arr):
        counts = {'positive': 0, 'negative': 0}
        for doc in doc_arr:
            feats = text_classifier.word_feats(doc)
            which = classifier.classify(feats)
            if which == 'neg':
                counts['negative'] += 1
            elif which == 'pos':
                counts['positive'] += 1
        return counts

    merged = data.merge(data[docs_column].apply(lambda docs: pd.Series(get_pos_neg(docs))), left_index=True, right_index=True)
    return merged


def correlate_data(data, method='pearson', delim_column='Ticker', columns=['growth', 'positive', 'negative']):
    grouped = data.groupby(delim_column)
    corr_dfs = []
    for delim, group in grouped:
        df = group[columns]
        corr_df = df.corr(method)
        corr_df[delim_column] = delim
        corr_dfs.append(corr_df)
    return pd.concat(corr_dfs)


def analyze_historical_data(correlation_method='pearson', refresh=False):
    pickle_path = assets.analyzed_historical_pickle.format(correlation_method)
    data = get_data(refresh=refresh)
    data = calculate_growth(data)
    data = score_docs(data)
    corr_matrix = correlate_data(data, method=correlation_method)
    utils.pickle_it(corr_matrix, pickle_path)
    return corr_matrix
