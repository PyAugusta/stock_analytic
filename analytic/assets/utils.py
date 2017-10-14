from datetime import timedelta as delta
import urllib.parse as urlparse
import pickle


def pickle_it(obj, fn):
    with open(fn, 'wb') as p:
        pickle.dump(obj, p)
    return


def unpickle_it(fn):
    with open(fn, 'rb') as p:
        obj = pickle.load(p)
    return obj


def parse_url_query(url):
    query = dict(urlparse.parse_qsl(urlparse.urlsplit(url).query))
    return query


def split_date_range(begin_date, end_date, chunks):
    days = (end_date - begin_date).days
    days_per_chunk = days//chunks
    ranges = []
    on_date = begin_date
    while on_date < end_date:
        end = on_date + delta(days=days_per_chunk - 1)
        if end > end_date:
            ranges.append((on_date, end_date))
        else:
            ranges.append((on_date, end))
        on_date = end + delta(days=1)
    return ranges
