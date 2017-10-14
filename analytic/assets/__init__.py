from .utils import *
from datetime import datetime as dt
from dateutil.parser import parse
from time import sleep
from bs4 import BeautifulSoup
import requests
import os


_here = os.path.abspath(os.path.dirname(__file__))
_nyt = os.path.join(_here, 'nyt')
_yahoo = os.path.join(_here, 'yahoo')
_cleaned = os.path.join(_here, 'cleaned')
_classifiers = os.path.join(_here, 'classifiers')
_analysis = os.path.join(_here, 'analysis')


for directory in [_nyt, _yahoo, _cleaned, _classifiers, _analysis]:
    if not os.path.exists(directory):
        os.makedirs(directory)


nyt_search_url = 'http://api.nytimes.com/svc/search/v2/articlesearch.json'
nyt_api_key = 'c88a5510a59d4e4c909472075ac603a1'
nyt_search_filter = 'section_name:("Business" "Financial")'
nyt_fields = 'snippet,pub_date'
nyt_dt_fmt = '%Y%m%d'
nyt_query = {
    'api-key': nyt_api_key,
    'fq': nyt_search_filter,
    'fl': nyt_fields,
}
nyt_historical_pickle = os.path.join(_nyt, 'nyt_historical.pickle')


sp500_list_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
sp500_list_pickle = os.path.join(_yahoo, 'sp500_list.pickle')
yahoo_historical_pickle = os.path.join(_yahoo, 'yahoo_historical.pickle')

nyt_yahoo_historical_pickle = os.path.join(_cleaned, 'nyt_yahoo_historical.pickle')

nyse_open_utc = {'hour': 13, 'minute': 30}
nyse_close_utc = {'hour': 20, 'minute': 0}

text_classifier_pickle = os.path.join(_classifiers, 'text_classifier.pickle')
analyzed_historical_pickle = os.path.join(_analysis, 'analyzed_historical_{}.pickle')


def parse_nyt_resp(resp_obj):
    if resp_obj.status_code != 200:
        if resp_obj.status_code == 429:
            print('API rate limit exceded. Waiting 2 seconds, then resending...')
            sleep(2)
            # resend
            resp_obj = requests.get(resp_obj.request.url)
        else:
            print('NYT request returned status {}:\n{}'.format(resp_obj.status_code, resp_obj.json()))
            return []
    try:
        resp_json = resp_obj.json()
    except:
        return []
    try:
        meta = resp_json['response']['meta']
    except KeyError:
        return []
    hits = meta['hits']
    print('got {} hits'.format(hits))
    if hits == 0:
        return []
    # if there are less than 10 hits, we're done
    if hits <= 10:
        return resp_json['response']['docs']
    # otherwise, we'll have to send more queries bc the results are paginated
    # with a max of 10 docs per query
    orig_query = parse_url_query(resp_obj.request.url)
    # on top of that, we are limited to 5000 results per date range
    # and one query per second
    if hits <= 5000:
        docs = resp_json['response']['docs']
        pages = hits // 10
        if hits % 10 > 0:
            pages += 1
        print('there are {} pages'.format(pages))
        for p in range(1, pages):
            print('page {}'.format(p + 1))
            query = orig_query.copy()
            query['page'] = p + 1
            sleep(1)
            resp = requests.get(nyt_search_url, params=query)
            try:
                p_docs = resp.json()['response']['docs']
                docs.extend(p_docs)
            except:
                continue
        print('...')
        return docs
    # if we've gotten this far, there are at least 5000 results
    # so we should split the query dates up
    orig_begin = parse(orig_query['begin_date'], ignoretz=True)
    orig_end = parse(orig_query['end_date'], ignoretz=True)
    ranges = split_date_range(orig_begin, orig_end, (hits//5000) + 1)
    docs = []
    for begin, end in ranges:
        print('getting articles from {} to {}'.format(begin, end))
        query = nyt_query.copy()
        query['begin_date'] = dt.strftime(begin, nyt_dt_fmt)
        query['end_date'] = dt.strftime(end, nyt_dt_fmt)
        sleep(1)
        resp = requests.get(nyt_search_url, params=query)
        r_docs = parse_nyt_resp(resp)
        docs.extend(r_docs)
    return docs


def parse_sp500_list_resp(resp_obj, save_list=True):
    soup = BeautifulSoup(resp_obj.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    if save_list:
        pickle_it(tickers, sp500_list_pickle)

    return tickers
