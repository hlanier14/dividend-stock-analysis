from google.cloud import bigquery
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = 'portfolio-website-378500'
DATASET_ID = 'dividend_stocks'
LOCATION = 'us-central1'

PRICE_TABLE = 'prices'
DIVIDEND_TABLE = 'dividends'
DIVIDEND_META_TABLE = 'dividend_metadata'
COMPANY_TABLE = 'companies'
FINANCIALS_TABLE = 'company_financials'
PRICE_METADATA_TABLE = 'price_metadata'
BENCHMARK_METADATA_TABLE = 'benchmark_metadata'

PRICE_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{PRICE_TABLE}'
PRICE_METADATA_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{PRICE_METADATA_TABLE}'
DIVIDEND_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_TABLE}'
DIVIDEND_META_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_META_TABLE}'
COMPANY_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{COMPANY_TABLE}'
FINANCIALS_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_TABLE}'
BENCHMARK_METADATA_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{BENCHMARK_METADATA_TABLE}'


def main(request):

    bg_client = bigquery.Client()

    dividend_metadata_job = bg_client.query(f"SELECT * FROM `{DIVIDEND_META_TABLE_ID}` WHERE consecutiveYears >= 5;")
    dividend_metadata_job.result()
    dividend_metadata = dividend_metadata_job.to_dataframe()
    dividend_metadata.fillna(-1, inplace=True)

    tickers = dividend_metadata['ticker'].to_list()

    company_data_job = bg_client.query(f"SELECT * FROM `{COMPANY_TABLE_ID}` WHERE ticker IN UNNEST({tickers});")
    company_data_job.result()
    company_data = company_data_job.to_dataframe()
    company_data.fillna(-1, inplace=True)

    financials_data_job = bg_client.query(f"SELECT * FROM `{FINANCIALS_TABLE_ID}` WHERE ticker IN UNNEST({tickers});")
    financials_data_job.result()
    financials_data = financials_data_job.to_dataframe()
    financials_data['exDividendDate'] = pd.to_datetime(financials_data['exDividendDate'])
    financials_data['exDividendDate'] = financials_data['exDividendDate'].dt.strftime('%Y-%m-%d')
    financials_data.fillna(-1, inplace=True)

    dividend_data_job = bg_client.query(f"SELECT * FROM `{DIVIDEND_TABLE_ID}` WHERE ticker IN UNNEST({tickers}) AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR);")
    dividend_data_job.result()
    dividend_data = dividend_data_job.to_dataframe()
    dividend_data['date'] = pd.to_datetime(dividend_data['date'])
    dividend_data.fillna(-1, inplace=True)

    price_data_job = bg_client.query(f"SELECT * FROM `{PRICE_TABLE_ID}` WHERE ticker IN UNNEST({tickers}) AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY);")
    price_data_job.result()
    price_data = price_data_job.to_dataframe()
    price_data['date'] = pd.to_datetime(price_data['date'])
    price_data.fillna(-1, inplace=True)

    price_metadata_job = bg_client.query(f"SELECT * FROM `{PRICE_METADATA_TABLE_ID}` WHERE ticker IN UNNEST({tickers});")
    price_metadata_job.result()
    price_metadata = price_metadata_job.to_dataframe()
    price_metadata.fillna(-1, inplace=True)

    benchmark_metadata_job = bg_client.query(f"SELECT * FROM `{BENCHMARK_METADATA_TABLE_ID}`;")
    benchmark_metadata_job.result()
    benchmark_metadata = benchmark_metadata_job.to_dataframe()
    benchmark_metadata.fillna(-1, inplace=True)

    data = []
    for ticker in tickers:

        ticker_prices = price_data.copy()
        ticker_prices = ticker_prices.loc[ticker_prices['ticker'] == ticker]
        last_price = ticker_prices.sort_values(by='date', ascending=False).iloc[0]['price']
        ticker_prices['date'] = ticker_prices['date'].dt.strftime('%Y-%m-%d')

        price = {
            'last': last_price,
            'historical': ticker_prices[['date', 'price']].to_dict('records')
        }

        ticker_dividends = dividend_data.copy()
        ticker_dividends = ticker_dividends.loc[ticker_dividends['ticker'] == ticker]
        last_dividend = ticker_dividends.sort_values(by='date', ascending=False).iloc[0]['dividend']
        ticker_dividends['date'] = ticker_dividends['date'].dt.strftime('%Y-%m-%d')

        dividends = {
            'last': last_dividend,
            'metadata': dividend_metadata.loc[dividend_metadata['ticker'] == ticker, ~dividend_metadata.columns.isin(['ticker'])].to_dict('records')[0],
            'historical': ticker_dividends[['date', 'dividend']].to_dict('records')
        }

        beta = price_metadata.loc[price_metadata['ticker'] == ticker, ~price_metadata.columns.isin(['ticker'])].to_dict('records')[0]['fiveYearBeta']
        risk_free_rate = benchmark_metadata.loc[benchmark_metadata['name'] == 'Latest 10 YR T-Bill Rate', ~benchmark_metadata.columns.isin(['name'])].to_dict('records')[0]['value']
        exp_market_rate = benchmark_metadata.loc[benchmark_metadata['name'] == 'S&P 500 5 Year CAGR', ~benchmark_metadata.columns.isin(['name'])].to_dict('records')[0]['value']

        required_rate = {
            'value': risk_free_rate + beta * (exp_market_rate - risk_free_rate),
            'components': {
                'riskFreeRate': risk_free_rate,
                'expectedMarketRate': exp_market_rate,
                'beta': beta
            }
        }

        valuation = (dividends['last'] * dividends['metadata']['dividendFrequency']) / (required_rate['value'] - dividends['metadata']['fiveYearCAGR'])

        ddm = {
            'value': valuation,
            'pctChange': (valuation - price['last']) / price['last'],
            'components': {
                'forwardDividends': dividends['last'] * dividends['metadata']['dividendFrequency'],
                'requiredRate': required_rate['value'],
                'dividendGrowth': dividends['metadata']['fiveYearCAGR']
            }
        }

        ticker_data = company_data.loc[company_data['ticker'] == ticker].to_dict('records')[0]
        ticker_data['financials'] = financials_data.loc[financials_data['ticker'] == ticker, ~financials_data.columns.isin(['ticker'])].to_dict('records')[0]
        ticker_data['dividends'] = dividends
        ticker_data['price'] = price
        ticker_data['requiredRate'] = required_rate
        ticker_data['ddm'] = ddm

        data.append(ticker_data)

    response = json.dumps(data)
    
    # response.headers.add('Access-Control-Allow-Origin', '*')
    # response.headers.add('Access-Control-Allow-Methods', 'GET')
    # response.headers.add('Access-Control-Allow-Headers', 'Content-Type')

    return response, 200

if __name__ == "__main__":
    main({})
