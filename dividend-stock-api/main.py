from google.cloud import bigquery
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

PRICE_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{PRICE_TABLE}'
DIVIDEND_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_TABLE}'
DIVIDEND_META_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_META_TABLE}'
COMPANY_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{COMPANY_TABLE}'
FINANCIALS_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_TABLE}'


def main(request):

    bg_client = bigquery.Client()

    dividend_metadata_job = bg_client.query(f"SELECT * FROM `{DIVIDEND_META_TABLE_ID}` WHERE consecutiveYears >= 5;")
    dividend_metadata_job.result()
    dividend_metadata = dividend_metadata_job.to_dataframe()

    tickers = dividend_metadata['ticker'].to_list()

    company_data_job = bg_client.query(f"SELECT * FROM `{COMPANY_TABLE_ID}` WHERE ticker IN UNNEST({tickers});")
    company_data_job.result()
    company_data = company_data_job.to_dataframe()

    financials_data_job = bg_client.query(f"SELECT * FROM `{FINANCIALS_TABLE_ID}` WHERE ticker IN UNNEST({tickers});")
    financials_data_job.result()
    financials_data = financials_data_job.to_dataframe()
    financials_data['exDividendDate'] = pd.to_datetime(financials_data['exDividendDate'])
    financials_data['exDividendDate'] = financials_data['exDividendDate'].dt.strftime('%Y-%m-%d')

    dividend_data_job = bg_client.query(f"SELECT * FROM `{DIVIDEND_TABLE_ID}` WHERE ticker IN UNNEST({tickers}) AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR);")
    dividend_data_job.result()
    dividend_data = dividend_data_job.to_dataframe()
    dividend_data['date'] = pd.to_datetime(dividend_data['date'])

    price_data_job = bg_client.query(f"SELECT * FROM `{PRICE_TABLE_ID}` WHERE ticker IN UNNEST({tickers}) AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY);")
    price_data_job.result()
    price_data = price_data_job.to_dataframe()
    price_data['date'] = pd.to_datetime(price_data['date'])

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
            'metadata': dividend_metadata.loc[dividend_metadata['ticker'] == ticker, ~dividend_metadata.columns.isin(['ticker'])].to_dict('records'),
            'historical': ticker_dividends[['date', 'dividend']].to_dict('records')
        }

        try:
            ticker_financials = financials_data.loc[financials_data['ticker'] == ticker, ~financials_data.columns.isin(['ticker'])].to_dict('records')[0]
        except:
            continue 

        ticker_data = company_data.loc[company_data['ticker'] == ticker].to_dict('records')[0]
        ticker_data['financials'] = ticker_financials
        ticker_data['dividends'] = dividends
        ticker_data['price'] = price

        data.append(ticker_data)

    return data, 200


if __name__ == '__main__':
    data = main({})
    print(data)
