from google.cloud import bigquery
import bs4 as bs
import requests
import yfinance as yf
import pandas as pd
from datetime import timedelta, datetime
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

load_dotenv()

app = Flask(__name__)
CORS(app, supports_credentials=True)

PROJECT_ID = 'portfolio-website-378500'
DATASET_ID = 'dividend_stocks'
LOCATION = 'us-central1'

PRICE_TABLE = 'prices'
DIVIDEND_TABLE = 'dividends'
DIVIDEND_METADATA_TABLE = 'dividend_metadata'
COMPANY_TABLE = 'companies'
BENCHMARK_TABLE = 'benchmarks'

PRICE_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{PRICE_TABLE}'
DIVIDEND_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_TABLE}'
DIVIDEND_METADATA_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_METADATA_TABLE}'
COMPANY_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{COMPANY_TABLE}'
BENCHMARK_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{BENCHMARK_TABLE}'

PRICE_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("price", "FLOAT")
]

DIVIDEND_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("dividend", "FLOAT")
]

DIVIDEND_METADATA_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("consecutiveYears", "INTEGER"),
    bigquery.SchemaField("fiveYearCAGR", "FLOAT"),
    bigquery.SchemaField("dividendFrequency", "INTEGER"),
    bigquery.SchemaField("fiveYearCV", "FLOAT"),
]

COMPANY_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("shortName", "STRING"),
    bigquery.SchemaField("longName", "STRING"),
    bigquery.SchemaField("website", "STRING"),
    bigquery.SchemaField("industry", "STRING"),
    bigquery.SchemaField("sector", "STRING"),
    bigquery.SchemaField("payoutRatio", "FLOAT"),
    bigquery.SchemaField("exDividendDate", "TIMESTAMP"),
    bigquery.SchemaField("forwardPE", "FLOAT"),
    bigquery.SchemaField("forwardEps", "FLOAT")
]

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)), 
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

bg_client = bigquery.Client()


def load_table_from_dataframe(bg_client: bigquery.Client, 
                              data: pd.DataFrame,
                              table_id: str,
                              schema: list,
                              write_disposition: str, 
                              ignore_unknown_values: bool) -> None:
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        write_disposition=write_disposition,
        ignore_unknown_values=ignore_unknown_values,
        schema=schema
    )

    load_job = bg_client.load_table_from_dataframe(
        data,
        table_id,
        location=LOCATION,
        job_config=job_config
    )

    try:
        load_job.result()
    except Exception as e:
        print(f"Error loading data into {table_id}: {e}")
        return None

def sp_tickers() -> list:
    response = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(response.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker.replace('\n', ''))
    return tickers

def slice_df(data: pd.DataFrame, last_date: datetime) -> pd.DataFrame:

    data = data.reset_index()
    data['Date'] = pd.to_datetime(data['Date'], utc=True)

    if not isinstance(last_date, pd._libs.tslibs.nattype.NaTType):
        start_date = last_date + timedelta(days=1)
        start_date = pd.to_datetime(start_date, utc=True)
        data = data.loc[data['Date'] >= start_date]
    
    return data

@app.route('/update/companies', methods=['POST'])
def update_companies():

    columns = [field.name for field in COMPANY_SCHEMA]
    companies = pd.DataFrame(columns=columns)
    columns.remove('ticker')

    tickers = sp_tickers()

    for ticker in tickers:

        stock = yf.Ticker(ticker, session=session)
        stock_data = stock.info

        company_data = {col: stock_data.get(col) for col in columns}

        if all(value is None for value in company_data.values()):
            continue

        company_data['ticker'] = ticker

        companies = pd.concat([companies, pd.DataFrame(company_data, index=[0])], 
                              ignore_index=True)

    load_table_from_dataframe(bg_client=bg_client,
                              data=companies,
                              table_id=COMPANY_TABLE_ID,
                              schema=COMPANY_SCHEMA,
                              write_disposition='WRITE_TRUNCATE',
                              ignore_unknown_values=True)

    return "Companies updated successfully", 200

@app.route('/update/history', methods=['POST'])
def update_history():
    
    with open('./sql/ticker_dates.sql', 'r') as file:
        query = file.read()
    ticker_dates = bg_client.query(query).to_dataframe()

    prices = pd.DataFrame(columns=[field.name for field in PRICE_SCHEMA])
    dividends = pd.DataFrame(columns=[field.name for field in DIVIDEND_SCHEMA])

    for ticker in ticker_dates['ticker']:
        
        ticker_data = ticker_dates.loc[ticker_dates['ticker'] == ticker].iloc[0]
        earliest_date = ticker_data['earliestDate']

        stock = yf.Ticker(ticker, session=session)

        if isinstance(earliest_date, pd._libs.tslibs.nattype.NaTType):
            historical_data = stock.history(interval='1d', 
                                            period='max')
        else:
            start_date = earliest_date + timedelta(days=1)
            historical_data = stock.history(interval='1d', 
                                            start=start_date)
        
        if len(historical_data.index) == 0:
            continue

        price_data = slice_df(data=historical_data['Close'],
                              last_date=ticker_data['lastPriceDate'])

        if len(price_data) != 0:
            price_data['date'] = price_data['Date'].dt.strftime('%Y-%m-%d')
            price_data['ticker'] = [ticker for _ in range(len(price_data.index))]
            price_data = price_data.rename(columns={'Close': 'price'})
            price_data = price_data[['date', 'ticker', 'price']]
            prices = pd.concat([prices, price_data], axis=0, ignore_index=True)


        dividend_data = slice_df(data=stock.dividends,
                                 last_date=ticker_data['lastDividendDate'])

        if len(dividend_data) != 0:
            dividend_data['date'] = dividend_data['Date'].dt.strftime('%Y-%m-%d')
            dividend_data['ticker'] = [ticker for _ in range(len(dividend_data.index))]
            dividend_data = dividend_data.rename(columns={'Dividends': 'dividend'})
            dividend_data = dividend_data[['date', 'ticker', 'dividend']]
            dividends = pd.concat([dividends, dividend_data], axis=0, ignore_index=True)

    load_table_from_dataframe(bg_client=bg_client,
                              data=prices,
                              table_id=PRICE_TABLE_ID,
                              schema=PRICE_SCHEMA,
                              write_disposition='WRITE_APPEND',
                              ignore_unknown_values=True)
    
    load_table_from_dataframe(bg_client=bg_client,
                              data=dividends,
                              table_id=DIVIDEND_TABLE_ID,
                              schema=DIVIDEND_SCHEMA,
                              write_disposition='WRITE_APPEND',
                              ignore_unknown_values=True)
    
    return "Price and dividend history updated successfully", 200

@app.route('/update/dividend_metadata', methods=['POST'])
def update_dividend_metadata():

    with open('./sql/dividend_metadata.sql', 'r') as file:
        query = file.read()

    dividend_metadata = bg_client.query(query).to_dataframe()

    load_table_from_dataframe(bg_client=bg_client,
                              data=dividend_metadata,
                              table_id=DIVIDEND_METADATA_TABLE_ID,
                              schema=DIVIDEND_METADATA_SCHEMA,
                              write_disposition='WRITE_TRUNCATE',
                              ignore_unknown_values=True)
    
    return "Dividend metadata updated successfully", 200

@app.route('/valuations', methods=['GET'])
def get_valuations():
    
    with open('./sql/dividend_payers.sql', 'r') as file:
        dividend_payers_query = file.read()

    dividend_payers = bg_client.query(dividend_payers_query).to_dataframe()
    dividend_payers = dividend_payers.fillna(0)
    dividend_payer_tickers = dividend_payers['ticker'].to_list()

    with open('./sql/dividend_history.sql', 'r') as file:
        dividend_history_query = file.read()
        dividend_history_query = dividend_history_query.replace('DIVIDEND_PAYERS', str(dividend_payer_tickers))

    with open('./sql/price_history.sql', 'r') as file:
        price_history_query = file.read()
        price_history_query = price_history_query.replace('DIVIDEND_PAYERS', str(dividend_payer_tickers))

    with open('./sql/t_bill_price.sql', 'r') as file:
        t_bill_price_query = file.read()

    with open('./sql/sp_cagr.sql', 'r') as file:
        sp_cagr_query = file.read()

    dividend_history = bg_client.query(dividend_history_query).to_dataframe()
    dividend_history['date'] = pd.to_datetime(dividend_history['date'])
    dividend_history['date'] = dividend_history['date'].dt.strftime('%Y-%m-%d')
    dividend_history = dividend_history.fillna(0)

    price_history = bg_client.query(price_history_query).to_dataframe()
    price_history['date'] = pd.to_datetime(price_history['date'])
    price_history['date'] = price_history['date'].dt.strftime('%Y-%m-%d')
    price_history = price_history.fillna(0)

    t_bill_price = bg_client.query(t_bill_price_query).to_dataframe().iloc[0,0]
    sp_cagr = bg_client.query(sp_cagr_query).to_dataframe().iloc[0,0]

    data = []
    for ticker in dividend_payer_tickers:

        ticker_data = dividend_payers.loc[dividend_payers['ticker'] == ticker].to_dict('records')[0]

        ticker_data['requiredRate'] = t_bill_price + ticker_data['fiveYearBeta'] * (sp_cagr - t_bill_price)

        ticker_data['valuation'] = (ticker_data['lastDividend'] * ticker_data['dividendFrequency']) / (ticker_data['requiredRate'] - ticker_data['fiveYearCAGR'])
        ticker_data['pctChange'] = (ticker_data['valuation'] - ticker_data['lastPrice']) / ticker_data['lastPrice']

        ticker_price_history = price_history.loc[price_history['ticker'] == ticker].sort_values(by='date', ascending=True)
        ticker_dividend_history = dividend_history.loc[dividend_history['ticker'] == ticker].sort_values(by='date', ascending=True)

        ticker_data['priceHistory'] = ticker_price_history[['date', 'price']].to_dict('records')
        ticker_data['dividendHistory'] = ticker_dividend_history[['date', 'dividend']].to_dict('records')

        data.append(ticker_data)

    response = {
        'benchmarks': {
            'tenYearTBill': t_bill_price,
            'sp500CAGR': sp_cagr
        },
        'companies': data
    }

    return jsonify(response), 200
    

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)