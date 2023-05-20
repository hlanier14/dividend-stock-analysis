from google.cloud import bigquery, storage
from requests_ratelimiter import MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
import yfinance as yf
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS

import utils

load_dotenv()

app = Flask(__name__)
CORS(app, supports_credentials=True)

PROJECT_ID = 'portfolio-website-378500'
DATASET_ID = 'dividend_stocks'
STORAGE_BUCKET_NAME = 'dividend-stock-analysis'
STORAGE_FILE_NAME = 'model.json'
LOCATION = 'us-central1'

PRICE_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.prices'
DIVIDEND_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.dividends'
DIVIDEND_METADATA_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.dividend_metadata'
COMPANY_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.companies'
BENCHMARK_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.benchmarks'

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

session = utils.LimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)), 
    bucket_class=MemoryQueueBucket
)

bg_client = bigquery.Client()
storage_client = storage.Client()


@app.route('/update/companies', methods=['POST'])
def update_companies():
    """
    Replace the company table with stock information of current S&P 500 companies from Yahoo Finance.
    """

    # create empty df with same columns as company table
    columns = [field.name for field in COMPANY_SCHEMA]
    companies = pd.DataFrame(columns=columns)
    columns.remove('ticker')

    # extract tickers of current S&P 500 companies
    tickers = utils.sp_tickers()

    for ticker in tickers:
        
        # get stock info from yfinance
        stock = yf.Ticker(ticker, session=session)
        stock_data = stock.info

        # use company table column names to get values from stock info
        company_data = {col: stock_data.get(col) for col in columns}

        # skip stock if all info values are none
        if all(value is None for value in company_data.values()):
            continue

        company_data['ticker'] = ticker

        # append stock info to companies df
        companies = pd.concat([companies, pd.DataFrame(company_data, index=[0])], 
                              ignore_index=True)

    # replace company table with response
    utils.load_table_from_dataframe(bg_client=bg_client,
                                    data=companies,
                                    table_id=COMPANY_TABLE_ID,
                                    location=LOCATION,
                                    schema=COMPANY_SCHEMA,
                                    write_disposition='WRITE_TRUNCATE',
                                    ignore_unknown_values=True)

    return "Companies updated successfully", 200

@app.route('/update/dividend_metadata', methods=['POST'])
def update_dividend_metadata():
    """
    Replace the dividend metadata table with updated values.
    """

    # calculate dividend metadata 
    dividend_metadata = utils.run_query(bg_client=bg_client,
                                        query_file='./sql/dividend_metadata.sql')

    # replace dividend metadata table with output
    utils.load_table_from_dataframe(bg_client=bg_client,
                                    data=dividend_metadata,
                                    table_id=DIVIDEND_METADATA_TABLE_ID,
                                    location=LOCATION,
                                    schema=DIVIDEND_METADATA_SCHEMA,
                                    write_disposition='WRITE_TRUNCATE',
                                    ignore_unknown_values=True)
    
    return "Dividend metadata updated successfully", 200

@app.route('/update/history', methods=['POST'])
def update_history():
    """
    Update the price and dividend tables.
    """
        
    print("Updating price and dividend history...")

    # get the last date when the price and dividend history was updated
    last_model_update = utils.read_storage_file(storage_client=storage_client,
                                                bucket_name=STORAGE_BUCKET_NAME,
                                                file_name=STORAGE_FILE_NAME)
    
    # convert the last updated string to datetime
    last_updated = datetime.strptime(last_model_update['lastUpdated'], "%Y-%m-%dT%H:%M:%S")

    # get all tickers from company table
    all_tickers = utils.run_query(bg_client=bg_client,
                                  query_file='./sql/tickers.sql')
    all_tickers = all_tickers['ticker'].to_list()

    # check for newly added tickers that do not have pricing history
    new_tickers = utils.run_query(bg_client=bg_client,
                                  query_file='./sql/new_tickers.sql')
    new_tickers = new_tickers['ticker'].to_list()

    print(f'{len(new_tickers)} new tickers out of {len(all_tickers)} total tickers.')
    
    # tickers that have a price history
    known_tickers = [ticker for ticker in all_tickers if ticker not in new_tickers]

    # extract data for known tickers from start date
    known_ticker_data = yf.download(known_tickers, 
                                    start=last_updated, 
                                    end=datetime.today(), 
                                    actions=True)
    
    # extract the price and dividend history from the yf.download request
    prices_df, dividends_df = utils.extract_prices_and_dividends(yf_data=known_ticker_data)

    # check if there are new tickers
    if new_tickers:

        # extract data for known tickers from start date
        new_ticker_data = yf.download(new_tickers, 
                                      start='max', 
                                      end=datetime.today(), 
                                      actions=True)

        # extract the price and dividend history from the yf.download request
        new_ticker_prices, new_ticker_dividends = utils.extract_prices_and_dividends(yf_data=new_ticker_data)
    
        prices_df = pd.concat([prices_df, new_ticker_prices], axis=0, ignore_index=True)
        dividends_df = pd.concat([dividends_df, new_ticker_dividends], axis=0, ignore_index=True)

    # append the price and dividend dfs to their respective bigquery tables
    utils.load_table_from_dataframe(bg_client=bg_client,
                                    data=prices_df,
                                    table_id=PRICE_TABLE_ID,
                                    location=LOCATION,
                                    schema=PRICE_SCHEMA,
                                    write_disposition='WRITE_APPEND',
                                    ignore_unknown_values=True)
    utils.load_table_from_dataframe(bg_client=bg_client,
                                    data=dividends_df,
                                    table_id=DIVIDEND_TABLE_ID,
                                    location=LOCATION,
                                    schema=DIVIDEND_SCHEMA,
                                    write_disposition='WRITE_APPEND',
                                    ignore_unknown_values=True)

    utils.update_model(bg_client=bg_client,
                       storage_client=storage_client,
                       bucket_name=STORAGE_BUCKET_NAME,
                       file_name=STORAGE_FILE_NAME)

    return "Successfully updated stock history and model storage file", 200

@app.route('/model', methods=['GET'])
def get_model():
    # read file from storage bucket 
    data = utils.read_storage_file(storage_client=storage_client,
                                   bucket_name=STORAGE_BUCKET_NAME,
                                   file_name=STORAGE_FILE_NAME)
    return jsonify(data), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)