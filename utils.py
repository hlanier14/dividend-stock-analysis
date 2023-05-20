import requests
from requests import Session
from requests_ratelimiter import LimiterMixin
from google.cloud import bigquery, storage
from datetime import timedelta, datetime
import pandas as pd
import bs4 as bs
import json
import pytz


class LimiterSession(LimiterMixin, Session):
    # yfinance requests rate limiter 
    pass

def sp_tickers() -> list:
    """
    Extract tickers of current S&P 500 companies from wikipedia.
    """

    response = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(response.text, 'lxml')

    # find company table on website
    table = soup.find('table', {'class': 'wikitable sortable'})

    tickers = []
    # iterate over all rows, excluding the header row
    for row in table.findAll('tr')[1:]:

        # get the text in the first column
        ticker = row.findAll('td')[0].text
        tickers.append(ticker.replace('\n', ''))

    return tickers

def load_table_from_dataframe(bg_client: bigquery.Client, 
                              data: pd.DataFrame,
                              table_id: str,
                              location: str, 
                              schema: list,
                              write_disposition: str, 
                              ignore_unknown_values: bool):
    """
    Add a dataframe to a BigQuery table.

    Params:
    -----------
    bg_client (bgiquery client): bigquery client
    data (pandas dataframe): dataframe to be added to bigquery
    table_id (string): id of target bigquery table
    location (string): location of target bigquery table
    schema (list of bigquery schemafields): target bigquery table schema
    write_disposition (string): instructs how to add the dataframe to the table
    ignore_unknown_values (boolean): ignore values in a row that are not present in the target table schema
    """

    # end if the df is empty
    if len(data.index) == 0:
        return 

    # create load job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        write_disposition=write_disposition,
        ignore_unknown_values=ignore_unknown_values,
        schema=schema
    )

    # load data to the target table 
    load_job = bg_client.load_table_from_dataframe(
        data,
        table_id,
        location=location,
        job_config=job_config
    )

    # wait for the load job to complete
    try:
        load_job.result()

    # print error 
    except Exception as e:
        print(f"Error loading data into {table_id}: {e}")

def run_query(bg_client: bigquery.Client,
              query_file: str,
              replacements: dict = None) -> pd.DataFrame:
    """
    Run a sql query from a file and return the output as a pandas dataframe.

    Params:
    -----------
    bg_client (bgiquery client): bigquery client
    query_file (string): path to file containing query
    replacements (dictionary): optional replacement strings for query
    """
        
    # read sql query file
    with open(query_file, 'r') as file:
        query = file.read()

    # check for replacement strings
    if replacements:
        for key, value in replacements.items():
            # replace each key with its value
            query = query.replace(key, value)

    # run query and convert to pandas df
    df = bg_client.query(query).to_dataframe()

    return df

def format_datetime_column(df: pd.DataFrame,
                           col: str,
                           format: str = "%Y-%m-%d") -> pd.DataFrame:
    """
    Convert column to datetime in given format.

    Params:
    -----------
    df (pandas dataframe): dataframe with target column
    col (string): target column name
    format (string): desired datetime format for target column
    """

    # convert column to datetime
    df[col] = pd.to_datetime(df[col])

    # format column 
    df[col] = df[col].dt.strftime(format)
    
    return df

def read_storage_file(storage_client: storage.Client,
                      bucket_name: str,
                      file_name: str) -> dict:
    """
    Read a json file in a storage bucket and return as a dictionary.

    Params:
    -----------
    storage_client (storage client): google storage client
    bucket_name (string): name of bucket with target file
    file_name (string): name of target file
    """
    
    # load file from bucket
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)

    # return None if file does not exist
    if blob is None:
        return None

    # download file as text
    json_string = blob.download_as_text()

    # convert text to dictionary
    json_data = json.loads(json_string)

    return json_data

def save_to_storage_file(storage_client: storage.Client,
                         bucket_name: str,
                         file_name: str,
                         data: dict):
    """
    Read a json file in a storage bucket and return as a dictionary.

    Params:
    -----------
    storage_client (storage client): google storage client
    bucket_name (string): name of bucket with target file
    file_name (string): name of target file
    data (dictionary): dictionary with data to add to target file
    """

    # load file from bucket
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # dump data into file
    json_string = json.dumps(data)

    # upload file to cloud
    blob.upload_from_string(json_string, content_type='application/json')

def extract_prices_and_dividends(yf_data: pd.DataFrame) -> pd.DataFrame:
    """
    Extract price and dividend history from a yf.download request 

    Params:
    -----------
    yf_data (pandas dataframe): dataframe output from a yf.download request
    """

    # create empty price and dividend dfs to add the parsed data
    prices_df = pd.DataFrame(columns=['date', 'ticker', 'price'])
    dividends_df = pd.DataFrame(columns=['date', 'ticker', 'dividend'])

    # extract the closing price and dividend payout columns
    price_history = yf_data['Close']
    dividend_history = yf_data['Dividends']

    # get the tickers from the price history df
    tickers = list(set(price_history.columns))

    for ticker in tickers:
        
        # get price history for ticker
        prices = price_history[ticker]

        # reset the index
        prices = prices.reset_index()

        # convert date column to datetime
        prices['Date'] = pd.to_datetime(prices['Date'], utc=True)
        prices = format_datetime_column(df=prices,
                                        col='Date')

        # change column names
        prices.columns = ['date', 'price']

        # add ticker column
        prices['ticker'] = [ticker for _ in range(len(prices.index))]

        # add to aggregate prices df
        prices_df = pd.concat([prices_df, prices], axis=0, ignore_index=True)

        # get dividend history for ticker
        dividends = dividend_history[ticker]

        # reset the index
        dividends = dividends.reset_index()

        # convert date column to datetime
        dividends['Date'] = pd.to_datetime(dividends['Date'], utc=True)
        dividends = format_datetime_column(df=dividends,
                                           col='Date')

        # exclude dates with no dividend payment
        dividends = dividends.loc[dividends[ticker] > 0]

        # change column names
        dividends.columns = ['date', 'dividend']

        # add ticker column
        dividends['ticker'] = [ticker for _ in range(len(dividends.index))]

        # add to aggregate dividend df
        dividends_df = pd.concat([dividends_df, dividends], axis=0, ignore_index=True)

    return prices_df, dividends_df

def update_model(bg_client: bigquery.Client,
                 storage_client: storage.Client,
                 bucket_name: str,
                 file_name: str):
    """
    Calculate DDM valuations and save output to a storage json file.

    Params:
    -----------
    bg_client (bigquery Client): google bigquery client
    storage_client (storage client): google storage client
    bucket_name (string): name of bucket with target file
    file_name (string): name of target file
    """

    # extract info on consistent dividend payers
    dividend_payers = run_query(bg_client=bg_client,
                                query_file='./sql/dividend_payers.sql')
    dividend_payers = dividend_payers.fillna(0)
    dividend_payers = format_datetime_column(df=dividend_payers,
                                             col='exDividendDate')

    # save dividend tickers as a list to use as a filter on price and dividend history
    dividend_payer_tickers = dividend_payers['ticker'].to_list()

    # extract dividend history for consistent dividend payers
    dividend_history = run_query(bg_client=bg_client,
                                 query_file='./sql/dividend_history.sql',
                                 replacements={'DIVIDEND_PAYERS': str(dividend_payer_tickers)})
    dividend_history = dividend_history.dropna()
    dividend_history = format_datetime_column(df=dividend_history,
                                              col='date')

    # extract price history for consistent dividend payers
    price_history = run_query(bg_client=bg_client,
                              query_file='./sql/price_history.sql',
                              replacements={'DIVIDEND_PAYERS': str(dividend_payer_tickers)})
    price_history = price_history.dropna()
    price_history = format_datetime_column(df=price_history,
                                           col='date')

    # remove update time and benchmark rates to exclude from ticker dictionaries
    dividend_payers_clean = dividend_payers.drop(columns=['riskFreeRate', 'marketRate'])

    data = []
    for ticker in dividend_payer_tickers:

        # get metadata on ticker and convert to dictionary
        ticker_data = dividend_payers_clean.loc[dividend_payers_clean['ticker'] == ticker].to_dict('records')[0]

        # get price and dividend history for ticker
        ticker_price_history = price_history.loc[price_history['ticker'] == ticker]
        ticker_dividend_history = dividend_history.loc[dividend_history['ticker'] == ticker]

        # convert price and dividend history dfs to dictionaries and add to metadata dictionary
        ticker_data['priceHistory'] = ticker_price_history[['date', 'price']].to_dict('records')
        ticker_data['dividendHistory'] = ticker_dividend_history[['date', 'dividend']].to_dict('records')

        # add ticker data to aggregate data
        data.append(ticker_data)

    # add ticker data to response
    # include update time and benchmark rates
    response = {
        'lastUpdated': datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        'benchmarks': {
            'riskFreeRate': max(dividend_payers['riskFreeRate']),
            'marketRate': max(dividend_payers['marketRate'])
        },
        'companies': data
    }

    # save dictionary as json file in cloud storage bucket
    save_to_storage_file(storage_client=storage_client,
                         bucket_name=bucket_name,
                         file_name=file_name,
                         data=response)