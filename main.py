from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import bs4 as bs
import requests
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = 'portfolio-website-378500'
DATASET_ID = 'dividend_stocks'
LOCATION = 'us-central1'

PRICE_TABLE = 'prices'
DIVIDEND_TABLE = 'dividends'
COMPANY_TABLE = 'companies'
FINANCIALS_TABLE = 'company_financials'

PRICE_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{PRICE_TABLE}'
DIVIDEND_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{DIVIDEND_TABLE}'
COMPANY_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{COMPANY_TABLE}'
FINANCIALS_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_TABLE}'

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

COMPANY_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("shortName", "STRING"),
    bigquery.SchemaField("longName", "STRING"),
    bigquery.SchemaField("website", "STRING"),
    bigquery.SchemaField("industry", "STRING"),
    bigquery.SchemaField("sector", "STRING")
]

FINANCIALS_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("payoutRatio", "FLOAT"),
    bigquery.SchemaField("exDividendDate", "TIMESTAMP"),
    bigquery.SchemaField("forwardPE", "FLOAT"),
    bigquery.SchemaField("forwardEps", "FLOAT")
]

def load_table_from_dataframe(bg_client: bigquery.Client, 
                              data: pd.DataFrame,
                              table_id: str,
                              schema: list,
                              write_disposition: str, 
                              ignore_unknown_values: bool):
    
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

def sp_tickers():

    response = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(response.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker.replace('\n', ''))
    return tickers

def main():

    tickers = sp_tickers()

    prices = pd.DataFrame({'date': [], 'ticker': [], 'price': []})
    dividends = pd.DataFrame({'date': [], 'ticker': [], 'dividend': []})
    companies = pd.DataFrame({'ticker': [], 'shortName': [], 'longName': [], 'website': [], 'industry': [], 'sector': []})
    financials = pd.DataFrame({'ticker': [], 'payoutRatio': [], 'exDividendDate': [], 'forwardPE': [], 'forwardEps': []})

    for ticker in tickers:

        print(ticker)

        stock = yf.Ticker(ticker)
        # historical_data = stock.history(period='max')

        stock_data = stock.info

        try: 
            company_keys = ['shortName', 'longName', 'website', 'industry', 'sector']
            company_data = {key: stock_data[key] for key in company_keys}
            company_data['ticker'] = ticker
            companies = pd.concat([companies, pd.DataFrame(company_data, index=[0])], ignore_index=True)
        except:
            print(f'no company data for {ticker}')

        try:
            financial_keys = ['payoutRatio', 'exDividendDate', 'forwardPE', 'forwardEps']
            company_financials = {key: stock_data[key] for key in financial_keys}
            company_financials['ticker'] = ticker
            financials = pd.concat([financials, pd.DataFrame(company_financials, index=[0])], ignore_index=True)
        except:
            print(f'no financial data for {ticker}')

        # try:
        #     price_data = historical_data['Close']
        #     price_data = price_data.reset_index()
        #     price_data['date'] = price_data['Date'].dt.strftime('%Y-%m-%d')
        #     price_data['ticker'] = [ticker for _ in range(len(price_data.index))]
        #     price_data = price_data.rename(columns={'Close': 'price'})
        #     price_data = price_data[['date', 'ticker', 'price']]
        #     prices = pd.concat([prices, price_data], axis=0, ignore_index=True)
        # except:
        #     print(f'no pricing data for {ticker}')

        # try:
        #     dividend_data = stock.dividends
        #     dividend_data = dividend_data.reset_index()
        #     dividend_data['date'] = dividend_data['Date'].dt.strftime('%Y-%m-%d')
        #     dividend_data['ticker'] = [ticker for _ in range(len(dividend_data.index))]
        #     dividend_data = dividend_data.rename(columns={'Dividends': 'dividend'})
        #     dividend_data = dividend_data[['date', 'ticker', 'dividend']]
        #     dividends = pd.concat([dividends, dividend_data], axis=0, ignore_index=True)
        # except:
        #     print(f'no dividend data for {ticker}')

        # try:
        #     dividend_data = dividend_data.reset_index()
        #     dividend_data['Date'] = dividend_data['Date'].dt.strftime('%Y-%m-%d')
        #     dividend_data['Ticker'] = [ticker for _ in range(len(dividend_data.index))]
        #     dividends = pd.concat([dividends, dividend_data], axis=0, ignore_index=True)
        # except:
        #     print(f'no dividend data for {ticker}')

    # prices.to_csv('prices.csv', index=False)
    # dividends.to_csv('dividends.csv', index=False)

    companies.to_csv('companies.csv', index=False)
    financials.to_csv('financials.csv', index=False)

    bg_client = bigquery.Client()

    # load_table_from_dataframe(bg_client=bg_client,
    #                           data=prices,
    #                           table_id=PRICE_TABLE_ID,
    #                           schema=PRICE_SCHEMA,
    #                           write_disposition='WRITE_APPEND',
    #                           ignore_unknown_values=True)
    
    # load_table_from_dataframe(bg_client=bg_client,
    #                           data=dividends,
    #                           table_id=DIVIDEND_TABLE_ID,
    #                           schema=DIVIDEND_SCHEMA,
    #                           write_disposition='WRITE_APPEND',
    #                           ignore_unknown_values=True)

    load_table_from_dataframe(bg_client=bg_client,
                              data=companies,
                              table_id=COMPANY_TABLE_ID,
                              schema=COMPANY_SCHEMA,
                              write_disposition='WRITE_TRUNCATE',
                              ignore_unknown_values=True)
    
    load_table_from_dataframe(bg_client=bg_client,
                              data=financials,
                              table_id=FINANCIALS_TABLE_ID,
                              schema=FINANCIALS_SCHEMA,
                              write_disposition='WRITE_TRUNCATE',
                              ignore_unknown_values=True)


    
if __name__ == '__main__':
    main()