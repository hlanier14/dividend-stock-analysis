import json
import pandas as pd
from datetime import datetime
import awswrangler as wr
import os

def lambda_handler(event, context):

    try:

        today = datetime.today()

        companies = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]

        benchmarks = pd.DataFrame({
            "Symbol": ["^TNX", "^GSPC"],
            "Security": ["10 Year Treasury Note", "S&P 500"]
        })

        tickers = pd.concat([companies, benchmarks])

        tickers['updatedAt'] = [today for row in tickers.index]

        if tickers.empty:
            return {
                "statusCode": 204,
                "body": json.dumps({
                    "message": "No data available"
                })
            }

        file_name = today.strftime('%Y-%m-%d-%H-%M-%S')
        wr.s3.to_csv(
            df = tickers, 
            path = f"s3://{os.environ['BUCKETNAME']}/{os.environ['TICKER_FOLDER']}/{os.environ['RAW_FOLDER']}/{file_name}.csv", 
            index = False, 
            sep = '|'
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Ticker collection successful"
            })
        }
    
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }



