import yfinance as yf
from io import StringIO
import boto3
import json
from datetime import datetime, timedelta

def get_yfinance_data(tickers, start_date, end_date):
    data = yf.download(
        tickers,
        start_date,
        end_date, 
        group_by="Ticker",
        actions=True
    )
    data = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index()
    return data

def save_df_to_s3(data, bucket_name, key):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, key).put(Body=csv_buffer.getvalue())

def lambda_handler(event, context):

    result = {}

    try:

        s3 = boto3.client('s3')

        response = s3.get_object(Bucket = event['bucket_name'], Key = f"{event['ticker_folder']}/{event['transform_folder']}/data_input.json")
        content = response['Body'].read().decode('utf-8')

        json_data = json.loads(content)
        new_tickers = json_data.get('New_Tickers', [])
        old_tickers = json_data.get('Old_Tickers', [])

        today = datetime.today()
        file_name = event['file_name'].split('.')[0]

        if old_tickers:

            daily_data = get_yfinance_data(tickers = old_tickers,
                                           start_date = today - timedelta(days=1),
                                           end_date = today)
            save_df_to_s3(data = daily_data,
                          bucket_name = event['bucket_name'],
                          key = f"{event['data_folder']}/{event['raw_folder']}/{file_name}-daily.csv")

        if new_tickers:

            batch_size = 100
            for batch in range(0, len(new_tickers), batch_size):

                historical_data = get_yfinance_data(tickers = new_tickers[batch:batch + batch_size],
                                                    start_date = datetime(1900, 1, 1),
                                                    end_date = today)
                save_df_to_s3(data = historical_data,
                              bucket_name = event['bucket_name'],
                              key = f"{event['data_folder']}/{event['raw_folder']}/{file_name}-historical-batch-{batch}.csv")

        result['Location'] = f"{event['data_folder']}/{event['raw_folder']}"
        result['Validation'] = 'SUCCESS'
        
        return(result)
    
    except Exception as e:
        result['Validation'] = 'FAILURE'
        result['Message'] = str(e)
        return(result)
