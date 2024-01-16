import os
import boto3


def lambda_handler(event, context):
    '''
    This function checks the status of the Crawler
    it expects 'crawler_name' key in the event object passed in the function
    '''

    result = {}

    client = boto3.client('glue')

    crawler_name = event['taskresult']['crawler_name']
    cnt = int(event['taskresult']['cnt']) + 1
    
    response = client.get_crawler(Name = crawler_name)
    
    # check last state
    last_state = "INITIAL"
    if 'LastCrawl' in response['Crawler']:
        if 'Status' in response['Crawler']['LastCrawl']:
            last_state = response['Crawler']['LastCrawl']['Status']

    result['Status'] = response['Crawler']['State']
    result['Validation'] = "RUNNING"
    
    # check current state
    state = response['Crawler']['State']
    if state == "READY":
        result['Validation'] = "SUCCESS"
        if last_state == "FAILED":
            result['Status'] = "FAILED"
            result['error'] = "Crawler Failed"
            result['Validation'] = "FAILURE"
    
    Retry_Count = int(os.environ['RETRYLIMIT'])
    
    # check number of retries
    if cnt > Retry_Count:
        result['Status'] = "RETRYLIMITREACH"
        result['error'] = "Retry limit reach"
        result['Validation'] = "FAILURE"

    result['crawler_name'] = crawler_name
    result['running_time'] = response['Crawler']['CrawlElapsedTime']
    result['cnt'] = cnt
    result['last_crawl_status'] = last_state

    location = f"{event['ticker_folder']}/{event['raw_folder']}"
    if event['data_folder'] in crawler_name:
        location = f"{event['data_folder']}/{event['raw_folder']}"

    result['Location'] = location

    return result