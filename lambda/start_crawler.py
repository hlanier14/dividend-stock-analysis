import boto3

def lambda_handler(event, context):

    '''
    This function start AWS Glue Crawler
    it expects 'Crawler_name' key in the event object passed in the function
    '''

    crawler_name = event['Crawler_Name']
    
    client = boto3.client('glue')    
    client.start_crawler(Name=crawler_name)

    result = {}
    result['crawler_name'] = crawler_name
    
    return(result)