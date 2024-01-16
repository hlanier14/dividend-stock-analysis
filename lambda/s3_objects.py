import boto3
import cfnresponse

def handler(event, context):
    # Init ...
    '''
    This function Creates required directory structure inside S3 bucket
    '''

    response_data = {}

    s3_resource = boto3.resource('s3')

    the_bucket = event['ResourceProperties']['the_bucket']
    file_content = event['ResourceProperties']['file_content']
    file_prefix = event['ResourceProperties']['file_prefix']

    try:
        if event['RequestType'] in ('Create', 'Update'):
            s3_resource.Object(the_bucket, file_prefix).put(Body = file_content)

        cfnresponse.send(event,
                         context,
                         cfnresponse.SUCCESS,
                         response_data)
        
    except Exception as e:
        response_data['Data'] = str(e)
        cfnresponse.send(event,
                        context,
                        cfnresponse.FAILED,
                        response_data)