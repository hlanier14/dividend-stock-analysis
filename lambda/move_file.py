import boto3

def lambda_handler(event, context):  

    '''
    This function Moves the source dataset to archive or error folder
    '''
    
    result = {}

    s3_resource = boto3.resource('s3')
    bucket_name = event['bucket_name']

    source_location = event['taskresult']['Location']
    base_folder = source_location.split("/")[0]

    # check for failure
    if "error-info" in event:
        status = "FAILURE"
    else:
        status = event['taskresult']['Validation']

    file_name = event['file_name']
    key_name = source_location + "/" + file_name

    # move to error if failure and archive for success
    if status == "FAILURE":
        end_folder = event['error_folder']
    elif status == "SUCCESS":
        end_folder = event['archive_folder']

    source_file_name_to_copy = bucket_name + "/" + source_location + "/" + file_name
    move_file_name = base_folder + "/" + end_folder + "/" + file_name
    
    s3_resource.Object(bucket_name, move_file_name).copy_from(CopySource = source_file_name_to_copy)
    s3_resource.Object(bucket_name, key_name).delete()
    
    result['Status'] = status
    result['msg'] = f"File moved to {move_file_name}"

    return(result)