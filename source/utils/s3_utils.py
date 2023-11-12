import re
import logging
import boto3


def delete_objects_by_partition_value(bucket, prefix, partition_col, value):
    session = boto3.Session()
    s3_client = session.client('s3')
    # bucket = "jcrasto-chess-analysis"
    # prefix = "lichess_api_data/"
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    delete_string = partition_col + "=" + value

    if not pages:
        logging.info("no objects found")

    for page in pages:
        if page.get('Contents', None):
            for obj in page['Contents']:
                if re.search(delete_string, obj['Key']):
                    response = s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
                    logging.info("delete_object response: " + str(response['ResponseMetadata']))
    return
