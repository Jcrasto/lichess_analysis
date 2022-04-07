import boto3
import re
from utils.query_utils import run_athena_query


def delete_objects_by_partition_value(logger, partition_col, value):
    bucket = "jcrasto-chess-analysis"
    prefix = "lichess_api_data/"
    s3_client = boto3.client()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    delete_string = partition_col + "=" + value

    if not pages:
        logger.info("no objects found")

    for page in pages:
        if page.get('Contents', None):
            for obj in page['Contents']:
                if re.search(delete_string, obj['Key']):
                    response = s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
                    logger.info("delete_object response: " + response['ResponseMetadata'])
    return
