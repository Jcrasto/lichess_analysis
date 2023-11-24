import re
import logging
import boto3


def delete_objects_by_partition_value(bucket, prefix, partition_col, value):
    session = boto3.Session()
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    delete_string = partition_col + "=" + value

    if not pages:
        logging.info("[delete_objects_by_partition_value] - no objects found in s3://" + bucket + "/" + prefix)

    for page in pages:
        if page.get('Contents', None):
            for obj in page['Contents']:
                if re.search(delete_string, obj['Key']):
                    logging.info(
                        "[delete_objects_by_partition_value] - found object: " + obj['Key'] +
                        " matching delete_string: " + delete_string + " in s3://" + bucket + "/" + prefix)
                    response = s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
                    logging.info("[delete_objects_by_partition_value] - delete_object response: " + str(
                        response['ResponseMetadata']))
    return
