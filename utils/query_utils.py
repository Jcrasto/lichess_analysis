import io
import pandas as pd
import boto3


def athena_query_to_df(query):
    session = boto3.Session()
    query_output_bucket = "s3://query-results-737934178320"
    athena_client = session.client("athena", region_name='us-east-1')
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': query_output_bucket
        }
    )
    response_key = response['QueryExecutionID'] + ".csv"
    s3_client = session.client('s3')
    try:
        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=query_output_bucket.replace("s3://", ""))
        obj = s3_client.get_object(Bucket=query_output_bucket.replace("s3://", ""), key = response_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        raise(e)
    return df