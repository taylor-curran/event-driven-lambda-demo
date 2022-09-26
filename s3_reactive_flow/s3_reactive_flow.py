import awswrangler as wr
import json
import logging
from prefect import task, flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook
from prefect_aws.s3 import S3Bucket
import pandas as pd


aws_logger = logging.getLogger()
aws_logger.setLevel(logging.INFO)

# TODO: Fix strftime so it has date now.strftime("%m/%d/%Y, %H:%M:%S")
@task
async def read_s3_data(ts_bucket_block_obj, file_name):

    df = await ts_bucket_block_obj.read_path(path=file_name)
    return df

@flow
def validate_input_data(s3_key: str = 'None') -> None:
    # logger = get_run_logger()
    # logger.info("Received file: %s", s3_key)


    # Load Time Series Bucket Block from Prefect Cloud
    ts_bucket_block_obj = S3Bucket.load("taylor-timeseries-data")
    ts_bucket_block_obj = S3Bucket.load("taylor-timeseries-data")

    csv_contents = read_s3_data(ts_bucket_block_obj, 'timeseries/ts_data_16:16:15.txt')
    decoded = csv_contents.decode("utf-8")

    # Why do I have to write to a file the text in order to get to a df format?
    # I can't just pass the decoded contents to pandas as a string to arrive at a df?
    with open('ts.csv', 'w') as f:
        f.write(decoded)

    df = pd.read_csv('ts.csv')
    # df = wr.s3.read_parquet(s3_key)
    max_value = max(df.value)
    breakpoint()
    if max_value > 42:
        alert = f"The max value {max_value} is bigger than 42! 🚨"
        # logger.warning(alert)
        # slack_webhook_block = SlackWebhook.load("hq")
        # slack_webhook_block.notify(alert)
    else:
        print('Elsssssssse')
        # logger.info("Data validation check passed ✅")


def handler(event, context):
    aws_logger.info("Received event: " + json.dumps(event))
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    s3_path = f"s3://{bucket}/{key}"
    validate_input_data(s3_path)
    # create_flow_run_from_deployment()

if __name__ == "__main__":
    validate_input_data()