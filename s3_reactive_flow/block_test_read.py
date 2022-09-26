from prefect import task, flow, get_run_logger
from prefect_aws.s3 import S3Bucket
import pandas as pd


# TODO: Fix strftime so it has date now.strftime("%m/%d/%Y, %H:%M:%S")
@task
async def read_s3_data(ts_bucket_block_obj, file_name):

    df = await ts_bucket_block_obj.read_path(path=file_name)
    return df

@flow
def validate_input_data():

    ts_bucket_block_obj = S3Bucket.load("taylor-timeseries-data")

    csv_contents = read_s3_data(ts_bucket_block_obj, 'timeseries/ts_data_16:16:15.txt')
    decoded = csv_contents.decode("utf-8")

    # Why do I have to write to a file the text in order to get to a df format?
    # I can't just pass the decoded contents to pandas as a string to arrive at a df?
    with open('ts.csv', 'w') as f:
        f.write(decoded)

    df = pd.read_csv('ts.csv')

if __name__ == "__main__":
    validate_input_data()