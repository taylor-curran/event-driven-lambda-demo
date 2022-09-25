from prefect_aws.s3 import S3Bucket
from prefect import task, flow

@task
async def write_s3_data(s3_block):
    await s3_block.write_path(path='test_005.txt', content=b'test 00')

@flow
def aws_test():
    ts_bucket = S3Bucket.load("taylor-timeseries-data")
    write_s3_data(ts_bucket)
    print('Done!')

if __name__ == '__main__':
    aws_test()
