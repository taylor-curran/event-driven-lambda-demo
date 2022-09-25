import awswrangler as wr
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from typing import Union
from prefect import task, flow, get_run_logger
from prefect.blocks.system import JSON
from prefect_aws.s3 import S3Bucket


class TimeseriesGenerator:
    def __init__(
        self,
        start_date: Union[str, datetime] = datetime.today(),
        end_date: Union[str, datetime] = datetime.today() + timedelta(days=7),
        frequency: str = "H",
        dt_column: str = "timestamp",
        nr_column: str = "value",
        min_value: int = 0,
        max_value: int = 42,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.dt_column = dt_column
        self.nr_column = nr_column
        self.min_value = min_value
        self.max_value = max_value

    def get_date_range(self) -> pd.date_range:
        return pd.date_range(
            start=self.start_date, end=self.end_date, freq=self.frequency
        )

    @classmethod
    def get_timeseries(cls, **kwargs) -> pd.DataFrame:
        ts = cls(**kwargs)
        timestamp_date_range = ts.get_date_range()
        timeseries_df = pd.DataFrame(timestamp_date_range, columns=[ts.dt_column])
        timeseries_df[ts.nr_column] = np.random.randint(
            ts.min_value, ts.max_value, size=len(timestamp_date_range)
        )
        return timeseries_df

# TODO: Fix strftime so it has date now.strftime("%m/%d/%Y, %H:%M:%S")
@task
async def write_s3_data(df, ts_bucket_block_obj, file_name=f"ts_data_{datetime.now().strftime('%H:%M:%S')}.txt"):
    # Convert DF to Text
    text_df = df.to_csv(index=False)

    await ts_bucket_block_obj.write_path(path=file_name, content=bytes(text_df, 'utf-8'))
    return file_name

@flow
def upload_timeseries_data_to_s3() -> None:
    dict_from_block = JSON.load("max-value-taylor").value
    max_val = dict_from_block["threshold"]
    df = TimeseriesGenerator.get_timeseries(max_value=max_val)
    text_df = df.to_csv(index=False)

    # Load Time Series Bucket Block from Prefect Cloud
    ts_bucket_block_obj = S3Bucket.load("taylor-timeseries-data")
    file_name = write_s3_data(df, ts_bucket_block_obj)
    
    # result = wr.s3.to_parquet(
    #     df,
    #     path=f"s3://prefectdata/timeseries/",
    #     index=False,
    #     dataset=True,
    #     database="default",
    #     table="lambda",
    # )

    logger = get_run_logger()
    logger.info(f"New file uploaded: {file_name} 🚀")


def handler(event, context):
    upload_timeseries_data_to_s3()

if __name__ == "__main__":
    upload_timeseries_data_to_s3()
