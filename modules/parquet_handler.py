from datetime import datetime
import os

import fastparquet
from pandas import DataFrame

DATA_ROOT_DIR = "D:/Crypto_Data"
DATA_NAME = "order_book_test"


class ParquetHandler:
    def __init__(
        self, topic, data_root_dir: str = DATA_ROOT_DIR, data_name: str = DATA_NAME
    ):
        self.topic = topic
        self.data_root_dir = data_root_dir
        self.data_name = data_name

    def prepare_dataframe(self, data, interval_timestamp) -> DataFrame:
        if self.topic == "book":
            assert type(data) == dict
            data["interval_timestamp"] = interval_timestamp
            df = DataFrame()
            df = df.append(data, ignore_index=True)
            return df
        else:
            raise NotImplementedError(
                f"Handler for topic '{self.topic}' not implemented!"
            )

    def save_to_parquet(self, data, interval_timestamp: datetime) -> None:
        df = self.prepare_dataframe(data=data, interval_timestamp=interval_timestamp)

        file_path = os.path.join(
            self.data_root_dir, self.data_name, interval_timestamp.strftime("%Y/%m/%d")
        )
        file_name = (
            f"{self.data_name}_{interval_timestamp.strftime('%Y_%m_%d')}.parquet"
        )

        if not os.path.exists(file_path):
            os.makedirs(file_path)
        parquet_file_name = os.path.join(file_path, file_name)

        if not os.path.exists(parquet_file_name):
            fastparquet.write(parquet_file_name, df, append=False)
        else:
            fastparquet.write(parquet_file_name, df, append=True)
