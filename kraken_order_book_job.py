import json
import time
import pprint
from datetime import datetime

from websocket._exceptions import *

from kraken_order_book import OrderBook, ChecksumException
from kraken_websocket.modules.websocket_handler import KrakenWebsocketHandler
from kraken_websocket.modules.interval_handler import IntervalHandler
from kraken_websocket.modules.parquet_handler import ParquetHandler


def main():
    saving_interval_minutes = 1
    # Outer control loop, handling starting/restarting behaviour
    while True:
        interval_handler = IntervalHandler(interval_in_minutes=saving_interval_minutes)
        websocket_handler = KrakenWebsocketHandler()
        websocket_handler.initialize_socket()
        parquet_handler = ParquetHandler(topic="book")
        order_book = OrderBook(depth=1000, number_aggregation_buckets=6)

        while datetime.now() < interval_handler.end_of_interval:
            if datetime.now().second % 5 == 0:
                print(
                    f"Next interval starting in {interval_handler.end_of_interval - datetime.now().replace(microsecond=0)}"
                )
            time.sleep(1)
        interval_handler.progress_to_next_interval()
        print(datetime.now())
        print(interval_handler.end_of_interval)

        websocket_handler.subscribe_to_topic()

        # Main event loop
        while True:
            try:
                # Fetch loop for defined interval period
                while datetime.now() < interval_handler.end_of_interval:
                    result = websocket_handler.websocket.recv()
                    result = json.loads(result)
                    if type(result) == list:
                        order_book.process_event(event=result)

                print("end of interval..")
                order_book.update_aggregated_book()
                pprint.pprint(order_book.bucket_book)
                pprint.pprint(order_book.delta_book)
                parquet_handler.save_to_parquet(
                    data=order_book.flatten_delta_bucket_book(),
                    interval_timestamp=interval_handler.start_of_interval,
                )

                order_book.reset_delta_book()
                interval_handler.progress_to_next_interval()

            except ChecksumException:
                print("Order book out of sync, restarting websocket...")
                websocket_handler.websocket.close()
                break
            except WebSocketTimeoutException as error:
                print(f"Encountered Error {repr(error)}, restarting websocket...")
                websocket_handler.websocket.close()
                break


if __name__ == "__main__":
    main()
