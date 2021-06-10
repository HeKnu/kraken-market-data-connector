import json
import time

from websocket import create_connection


class KrakenWebsocketHandler:
    def __init__(
        self,
        topic: str = "book",
        currency_pair: str = "XBT/EUR",
        websocket_timeout: int = 5,
    ):
        self.websocket = None
        self.topic = topic
        self.currency_pair = currency_pair
        self.websocket_timeout = websocket_timeout

    def initialize_socket(self, retry_duration: int = 3600, retry_delay: int = 5) -> None:
        tries = retry_duration / retry_delay
        while tries > 0:
            try:
                self.websocket = create_connection(
                    "wss://ws.kraken.com/", timeout=self.websocket_timeout
                )
                return
            except Exception as error:
                print("Caught this error: " + repr(error))
                print(f"Trying again in {retry_delay} seconds...")
                time.sleep(retry_delay)
                tries -= 1

        print("Could not establish a websocket connection!")
        # TODO escalate alerting

    def subscribe_to_topic(self) -> None:
        if self.websocket:
            self.websocket.send(
                json.dumps(
                    {
                        "event": "subscribe",
                        "pair": [self.currency_pair],
                        "subscription": {"name": self.topic, "depth": 1000},
                    }
                )
            )
        else:
            print("Websocket not initialized!")
