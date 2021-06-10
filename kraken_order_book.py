from binascii import crc32
from typing import Dict


class OrderBook:
    def __init__(
        self,
        depth,
        number_aggregation_buckets=6,
        bucket_size_exponent=5,
        bucket_init_factor=0.00002,
    ):
        self.depth = depth
        self.number_aggregation_buckets = number_aggregation_buckets
        self.bucket_size_exponent = bucket_size_exponent
        self.bucket_init_factor = bucket_init_factor
        self.book = {"bid": {}, "ask": {}}
        self.best_price = {"bid": float("-inf"), "ask": float("inf")}
        self.bucket_book = {}
        self.delta_book = {}
        self.reset_delta_book()
        self.aggregation_buckets = {"bid": [], "ask": []}

    @staticmethod
    def dict_to_float(key_value):
        return float(key_value[0])

    def reset_delta_book(self):
        zeroed_bucket_list = [0] * self.number_aggregation_buckets
        self.delta_book = {
            "bid": {
                "add_count": zeroed_bucket_list.copy(),
                "sub_count": zeroed_bucket_list.copy(),
                "add_volume": zeroed_bucket_list.copy(),
                "sub_volume": zeroed_bucket_list.copy(),
            },
            "ask": {
                "add_count": zeroed_bucket_list.copy(),
                "sub_count": zeroed_bucket_list.copy(),
                "add_volume": zeroed_bucket_list.copy(),
                "sub_volume": zeroed_bucket_list.copy(),
            },
        }
        self.bucket_book = {
            "bid": zeroed_bucket_list.copy(),
            "ask": zeroed_bucket_list.copy(),
        }

    def calculate_order_book_checksum(self):
        top_ten = (
            sorted(self.book["ask"].items(), key=self.dict_to_float)[0:10]
            + sorted(self.book["bid"].items(), key=self.dict_to_float, reverse=True)[
                0:10
            ]
        )

        flattened = [item for tup in top_ten for item in tup]
        expanded = [
            format(item, ".8f") if type(item) == float else item for item in flattened
        ]
        transformed = [str(item).replace(".", "").lstrip("0") for item in expanded]
        concatenated = "".join(transformed)

        return crc32(bytes(concatenated, encoding="utf-8"))

    def flatten_delta_bucket_book(self) -> Dict:
        flattened_dict = {}
        for side, items in self.delta_book.items():
            for key, values in items.items():
                for i, value in enumerate(values):
                    flattened_dict[f"{side}_{key}_interval_{i}"] = value

        for side, items in self.bucket_book.items():
            for i, value in enumerate(items):
                flattened_dict[f"{side}_volume_snapshot_interval_{i}"] = value

        return flattened_dict

    def api_output_book(self):
        bid = sorted(self.book["bid"].items(), key=self.dict_to_float, reverse=True)
        ask = sorted(self.book["ask"].items(), key=self.dict_to_float)
        print("Bid\t\t\t\t\t\tAsk")
        for item in range(int(self.depth)):
            print(
                "%(bidprice)s (%(bidvolume)s)\t\t\t\t%(askprice)s (%(askvolume)s)"
                % {
                    "bidprice": bid[item][0],
                    "bidvolume": bid[item][1],
                    "askprice": ask[item][0],
                    "askvolume": ask[item][1],
                }
            )

    def api_output_aggregated_book(self):
        bid = sorted(self.book["bid"].items(), key=self.dict_to_float, reverse=True)
        ask = sorted(self.book["ask"].items(), key=self.dict_to_float)
        print(f"best bid {float(bid[0][0])}")
        print(f"best ask {float(ask[0][0])}")
        current_price = (float(bid[0][0]) + float(ask[0][0])) / 2

        bid_agg = list()
        ask_agg = list()

        agg_range = 0.0005
        agg_value = 0
        for item in range(int(self.depth)):
            current_threshold = round(current_price - current_price * agg_range, 1)
            # print(current_threshold)
            if float(bid[item][0]) >= current_threshold:
                agg_value += float(bid[item][1])
                # print(agg_value)
            elif item == self.depth:
                agg_value += float(bid[item][1])
                bid_agg.append((current_threshold, round(agg_value, 3)))
            else:
                bid_agg.append((current_threshold, round(agg_value, 3)))
                agg_range *= 2
                # print(agg_range)
                agg_value = 0

        for item in bid_agg:
            print(item)

    def update_aggregated_book(self):
        self.update_aggregation_buckets()

        for side in ["bid", "ask"]:
            for item in self.book[side].items():

                price_level = float(item[0])
                volume = float(item[1])

                bucket_level = 0
                if side == "bid":
                    # Should not run out of range, since last element is always -inf/inf
                    while price_level < self.aggregation_buckets["bid"][bucket_level]:
                        bucket_level += 1
                elif side == "ask":
                    while price_level > self.aggregation_buckets["ask"][bucket_level]:
                        bucket_level += 1

                self.bucket_book[side][bucket_level] += volume

    def api_update_book(self, side, data):
        for item in data:
            price_level = item[0]
            # Amount of 0.0 in the event indicates a deletion of that order, so in that case, pop the item
            if float(item[1]) != 0.0:
                self.book[side].update({price_level: float(item[1])})
            else:
                if price_level in self.book[side]:
                    self.book[side].pop(price_level)

        # Re-sort the updated order book, truncate to correct depth, update best price
        if side == "bid":
            sorted_bids = sorted(
                self.book["bid"].items(), key=self.dict_to_float, reverse=True
            )[: int(self.depth)]
            self.best_price["bid"] = float(sorted_bids[0][0])
            self.book["bid"] = dict(sorted_bids)
        elif side == "ask":
            sorted_asks = sorted(self.book["ask"].items(), key=self.dict_to_float)[
                : int(self.depth)
            ]
            self.best_price["ask"] = float(sorted_asks[0][0])
            self.book["ask"] = dict(sorted_asks)

    def update_delta_book(self, side, data):
        for item in data:
            # Ignore republishing updates, since they don´t contain new orders
            if "r" in item:
                break

            price_level = float(item[0])
            new_volume = float(item[1])

            bucket_level = 0
            if side == "bid":
                # Should not run out of range, since last element is always -inf/inf
                while price_level < self.aggregation_buckets["bid"][bucket_level]:
                    bucket_level += 1
            elif side == "ask":
                while price_level > self.aggregation_buckets["ask"][bucket_level]:
                    bucket_level += 1

            if item[0] in self.book[side]:
                if new_volume < float(self.book[side][item[0]]):
                    # Event decreases an existing price level
                    subbed_volume = float(self.book[side][item[0]]) - new_volume
                    self.delta_book[side]["sub_volume"][bucket_level] += round(
                        subbed_volume, 4
                    )
                    self.delta_book[side]["sub_count"][bucket_level] += 1
                    # print(f"{price_level}, {subbed_volume} taken off the book!")
                else:
                    # Event increases an existing price level
                    added_volume = new_volume - float(self.book[side][item[0]])
                    self.delta_book[side]["add_volume"][bucket_level] += round(
                        added_volume, 4
                    )
                    self.delta_book[side]["add_count"][bucket_level] += 1
                    # print(self.delta_book[side])
                    # print(f"{price_level}, {added_volume} added to the book!")
            else:
                # Event adds a new price level
                self.delta_book[side]["add_volume"][bucket_level] += round(
                    new_volume, 4
                )
                self.delta_book[side]["add_count"][bucket_level] += 1
                # print(self.delta_book[side]["add_count"])
                # print(f"{price_level}, {volume} added to the book")

    def add_to_aggregation_bucket(self):
        pass

    def update_aggregation_buckets(self):
        self.aggregation_buckets["bid"] = [
            round(
                self.best_price["bid"]
                - self.best_price["bid"]
                * self.bucket_init_factor
                * (self.bucket_size_exponent ** i),
                2,
            )
            for i in range(1, self.number_aggregation_buckets)
        ] + [float("-inf")]

        self.aggregation_buckets["ask"] = [
            round(
                self.best_price["ask"]
                + self.best_price["ask"]
                * self.bucket_init_factor
                * (self.bucket_size_exponent ** i),
                2,
            )
            for i in range(1, self.number_aggregation_buckets)
        ] + [float("inf")]

    def process_event(self, event, validate_checksum=True):
        if "as" in event[1]:
            self.api_update_book("ask", event[1]["as"])
            self.api_update_book("bid", event[1]["bs"])
        elif "a" in event[1] or "b" in event[1]:
            for item in event[1 : len(event[1:]) - 1]:
                if "a" in item:
                    self.update_aggregation_buckets()
                    self.update_delta_book("ask", item["a"])
                    self.api_update_book("ask", item["a"])
                elif "b" in item:
                    self.update_aggregation_buckets()
                    self.update_delta_book("bid", item["b"])
                    self.api_update_book("bid", item["b"])
                if validate_checksum:
                    if "c" in item:
                        checksum_received = str(item["c"])
                        checksum_calculated = str(self.calculate_order_book_checksum())
                        if checksum_received != checksum_calculated:
                            raise ChecksumException(
                                f"Check received: {checksum_received} - Check calculated: {checksum_calculated}"
                            )


class ChecksumException(Exception):
    pass
