from typing import Tuple
from datetime import datetime, timedelta


class IntervalHandler:
    def __init__(self, interval_in_minutes: int):
        assert 60 % interval_in_minutes == 0
        self.interval = interval_in_minutes
        self.start_of_interval, self.end_of_interval = self.initialize()

    def initialize(self) -> Tuple[datetime, datetime]:
        current_minute = datetime.now().minute
        start_of_interval = datetime.now().replace(
            minute=(current_minute // self.interval) * self.interval,
            second=0,
            microsecond=0,
        )
        return start_of_interval, start_of_interval + timedelta(minutes=self.interval)

    def progress_to_next_interval(self) -> None:
        self.start_of_interval += timedelta(minutes=self.interval)
        self.end_of_interval += timedelta(minutes=self.interval)

    def has_reached_end_of_interval(self) -> bool:
        if datetime.now() >= self.end_of_interval:
            return True
        else:
            return False
