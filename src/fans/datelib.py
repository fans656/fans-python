import datetime

import pytz
import pandas as pd


timezone = pytz.timezone('Asia/Shanghai')


class Timestamp:

    def __init__(self, value: pd.Timestamp):
        self.value = value

    def date_str(self):
        return self.value.strftime('%Y-%m-%d')

    def datetime_str(self):
        return self.value.strftime('%Y-%m-%d %H:%M:%S')

    def offset(self, *args, **kwargs):
        return Timestamp(self.value + pd.DateOffset(*args, **kwargs))

    def round(self, freq = 'D'):
        return Timestamp(self.value.round(freq = freq))

    def ms(self):
        return int(self.value.timestamp() * 1000)

    def __repr__(self):
        return repr(self.value)


def now(timezone = timezone):
    return Timestamp(pd.to_datetime(native_now(timezone = timezone)))


def today(timezone = timezone):
    return now(timezone = timezone)


def yesterday(timezone = timezone):
    return today(timezone = timezone).offset(days = -1)


def from_ms(ms, timezone = timezone):
    return Timestamp(pd.to_datetime(datetime.datetime.fromtimestamp(ms / 1000, timezone)))


def from_native(datetime):
    return Timestamp(pd.to_datetime(datetime))


def native_now(timezone = timezone):
    return datetime.datetime.now(timezone)
