import datetime

import pytz


timezone = pytz.timezone('Asia/Shanghai')


class Timestamp:

    date_str_fmt = '%Y-%m-%d'
    datetime_str_fmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self, value: 'pd.Timestamp'):
        self.value = value

    def date_str(self, fmt = '%Y-%m-%d'):
        return self.value.strftime(fmt)

    def time_str(self, fmt = '%H:%M:%S'):
        return self.value.strftime(fmt)

    def datetime_str(self):
        return self.value.strftime(Timestamp.datetime_str_fmt)

    @staticmethod
    def from_date_str(value):
        if isinstance(value, (Timestamp, NoneType)):
            return value
        return from_native(
            datetime.datetime.strptime(value, Timestamp.date_str_fmt).astimezone(timezone)
        )

    @staticmethod
    def from_datetime_str(value):
        if isinstance(value, (Timestamp, NoneType)):
            return value
        return from_native(
            datetime.datetime.strptime(value, Timestamp.datetime_str_fmt).astimezone(timezone)
        )

    @classmethod
    def to_datetime_str(cls, value):
        if isinstance(value, cls):
            return value.datetime_str()
        else:
            return value

    def to_native(self):
        return self.value.to_pydatetime()

    def offset(self, *args, **kwargs):
        import pandas as pd
        return Timestamp(self.value + pd.DateOffset(*args, **kwargs))

    def round(self, freq = 'D'):
        return Timestamp(self.value.round(freq = freq))

    def floor(self, freq = 'D'):
        return Timestamp(self.value.floor(freq = freq))

    def ms(self):
        return int(self.value.timestamp() * 1000)

    def from_now(self) -> 'pd.Timedelta':
        return self.value - now().value

    def from_today(self) -> 'pd.Timedelta':
        return self.value - today().value

    def till_now(self) -> 'pd.Timedelta':
        return now().value - self.value

    def till_today(self) -> 'pd.Timedelta':
        return today().value - self.value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __gt__(self, other):
        return self.value > other.value

    def __ge__(self, other):
        return self.value >= other.value

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return self.value != other.value

    def __str__(self):
        return str(self.value.to_pydatetime())

    def __repr__(self):
        return repr(self.value)


def now(timezone = timezone):
    import pandas as pd
    return Timestamp(pd.to_datetime(native_now(timezone = timezone)))


def today(timezone = timezone):
    return now(timezone = timezone).floor()


def yesterday(timezone = timezone):
    return today(timezone = timezone).offset(days = -1)


def from_ms(ms, timezone = timezone):
    import pandas as pd
    return Timestamp(pd.to_datetime(datetime.datetime.fromtimestamp(ms / 1000, timezone)))


def from_native(datetime):
    import pandas as pd
    return Timestamp(pd.to_datetime(datetime))


from_date_str = Timestamp.from_date_str
from_datetime_str = Timestamp.from_datetime_str


def native_now(timezone = timezone):
    return datetime.datetime.now(timezone)


NoneType = type(None)
