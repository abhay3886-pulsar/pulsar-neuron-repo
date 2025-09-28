from __future__ import annotations

import datetime as dt
from statistics import median
from typing import Iterable, Iterator, Sequence


def to_datetime(values):
    if isinstance(values, (str, dt.datetime)):
        return _convert_datetime(values)
    return [_convert_datetime(v) for v in values]


def _convert_datetime(value):
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time())
    return dt.datetime.fromisoformat(str(value))


class Series:
    def __init__(self, data: Iterable, name: str | None = None) -> None:
        self._data = list(data)
        self.name = name

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return Series(self._data[item], name=self.name)
        return self._data[item]

    @property
    def iloc(self) -> _ILocSeries:
        return _ILocSeries(self)

    def rolling(self, window: int, min_periods: int | None = None) -> _Rolling:
        return _Rolling(self, window, min_periods or window)

    def dropna(self) -> Series:
        return Series([x for x in self._data if x is not None], name=self.name)

    def sum(self) -> float:
        return float(sum(x for x in self._data if x is not None))

    def max(self):
        values = [x for x in self._data if x is not None]
        return max(values) if values else None

    def min(self):
        values = [x for x in self._data if x is not None]
        return min(values) if values else None

    def mean(self) -> float | None:
        values = [x for x in self._data if x is not None]
        if not values:
            return None
        return float(sum(values) / len(values))

    def median(self) -> float:
        values = [x for x in self._data if x is not None]
        return float(median(values)) if values else 0.0

    def between(self, left, right) -> Series:
        return Series([(left <= v <= right) for v in self._data], name=self.name)

    def any(self) -> bool:
        return any(bool(v) for v in self._data)

    @property
    def dt(self) -> _DatetimeAccessor:
        return _DatetimeAccessor(self)

    def to_list(self) -> list:
        return list(self._data)

    def _binary_op(self, other, op):
        if isinstance(other, Series):
            data = [op(a, b) for a, b in zip(self._data, other._data)]
        else:
            data = [op(a, other) for a in self._data]
        return Series(data, name=self.name)

    def __add__(self, other):
        return self._binary_op(other, lambda a, b: a + b)

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        return self._binary_op(other, lambda a, b: a - b)

    def __mul__(self, other):
        return self._binary_op(other, lambda a, b: a * b)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __truediv__(self, other):
        return self._binary_op(other, lambda a, b: a / b)


class _ILocSeries:
    def __init__(self, series: Series) -> None:
        self.series = series

    def __getitem__(self, item):
        data = self.series._data
        if isinstance(item, slice):
            return Series(data[item], name=self.series.name)
        return data[item]


class _Rolling:
    def __init__(self, series: Series, window: int, min_periods: int) -> None:
        self.series = series
        self.window = window
        self.min_periods = min_periods

    def mean(self) -> Series:
        data: list[float | None] = []
        values = self.series._data
        for i in range(len(values)):
            start = max(0, i - self.window + 1)
            window_vals = [v for v in values[start : i + 1] if v is not None]
            if len(window_vals) < self.min_periods:
                data.append(None)
            else:
                data.append(float(sum(window_vals) / len(window_vals)))
        return Series(data, name=self.series.name)


class _DatetimeAccessor:
    def __init__(self, series: Series) -> None:
        self.series = series

    def strftime(self, fmt: str) -> Series:
        return Series([v.strftime(fmt) for v in self.series._data], name=self.series.name)


class DataFrame:
    def __init__(self, rows: Sequence[dict] | None = None) -> None:
        rows = list(rows or [])
        self._rows = rows
        self._build_columns()

    def _build_columns(self) -> None:
        self._data: dict[str, Series] = {}
        if not self._rows:
            return
        keys = self._rows[0].keys()
        for key in keys:
            self._data[key] = Series([row[key] for row in self._rows], name=key)

    def __len__(self) -> int:
        return len(self._rows)

    @property
    def empty(self) -> bool:
        return len(self) == 0

    def __getitem__(self, key: str) -> Series:
        return self._data[key]

    def __setitem__(self, key: str, value) -> None:
        if isinstance(value, Series):
            data = value.to_list()
        else:
            data = list(value)
        if len(data) != len(self._rows):
            raise ValueError("Column length mismatch")
        for row, item in zip(self._rows, data):
            row[key] = item
        self._data[key] = Series(data, name=key)

    @property
    def iloc(self) -> _ILocDataFrame:
        return _ILocDataFrame(self)

    @property
    def loc(self) -> _LocDataFrame:
        return _LocDataFrame(self)

    def _row_dict(self, idx: int) -> dict:
        return dict(self._rows[idx])

    def _subset(self, indices: list[int]) -> DataFrame:
        return DataFrame([self._rows[i] for i in indices])


class _ILocDataFrame:
    def __init__(self, df: DataFrame) -> None:
        self.df = df

    def __getitem__(self, item):
        if isinstance(item, slice):
            indices = list(range(len(self.df)))[item]
            return self.df._subset(list(indices))
        if item < 0:
            item += len(self.df)
        return _Row(self.df, item)


class _LocDataFrame:
    def __init__(self, df: DataFrame) -> None:
        self.df = df

    def __getitem__(self, mask_series: Series) -> DataFrame:
        indices = [i for i, flag in enumerate(mask_series) if flag]
        return self.df._subset(indices)


class _Row:
    def __init__(self, df: DataFrame, idx: int) -> None:
        self.df = df
        self.idx = idx

    def __getitem__(self, key: str):
        return self.df._data[key]._data[self.idx]
