import os
import warnings

import numpy as np
from pathlib import Path
from typing import Union

import pandas as pd


class ParquetFileReader:
    """
    Parquet file, reads the file and can access trough properties

    """

    def __init__(self, filepath: str, qa=True, verbose=False, unit=''):
        self.filepath = filepath

        self._df = self._set_datetime_index(pd.read_parquet(filepath))
        self._df.sort_index(inplace=True) # Sort the index
        self._update_properties()

        self.unit = unit
        self.verbose = verbose

        if self.verbose:
            print(f"Loaded {len(self.channels)} channels: {','.join(self.channels)}")

        if qa:
            self.missing_samples
            self.nan_samples

    @staticmethod
    def _set_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
        """
        Make sure the timestamps are in a DatetimeIndex. Two layouts are supported:

        - the timestamps are stored as the index (datetime typed)
        - the timestamps are stored in a column (anywhere in the frame), either datetime typed
          or as integer epoch values (s, ms, us or ns, inferred from the magnitude)

        :return: DataFrame with a DatetimeIndex and only the data channels as columns
        """
        if isinstance(df.index, pd.DatetimeIndex):
            return df

        # Index is a plain range index, look for the timestamp column
        ts_col = next((c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])), None)
        if ts_col is None:
            candidates = [c for c in df.columns
                          if str(c).lower() in ('timestamp', 'time', 'datetime', 'date')]
            if not candidates:
                if isinstance(df.index, pd.RangeIndex):
                    raise ValueError('Could not find a timestamp column or index in the parquet file')
                # Non-range index, e.g. strings: let pandas try to parse it
                df.index = pd.to_datetime(df.index)
                return df
            ts_col = candidates[0]

        ts = df[ts_col]
        if pd.api.types.is_numeric_dtype(ts):
            # Infer the epoch resolution from the magnitude of the values
            magnitude = abs(ts.iloc[0])
            unit = next(u for u, limit in (('s', 1e11), ('ms', 1e14), ('us', 1e17), ('ns', np.inf))
                        if magnitude < limit)
            index = pd.DatetimeIndex(pd.to_datetime(ts.to_numpy(), unit=unit, utc=True), name=ts_col)
        else:
            index = pd.DatetimeIndex(pd.to_datetime(ts), name=ts_col)

        df = df.drop(columns=ts_col)
        df.index = index
        return df

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, new_df):
        self._df = new_df
        self._df.index = pd.to_datetime(new_df.index)
        self._update_properties() # Also update all related properties

    def _update_properties(self):
        """
        Compute properties for ease of use whenever a dataframe is updated

        :return:
        """
        self.channels = list(self.df.columns)
        self.start_timestamp = self.df.index[0]
        self.time = (self.df.index - self.start_timestamp).total_seconds()
        # Sampling frequency in Hz, from the median step so timestamp jitter or gaps do not matter
        self.fs = 1 / pd.Series(self.df.index).diff().median().total_seconds()
        self.duration = len(self.df) / self.fs

    def to_sep005(self):
        """_summary_

        Args:

        Returns:
            list: signals
        """
        signals = []
        for chan in self.channels:
            data = self.df[chan].to_numpy()
            fs_signal = len(data) / self.duration

            signal = {
                'name': chan,
                'data': data,
                'start_timestamp': str(self.start_timestamp),
                'fs': fs_signal,
                'unit_str': self.unit
            }
            signals.append(signal)

        return signals

    @property
    def missing_samples(self):
        """
        Check if the sampling frequency is maintained properly
        :return:
        """
        # check the index matches the sampling frequency
        differences_2nd_order = np.diff(self.time, n=2)  # Calculate the 2nd order differences to find gaps
        # Timestamps may jitter slightly (e.g. rounding to us), a missing sample is a full period off
        is_equidistant = np.all(np.isclose(differences_2nd_order, 0, atol=0.01 / self.fs))
        if not is_equidistant:
            differences = np.diff(self.time) # Slower but required to estimate the number of missing samples
            no_missing_samples = round(sum(differences*self.fs-1))
            raise ValueError(f'{no_missing_samples} Sample(s) missing from all channels')
        if self.verbose and is_equidistant:
            print('QA (missing samples) : Imported signals are equidistant spaced on index')

    @property
    def nan_samples(self):
        """
        Check if there are samples as NaN
        :return:
        """
        nan_counts = self.df.isnull().sum()
        nan_counts = nan_counts[nan_counts > 0]

        if not nan_counts.empty:
            summary = ', '.join(f'{col}: {count}' for col, count in nan_counts.items())
            raise ValueError(f'Channels contain NaN samples. NaN counts: ({summary})')
        if self.verbose:
            print('QA (NaN samples) : Imported signals contain no NaNs')

    def resolve_missing_samples(self, inplace=True, **kwargs):
        """
        Interpolate missing samples using a linear interpolation

        Additional kwargs are passed on directly in pd.DataFrame.interpolate

        :return:
        """

        dt = round(1/self.fs*1e6) # micro seconds

        # Position of every sample on the ideal grid, robust against small jitter on the timestamps
        positions = np.round(self.time * self.fs).astype(int)
        grid = pd.date_range(start=self.start_timestamp, periods=positions[-1] + 1, freq=f'{dt}us')

        # Existing samples keep their original timestamp, the missing ones get the grid timestamp
        new_index = pd.Series(grid, index=range(len(grid)))
        new_index.iloc[positions] = self.df.index

        df_rs = self.df.set_axis(positions).reindex(range(len(grid)))
        df_rs.index = pd.DatetimeIndex(new_index, name=self.df.index.name)

        # Now, perform linear interpolation
        df_interpolated = df_rs.interpolate(method='linear', **kwargs)

        if inplace:
            self.df = df_interpolated
        else:
            return df_interpolated



def read_parquet(path: Union[str, Path], **kwargs) -> list:
    """
    Primary function to read fbgs files based on path


    """
    if not os.path.isfile(path):
        warnings.warn('FAILED IMPORT: No parquet file at: ' + path, UserWarning)
        signals = []
        return signals

    meas_file = ParquetFileReader(path, **kwargs)

    return meas_file.to_sep005()