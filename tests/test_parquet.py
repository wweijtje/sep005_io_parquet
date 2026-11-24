import datetime
import os
import sys

import numpy as np
import pytest
from sdypy_sep005.sep005 import assert_sep005

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sep005_io_parquet.parquet import read_parquet, ParquetFileReader

current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(current_dir, 'static')
GOOD_FILES = os.listdir(os.path.join(static_dir, 'good'))


@pytest.mark.parametrize("filename", GOOD_FILES)
def test_compliance_sep005(filename):
    """
    Test the compliance with the SEP005 guidelines
    """
    file_path = os.path.join(static_dir, 'good', filename)
    signals = read_parquet(file_path)  # should already not crash here

    assert len(signals) != 0  # Not an empty response
    assert_sep005(signals)

@pytest.mark.parametrize("filename", GOOD_FILES)
def test_start_timestamp(filename):
    """
    The start timestamp of the pf object should match the first timestamp stored in the parquet file,
    independent of how the timestamps are stored (index or column, datetime or integer epoch)
    """
    file_path = os.path.join(static_dir, 'good', filename)

    # Read the raw file with pyarrow, to not depend on the logic of the reader
    table = pq.read_table(file_path)
    ts_field = next((f for f in table.schema if pa.types.is_timestamp(f.type)), None) \
        or table.schema.field('timestamp')
    first_raw = table[ts_field.name][0]

    if pa.types.is_timestamp(ts_field.type):
        expected = pd.Timestamp(first_raw.as_py())
    else:  # Integer epoch in nanoseconds
        expected = pd.Timestamp(first_raw.as_py(), unit='ns', tz='UTC')
    if expected.tzinfo is None:
        expected = expected.tz_localize('UTC')

    pf = ParquetFileReader(file_path, qa=False)
    assert pf.start_timestamp == expected
    assert pf.df.index[0] == expected

    # Also propagated to the SEP005 signals
    for signal in pf.to_sep005():
        assert pd.Timestamp(signal['start_timestamp']) == expected


@pytest.mark.parametrize("filename", GOOD_FILES)
def test_qa(filename):
    """
    Test the Quality assurance functions
    """
    file_path = os.path.join(static_dir, 'good', filename)

    # good pf should pass all quality assurance
    pf = ParquetFileReader(file_path, qa=True)

    # Inserting a NaN should trip nan_samples
    pf.df.iloc[10, 1] = np.nan

    pf.missing_samples # Should still be fine
    with pytest.raises(ValueError) as excinfo:
        pf.nan_samples
    assert 'Channels contain NaN samples' in str(excinfo.value)
    assert f'({pf.df.columns[1]}: 1)' in str(excinfo.value)

    # Removing 5 row(s) should trip missing_samples
    for i in range(5):
        pf.df = pf.df.drop(index=pf.df.iloc[10].name)
        pf.nan_samples  # Should still be fine
        with pytest.raises(ValueError) as excinfo:
            pf.missing_samples
        assert f'{i+1} Sample(s) missing from all channels' in str(excinfo.value) # Increasing values of the number of missing samples

@pytest.mark.parametrize("filename", GOOD_FILES)
def test_df_update(filename):
    """
    Test the behaviour when a DataFrame is updated, other properties also
    need to be updated accordingly

    """
    file_path = os.path.join(static_dir, 'good', filename)

    # good pf should pass all quality assurance
    pf = ParquetFileReader(file_path, qa=True)

    dur_0 = pf.duration
    dt_s = pf.start_timestamp
    pf.df = pf.df.drop(pf.df.index[:100]) # Drop the first 100 samples

    assert pf.start_timestamp == dt_s+datetime.timedelta(seconds=(1/pf.fs*100))
    assert pf.duration == dur_0 - 1/pf.fs*100
    assert len(pf.time) == len(pf.df)

@pytest.mark.parametrize("filename", GOOD_FILES)
def test_resolve_missing_samples(filename):
    file_path = os.path.join(static_dir, 'good', filename)
    # good pf should pass all quality assurance
    pf = ParquetFileReader(file_path, qa=True)
    no_samples = len(pf.df)

    dt_del = pf.df.iloc[10].name
    pf.df = pf.df.drop(index=dt_del)
    with pytest.raises(ValueError):
        pf.missing_samples

    assert len(pf.df) == no_samples-1
    assert dt_del not in pf.df.index

    #%% Fix the missing sample
    pf.resolve_missing_samples(inplace=True)
    # Both QA's should pass now
    pf.missing_samples
    pf.nan_samples

    assert len(pf.df) == no_samples
    assert dt_del in pf.df.index
    assert pf.df.index.is_monotonic_increasing
    for i in range(len(pf.channels)):
        # All channels are interpolated
        assert pf.df.iloc[10,i] == pytest.approx((pf.df.iloc[11,i]+pf.df.iloc[9,i])/2) # Linear interpolation


    #%% Check if the limit on the number of samples to resolve is respected
    pf.df = pf.df.drop(pf.df.index[90:100])  # Drop 10 samples
    pf.resolve_missing_samples(inplace=True)
    pf.missing_samples
    pf.nan_samples
    assert len(pf.df) == no_samples

    pf.df = pf.df.drop(pf.df.index[90:100])  # Drop 10 samples
    pf.resolve_missing_samples(inplace=True, limit=5)
    pf.missing_samples # No missing samples, just NaN
    with pytest.raises(ValueError):
        pf.nan_samples