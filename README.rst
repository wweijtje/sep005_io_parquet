SEP005 24SEA parquet io
-----------------------

Basic package to import data from parquet files as stored by 24SEA's SHM solutions.

NOTE: This wont work for any parquet file, but assumes an underlying structure in the parquet file.

Using the package
------------------

In its most basic usage the package allows to load the parquet file from a specified path:

    .. code-block:: python

        from sep005_io_parquet import read_parquet

        file_path = # Path to the parquet file of interest
        signals = read_parquet(file_path)


The imported signals respect the SEP005 set of rules for timeseries.

Expected file structure
-----------------------

A parquet file holds a table in which every column is either a data channel or the timestamp.
All data channels share the same timestamps, i.e. they are sampled synchronously at a constant sampling frequency.

- Every data channel is a numeric column (e.g. ``float64``). The column name becomes the signal ``name``.
- The timestamps are stored in one of two ways, both of which are supported:

**1. Timestamp as the index**

The timestamps are stored as a datetime index (typically named ``time``) of the table, usually timezone aware (UTC).
The remaining columns are all data channels.

.. code-block:: text

    time (index)                      AO1_A04_NAC_ACC_LAT110_FA   AO1_A04_NAC_ACC_LAT110_SS  ...
    2023-12-03 02:00:00+00:00         0.649444                    0.648403                   ...
    2023-12-03 02:00:00.032000+00:00  0.648403                    0.710282                   ...

**2. Timestamp as a column**

The table has a default (range) index, and the timestamps are stored in a regular column.
This column can be at any position in the table, it does not have to be the first (or last) one.
It is recognised by its datetime type, or, when stored as an integer, by its name: ``timestamp``, ``time``,
``datetime`` or ``date`` (case insensitive). Integer timestamps are interpreted as time since the Unix epoch (UTC) and
the unit (seconds, milliseconds, microseconds or nanoseconds) is inferred from the magnitude of the values.
The timestamp column is not returned as a signal.

.. code-block:: text

    BB_D03_TP_TEMP_LAT019_DEG018_0_nr2  ...  timestamp (int64, ns since epoch)
    9.088255                            ...  1793926200002003000
    9.087077                            ...  1793926200102003000

In both cases the signals are sorted by time, and the following is derived from the timestamps:

- ``start_timestamp``: the first timestamp, as a string.
- ``fs``: the sampling frequency in Hz, based on the median time step between samples. Small jitter on the timestamps
  (e.g. due to rounding to microseconds) is therefore tolerated.

If no timestamp can be found, a ``ValueError`` is raised.

Quality assurance
-----------------

By default the file is checked when it is loaded (``qa=True``, pass ``qa=False`` to skip this):

- **Missing samples**: the timestamps must be equidistant. If one or more samples are missing a ``ValueError`` is raised
  reporting the number of missing samples.
- **NaN samples**: none of the channels may contain NaN values, otherwise a ``ValueError`` is raised.

For finer control, e.g. to fill gaps by linear interpolation, use the ``ParquetFileReader`` class:

.. code-block:: python

    from sep005_io_parquet.parquet import ParquetFileReader

    reader = ParquetFileReader(file_path, qa=False)
    reader.resolve_missing_samples()  # linear interpolation, kwargs are passed to pandas' DataFrame.interpolate
    signals = reader.to_sep005()

Acknowledgements
----------------
