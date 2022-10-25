# -*- coding: utf-8 -*-
"""Module use to load csv files into memory as DataFrames.
"""
import logging
from typing import Optional
import pandas as pd
from util.functions import is_reachable_url

def load_flat_file(url: str,delimiter: Optional[str] = ',',
                   header=True):
    logging.info('Processing csv %s',url)
    file_conn = data_file.data_file_conn
    header = str(file_conn.readline())
    header = header.strip().split(delimiter)
    row_chunks = pd.read_csv(file_conn, dtype=str,
                             sep=data_file.field_sep,
                             error_bad_lines=False,
                             index_col=False,
                             skiprows=data_file.skip_records,
                             names=header,
                             low_memory=False,
                             engine='c',
                             compression='infer',
                             header=None,
                             chunksize=data_file.chunk_size)
