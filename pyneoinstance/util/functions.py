# -*- coding: utf-8 -*-
"""Module containing utility functions used by other modules in the package."""

import functools
import time
import logging
import re
import numpy as np
from pandas import DataFrame
from datetime import datetime
from typing import Tuple, Any, Callable, List
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

def get_file_name(file_type: str, file_parts: List[str]) -> str:
    """Create a string representation of a file name.

    Parameters
    ----------
    file_type : str
        The file extension.
    file_parts : List[str]
        List of strings that are use as part of the file separeted by
        underscore

    Returns
    -------
    str
        String combining the file parts provided by underscores
        and appending the date and time the function was executed.
    """
    file_name_parts = file_parts.copy()
    file_name_parts.append(datetime.today().strftime('%Y%m%d'))
    file_name_parts.append(datetime.now().strftime('%H%M%S'))
    file_name = '_'.join(file_name_parts) + '.' + file_type
    return file_name

def get_logger(logger_name : str, logger_level: int) -> logging.Logger:
    """Get a logger with a specific name and level

    Parameters
    ----------
    logger_name : str
        String use to name the logger.
    logger_level : int
        Level to use for logging.
        The options are logging.[DEBUG, INFO, WARNING, ERROR, CRITICAL]

    Returns
    -------
    Logger
        Logger instance to use.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)
    return logger

def is_reachable_url(url: str) -> bool:
    """Verified if a url exists or if a url is reachable.

    Parameters
    ----------
    url: str
        Uniform Resource Locator.

    Return
    ------
    bool
        True if the file exists or the url is reachable, false otherwise.
    """
    code = None
    try:
        with urlopen(url):
            code = 200
    except (HTTPError, URLError) as error:
        code = error.code
    return code == 200

def timing(function : Callable) -> Tuple[Any, float]:
    """Times annotated functions

    Parameters
    ----------
    function : Callable
        Function or method to time.

    Returns
    -------
    Tuble[Any, float]
        Tuple containing the result of the function and execution time.
    """
    @functools.wraps(function)
    def wrap(*args, **kwargs) -> Tuple[Any, float]:
        start = time.perf_counter()
        result = function(*args, **kwargs)
        end = time.perf_counter()
        elapsed_time = end - start * 1.0
        return result, elapsed_time
    return wrap

def get_batches(data: DataFrame, batch_size) -> List[List[int]]:
    """Partition the data indexes in batches of the specified size.

    Parameters
    ----------
    data : DataFrame
        Pandas DataFrame containing the data
    batch_size : int
        Number of records per partition

    Returns
    -------
    List[List[int, bool]]
        List containing the list of indexes of the DataFrame per partition.
    """
    records = data.shape[0]
    indexes = data.index
    partitions = records // batch_size
    if partitions < 2:
        batches = [list(indexes)]
    else:
        remainder = records % batch_size
        array = np.array(
            indexes[:records-remainder]).reshape(partitions,-1)
        batches = array.tolist()
        if remainder > 0:
            batches.append(indexes[-remainder:])
    return batches

def get_columns_diff(query: str, columns: List[str]) -> List[str]:
    """Get a list of the columns that a Cypher query is trying to access that are
    not in the list of columns provided.

    Parameters
    ----------
    query : str
       Cypher query
    columns : List[str]
        List containing the column names of a Pandas Data Frame
    Returns
    -------
    List[str]
       List containing the columns that Cypher is trying to access that are not
       included in the columns names of the data frame.
    """
    cypher_attributes = set([w.split('.')[1] for w in re.findall(r'\brow\.\w+',query)])
    return [a for a in cypher_attributes if a not in columns]
