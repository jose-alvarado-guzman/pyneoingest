# -*- coding: utf-8 -*-
"""Module containing utility functions used by other modules in the package."""

import os
import functools
import threading
import time
from datetime import datetime
from urllib.parse import urlparse
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from typing import Tuple, Any, Callable, List

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


def timing(function: Callable) -> Tuple[Any, float]:
    """Prints the timing for the annotated function

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
        thread_id = threading.get_ident()
        start = time.time()
        ret = function(*args, **kwargs)
        end = time.time()
        diff = end - start * 1.0
        print(f'Thread [{thread_id}] "{function.__name__}" function took {diff:.3f} s')
        return ret, diff
    return wrap
