# -*- coding: utf-8 -*-
"""Module use to load csv files into memory as DataFrames.
"""
import pandas as pd
from util.functions import is_reachable_url
from typing import Optional

def load_flat_file(url: str,delimiter: Optional[str] = ',',
                   header=True)

