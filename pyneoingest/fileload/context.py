"""Use to provide context to the test module"""
import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from pyneoingest.util.functions import is_reachable_url
