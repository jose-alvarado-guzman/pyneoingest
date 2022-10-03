# -*- coding: utf-8 -*-
"""Module containing helpers functions used to load YAML files.

    Functions
    ---------
    lower_dict_keys(dictionary: Dict[str, Any])
        Convert the keys of a dictionary to lower case.
    check_required_keys(required_keys: List[str], keys: List[str])
        Verify if the elements in one list are contained in the other list.
"""

from typing import Dict, Any, List

def lower_dict_keys(dictionary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the keys of a dictionary to lower case.

    Provided a dictionary, it returns the dictionary with
    the same values but with the keys in lower case.

    Parameters
    ----------
    dictionary : Dict[str, Any]
        Source dictionary.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the same values as the source dictionary but all keys
        are converted to lower case.
    """
    lower_keys = [key.lower() for key in dictionary.keys()]
    return dict(zip(lower_keys, dictionary.values()))

def check_required_keys(required_keys: List[str], keys: List[str]) -> List[str]:
    """Verify if the elements in one list are contained in the other list.

    Verify if the elements of the required_keys_list are contained in the keys
    list.

    Parameters
    ----------
    required_keys : List[str]
        List containing the elements to search for.
    keys : List[str]
        List containing elements to search from.

    Returns
    -------
    List[str]
        List of elements in the required_keys that are not included in keys.
    """
    missing_keys = [key for key in required_keys if key not in keys]
    return missing_keys
