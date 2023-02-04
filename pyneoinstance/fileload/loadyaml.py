# -*- coding: utf-8 -*-
"""Module use for parsing a YAML file.

This module parses and return a provided YAML file after verifying
that it complies with all the characteristics provided below.
Otherwise it will raise a proper exception.
 - The path provided exist and is readable by the user.
 - The file is properly formatted as a YAML file.
 - The file contains the expected configuration parameters.
"""

import logging
from typing import List, Dict, Any, Optional
import yaml
from yaml.parser import ParserError
from .helpers import lower_dict_keys
from .helpers import check_required_keys

def load_yaml_file(yaml_file: str,
                   required_keys: Optional[List[str]] = None
                  ) -> Dict[str, Any]:
    """Load the configuration file.

    Parse and return the YAML file if the file is readable and
    formatted propertly. Otherwise it raised exceptions.

    Parameters
    ----------
    yaml_file: str
        Full path of the YAML file.
    required_keys : List[str]
        List of the required keys.

    Returns
    -------
    Dict[str, Any]
        Python dictionary containing the YAML file data.

    Raises
    ------
    ValueError
        If the file is missing the required keys.
    FileNotFoundException
        If the YAML file does not exists.
    ParserError
        If the YAML file is not formated correctly.
    """
    error_messages = {
        'FileNotFoundError': 'YAML file not found: ',
        'ParserError': 'Wrong YAML file format: ',
        'ValueError': 'Missing the following configuration key(s): '
    }
    configuration_object = None
    try:
        with open(yaml_file, encoding='utf8') as config_file:
            configuration_object = lower_dict_keys(
                yaml.load(config_file, yaml.SafeLoader))
        config_keys = configuration_object.keys()
        if required_keys:
            missing_keys = check_required_keys(required_keys, config_keys)
            if len(missing_keys) > 0:
                error_msg = error_messages[
                    'ValueError'] + ','.join( missing_keys)
                raise ValueError(error_msg)
    except FileNotFoundError as exception:
        error_msg = error_messages[exception.__class__.__name__] + str(exception)
        raise FileNotFoundError(error_msg) from exception
    except ParserError as exception:
        error_msg = error_messages[exception.__class__.__name__] + str(exception)
        raise ParserError(error_msg) from exception
    return configuration_object
