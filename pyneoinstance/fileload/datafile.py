# -*- coding: utf-8 -*-
"""Module use to load the data file configuration to memory.

An instance of the DataFile class contains all the configuration
parameters for a specific data file if it complies with all the
characteristics provided below.
 - The path provided exist and is readable by the user.
 - The file contains the expected configuration parameters.
 - The file extension exists and in a supported format.
"""

from typing import Dict
import pathlib
from urllib.parse import urlparse
from tarfile import TarFile
from zipfile import ZipFile
import gzip
import io
import boto3
from .helpers import check_required_keys, lower_dict_keys


class DataFile():
    """Class used to model a data file.

    An instance of this class contains all the configuration
    parameters for a specific data file.

    Attributes
    ----------
    data_file : Dict[str, str]
        Contains the configuration parameters for a data file.
    url : str
        String containing the URL of the datafile.
    cypher_queries: List[str]
        List containing the Cypher queries used to load this data file.
    scheme : str
        String containing the url scheme
    netloc : str
        String containing the network location of the data file.
    path : str
        String with the full path of the data file.
    format : str
        Data file extension.
    skip_records : int
        Number of records or lines to skip in the data file.
    chuck_size : int
        Number of records per chunk of the data file to process.
    field_sep : str
        Data file field delimiter.
    skip : bool
        Whether to skip this data file from being process.
    data_file_conn : io.TextIOWrapper
        Object representing the connection to a data file.
    encoding: str
        File encoding
    """
    def __init__(self, data_file: Dict[str, str]):
        """Class constructor.

        Used to create an intance of the data files that are correct.
        Otherwise it thows the appropiate exception.

        Parameters
        ----------
        data_file : Dict[str, str]
            Contains the configuration parameters for a data file.

        Raises
        ------
        ValueError
            When the data file is missing the required configuration
            parameters.
        FileNotFoundError
            When the data file cannot to access.
        TypeError
            When the data file does not contains the file extension or
            when the extension is not supported.
        """
        self.data_file = lower_dict_keys(data_file)
        self.__required_keys = ['url', 'cypher']
        self.__file_keys = data_file.keys()
        missing_keys = check_required_keys(
            self.__required_keys, self.__file_keys)
        if len(missing_keys) > 0:
            raise ValueError(
                'Missing the following configuration key(s): '
                + ','.join(missing_keys))
        self.url = data_file['url']
        self.cql = data_file['cypher']
        url_parsed = urlparse(self.url)
        self.scheme = url_parsed.scheme.lower()
        self.netloc = url_parsed.netloc
        self.path = url_parsed.path
        self.formats = [f.replace('.','').lower() for f in
                        pathlib.Path(self.url).suffixes]
        self.format = pathlib.Path(
            self.url).suffix.replace('.', '').lower()
        self.__supported_schemes = ['s3','file','http','https']
        self.__supported_formats = ['gz', 'zip', 'csv', 'txt', 'json', 'tgz']
        self.skip_records = data_file.get('skip_records') or 0
        self.chunk_size = data_file.get('chunk_size') or 1000
        self.field_sep = data_file.get('field_separator') or ','
        self.skip = data_file.get('skip_file') or False
        self.encoding = data_file.get('encoding') or 'utf-8'
        if self.format == '':
            raise TypeError(
                f'Error reading url {self.url}: No file extension found')
        if self.format not in self.__supported_formats:
            raise TypeError(
                f'Error reading url {self.url}: File format {self.format} ' +
                 'not supported, only {self.__supported_formats} are allowed')
        if self.scheme not in self.__supported_schemes:
            raise TypeError(
                f'Error reading url {self.url}: URL scheme {self.scheme} ' +
                 'not supported, only {self.__supported_schemes} are allowed')
        self.open()


    def open(self):
        """Set the connection to the file depending on the scheme and compression.

        This method sets the connection to a file depending on the file scheme and
        the file compression.

        Raises
        ------
        Exception
            Exception raise by the open method.
        """
        if self.scheme in ['file','http','https']:
            if self.format == 'zip':
                self.open_zip_file()
            elif self.format == 'tgz':
                self.open_tar_file()
            elif self.format == 'gz':
                self.open_gzip_file()
            else:
                self.open_text_file()

    def open_tar_file(self):
        """Open a tar file"""
        self.set_file_buffer()
        try:
            with TarFile.open(self.__file_buffer) as compress_file:
                file = compress_file.members.pop()
                self.format = pathlib.Path(file.name).suffix.replace(
                    '.','').lower()
                self.data_file_conn = compress_file.extractfile(file)
        except Exception as exception:
            raise exception

    def open_zip_file(self):
        """Open a zip file"""
        self.set_file_buffer()
        try:
            with ZipFile(self.__file_buffer) as zip_file:
                file_name = zip_file.infolist().pop().filename
                self.data_file_conn = zip_file.open(file_name)
                self.format = pathlib.Path(file_name).suffix.replace(
                    '.','').lower()
        except Exception as exception:
            raise exception

    def open_s3_file(self):
        try:
            if self.format == 'tgz':
                file = boto3.Session().client('s3').get_object(
                    Bucket = self.netloc,
                    Key = self.path[1:])['Body']
                compress_file = TarFile.open(fileobj=file,
                                  mode='r:gz')
                file_name = compress_file.members.pop()
                self.data_file_conn = compress_file.extractfile(
                    file_name)
                self.format = pathlib.Path(
                    file_name.name).suffix.replace(
                        '.','').lower()
                compress_file.close()
        except Exception as exception:
            raise exception

    def open_gzip_file(self):
        """Open a gzip file"""
        self.set_file_buffer()
        self.data_file_conn = gzip.open(self.__file_buffer, 'rt')

    def open_text_file(self):
        """Open a text file"""
        self.data_file_conn = open(self.path, 'r', encoding=self.encoding)

    def close(self):
        """Close the data file connection."""
        self.data_file_conn.close()

    def set_file_buffer(self):
        """Set the file location"""
        if self.scheme == 'file':
            file_location = self.path
        else:
            file_location = self.url
        try:
            if isinstance(file_location, str):
                self.__file_buffer = file_location
            else:
                self.__file_buffer = io.BytesIO(file_location.read())
        except Exception as exception:
            raise exception
