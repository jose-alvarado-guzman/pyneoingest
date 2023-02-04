# -*- coding: utf-8 -*-
"""Module responsible for handling Neo4j transactions.

This module is responsible for the folling task:
  - Create a Neo4j driver connection or connections if executed in parallel.
  - Handle write and read queries (with and without Cypher parameters)
    to Neo4j using transaction functions.
  - Ingest data into Neo4j with the provided Pandas DataFrame and Cypher query.
  - Close the driver connections to Neo4j.
"""

from typing import Optional, List, Dict, Any
from collections import defaultdict
import multiprocessing as mp
import logging
import numpy as np
from pandas import DataFrame
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.exceptions import AuthError
from neo4j.exceptions import ClientError
from neo4j.exceptions import ConfigurationError
from .context import get_logger

def _get_driver(neo_info : Dict[str, str]):
    try:
        if neo_info['encrypted'] != '':
            driver = GraphDatabase.driver(
                neo_info['uri'],
                auth=(neo_info['user'], neo_info['password']),
                encrypted=neo_info['encrypted'])
        else:
            driver = GraphDatabase.driver(
                neo_info['uri'],
                auth=(neo_info['user'], neo_info['password']))
    except ServiceUnavailable as exception:
        raise ServiceUnavailable() from exception
    except AuthError as exception:
        raise  AuthError() from exception
    except ConfigurationError as exception:
        raise ConfigurationError() from exception
    return driver

def _get_session(driver, database: Optional[str] = None):
    if database:
        session = driver.session(database=database)
    else:
        session = driver.session()
    return session

def _write_transaction_function(transaction, query, **kwargs):
    result = transaction.run(query, **kwargs)
    return result.consume()

def _read_transaction_function(transaction, query, **kwargs):
    results = transaction.run(query, **kwargs)
    data = DataFrame(results.values(), columns=results.keys())
    return data

def _execute_write(session, query,
                   rows: Optional[Dict[str, Any]] = None,
                   parameters: Optional[Dict[str, Any]] = None
                  ):
    if parameters:
        params = parameters
    else:
        params = {}
    try:
        if rows:
            results = session.write_transaction(
                _write_transaction_function, query,
                rows = rows, **params).counters.__dict__
        else:
            results = session.write_transaction(
                _write_transaction_function, query,
                **params).counters.__dict__
    except ServiceUnavailable() as exception:
        raise ServiceUnavailable() from exception
    except ClientError as exception:
        raise ClientError() from exception
    return results

def _execute_write_parallel(neo_info, database, query,
                   rows: Optional[Dict[str, Any]] = None,
                   parameters: Optional[Dict[str, Any]] = None
                  ):
    if parameters:
        params = parameters
    else:
        params = {}
    with _get_driver(neo_info) as driver:
        with _get_session(driver, database) as session:
            try:
                if rows:
                    results = session.write_transaction(
                        _write_transaction_function, query,
                        rows = rows, **params).counters.__dict__
                else:
                    results = session.write_transaction(
                        _write_transaction_function, query,
                        **params).counters.__dict__
            except ServiceUnavailable() as exception:
                raise ServiceUnavailable() from exception
            except ClientError as exception:
                raise ClientError() from exception
    return results

class Neo4jInstance:
    """Class use to handle Neo4j requests.

        Methods
        -------
        execute_read_query(query: str,database Optional[str],
                           kwargs Optional[Dict[str, Any]]) -> DataFrame
            Execute a read query to a specific database.
        execute_write_queries(queries: List[str], database: Optional[str],
                            kwargs: Optional[Dict[str, Any]]) -> None
            Execute a list of write queries to a specific database.
        execute_write_query(query: str, database: Optional[str],
                            kwargs: Optional[Dict[str, Any]]) -> None
            Execute a write query to a specific database.
        execute_write_query_with_data(self, query: str, data: DataFrame,
                                      database: Optional[str] = None,
                                      partitions: Optional[int] = 1,
                                      parallel: Optional[bool] = False,
                                      workers: Optional[int] = None,
                                      parameters: Optional[Dict[str, Any]]
                                      ) -> Dict[str, int]
            Execute a write query using data on a DataFrame.
        execute_write_queries_with_data(self, query: List[str], data: DataFrame,
                                        database: Optional[str] = None,
                                        partitions: Optional[int] = 1,
                                        parallel: Optional[bool] = False,
                                        workers: Optional[int] = None,
                                        parameters: Optional[Dict[str, Any]]
                                        ) -> Dict[str, int]
            Execute a list of write queries using data on a DataFrame.
    """
    def __init__(self, uri: str, user: str, password: str,
                 **kwargs: Optional[Dict[str, Any]]) -> None:
        """Class constructor.

        Parameters
        ----------
        uri : str
           The Uniform Resource Identifier of the database.
        user : str
            The Neo4j username.
        password : str
            The unencrypted Neo4j user password.
        kwargs : Dict[str, Any], optional
            Extra arguments, more notably encrypted (bool)

        Raises
        ------
        AuthError
            When the authentication to Neo4j fails.
        ServiceUnavailable
            When the Neo4j instance is not available.
        ConfigurationError
            When the Neo4j URI is not valid.
        """

        self.neo_info = {}
        self.neo_info['uri'] = uri
        self.neo_info['user'] = user
        self.neo_info['password'] = password
        self.neo_info['encrypted'] = kwargs.get('encrypted') or ''
        self.__results = defaultdict(int)
        self.__logger = get_logger('pyneoingest', logging.INFO)


    def execute_read_query(self, query: str,
                           database: Optional[str] = None,
                           **kwargs: Optional[Dict[str, Any]]
                          ) -> DataFrame:
        """Execute a read transaction to a specific database.

        Parameters
        ----------
        query : str
            String containing the Cypher query to execute.
        database : str, optional
            Name of the Neo4j database of which to execute the
            transaction. If not provided the default database is going to
            be use.
        kwargs : Dict[str, Any], optional
            Extra arguments containing optional cypher parameters.

        Return
        ------
        DataFrame
            Pandas DataFrame with the results of the transaction.

        Raises
        ------
        ServiceUnavailable
            When the Neo4j instance is not available.
        ClientError
            When there is a Cypher syntax error or datatype error.
    """
        with _get_driver(self.neo_info) as driver:
            with _get_session(driver, database) as session:
                try:
                    result = session.read_transaction(
                        _read_transaction_function,query=query,**kwargs)
                except ServiceUnavailable as exception:
                    raise ServiceUnavailable() from exception
                except ClientError as exception:
                    raise ClientError() from exception
        return result

    def execute_write_queries(self, queries: List[str],
                            database: Optional[str] = None,
                            **kwargs: Optional[Dict[str, Any]]
                           ) -> Dict[str, int]:
        """Execute a write query to a specific database.

            Parameters
            ----------
            queries : List[str]
                List of Cypher queries to execute.
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.
            kwargs : Dict[str, Any], optional
                Extra arguments containing optional cypher parameters.

            Returns
            -------
            Dictionary
                Python dictionary containing Neo4j write counts (nodes_created,
                labels_added, properties_set, ect.)

            Raises
            ------
            ServiceUnavailable
                When the Neo4j instance is not available.
            ClientError
                When their is a Cypher syntax error or datatype error.
        """
        results = defaultdict(int)
        with _get_driver(self.neo_info) as driver:
            with _get_session(driver, database) as session:
                for query in queries:
                    result = _execute_write(session, query, **kwargs)
                    for key, value in result.items():
                        if key != '_contains_updates':
                            results[key] += value
                self.__logger.info(f'Loading stats: {dict(results)}')
        return dict(results)

    def execute_write_query(self, query: str,
                            database: Optional[str] = None,
                            **kwargs: Optional[Dict['str',Any]]
                           ) -> Dict[str, Any]:
        """Execute a write query to a specific database.

            Parameters
            ----------
            query : str
                Cypher query to execute.
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.
            kwargs : Dict[str, Any], optional
                Extra arguments containing optional cypher parameters.

            Returns
            -------
            Dictionary
                Python dictionary containing Neo4j write counts (nodes_created,
                labels_added, properties_set, ect.)

            Raises
            ------
            ServiceUnavailable
                When the Neo4j instance is not available.
            ClientError
                When their is a Cypher syntax error or datatype error.
        """
        result = self.execute_write_queries([query], database, **kwargs)
        return  result

    def execute_write_queries_with_data(self, queries: List[str], data: DataFrame,
                                        database: Optional[str] = None,
                                        partitions: Optional[int] = 1,
                                        parallel: Optional[bool] = False,
                                        workers: Optional[int] = None,
                                        parameters: Optional[Dict[str, Any]] = None
                                       ) -> Dict[str, int]:
        """Execute a list of write queries using data to update a specific database.

            Parameters
            ----------
            queries : List[str]
                List of strings constaining the Cyphrer queries to execute.
            data : DataFrame
                Pandas DataFrame containing data to process.
            database : str, optional
                Name of the Neo4j database of which to execute the transactions.
                If not provided the default database is going to be use.
            partitions : int, optional
                The number of partitions in which to split the data frame.
            parallel : bool, optional
                Wheather to execute the load in parallel.
            workers : int, optional
                Number of processes to spawn to load the data.
            parameters : Dict[str, Any], optional
                Extra arguments containing optional cypher parameters.

            Returns
            -------
            Dictionary
                Python dictionary containing Neo4j write counts (nodes_created,
                labels_added, properties_set, ect.)

            Raises
            ------
            ServiceUnavailable
                When the Neo4j instance is not available.
            ClientError
                When their is a Cypher syntax error or datatype error.
            ValueError
                When the number of batch sizes to split the DataFrame on
                is larger than the number of rows in it.
        """
        if partitions > data.shape[0]:
            raise ValueError(
                "The batch size cannot be greater than the number of rows in the data.")
        data_chunks = np.array_split(data,partitions)
        if parameters:
            params = parameters
        else:
            params = {}
        if parallel:
            if workers and workers > 0 and workers <= mp.cpu_count():
                workers_num = workers
            else:
                workers_num = mp.cpu_count()
            for query in queries:
                with mp.Pool(workers_num) as worker_pool:
                    results = worker_pool.starmap(
                        _execute_write_parallel,
                        [(self.neo_info, database,query, d.fillna(value='').to_dict('records'),
                          params) for d in data_chunks]
                    )
        else:
            results = []
            with _get_driver(self.neo_info) as driver:
                with _get_session(driver, database) as session:
                    for rows in data_chunks:
                        rows_dict = {'rows': rows.fillna(value="").to_dict('records')}
                        for query in queries:
                            results.append(_execute_write(session,
                                                          query,
                                                          rows_dict['rows'],
                                                          params))
        for result in results:
            for key, value in result.items():
                if key != '_contains_updates':
                    self.__results[key] += value
        results = dict(self.__results.copy())
        self.__results.clear()
        self.__logger.info(f'Loading stats: {results}')
        return results

    def execute_write_query_with_data(self,
                                      query: str, data: DataFrame,
                                      database: Optional[str] = None,
                                      partitions: Optional[int] = 1,
                                      parallel: Optional[bool] = False,
                                      workers: Optional[int] = None,
                                      parameters: Optional[Dict[str, Any]] = None
                                     ) -> Dict[str, int]:
        """Execute a write query with data to update a specific database.

            Parameters
            ----------
            query : str
                Cypher query to execute.
            data : DataFrame
                Pandas DataFrame containing data to process.
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.
            partitions : int, optional
                The number of partitions in which to split the data frame.
            parallel : bool, optional
                Wheather to execute the load in parallel.
            workers : int, optional
                Number of processes to spawn to load the data.
            parameters : Dict[str, Any], optional
                Extra arguments containing optional cypher parameters.

            Returns
            -------
            Dictionary
                Python dictionary containing Neo4j write counts (nodes_created,
                labels_added, properties_set, ect.)

            Raises
            ------
            ServiceUnavailable
                When the Neo4j instance is not available.
            ClientError
                When their is a Cypher syntax error or datatype error.
            ValueError
                When the number of batch sizes to split the DataFrame on
                is larger than the number of rows in it.
        """
        if parameters:
            params = parameters
        else:
            params = {}
        result = self.execute_write_queries_with_data(
            [query], data,database, partitions, parallel, workers, params)
        return result
