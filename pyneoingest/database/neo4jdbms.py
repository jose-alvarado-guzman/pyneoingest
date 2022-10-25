# -*- coding: utf-8 -*-
"""Module responsible for handling Neo4j transactions.

This module is responsible for the folling task:
  - Create a Neo4j driver connection.
  - Handle write and read queries (with and without Cypher parameters)
    to Neo4j using transaction functions.
  - Ingest data into Neo4j with the provided Pandas DataFrame and Cypher query.
  - Close the driver connection to Neo4j.
"""

from typing import Optional, List, Dict, Any
from collections import defaultdict
import threading
import logging
import numpy as np
from pandas import DataFrame
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.exceptions import AuthError
from neo4j.exceptions import ClientError
from neo4j.exceptions import ConfigurationError
from .context import get_logger

class Neo4jInstance:
    """Class use to represent a connection to Neo4j.

        Methods
        -------
        close()
            Close the Neo4j connection.
        execute_read_query(query: str,database Optional[str],
                           kwargs Optional[Dict[str, Any]]) -> DataFrame
            Execute a read query to a specific database.
        execute_write_queries(queries: List[str], database: Optional[str],
                            kwargs: Optional[Dict[str, Any]]) -> None
            Execute a list of write query to a specific database.
        execute_write_query(query: str, database: Optional[str],
                            kwargs: Optional[Dict[str, Any]]) -> None
            Execute a write query to a specific database.
        execute_write_query_with_data(self, query: str, data: DataFrame,
                                            database: Optional[str] = None,
                                            partitions: Optional[int] = 1,
                                            kwargs: Optional[Dict[str, Any]]) -> None
            Execute a write query using data on a DataFrame.
        execute_write_queries_with_data(self, query: List[str], data: DataFrame,
                                            database: Optional[str] = None,
                                            partitions: Optional[int] = 1,
                                            kwargs: Optional[Dict[str, Any]]) -> None
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
        encrypted = kwargs.get('encrypted') or ''
        self.__results = defaultdict(int)
        self.__logger = get_logger('pyneoingest', logging.INFO)

        try:
            if encrypted != '':
                self.__driver = GraphDatabase.driver(
                    uri,auth=(user, password),
                    encrypted=encrypted)
            else:
                self.__driver = GraphDatabase.driver(
                    uri,auth=(user, password))
        except ServiceUnavailable as exception:
            raise ServiceUnavailable() from exception
        except AuthError as exception:
            raise  AuthError() from exception
        except ConfigurationError as exception:
            raise ConfigurationError() from exception

    def close(self) -> None:
        """Close the Neo4j connection.

            Is extremely important to always call this method to close the
            connection when all transactions had been completed.
        """
        if self.__driver is not None:
            self.__driver.close()

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
        session = self._get_session(database)
        try:
            result = session.read_transaction(
                self._read_transaction_function,query=query,**kwargs)
        except ServiceUnavailable as exception:
            raise ServiceUnavailable() from exception
        except ClientError as exception:
            raise ClientError() from exception
        finally:
            session.close()
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
        session = self._get_session(database)
        results = defaultdict(int)
        for query in queries:
            result = self._execute_write(session, query, **kwargs)
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
                                        concurrency: Optional[bool] = False,
                                        **kwargs: Optional[Dict[str, Any]]
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
            concurrency : bool, optional
                Whether to use multple threads to load the data.
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
            ValueError
                When the number of batch sizes to split the DataFrame on
                is larger than the number of rows in it.
        """
        if partitions > data.shape[0]:
            raise ValueError(
                "The batch size cannot be greater than the number of rows in the data.")
        data_chunks = np.array_split(data,partitions)
        for i,rows in enumerate(data_chunks):
            rows_dict = {'rows': rows.fillna(value="").to_dict('records')}
            for query in queries:
                if concurrency and partitions > 1:
                    thread_name = f'pyingest-{i + 1}'
                    thread = threading.Thread(target=self._write_transaction,
                                     args=(database,
                                           rows_dict['rows'],query),
                                           kwargs=kwargs,name=thread_name)
                    thread.start()
                    thread.join()
                else:
                    session = self._get_session(database)
                    result = self._execute_write(session, query, rows=rows_dict['rows'],
                                                 **kwargs)
                    for key, value in result.items():
                        if key != '_contains_updates':
                            self.__results[key] += value
        results = self.__results.copy()
        self.__results.clear()
        self.__logger.info(f'Loading stats: {dict(results)}')
        return results

    def execute_write_query_with_data(self,
                                      query: str, data: DataFrame,
                                      database: Optional[str] = None,
                                      partitions: Optional[int] = 1,
                                      concurrency: Optional[bool] = False,
                                      **kwargs: Optional[Dict[str, Any]]
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
            concurrency : bool, optional
                Whether to use multple threads to load the data.
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
            ValueError
                When the number of batch sizes to split the DataFrame on
                is larger than the number of rows in it.
        """
        result = self.execute_write_queries_with_data(
            [query], data,database, partitions, concurrency, **kwargs)
        return result

    def _get_session(self, database: Optional[str] = None):
        if database:
            session = self.__driver.session(database=database)
        else:
            session = self.__driver.session()
        return session

    def __enter__(self):
        return self

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.close()

    def _write_transaction(self, database, rows, query, **kwargs):
        lock = threading.Lock()
        thread_name = threading.current_thread().name
        if database:
            session = self.__driver.session(database=database)
        else:
            session = self.__driver.session()
        try:
            results = session.write_transaction(
            self._write_transaction_function, query=query,
            rows=rows, **kwargs).counters.__dict__
            results.pop('_contains_updates','')
            for key, value in results.items():
                lock.acquire()
                self.__results[key] += value
                lock.release()
            self.__logger.info(
                f'Thread {thread_name} loading stats: {dict(results)}'
            )
        except ClientError as exception:
            raise ClientError() from exception
        except ServiceUnavailable as exception:
            raise ServiceUnavailable() from exception
        finally:
            session.close()

    @staticmethod
    def _write_transaction_function(transaction, query, **kwargs):
        result = transaction.run(query, **kwargs)
        return result.consume()

    @staticmethod
    def _read_transaction_function(transaction, query, **kwargs):
        results = transaction.run(query, **kwargs)
        data = DataFrame(results.values(), columns=results.keys())
        return data

    def _execute_write(self, session, query,
                       rows: Optional[Dict[str, Any]] = None,
                      **kwargs
                      ):
        try:
            if rows:
                results = session.write_transaction(
                    self._write_transaction_function, query,
                    rows = rows, **kwargs).counters.__dict__
            else:
                results = session.write_transaction(
                    self._write_transaction_function, query,
                    **kwargs).counters.__dict__
        except ServiceUnavailable() as exception:
            raise ServiceUnavailable() from exception
        except ClientError as exception:
            raise ClientError() from exception
        finally:
            session.close()
        return results
