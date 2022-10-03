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
import numpy as np
from pandas import DataFrame
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.exceptions import AuthError
from neo4j.exceptions import ClientError
from neo4j.exceptions import ConfigurationError

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
                                            batch_size: Optional[int] = 1,
                                            kwargs: Optional[Dict[str, Any]]) -> None
            Execute a write query using data on a DataFrame.
        execute_write_queries_with_data(self, query: List[str], data: DataFrame,
                                            database: Optional[str] = None,
                                            batch_size: Optional[int] = 1,
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
        self.results = defaultdict(int)
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
        if database:
            session = self.__driver.session(database=database)
        else:
            session = self.__driver.session()
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
        results = defaultdict(int)
        if database:
            session = self.__driver.session(database=database)
        else:
            session = self.__driver.session()
        try:
            for query in queries:
                result = session.write_transaction(
                    self._write_transaction_function, query=query,
                    **kwargs).counters.__dict__
                for key, value in result.items():
                    if key != '_contains_updates':
                        results[key] += value
        except ServiceUnavailable() as exception:
            raise ServiceUnavailable() from exception
        except ClientError as exception:
            raise ClientError() from exception
        finally:
            session.close()
        return results

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
                                        batch_size: Optional[int] = 1,
                                        parallel: Optional[bool] = False,
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
            batch_size : int, optional
                The number of partitions in which to split the data frame.
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
        if batch_size > data.shape[0]:
            raise ValueError(
                "The batch size cannot be greater than the number of rows in the data.")
        if database:
            session = self.__driver.session(database=database)
        else:
            session = self.__driver.session()
        data_chunks = np.array_split(data,batch_size)
        for _,rows in enumerate(data_chunks):
            rows_dict = {'rows': rows.fillna(value="").to_dict('records')}
            for query in queries:
                try:
                    self._write_transaction(database, rows_dict['rows'],query,
                                            **kwargs)
                except ClientError as exception:
                    raise ClientError() from exception
                except ServiceUnavailable as exception:
                    raise ServiceUnavailable() from exception
                finally:
                    session.close()
        return self.results

    def execute_write_query_with_data(self,
                                      query: str, data: DataFrame,
                                      database: Optional[str] = None,
                                      batch_size: Optional[int] = 1,
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
            batch_size : int, optional
                The number of partitions in which to split the data frame.
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
            [query], data,database, batch_size, **kwargs)
        return result

    def __enter__(self):
        return self.__driver

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.close()

    def _write_transaction(self, database, rows, query, **kwargs):
        if database:
            session = self.__driver.session(database=database)
        else:
            session = self.__driver.session()
        results = session.write_transaction(
        self._write_transaction_function, query=query,
        rows=rows, **kwargs).counters.__dict__
        for key, value in results.items():
            if key != '_contains_updates':
                self.results[key] += value

    @staticmethod
    def _write_transaction_function(transaction, query, **kwargs):
        result = transaction.run(query, **kwargs)
        return result.consume()

    @staticmethod
    def _read_transaction_function(transaction, query, **kwargs):
        results = transaction.run(query, **kwargs)
        data = DataFrame(results.values(), columns=results.keys())
        return data
