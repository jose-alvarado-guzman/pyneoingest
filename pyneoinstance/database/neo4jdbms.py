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
from concurrent.futures import ThreadPoolExecutor
import logging
import re
import numpy as np
from pandas import DataFrame
from neo4j import GraphDatabase
from pyvis.network import Network
from neo4j.exceptions import ServiceUnavailable
from neo4j.exceptions import AuthError
from neo4j.exceptions import ClientError
from neo4j.exceptions import ConfigurationError
from .context import get_logger, get_batches, get_columns_diff

_NODE_LABEL_RE = re.compile(r"\(:?(\w*)\)")
_REL_TYPE_RE = re.compile(r"\[:?(\w*)\]")

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
        raise ServiceUnavailable()
        sys.exit()
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
                 verbose=False, **kwargs: Optional[Dict[str, Any]]) -> None:
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
        logging_type = logging.INFO if verbose else logging.WARNING
        self.__logger = get_logger('pyneoinstance', logging_type)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)
        self.__logger.addHandler(stream_handler)
        self._driver = _get_driver(self.neo_info)

    def close(self):
        self._driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    _apoc_error_msg = "APOC Library not detected. Please install it before proceeding."

    def _execute_read(self, query: str,
                      database: Optional[str] = None,
                      error_msg: Optional[str] = None) -> DataFrame:
        with _get_session(self._driver, database) as session:
            try:
                return session.execute_read(
                    _read_transaction_function, query=query)
            except ServiceUnavailable as exception:
                raise ServiceUnavailable() from exception
            except ClientError:
                raise ClientError(error_msg)

    def execute_read_query(self, query: str,
                           database: Optional[str] = None,
                           parameters: Optional[Dict[str, Any]] = None
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
        parameters: Dict[str, Any], optional
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
        if parameters:
            params = parameters
        else:
            params = {}
        with _get_session(self._driver, database) as session:
            try:
                result = session.execute_read(
                    _read_transaction_function,query=query,**params)
            except ServiceUnavailable as exception:
                raise ServiceUnavailable() from exception
            except ClientError as exception:
                raise ClientError() from exception
        return result

    def execute_write_queries(self, queries: List[str],
                            database: Optional[str] = None,
                            parameters: Optional[Dict[str, Any]] = None
                           ) -> Dict[str, int]:
        """Execute a write query to a specific database.

            Parameters
            ----------
            queries : List[str]
                List of Cypher queries to execute.
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.
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
        """
        if parameters:
            params = parameters
        else:
            params = {}
        results = defaultdict(int)
        with _get_session(self._driver, database) as session:
            for query in queries:
                result = self._execute_write(session, query, parameters=params)
                for key, value in result.items():
                    if key != '_contains_updates':
                        results[key] += value
        return dict(results)

    def execute_write_query(self, query: str,
                            database: Optional[str] = None,
                            parameters: Optional[Dict['str',Any]] = None
                           ) -> Dict[str, Any]:
        """Execute a write query to a specific database.

            Parameters
            ----------
            query : str
                Cypher query to execute.
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.
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
        """
        result = self.execute_write_queries([query], database, parameters)
        return  result

    def execute_write_queries_with_data(self, queries: List[str],
                                        data: DataFrame,
                                        database: Optional[str] = None,
                                        batchSize: Optional[int] = 100_000,
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
            batchSize : int, optional
                The number of records per batch or partitions of the data frame.
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
        row_num = data.shape[0]
        batchSize = batchSize if batchSize <= row_num else row_num
        msg = f'Partitioning the data in batches of size {batchSize:,.0f}'
        self.__logger.info(msg)
        chunks = get_batches(data, batchSize)
        data_chunks = [data.iloc[d].to_dict('records') for d in chunks]
        partitions = len(chunks)
        parallel = parallel if len(chunks)>1 else False
        if parameters:
            params = parameters
        else:
            params = {}

        if parallel:
            import os
            workers_num = workers if (workers and workers > 0) else os.cpu_count()
            msg = f'Loading {partitions:,.0f} data chunks using {workers_num} thread(s)'
            self.__logger.info(msg)
            results = []
            for query in queries:
                col_diff = get_columns_diff(query, data.columns)
                if len(col_diff) != 0:
                    self.__logger.warning(
                        f'These columns are not in your data: {col_diff}')

                def run_chunk(chunk, q=query):
                    with _get_session(self._driver, database) as session:
                        return self._execute_write(session, q, chunk, params)

                with ThreadPoolExecutor(max_workers=workers_num) as executor:
                    results.extend(executor.map(run_chunk, data_chunks))
        else:
            msg = f'Loading {partitions:,.0f} data chunks sequentially'
            self.__logger.info(msg)
            results = []
            with _get_session(self._driver, database) as session:
                for d in data_chunks:
                        for query in queries:
                            col_diff = get_columns_diff(query,data.columns)
                            msg = f'This columns are not in your data: {col_diff}'
                            if len(col_diff)!=0:
                                self.__logger.warning(msg)
                            results.append(self._execute_write(session,
                                                          query,
                                                          d,
                                                          params))
        results_agg = defaultdict(int)
        for result in results:
            for key, value in result.items():
                if key != '_contains_updates':
                    results_agg[key] += value
        return dict(results_agg)

    def execute_write_query_with_data(self,
                                      query: str, data: DataFrame,
                                      database: Optional[str] = None,
                                      batchSize: Optional[int] = 100_000,
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
            batchSize : int, optional
                The number of records per batch or partitions of the data frame.
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
            [query], data,database, batchSize, parallel, workers, params)
        return result

    def get_node_label_freq(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the graph node label frequency.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing the frequency and relative
                frequency of node labels.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            MATCH(n)
            WITH count(*) AS nodeCount
            CALL db.labels() YIELD label
            CALL apoc.cypher.run('MATCH (:`'+label+'`) RETURN count(*) as freq',{}) YIELD value
            WITH nodeCount,label,value.freq AS freq
            WITH *, 10^3 AS scaleFactor, toFloat(freq)/toFloat(nodeCount) AS relFreq
            RETURN label AS nodeLabel,
                freq AS frequency,
                round(relFreq*scaleFactor)/scaleFactor AS relativeFrequency
            ORDER BY freq DESC
        """
        return self._execute_read(query, database, self._apoc_error_msg)

    def get_node_multilabel_freq(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the graph node multi-label frequency.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing the frequency and relative
                frequency of node with multiple labels.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            MATCH (n)
            WITH labels(n) as nodeLabels
            WHERE size(nodeLabels)>1
            RETURN nodeLabels, count(*) as frequency
        """
        return self._execute_read(query, database, self._apoc_error_msg)

    def get_rela_type_freq(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the graph relationship type frequency.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing the frequency and relative
                frequency of relationship types.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            MATCH()-[]->()
            WITH count(*) AS relCount
            CALL db.relationshipTypes() YIELD relationshipType as type
            CALL apoc.cypher.run('MATCH ()-[:`'+type+'`]->() RETURN count(*) as freq',{})
            YIELD value
            WITH type AS relationshipType, value.freq AS freq,relCount
            WITH *,3 AS presicion
            WITH *, 10^presicion AS factor,toFloat(freq)/toFloat(relCount) as relFreq
            RETURN relationshipType, freq AS frequency,
            round(relFreq*factor)/factor AS relativeFrequency
            ORDER BY freq DESC;
        """
        return self._execute_read(query, database, self._apoc_error_msg)

    def get_properties(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the node and relationship properties.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing information about all nodes
                and relationships properties.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            CALL apoc.meta.data() YIELD label,property,type,elementType
            WHERE type<>'RELATIONSHIP'
            RETURN elementType,label,property,type
            ORDER BY elementType,label,property;
        """
        return self._execute_read(query, database, self._apoc_error_msg)

    def get_constraints(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the constraints in the graph.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing information about all the
                constraints.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            SHOW CONSTRAINTS
        """
        return self._execute_read(query, database, self._apoc_error_msg)

    def get_indexes(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the indexes in the graph.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing information about all the
                indexes.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            SHOW INDEX
        """
        return self._execute_read(query, database, self._apoc_error_msg)

    def get_rela_source_target_freq(self, database: Optional[str] = None) -> DataFrame:
        """Use to obtain the relationships source and target frequency.

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            DataFrame
                Pandas DataFrame object containing the frequency of
                relationships cource and target frequency.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            CALL apoc.meta.stats() YIELD relTypes
        """
        stat_dict = self._execute_read(query, database, self._apoc_error_msg).iloc[0,0]
        stats_dicts = []
        for key,value in stat_dict.items():
            nodes = _NODE_LABEL_RE.findall(key)
            relationship = _REL_TYPE_RE.findall(key)[0]
            info = {'sourceLabel':nodes[0],'relationshipType':relationship,'targetLabel':nodes[1],
                    'frequency':value}
            stats_dicts.append(info)
        stats_df = DataFrame(stats_dicts)
        stats_df.sort_values(by=['relationshipType','sourceLabel','targetLabel'],inplace=True)
        return stats_df

    def get_schema_visualization(self, database: Optional[str] = None) -> Network:
        """Use to visualize the graph data model (schema).

            Parameters
            ----------
            database : str, optional
                Name of the Neo4j database of which to execute the transaction.
                If not provided the default database is going to be use.

            Returns
            -------
            Network
                Pyvis Network object containing the visualization information.

            Raises
            ------
            ClientError
                When APOC Library is not install correctly in your Neo4j
                deployment.
        """

        query = """
            CALL apoc.meta.schema() YIELD value
        """
        schema = self._execute_read(query, database, self._apoc_error_msg).iloc[0,0]
        network = Network(notebook=True,cdn_resources = "remote",directed=True,
                          filter_menu=True,height="800px", width="100%")
        options = """
        const options = {
            "physics": {
                "barnesHut": {
                  "gravitationalConstant": -13950,
                  "centralGravity": 6.15,
                  "springLength": 160,
                  "damping": 0.23
                },
                "minVelocity": 0.75
              }
            }
        """
        network.set_options(options)
        nodes = []
        relationships = set()
        for key in schema.keys():
            if schema[key]['type']=='node':
                nodes.append(key)
        network.add_nodes(nodes)
        for node in nodes:
            for rel in schema[node]['relationships'].keys():
                title = rel
                if schema[node]['relationships'][rel]['direction']=='in':
                    target = node
                    for label in schema[node]['relationships'][rel]['labels']:
                        source = label
                else:
                    source = node
                    for label in schema[node]['relationships'][rel]['labels']:
                        target = label
                relationships.add(source + "|" + title + "|" + target)
        for relationship in relationships:
            rela_parts = relationship.split("|")
            network.add_edge(rela_parts[0],rela_parts[2],title=rela_parts[1])
        return network

    def _execute_write(self, session, query,
                       rows: Optional[Dict[str, Any]] = None,
                       parameters: Optional[Dict[str, Any]] = None
                      ):
        if parameters:
            params = parameters
        else:
            params = {}
        try:
            if rows:
                results = session.execute_write(
                    _write_transaction_function, query,
                    rows = rows, **params).counters.__dict__
            else:
                results = session.execute_write(
                    _write_transaction_function, query,
                    **params).counters.__dict__
        except ServiceUnavailable as exception:
            raise ServiceUnavailable() from exception
        except ClientError as exception:
            raise ClientError() from exception
        return results
