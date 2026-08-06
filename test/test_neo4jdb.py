# -*- coding: utf-8 -*-
"""Module use for testing the neo4jdb package

    Classes
    -------
    TestWriteMethods
        Tests write operations against a fresh database per test.
    TestReadAndEdaMethods
        Tests read and EDA operations against a shared, fully-loaded database.
"""
import os
import unittest
import pandas as pd
import numpy as np
import pandas.testing as pd_testing
from neo4j.exceptions import ClientError
from neo4j_viz import VisualizationGraph
from .context import database
from .context import fileload
from . import test_dir


def _get_credentials():
    try:
        return (os.environ['NEO4J_URI'],
                os.environ['NEO4J_USER'],
                os.environ['NEO4J_PASSWORD'])
    except KeyError as exception:
        raise KeyError() from exception


def _load_queries():
    required_keys = ['database', 'pre_ingest', 'queries']
    yaml_file = os.path.join(test_dir, 'cypher_queries.yaml')
    return fileload.load_yaml_file(yaml_file, required_keys)


def _clean_db(graph, db):
    graph.execute_write_queries(
        ["CALL apoc.schema.assert({},{})", "MATCH(n) DETACH DELETE n"], db)


class TestWriteMethods(unittest.TestCase):
    """Test write operations. Each test gets a clean database."""

    def assertDataframeEqual(self, a, b, msg):
        try:
            pd_testing.assert_frame_equal(a, b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def setUp(self):
        uri, user, password = _get_credentials()
        self.graph = database.Neo4jInstance(uri, user, password)
        self.queries = _load_queries()
        self.db = self.queries['database']
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataframeEqual)

    def tearDown(self):
        _clean_db(self.graph, self.db)
        self.graph.close()

    def test_execute_write_queries(self):
        """Test constraint creation via execute_write_queries."""
        result = self.graph.execute_write_queries(
            self.queries['pre_ingest'], self.db)
        self.assertEqual({'constraints_added': 2}, result)

    def test_execute_write_query_with_data(self):
        """Test parallel and sequential data loading via execute_write_query_with_data."""
        people_df = pd.read_csv(os.path.join(test_dir, 'people.csv'))
        people_num = people_df.shape[0]
        result = self.graph.execute_write_query_with_data(
            self.queries['queries']['load_person'],
            people_df, self.db, batchSize=1000, parallel=True)
        self.assertEqual({'nodes_created': people_num,
                          'labels_added': people_num,
                          'properties_set': people_num * people_df.shape[1]},
                         result)

        movie_df = pd.read_csv(os.path.join(test_dir, 'movies.csv'))
        movie_num = movie_df.shape[0]
        result = self.graph.execute_write_query_with_data(
            self.queries['queries']['load_movie'],
            movie_df, self.db, batchSize=1000)
        self.assertEqual({'nodes_created': movie_num,
                          'labels_added': movie_num,
                          'properties_set': movie_num * movie_df.shape[1] + movie_num},
                         result)

    def test_execute_write_query_with_parameters(self):
        """Test parameterized write via execute_write_query."""
        result = self.graph.execute_write_query(
            self.queries['queries']['create_actor'],
            self.db,
            parameters={'id': -1, 'name': 'Darth Vader', 'year': '1977'})
        self.assertEqual(
            {'nodes_created': 1, 'labels_added': 2, 'properties_set': 4}, result)


class TestReadAndEdaMethods(unittest.TestCase):
    """Test read and EDA methods. Data is loaded once for the entire class."""

    @classmethod
    def setUpClass(cls):
        uri, user, password = _get_credentials()
        cls.graph = database.Neo4jInstance(uri, user, password)
        cls.queries = _load_queries()
        cls.db = cls.queries['database']

        cls.graph.execute_write_queries(cls.queries['pre_ingest'], cls.db)

        cls.people_df = pd.read_csv(os.path.join(test_dir, 'people.csv'))
        cls.people_num = cls.people_df.shape[0]
        cls.graph.execute_write_query_with_data(
            cls.queries['queries']['load_person'],
            cls.people_df, cls.db, batchSize=1000, parallel=True)

        cls.movie_df = pd.read_csv(os.path.join(test_dir, 'movies.csv'))
        cls.movie_num = cls.movie_df.shape[0]
        cls.graph.execute_write_query_with_data(
            cls.queries['queries']['load_movie'],
            cls.movie_df, cls.db, batchSize=1000)

        cls.graph.execute_write_query(
            cls.queries['queries']['create_actor'],
            cls.db,
            parameters={'id': -1, 'name': 'Darth Vader', 'year': '1977'})

        role_df = pd.read_csv(os.path.join(test_dir, 'roles.csv')).replace(np.nan, '')
        cls.graph.execute_write_query_with_data(
            cls.queries['queries']['load_role'], role_df, cls.db)

    @classmethod
    def tearDownClass(cls):
        _clean_db(cls.graph, cls.db)
        cls.graph.close()

    def assertDataframeEqual(self, a, b, msg):
        try:
            pd_testing.assert_frame_equal(a, b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def setUp(self):
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataframeEqual)

    def test_execute_read_query(self):
        """Test basic read query returns expected node count."""
        query = "MATCH(p) RETURN count(*) AS node_num"
        result = self.graph.execute_read_query(query, self.db)
        self.assertEqual(self.people_num + self.movie_num + 1, result['node_num'][0])

    def test_execute_read_query_with_parameters(self):
        """Test read query with Cypher parameters."""
        movie = "The Matrix"
        query = "MATCH(m:Movie) WHERE m.title=$movie_name RETURN m.title AS movie"
        result = self.graph.execute_read_query(
            query, self.db, {'movie_name': movie})
        self.assertEqual(movie, result['movie'][0])

    def test_get_node_label_freq(self):
        """Test node label frequency EDA."""
        result = self.graph.get_node_label_freq(self.db)
        solution = pd.DataFrame([
            {'nodeLabel': 'Person', 'frequency': 18727, 'relativeFrequency': .75},
            {'nodeLabel': 'Movie', 'frequency': 6231, 'relativeFrequency': .25},
            {'nodeLabel': 'Actor', 'frequency': 1, 'relativeFrequency': .00}
        ])
        self.assertEqual(result, solution)

    def test_get_node_multilabel_freq(self):
        """Test multi-label node frequency EDA."""
        result = self.graph.get_node_multilabel_freq(self.db)
        solution = pd.DataFrame([
            {'nodeLabels': ['Person', 'Actor'], 'frequency': 1}])
        self.assertEqual(result, solution)

    def test_get_rela_type_freq(self):
        """Test relationship type frequency EDA."""
        result = self.graph.get_rela_type_freq(self.db)
        solution = pd.DataFrame([
            {'relationshipType': 'ACTED_IN', 'frequency': 56914,
             'relativeFrequency': 1.0}])
        self.assertEqual(result, solution)

    def test_get_properties(self):
        """Test node and relationship properties EDA."""
        query = """
            CALL apoc.meta.data() YIELD label,property,type,elementType
            WHERE type<>'RELATIONSHIP'
            RETURN elementType,label,property,type
            ORDER BY elementType,label,property;
        """
        solution = self.graph.execute_read_query(query, self.db)
        result = self.graph.get_properties(self.db)
        self.assertEqual(result, solution)

    def test_get_constraints(self):
        """Test constraint listing."""
        solution = self.graph.execute_read_query("SHOW CONSTRAINTS", self.db)
        result = self.graph.get_constraints(self.db)
        self.assertEqual(result, solution)

    def test_get_indexes(self):
        """Test index listing."""
        solution = self.graph.execute_read_query("SHOW INDEX", self.db)
        result = self.graph.get_indexes(self.db)
        self.assertEqual(result, solution)

    def test_get_query_visualization(self):
        """Test that get_query_visualization returns a populated pyvis Network."""
        query = """
            MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
            RETURN p, r, m LIMIT 10
        """
        result = self.graph.get_query_visualization(query, self.db)
        self.assertIsInstance(result, VisualizationGraph)
        self.assertGreater(len(result.nodes), 0)
        self.assertGreater(len(result.relationships), 0)

    def test_get_rela_source_target_freq(self):
        """Test relationship source/target frequency EDA."""
        result = self.graph.get_rela_source_target_freq(self.db)
        solution = pd.DataFrame([
            {'sourceLabel': '', 'relationshipType': 'ACTED_IN',
             'targetLabel': '', 'frequency': 56914},
            {'sourceLabel': '', 'relationshipType': 'ACTED_IN',
             'targetLabel': 'Movie', 'frequency': 56914},
            {'sourceLabel': 'Person', 'relationshipType': 'ACTED_IN',
             'targetLabel': '', 'frequency': 56914}
        ])
        self.assertEqual(result, solution)


if __name__ == '__main__':
    unittest.main()
