# -*- coding: utf-8 -*-
"""Module use for testing the neo4jdb package

    Classes
    -------
    TestNeo4jInstance
        Test all methos in the Neo4jInstance class.
"""
import os
import unittest
import pandas as pd
import pandas.testing as pd_testing
from neo4j.exceptions import ClientError
from .context import database
from .context import fileload
from . import test_dir

class TestNeo4jInstance(unittest.TestCase):
    """Class use for testing the functions in the the helper module.
        Method
        ------
        setUp()
            Create the required environment for the tests.
        tearDown()
            Remove the created test environment.
        test_neo4j_instance()
            Test all the methods in the Neo4jInstance class.
    """
    def assertDataframeEqual(self, a, b, msg):
        try:
            pd_testing.assert_frame_equal(a, b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def setUp(self):
        """Set authentication variables and create Neo4j driver connection."""
        try:
            user = os.environ['NEO4J_USER']
            password = os.environ['NEO4J_PASSWORD']
            uri = os.getenv('NEO4J_URI')
        except KeyError as exception:
            raise KeyError() from exception
        required_keys = ['database','pre_ingest','queries']
        self.graph = database.Neo4jInstance(uri,user,password)
        self.yaml_file = os.path.join(
            test_dir,'cypher_queries.yaml')
        self.queries = fileload.load_yaml_file(self.yaml_file,required_keys)
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataframeEqual)

    def tearDown(self):
        """Close the neo4j driver and delete the test data."""
        queries = ["CALL apoc.schema.assert({},{})","MATCH(n) DETACH DELETE n"]
        self.graph.execute_write_queries(queries, self.queries['database'])

    def test_neo4j_instance(self):
        """Test all methods of the Neo4jInstance class."""
        # Testing the execute_write_queries method with the pre_ingest queries
        print('Constraints')
        result = self.graph.execute_write_queries(self.queries['pre_ingest'],
                                        self.queries['database'])
        solution = {'constraints_added':2}
        self.assertEqual(solution, result)

        # Testing the execute_write_query_with_data method using the people.csv
        # dataset, partitions=5 and concurrency=True
        people_file = os.path.join(test_dir,'people.csv')
        people_df = pd.read_csv(people_file)
        people_num = people_df.shape[0]
        property_num = people_num * people_df.shape[1]
        print('Parallel')
        result = self.graph.execute_write_query_with_data(self.queries['queries']['load_person'],
                                                          people_df,
                                                          self.queries['database'],
                                                          partitions=5,
                                                          parallel=True)
        solution = {'nodes_created':people_num,'labels_added':people_num,
                    'properties_set':property_num}
        self.assertEqual(solution, result)

        # Testing the execute_write_query_with_data method using the movies.csv
        # dataset and partitions=2
        movie_file = os.path.join(test_dir,'movies.csv')
        movie_df = pd.read_csv(movie_file)
        movie_num = movie_df.shape[0]
        property_num = movie_num * movie_df.shape[1] + movie_num
        print('Partitions')
        result = self.graph.execute_write_query_with_data(self.queries['queries']['load_movie'],
                                                          movie_df, self.queries['database'],
                                                          partitions=2
                                                         )
        solution = {'nodes_created':movie_num,'labels_added':movie_num,
                    'properties_set':property_num}
        self.assertEqual(solution, result)

        # Testing the execute_read_query method
        query = "MATCH(p) RETURN count(*) AS node_num"
        print('Read')
        result = self.graph.execute_read_query(query,self.queries['database'])
        solution = people_num + movie_num
        self.assertEqual(solution, result['node_num'][0])

        # Test the execute_read_query_using cypher parameters
        movie = "The Matrix"
        query = "MATCH(m:Movie) WHERE m.title=$movie_name RETURN m.title AS movie"
        print('Read Parameters')
        result = self.graph.execute_read_query(query,
                                               self.queries['database'],
                                               {'movie_name':movie})
        self.assertEqual(movie,result['movie'][0])

        # Test the execute_write_query_using cypher parameters
        print('Write Parameters')
        result = self.graph.execute_write_query(
            self.queries['queries']['create_actor'],
            self.queries['database'],
            parameters={'id':-1,'name':'Darth Vader','year':'1977'})
        solution = {'nodes_created':1,'labels_added':2,
                    'properties_set':4}
        self.assertEqual(solution, result)

        # Test get_node_labels_freq
        print('Get node labels freq')
        role_file = os.path.join(test_dir,'roles.csv')
        role_df = pd.read_csv(role_file)
        result = self.graph.get_node_label_freq(
                self.queries['database'])
        solution = pd.DataFrame([
            {'nodeLabel':'Person','frequency':18727,'relativeFrequency':.75},
            {'nodeLabel':'Movie','frequency':6231,'relativeFrequency':.25},
            {'nodeLabel':'Actor','frequency':1,'relativeFrequency':.00}
            ])
        self.assertEqual(result,solution)

        # Test get_node_multilabels_freq
        print('Get node multilabel freq')
        result = self.graph.get_node_multilabel_freq(
            self.queries['database'])
        solution = pd.DataFrame([
            {'nodeLabels':['Person','Actor'],'frequency':1}])
        self.assertEqual(result,solution)

        # Test get_rela_labels_freq
        print('Get relationship type freq')
        self.graph.execute_write_query_with_data(
            self.queries['queries']['load_role'],
            role_df,
            self.queries['database'])
        result = self.graph.get_rela_type_freq(
            self.queries['database'])
        solution = pd.DataFrame([
            {'relationshipType':'ACTED_IN','frequency':56914,'relativeFrequency':1.0}])
        self.assertEqual(result,solution)

        # Test get_properties
        print('Get properties')
        query = """
            CALL apoc.meta.data() YIELD label,property,type,elementType
            WHERE type<>'RELATIONSHIP'
            RETURN elementType,label,property,type
            ORDER BY elementType,label,property;
        """
        solution = self.graph.execute_read_query(query,self.queries['database'])
        result = self.graph.get_properties(
            self.queries['database'])
        self.assertEqual(result,solution)

        # Test constraints
        print('Get constraints')
        query = """
            SHOW CONSTRAINTS
        """
        solution = self.graph.execute_read_query(query,self.queries['database'])
        result = self.graph.get_constraints(
            self.queries['database'])
        self.assertEqual(result,solution)

        # Test indexes
        print('Get indexes')
        query = """
            SHOW INDEX
        """
        solution = self.graph.execute_read_query(query,self.queries['database'])
        result = self.graph.get_indexes(
            self.queries['database'])
        self.assertEqual(result,solution)

        # Test indexes
        print('Get rela details')
        result = self.graph.get_rela_source_target_freq(
            self.queries['database'])
        solution = pd.DataFrame([
            {'sourceLabel':'','relationshipType':'ACTED_IN','targetLabel':'','frequency':56914},
            {'sourceLabel':'','relationshipType':'ACTED_IN','targetLabel':'Movie','frequency':56914},
            {'sourceLabel':'Person','relationshipType':'ACTED_IN','targetLabel':'','frequency':56914}
        ]
        )
        solution.index = [2,0,1]
        self.assertEqual(result,solution)

if __name__ == '__main__':
    unittest.main()
