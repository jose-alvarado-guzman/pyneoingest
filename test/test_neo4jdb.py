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

    def tearDown(self):
        """Close the neo4j driver and delete the test data."""
        queries = ["CALL apoc.schema.assert({},{})","MATCH(n) DELETE n"]
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
        print('Parameters')
        result = self.graph.execute_read_query(query,
                                               self.queries['database'],
                                               movie_name=movie)
        self.assertEqual(movie,result['movie'][0])

if __name__ == '__main__':
    unittest.main()
