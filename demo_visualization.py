"""Demo script for get_query_visualization.

Loads a small subset of connected test data into Neo4j, runs a query,
serves the result via a local HTTP server, and opens it in the browser.

Usage:
    python demo_visualization.py
"""

import os
import threading
import webbrowser
import pandas as pd
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pyneoinstance import Neo4jInstance

# Load credentials from .env
env = {}
for line in Path('.env').read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k, v = line.split('=', 1)
        env[k.strip()] = v.strip()

URI      = env.get('NEO4J_URI', 'bolt://localhost:7687')
USER     = env.get('NEO4J_USER', 'neo4j')
PASSWORD = env.get('NEO4J_PASSWORD')
DATABASE = 'neo4j'

LOAD_PERSON = """
    WITH $rows AS rows UNWIND rows AS row
    MERGE (p:Person {personId: row.personId})
      ON CREATE SET p.name = row.name, p.birthYear = toInteger(row.birthYear)
"""

LOAD_MOVIE = """
    WITH $rows AS rows UNWIND rows AS row
    MERGE (m:Movie {movieId: row.movieId})
      ON CREATE SET m.title = row.title, m.year = toInteger(row.releaseYear)
"""

LOAD_ROLE = """
    WITH $rows AS rows UNWIND rows AS row
    MATCH (p:Person {personId: row.personId})
    MATCH (m:Movie {movieId: row.movieId})
    MERGE (p)-[:ACTED_IN]->(m)
"""

QUERY = """
    MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
    RETURN p, r, m
    LIMIT 50
"""

PORT = 8765
OUTPUT = 'demo_graph.html'

if __name__ == '__main__':
    print("Connecting to Neo4j...")
    graph = Neo4jInstance(URI, USER, PASSWORD)

    # Load roles first so people/movies are guaranteed to be connected
    print("Loading demo data...")
    roles_df   = pd.read_csv('test/roles.csv').head(30)
    people_ids = roles_df['personId'].unique()
    movie_ids  = roles_df['movieId'].unique()

    people_df = pd.read_csv('test/people.csv')
    people_df = people_df[people_df['personId'].isin(people_ids)]

    movie_df  = pd.read_csv('test/movies.csv')
    movie_df  = movie_df[movie_df['movieId'].isin(movie_ids)]

    graph.execute_write_query_with_data(LOAD_PERSON, people_df, DATABASE)
    graph.execute_write_query_with_data(LOAD_MOVIE,  movie_df,  DATABASE)
    graph.execute_write_query_with_data(LOAD_ROLE,   roles_df,  DATABASE)
    print(f"Loaded {len(people_df)} people, {len(movie_df)} movies, {len(roles_df)} roles.")

    print("Generating visualization...")
    vg = graph.get_query_visualization(QUERY, DATABASE)
    print(f"Graph has {len(vg.nodes)} nodes and {len(vg.relationships)} relationships.")

    with open(OUTPUT, 'w') as f:
        f.write(f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Graph Visualization</title></head>
<body>
{vg.render().data}
</body>
</html>""")

    # Serve via HTTP so browser security restrictions don't block rendering
    handler = SimpleHTTPRequestHandler
    handler.log_message = lambda *args: None  # silence request logs
    server = HTTPServer(('localhost', PORT), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    url = f'http://localhost:{PORT}/{OUTPUT}'
    print(f"Serving at {url} — opening in browser...")
    webbrowser.open(url)

    input("\nPress Enter to stop the server and clean up...\n")

    print("Cleaning up demo data...")
    graph.execute_write_query("MATCH (n) DETACH DELETE n", DATABASE)
    graph.close()
    server.shutdown()
    print("Done.")
