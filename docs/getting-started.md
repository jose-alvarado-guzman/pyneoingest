# Getting Started

## Connecting to Neo4j

Import and instantiate `Neo4jInstance` with your connection details:

```python
from pyneoinstance import Neo4jInstance

graph = Neo4jInstance(uri, user, password)
```

The recommended approach is to load credentials from environment variables:

```python
import os
from pyneoinstance import Neo4jInstance

graph = Neo4jInstance(
    uri=os.environ['NEO4J_URI'],
    user=os.environ['NEO4J_USER'],
    password=os.environ['NEO4J_PASSWORD']
)
```

Or from a YAML config file:

```python
from pyneoinstance import Neo4jInstance, load_yaml_file

config = load_yaml_file('configuration.yaml')
db = config['db_info']
graph = Neo4jInstance(db['uri'], db['user'], db['password'])
```

The class also supports use as a context manager so the driver is closed automatically:

```python
with Neo4jInstance(uri, user, password) as graph:
    result = graph.execute_read_query("MATCH (n) RETURN count(n) AS total")
```

---

## Submitting Read Queries

```python
query = """
    MATCH (m:Movie {title: $title})
    RETURN m.title AS title, m.year AS year
"""
df = graph.execute_read_query(query, database='mydb', parameters={'title': 'The Matrix'})
```

---

## Submitting Write Queries

```python
query = """
    MERGE (p:Person {id: $id})
    ON CREATE SET p.name = $name
"""
result = graph.execute_write_query(query, database='mydb', parameters={'id': 1, 'name': 'Alice'})
```

To run multiple write queries in one call:

```python
result = graph.execute_write_queries([query1, query2], database='mydb')
```

---

## Loading a DataFrame into Neo4j

```python
import pandas as pd

df = pd.read_csv('people.csv')

query = """
    WITH $rows AS rows UNWIND rows AS row
    MERGE (p:Person {personId: row.personId})
      ON CREATE SET p.name = row.name,
                    p.birthYear = toInteger(row.birthYear)
"""

result = graph.execute_write_query_with_data(query, df, database='mydb')
```

### Batching

For large DataFrames, split the load into smaller batches:

```python
result = graph.execute_write_query_with_data(
    query, df, database='mydb', batchSize=10_000)
```

### Parallel loading

```python
result = graph.execute_write_query_with_data(
    query, df, database='mydb', batchSize=10_000, parallel=True, workers=5)
```

### Multiple queries

```python
result = graph.execute_write_queries_with_data(
    [person_query, movie_query], df, database='mydb', batchSize=10_000)
```

---

## Storing Cypher Queries in YAML

Keeping queries out of Python code makes them easier to maintain:

```yaml
# queries.yaml
database: mydb
queries:
  load_person: |
    WITH $rows AS rows UNWIND rows AS row
    MERGE (p:Person {personId: row.personId})
      ON CREATE SET p.name = row.name
```

```python
from pyneoinstance import load_yaml_file

config = load_yaml_file('queries.yaml', required_keys=['database', 'queries'])
result = graph.execute_write_query_with_data(
    config['queries']['load_person'], df, config['database'])
```
