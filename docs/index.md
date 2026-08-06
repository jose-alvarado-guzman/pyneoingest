# PyNeoInstance

<p align="center">
  <img src="images/PyNeoInstanceLogo.png" alt="PyNeoInstance Logo" width="250">
</p>

PyNeoInstance is a Python package that provides a user-friendly API for submitting Cypher queries to Neo4j and performing Exploratory Data Analysis (EDA) of your graph.

## Features

- **Read query** — Submit a Cypher read query, with or without parameters, and get a Pandas DataFrame with the results.
- **Write query** — Submit a write Cypher query, with or without parameters, to update an existing database.
- **Write queries** — Submit a list of Cypher queries to update an existing database.
- **Write query with data** — Update a database based on a Pandas DataFrame and a Cypher query.
- **Write queries with data** — Update a database based on a Pandas DataFrame and a list of Cypher queries.
- **Node label frequency** — Get the distribution of graph node labels.
- **Multi-label node frequency** — Get the distribution of multi-label nodes.
- **Relationship type frequency** — Get the distribution of relationship types.
- **Relationship source-target frequency** — Get the frequency of relationships by source and target label.
- **Node and relationship properties** — Get information about all properties in the graph.
- **Constraints** — List all constraints in the graph.
- **Indexes** — List all indexes in the graph.
- **Schema visualization** — Visualize the graph schema interactively.
- **Query graph visualization** — Visualize the result of a Cypher query as an interactive graph.

## Installation

```bash
pip install pyneoinstance
```

The Rust-accelerated Neo4j driver (`neo4j-rust-ext`) is included by default for faster data serialization.

## Requirements

- Python >= 3.10
- A running Neo4j instance (>= 5.0)
- APOC plugin — required for EDA methods
