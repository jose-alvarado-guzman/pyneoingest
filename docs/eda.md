# Graph EDA

All EDA methods require the **APOC plugin** to be installed in your Neo4j instance.

---

## Node Label Frequency

Returns the frequency and relative frequency of all node labels in the graph.

```python
df = graph.get_node_label_freq(database='mydb')
```

![Node Label Frequency](images/nodefreq.png)

---

## Multi-Label Node Frequency

Returns the frequency of nodes that have more than one label.

```python
df = graph.get_node_multilabel_freq(database='mydb')
```

---

## Relationship Type Frequency

Returns the frequency and relative frequency of all relationship types.

```python
df = graph.get_rela_type_freq(database='mydb')
```

![Relationship Type Frequency](images/relaFreq.png)

---

## Relationship Source-Target Frequency

Returns the frequency of relationships broken down by source node label and target node label.

```python
df = graph.get_rela_source_target_freq(database='mydb')
```

![Relationship Source-Target Frequency](images/relaSourceTargetFreq.png)

---

## Node and Relationship Properties

Returns information about all node and relationship properties in the graph.

```python
df = graph.get_properties(database='mydb')
```

![Properties](images/properties.png)

---

## Constraints

Lists all constraints defined in the graph.

```python
df = graph.get_constraints(database='mydb')
```

![Constraints](images/constraints.png)

---

## Indexes

Lists all indexes defined in the graph.

```python
df = graph.get_indexes(database='mydb')
```

![Indexes](images/indexes.png)
