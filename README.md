# Monarch Closurizer
Closurizer adds expansion fields to kgx files following the Golr pattern

## Usage

### As a module

**Archive input (tar.gz file):**
```python
from closurizer.closurizer import add_closure

add_closure(
    closure_file="my-relations-non-redundant.tsv",
    nodes_output_file="output/nodes-with-closures.tsv", 
    edges_output_file="output/edges-denormalized.tsv",
    kg_archive="my-kg.tar.gz",
    edge_fields=["subject", "object"]
)
```

**Database input (existing DuckDB):**
```python
from closurizer.closurizer import add_closure

add_closure(
    closure_file="my-relations-non-redundant.tsv",
    nodes_output_file="output/nodes-with-closures.tsv",
    edges_output_file="output/edges-denormalized.tsv", 
    input_database="existing-kg.duckdb",
    database_path="output/processed-kg.duckdb",  # optional output database
    edge_fields=["subject", "object"]
)
```

### As a command line tool

**Archive input:**
```bash
closurizer --kg my-kg.tar.gz --closure relations.tsv --nodes-output nodes.tsv --edges-output edges.tsv
```

**Database input:**
```bash
closurizer --input-db existing.duckdb --closure relations.tsv --nodes-output nodes.tsv --edges-output edges.tsv
```


## Example

Closurizer will produce a denormalized edge file including subject namespace and category along with ID and label closures 

| subject_category | subject_closure | subject_closure_label | subject_namespace |  subject  |               predicate                |    object     | object_namespace | object_closure_label | object_closure | object_category |
|------------------|-----------------|-----------------------|-------------------|-----------|----------------------------------------|---------------|------------------|----------------------|----------------|-----------------|
| biolink:Gene     |                 |                       | HGNC              | HGNC:4851 | biolink:gene_associated_with_condition | MONDO:0007739 | MONDO            |  Huntington disease and related disorders, movement disorder     | MONDO:0000167, MONDO:0005395              | biolink:Disease |

### Example source KG

Nodes:
| category       | id             | name | in_taxon  |  
|----------------|----------------|----- |-----------|
| biolink:Gene    | HGNC:4851     | HTT  | NCBITaxon:9606 |
| biolink:Disease | MONDO:0007739 | Huntington disease | |

Edges:
|    subject    |               predicate                |    object     |
|---------------|----------------------------------------|---------------|
| HGNC:4851     | biolink:gene_associated_with_condition | MONDO:0007739 |

and a Relation Graph closure tsv file with:

| subject    |  predicate    | object      |
-------------|---------------|-------------|
MONDO:0007739|rdfs:subClassOf|MONDO:0000167|
MONDO:0007739|rdfs:subClassOf|MONDO:0005395|

