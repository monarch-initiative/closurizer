# Monarch Closurizer
Closurizer adds expansion fields to kgx files following the Golr pattern

### Example

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

