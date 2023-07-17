from typing import List

import petl as etl
import os
import tarfile


def _string_agg(key, rows):
    return [key, "|".join(row[1] for row in rows)]


def _cut_left_join(ltable, rtable, field, attribute, rename_attribute = None):
    if rename_attribute is None:
        rename_attribute = attribute
    return etl.leftjoin(ltable,
                        (etl.cut(rtable, ['id', attribute]).rename(attribute, f"{field}_{rename_attribute}")),
                        lkey=field,
                        rkey="id")

def _length(value):
    if value is None or value == "":
        return 0
    else:
        return len(value.split("|"))

def _length_of_field_values(rec, fields):
    value = 0
    for field in fields:
        if (field_value := rec[field]) is not None:
            value += _length(field_value)
    return value

def add_closure(kg_archive: str,
                closure_file: str,
                output_file: str,
                fields: List[str] = ['subject', 'object'],
                evidence_fields: List[str] = ['has_evidence', 'publications', 'primary_knowledge_source']
                ):
    print("Generating closure KG...")
    print(f"kg_archive: {kg_archive}")
    print(f"closure_file: {closure_file}")

    if fields is None or len(fields) == 0:
        fields = ['subject', 'object']

    if evidence_fields is None or len(evidence_fields) == 0:
        evidence_fields = ['has_evidence', 'publications', 'primary_knowledge_source', 'provided_by']

    print(f"fields: {','.join(fields)}")
    print(f"output_file: {output_file}")

    tar = tarfile.open(f"{kg_archive}")

    print("Loading node table...")
    node_file_name = [member.name for member in tar.getmembers() if member.name.endswith('_nodes.tsv') ][0]
    tar.extract(node_file_name,)
    node_file = f"{node_file_name}"
    print(f"node_file: {node_file}")
    nodes = etl.fromtsv(node_file)
    nodes = etl.addfield(nodes, 'namespace', lambda rec: rec['id'][:rec['id'].index(":")])

    edge_file_name = [member.name for member in tar.getmembers() if member.name.endswith('_edges.tsv') ][0]
    tar.extract(edge_file_name)
    edge_file = f"{edge_file_name}"
    print(f"edge_file: {edge_file}")
    edges = etl.fromtsv(edge_file)

    # Load the relation graph tsv in long format mapping a node to each of it's ancestors
    closure_table = (etl
                     .fromtsv(closure_file)
                     .setheader(['id', 'predicate', 'ancestor'])
                     .cutout('predicate')  # assume all predicates for now
                     )

    # Prepare the closure id table, mapping node IDs to pipe separated lists of ancestors
    closure_id_table = (etl.rowreduce(closure_table, key='id',
                                      reducer=_string_agg,
                                      header=['id', 'ancestors'])
                        .rename('ancestors', 'closure'))

    # Prepare the closure label table, mapping node IDs to pipe separated lists of ancestor names
    closure_label_table = (etl.leftjoin(closure_table,
                                        etl.cut(nodes, ["id", "name"]),
                                        lkey="ancestor",
                                        rkey="id")
                           .cutout("ancestor")
                           .rename("name", "closure_label")
                           .selectnotnone("closure_label")
                           .rowreduce(key='id', reducer=_string_agg, header=['id', 'ancestor_labels'])
                           .rename('ancestor_labels', 'closure_label'))

    for field in fields:
        edges = _cut_left_join(edges, nodes, field, "namespace")
        edges = _cut_left_join(edges, nodes, field, "category")
        edges = _cut_left_join(edges, closure_id_table, field, "closure")
        edges = _cut_left_join(edges, closure_label_table, field, "closure_label")
        # only add taxon labels to subject & object
        edges = _cut_left_join(edges, nodes, field, "name", rename_attribute="label")

        if field in ['subject', 'object']:
            edges = _cut_left_join(edges, nodes, field, "in_taxon", rename_attribute="taxon")
            edges = _cut_left_join(edges, nodes, field, "in_taxon_label", rename_attribute="taxon_label")



    print("Adding evidence counts...")

    edges = etl.addfield(edges, 'evidence_count',
                         lambda rec: _length_of_field_values(rec, evidence_fields))

    print("Denormalizing...")
    etl.totsv(edges, f"{output_file}")

    # Clean up extracted node & edge files
    if os.path.exists(f"{node_file}"):
        os.remove(f"{node_file}")
    if os.path.exists(f"{edge_file}"):
        os.remove(f"{edge_file}")
