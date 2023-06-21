from typing import List

import petl as etl
import os
import tarfile


def _string_agg(key, rows):
    return [key, "|".join(row[1] for row in rows)]


def _cut_left_join(ltable, rtable, field, attribute):
    return etl.leftjoin(ltable,
                        (etl.cut(rtable, ['id', attribute]).rename(attribute, f"{field}_{attribute}")),
                        lkey=field,
                        rkey="id")


def add_closure(node_file: str,
                edge_file: str,
                kg_archive: str,
                closure_file: str,
                path: str,
                node_fields_to_label_expand: List[str],
                edge_fields_to_expand: List[str],
                node_output_file: str,
                edge_output_file: str
                ):
    print("Generating closure KG...")
    print(f"node_file: {node_file}")
    print(f"edge_file: {edge_file}")
    print(f"kg_archive: {kg_archive}")
    print(f"closure_file: {closure_file}")
    print(f"node_fields_to_label_expand: {', '.join(node_fields_to_label_expand)}")
    print(f"edge_fields_to_expand: {', '.join(edge_fields_to_expand)}")
    print(f"node_output_file: {node_output_file}")
    print(f"edge_output_file: {edge_output_file}")

    tar = tarfile.open(f"{path}/{kg_archive}")
    tar.extract(node_file, path=path)
    tar.extract(edge_file, path=path)

    # add paths, so that steps below can find the file
    node_file = f"{path}/{node_file}"
    edge_file = f"{path}/{edge_file}"

    nodes = etl.fromtsv(node_file)
    nodes = etl.addfield(nodes, 'namespace', lambda rec: rec['id'][:rec['id'].index(":")])

    edges = etl.fromtsv(edge_file)

    # Load the relation graph tsv in long format mapping a node to each of it's ancestors
    closure_table = (etl
                     .fromtsv(closure_file)
                     .setheader(['id', 'predicate', 'ancestor'])
                     .cutout('predicate')  # assume all predicates for now
                     )
    print("closure table")
    print(etl.head(closure_table))
    # Prepare the closure id table, mapping node IDs to pipe separated lists of ancestors
    closure_id_table = (etl.rowreduce(closure_table, key='id',
                                      reducer=_string_agg,
                                      header=['id', 'ancestors'])
                        .rename('ancestors', 'closure'))
    print("closure_id_table")
    print(etl.head(closure_id_table))

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
    print("closure_label_table")
    print(etl.head(closure_label_table))

    for field in node_fields_to_label_expand:
        nodes = _cut_left_join(nodes, nodes, field, "label")

    for field in edge_fields_to_expand:
        edges = _cut_left_join(edges, nodes, field, "namespace")
        edges = _cut_left_join(edges, nodes, field, "category")
        edges = _cut_left_join(edges, closure_id_table, field, "closure")
        edges = _cut_left_join(edges, closure_label_table, field, "closure_label")
        edges = etl.leftjoin(edges, (etl.cut(nodes, ["id", "name"]).rename("name", f"{field}_label")), lkey=field,
                             rkey="id")

    print("edges table")
    print(etl.head(edges))

    etl.totsv(edges, f"{path}/{edge_output_file}")
    etl.totsv(nodes, f"{path}/{node_output_file}")

    # Clean up extracted node & edge files
    if os.path.exists(f"{path}/{node_file}"):
        os.remove(f"{path}/{node_file}")
    if os.path.exists(f"{path}/{edge_file}"):
        os.remove(f"{path}/{edge_file}")
