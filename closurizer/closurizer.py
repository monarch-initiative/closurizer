from typing import List

import petl as etl
import sqlite3
import os
import tarfile


def _string_agg(key, rows):
    return [key, "|".join(row[1] for row in rows)]


def add_closure(node_file: str,
                edge_file: str,
                kg_archive: str,
                closure_file: str,
                path: str,
                fields: List[str],
                output_file: str):

    print("Generating closure KG...")
    print(f"node_file: {node_file}")
    print(f"edge_file: {edge_file}")
    print(f"kg_archive: {kg_archive}")
    print(f"closure_file: {closure_file}")
    print(f"fields: {','.join(fields)}")
    print(f"output_file: {output_file}")

    tar = tarfile.open(f"{path}/{kg_archive}")
    tar.extract(node_file, path=path)
    tar.extract(edge_file, path=path)

    db = "closurizer.db"

    if os.path.exists(db):
        os.remove(db)
    sqlite = sqlite3.connect(db)

    nodes = etl.fromtsv(node_file)
    etl.todb(nodes, sqlite, "nodes", create=True)

    edges = etl.fromtsv(edge_file)

    for field in fields:
        edges = etl.addfield(edges, f"{field}_namespace")
        edges = etl.addfield(edges, f"{field}_category")
        edges = etl.addfield(edges, f"{field}_closure")
        edges = etl.addfield(edges, f"{field}_label")
        edges = etl.addfield(edges, f"{field}_closure_label")

    etl.todb(edges, sqlite, "edges", create=True)

    closure_table = (etl
                     .fromtsv(closure_file)
                     .setheader(['id', 'predicate', 'ancestor'])
                     .cutout('predicate')  # assume all predicates for now
                     )

    closure_id_table = etl.rowreduce(closure_table, key='id', reducer=_string_agg, header=['id', 'ancestors'])
    etl.todb(closure_id_table, sqlite, "closure", create=True)

    closure_label_table = (etl.leftjoin(closure_table,
                                        etl.cut(nodes, ["id", "name"]),
                                        lkey="ancestor",
                                        rkey="id")
                           .cutout("ancestor")
                           .rename("name", "closure_label")
                           .selectnotnone("closure_label")
                           .rowreduce(key='id', reducer=_string_agg, header=['id', 'ancestor_labels']))
    etl.todb(closure_label_table, sqlite, "closure_label", create=True)

    cur = sqlite.cursor()

    for field in fields:
        etl.leftjoin(edges, closure_id_table, lkey=f"{field}", rkey="id")

    for field in fields:

        cur.execute(f"""
        update edges 
        set {field}_namespace = SUBSTR(nodes.id,1,INSTR(nodes.id,':') -1)
        from nodes
        where edges.{field} = nodes.id;
        """)

        cur.execute(f"""
        update edges 
        set {field}_category = nodes.category
        from nodes
        where edges.{field} = nodes.id;
        """)

        cur.execute(f"""
        update edges
        set {field}_closure = ancestors 
        from closure
        where edges.{field} = closure.id;
        """)

        cur.execute(f"""
        update edges 
        set {field}_label = nodes.name
        from nodes
        where edges.{field} = nodes.id;
        """)

        cur.execute(f"""
        update edges
        set {field}_closure_label = closure_label.ancestor_labels
        from closure_label
        where edges.{field} = closure_label.id;
        """)

    etl.fromdb(sqlite, 'select * from edges').totsv(f"{path}/{output_file}")

    # Clean up the database
    if os.path.exists(db):
        os.remove(db)

    # Clean up extracted node & edge files
    if os.path.exists(f"{path}/{node_file}"):
        os.remove(f"{path}/{node_file}")
    if os.path.exists(f"{path}/{edge_file}"):
        os.remove(f"{path}/{edge_file}")
