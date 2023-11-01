from typing import List

import os
import tarfile
import duckdb

def columns(field):
    column_text = f"""
       {field}.name as {field}_label, 
       {field}.category as {field}_category,
       {field}.namespace as {field}_namespace,
       {field}_closure.closure as {field}_closure,
       {field}_closure_label.closure_label as {field}_closure_label,    
    """
    if field in ['subject', 'object']:
        column_text += f"""
        {field}.in_taxon as {field}_taxon,
        {field}.in_taxon_label as {field}_taxon_label,
        """
    return column_text

def joins(field):
    return f"""
    left outer join nodes as {field} on edges.{field} = {field}.id
    left outer join closure_id as {field}_closure on {field}.id = {field}_closure.id
    left outer join closure_label as {field}_closure_label on {field}.id = {field}_closure_label.id
    """

def evidence_sum(evidence_fields):
    """ Sum together the length of each field after splitting on | """
    evidence_count_sum = "+".join([f"len(split({field}, '|'))" for field in evidence_fields])
    return f"{evidence_count_sum} as evidence_count,"

def grouping_key(grouping_fields):
    fragments = []
    for field in grouping_fields:
        if field == 'negated':
            fragments.append(f"coalesce({field}.replace('True','NOT'), '')")
        else:
            fragments.append(field)
    grouping_key_fragments = ", ".join(fragments)
    return f"concat_ws('🍪', {grouping_key_fragments}) as grouping_key"

def add_closure(kg_archive: str,
                closure_file: str,
                output_file: str,
                fields: List[str] = ['subject', 'object'],
                dry_run: bool  = False,
                evidence_fields: List[str] = None,
                grouping_fields: List[str] = None
                ):
    print("Generating closure KG...")
    print(f"kg_archive: {kg_archive}")
    print(f"closure_file: {closure_file}")

    db = duckdb.connect(database='monarch-kg.duckdb')

    if fields is None or len(fields) == 0:
        fields = ['subject', 'object']

    if evidence_fields is None or len(evidence_fields) == 0:
        evidence_fields = ['has_evidence', 'publications']

    if grouping_fields is None or len(grouping_fields) == 0:
        grouping_fields = ['subject', 'negated', 'predicate', 'object']


    if not dry_run:
        print(f"fields: {','.join(fields)}")
        print(f"output_file: {output_file}")

        tar = tarfile.open(f"{kg_archive}")

        print("Loading node table...")
        node_file_name = [member.name for member in tar.getmembers() if member.name.endswith('_nodes.tsv') ][0]
        tar.extract(node_file_name,)
        node_file = f"{node_file_name}"
        print(f"node_file: {node_file}")

        db.sql(f"""
        create or replace table nodes as select *,  substr(id, 1, instr(id,':') -1) as namespace from read_csv('{node_file_name}', header=True, sep='\t', AUTO_DETECT=TRUE)
        """)

        edge_file_name = [member.name for member in tar.getmembers() if member.name.endswith('_edges.tsv') ][0]
        tar.extract(edge_file_name)
        edge_file = f"{edge_file_name}"
        print(f"edge_file: {edge_file}")

        db.sql(f"""
        create or replace table edges as select * from read_csv('{edge_file_name}', header=True, sep='\t', AUTO_DETECT=TRUE)
        """)

        # Load the relation graph tsv in long format mapping a node to each of it's ancestors
        db.sql(f"""
        create or replace table closure as select * from read_csv('{closure_file}', sep='\t', names=['subject_id', 'predicate_id', 'object_id'], AUTO_DETECT=TRUE)
        """)

        db.sql("""
        create or replace table closure_id as select subject_id as id, string_agg(object_id, '|') as closure from closure group by subject_id
        """)

        db.sql("""
        create or replace table closure_label as select subject_id as id, string_agg(name, '|') as closure_label from closure join nodes on object_id = id
        group by subject_id
        """)

    query = f"""
    create or replace table denormalized_edges as
    select edges.*, 
           {"".join([columns(field) for field in fields])}
           {evidence_sum(evidence_fields)}
           {grouping_key(grouping_fields)}  
    from edges
        {"".join([joins(field) for field in fields])}
    """

    print(query)

    if not dry_run:
        db.query(query)
        db.query(f"""
        -- write denormalized_edges as tsv
        copy (select * from denormalized_edges) to '{output_file}' (header, delimiter '\t')
        """)

        # Clean up extracted node & edge files
        if os.path.exists(f"{node_file}"):
            os.remove(f"{node_file}")
        if os.path.exists(f"{edge_file}"):
            os.remove(f"{edge_file}")
