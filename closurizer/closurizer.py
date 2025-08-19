from typing import List, Optional

import os
import tarfile
import duckdb

def edge_columns(field: str, include_closure_fields: bool =True):
    column_text = f"""
       {field}.name as {field}_label, 
       {field}.category as {field}_category,
       {field}.namespace as {field}_namespace,       
    """
    if include_closure_fields:
        column_text += f"""
        {field}_closure.closure as {field}_closure,
        {field}_closure_label.closure_label as {field}_closure_label,
        """

    if field in ['subject', 'object']:
        column_text += f"""
        {field}.in_taxon as {field}_taxon,
        {field}.in_taxon_label as {field}_taxon_label,
        """
    return column_text

def edge_joins(field: str, include_closure_joins: bool =True):
    return f"""
    left outer join nodes as {field} on edges.{field} = {field}.id
    left outer join closure_id as {field}_closure on {field}.id = {field}_closure.id
    left outer join closure_label as {field}_closure_label on {field}.id = {field}_closure_label.id
    """

def evidence_sum(evidence_fields: List[str], multivalued_fields: List[str]):
    """ Sum together the length of each field after splitting on | """
    evidence_count_parts = []
    for field in evidence_fields:
        if field in multivalued_fields:
            # Field is already an array, use array_length
            evidence_count_parts.append(f"ifnull(array_length({field}),0)")
        else:
            # Field is still string, split and count
            evidence_count_parts.append(f"ifnull(len(split({field}, '|')),0)")
    
    evidence_count_sum = "+".join(evidence_count_parts) if evidence_count_parts else "0"
    return f"{evidence_count_sum} as evidence_count,"


def node_columns(predicate):
    # strip the biolink predicate, if necessary to get the field name
    field = predicate.replace('biolink:','')

    return f"""
    string_agg({field}_edges.object, '|') as {field},
    string_agg({field}_edges.object_label, '|') as {field}_label,
    count (distinct {field}_edges.object) as {field}_count,
    list_aggregate(list_distinct(flatten(array_agg({field}_closure.closure))), 'string_agg', '|') as {field}_closure,
    list_aggregate(list_distinct(flatten(array_agg({field}_closure_label.closure_label))), 'string_agg', '|') as {field}_closure_label,
    """

def node_joins(predicate):
    # strip the biolink predicate, if necessary to get the field name
    field = predicate.replace('biolink:','')
    return f"""
      left outer join denormalized_edges as {field}_edges 
        on nodes.id = {field}_edges.subject 
           and {field}_edges.predicate = 'biolink:{field}'
      left outer join closure_id as {field}_closure
        on {field}_edges.object = {field}_closure.id
      left outer join closure_label as {field}_closure_label
        on {field}_edges.object = {field}_closure_label.id
    """


def grouping_key(grouping_fields):
    if not grouping_fields:
        return "null as grouping_key"
    fragments = []
    for field in grouping_fields:
        if field == 'negated':
            fragments.append(f"coalesce(cast({field} as varchar).replace('true','NOT'), '')")
        else:
            fragments.append(field)
    grouping_key_fragments = ", ".join(fragments)
    return f"concat_ws('|', {grouping_key_fragments}) as grouping_key"




def load_from_archive(kg_archive: str, db, multivalued_fields: List[str]):
    """Load nodes and edges tables from tar.gz archive"""
    import tarfile
    import os
    
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
    
    # Convert multivalued fields to arrays
    prepare_multivalued_fields(db, multivalued_fields)
    
    # Clean up extracted files
    if os.path.exists(f"{node_file}"):
        os.remove(f"{node_file}")
    if os.path.exists(f"{edge_file}"):
        os.remove(f"{edge_file}")


def prepare_multivalued_fields(db, multivalued_fields: List[str]):
    """Convert specified fields to varchar[] arrays in both nodes and edges tables"""
    
    # Convert multivalued fields in nodes table to varchar[] arrays
    node_column_names = [col[0] for col in db.sql("DESCRIBE nodes").fetchall()]
    for field in multivalued_fields:
        if field in node_column_names:
            # Create a new column with proper array type and replace the original
            db.sql(f"""
            alter table nodes add column {field}_array VARCHAR[]
            """)
            db.sql(f"""
            update nodes set {field}_array = 
                case 
                    when {field} is null or {field} = '' then null
                    else split({field}, '|')
                end
            """)
            db.sql(f"""
            alter table nodes drop column {field}
            """)
            db.sql(f"""
            alter table nodes rename column {field}_array to {field}
            """)

    # Convert multivalued fields in edges table to varchar[] arrays
    edge_column_names = [col[0] for col in db.sql("DESCRIBE edges").fetchall()]
    for field in multivalued_fields:
        if field in edge_column_names:
            # Create a new column with proper array type and replace the original
            db.sql(f"""
            alter table edges add column {field}_array VARCHAR[]
            """)
            db.sql(f"""
            update edges set {field}_array = 
                case 
                    when {field} is null or {field} = '' then null
                    else split({field}, '|')
                end
            """)
            db.sql(f"""
            alter table edges drop column {field}
            """)
            db.sql(f"""
            alter table edges rename column {field}_array to {field}
            """)


def add_closure(closure_file: str,
                nodes_output_file: str,
                edges_output_file: str,
                kg_archive: Optional[str] = None,
                database_path: str = 'monarch-kg.duckdb',
                node_fields: List[str] = [],
                edge_fields: List[str] = ['subject', 'object'],
                edge_fields_to_label: List[str] = [],
                additional_node_constraints: Optional[str] = None,
                dry_run: bool  = False,
                evidence_fields: List[str] = ['has_evidence', 'publications'],
                grouping_fields: List[str] = ['subject', 'negated', 'predicate', 'object'],
                multivalued_fields: List[str] = ['has_evidence', 'publications', 'in_taxon', 'in_taxon_label']
                ):
    # Validate input parameters
    if not kg_archive and not os.path.exists(database_path):
        raise ValueError("Either kg_archive must be specified or database_path must exist")
    
    print("Generating closure KG...")
    if kg_archive:
        print(f"kg_archive: {kg_archive}")
    print(f"database_path: {database_path}")
    print(f"closure_file: {closure_file}")

    # Connect to database
    db = duckdb.connect(database=database_path)

    if not dry_run:
        print(f"fields: {','.join(edge_fields)}")
        print(f"output_file: {edges_output_file}")

        # Load data based on input method
        if kg_archive:
            load_from_archive(kg_archive, db, multivalued_fields)
        else:
            # Database already exists and contains data
            # Check if namespace column exists, add it if needed
            node_column_names = [col[0] for col in db.sql("DESCRIBE nodes").fetchall()]
            if 'namespace' not in node_column_names:
                print("Adding namespace column to nodes table...")
                db.sql("ALTER TABLE nodes ADD COLUMN namespace VARCHAR")
                db.sql("UPDATE nodes SET namespace = substr(id, 1, instr(id,':') -1)")
            
            # Convert multivalued fields to arrays
            prepare_multivalued_fields(db, multivalued_fields)

        # Load the relation graph tsv in long format mapping a node to each of it's ancestors
        db.sql(f"""
        create or replace table closure as select * from read_csv('{closure_file}', sep='\t', names=['subject_id', 'predicate_id', 'object_id'], AUTO_DETECT=TRUE)
        """)

        db.sql("""
        create or replace table closure_id as select subject_id as id, array_agg(object_id) as closure from closure group by subject_id
        """)

        db.sql("""
        create or replace table closure_label as select subject_id as id, array_agg(name) as closure_label from closure join nodes on object_id = id
        group by subject_id
        """)

    edges_query = f"""
    create or replace table denormalized_edges as
    select edges.*, 
           {"".join([edge_columns(field) for field in edge_fields])}
           {"".join([edge_columns(field, include_closure_fields=False) for field in edge_fields_to_label])} 
           {evidence_sum(evidence_fields, multivalued_fields)}
           {grouping_key(grouping_fields)}  
    from edges
        {"".join([edge_joins(field) for field in edge_fields])}
        {"".join([edge_joins(field, include_closure_joins=False) for field in edge_fields_to_label])}
    """

    print(edges_query)

    additional_node_constraints = f"where {additional_node_constraints}" if additional_node_constraints else ""
    nodes_query = f"""        
    create or replace table denormalized_nodes as
    select nodes.*, 
        {"".join([node_columns(node_field) for node_field in node_fields])}
    from nodes
        {node_joins('has_phenotype')}
    {additional_node_constraints}
    group by nodes.*
    """
    print(nodes_query)


    if not dry_run:

        db.sql(edges_query)

        edge_closure_replacements = [
            f"""
            list_aggregate({field}_closure, 'string_agg', '|') as {field}_closure,
            list_aggregate({field}_closure_label, 'string_agg', '|') as {field}_closure_label
            """
            for field in edge_fields
        ]
        
        # Add conversions for original multivalued fields back to pipe-delimited strings
        edge_table_info = db.sql("DESCRIBE denormalized_edges").fetchall()
        edge_table_column_names = [col[0] for col in edge_table_info]
        edge_table_types = {col[0]: col[1] for col in edge_table_info}
        
        # Create set of closure fields already handled by edge_closure_replacements
        closure_fields_handled = set()
        for field in edge_fields:
            closure_fields_handled.add(f"{field}_closure")
            closure_fields_handled.add(f"{field}_closure_label")
        
        multivalued_replacements = [
            f"list_aggregate({field}, 'string_agg', '|') as {field}"
            for field in multivalued_fields 
            if field in edge_table_column_names and 'VARCHAR[]' in edge_table_types[field].upper()
            and field not in closure_fields_handled
        ]
        
        all_replacements = edge_closure_replacements + multivalued_replacements
        edge_closure_replacements = "REPLACE (\n" + ",\n".join(all_replacements) + ")\n"

        edges_export_query = f"""
        -- write denormalized_edges as tsv
        copy (select * {edge_closure_replacements} from denormalized_edges) to '{edges_output_file}' (header, delimiter '\t')
        """
        print(edges_export_query)
        db.sql(edges_export_query)

        db.sql(nodes_query)
        
        # Convert multivalued fields back to pipe-delimited strings for nodes export
        nodes_table_info = db.sql("DESCRIBE denormalized_nodes").fetchall()
        nodes_table_column_names = [col[0] for col in nodes_table_info]
        nodes_table_types = {col[0]: col[1] for col in nodes_table_info}
        
        nodes_multivalued_replacements = [
            f"list_aggregate({field}, 'string_agg', '|') as {field}"
            for field in multivalued_fields 
            if field in nodes_table_column_names and 'VARCHAR[]' in nodes_table_types[field].upper()
        ]
        
        if nodes_multivalued_replacements:
            nodes_replacements = "REPLACE (\n" + ",\n".join(nodes_multivalued_replacements) + ")\n"
            nodes_export_query = f"""
            -- write denormalized_nodes as tsv
            copy (select * {nodes_replacements} from denormalized_nodes) to '{nodes_output_file}' (header, delimiter '\t')
            """
        else:
            nodes_export_query = f"""
            -- write denormalized_nodes as tsv
            copy (select * from denormalized_nodes) to '{nodes_output_file}' (header, delimiter '\t')
            """
        print(nodes_export_query)
        db.sql(nodes_export_query)
