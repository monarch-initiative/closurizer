from typing import List, Optional

import os
import tarfile
import duckdb

def edge_columns(field: str, include_closure_fields: bool =True, node_column_names: list = None):
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

    # Only add taxon fields if they exist in the nodes table
    if field in ['subject', 'object'] and node_column_names:
        if 'in_taxon' in node_column_names:
            column_text += f"""
        {field}.in_taxon as {field}_taxon,"""
        if 'in_taxon_label' in node_column_names:
            column_text += f"""
        {field}.in_taxon_label as {field}_taxon_label,"""
        column_text += """
        """
    return column_text

def edge_joins(field: str, include_closure_joins: bool =True, is_multivalued: bool = False):
    if is_multivalued:
        # For VARCHAR[] fields, use array containment with list_contains
        join_condition = f"list_contains(edges.{field}, {field}.id)"
    else:
        # For VARCHAR fields, use direct equality
        join_condition = f"edges.{field} = {field}.id"
    
    joins = f"""
    left outer join nodes as {field} on {join_condition}"""
    
    if include_closure_joins:
        joins += f"""
    left outer join closure_id as {field}_closure on {field}.id = {field}_closure.id
    left outer join closure_label as {field}_closure_label on {field}.id = {field}_closure_label.id"""
    
    return joins + "\n    "

def evidence_sum(evidence_fields: List[str], edges_column_names: list = None):
    """ Sum together the length of each field - assumes fields are VARCHAR[] arrays """
    evidence_count_parts = []
    for field in evidence_fields:
        # Only include fields that actually exist in the edges table
        if not edges_column_names or field in edges_column_names:
            # All evidence fields are expected to be VARCHAR[] arrays
            evidence_count_parts.append(f"ifnull(array_length({field}),0)")
    
    evidence_count_sum = "+".join(evidence_count_parts) if evidence_count_parts else "0"
    return f"{evidence_count_sum} as evidence_count,"


def node_columns(predicate):
    # strip the biolink predicate, if necessary to get the field name
    field = predicate.replace('biolink:','')

    return f"""
    case when count(distinct {field}_edges.object) > 0 then array_agg(distinct {field}_edges.object) else null end as {field},
    case when count(distinct {field}_edges.object_label) > 0 then array_agg(distinct {field}_edges.object_label) else null end as {field}_label,
    count (distinct {field}_edges.object) as {field}_count,
    case when count({field}_closure.closure) > 0 then list_distinct(flatten(array_agg({field}_closure.closure))) else null end as {field}_closure,
    case when count({field}_closure_label.closure_label) > 0 then list_distinct(flatten(array_agg({field}_closure_label.closure_label))) else null end as {field}_closure_label,
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


def grouping_key(grouping_fields, edges_column_names: list = None):
    if not grouping_fields:
        return "null as grouping_key"
    fragments = []
    for field in grouping_fields:
        # Only include fields that actually exist in the edges table
        if not edges_column_names or field in edges_column_names:
            if field == 'negated':
                fragments.append(f"coalesce(cast({field} as varchar).replace('true','NOT'), '')")
            else:
                fragments.append(field)
    if not fragments:
        return "null as grouping_key"
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
    nodes_table_info = db.sql("DESCRIBE nodes").fetchall()
    node_column_names = [col[0] for col in nodes_table_info]
    node_column_types = {col[0]: col[1] for col in nodes_table_info}
    
    for field in multivalued_fields:
        if field in node_column_names:
            # Check if field is already VARCHAR[] - if so, skip conversion
            if 'VARCHAR[]' in node_column_types[field].upper():
                print(f"Field '{field}' in nodes table is already VARCHAR[], skipping conversion")
                continue
                
            print(f"Converting field '{field}' in nodes table to VARCHAR[]")
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
    edges_table_info = db.sql("DESCRIBE edges").fetchall()
    edge_column_names = [col[0] for col in edges_table_info]
    edge_column_types = {col[0]: col[1] for col in edges_table_info}
    
    for field in multivalued_fields:
        if field in edge_column_names:
            # Check if field is already VARCHAR[] - if so, skip conversion
            if 'VARCHAR[]' in edge_column_types[field].upper():
                print(f"Field '{field}' in edges table is already VARCHAR[], skipping conversion")
                continue
                
            print(f"Converting field '{field}' in edges table to VARCHAR[]")
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
                multivalued_fields: List[str] = ['has_evidence', 'publications', 'in_taxon', 'in_taxon_label'],
                export_edges: bool = False,
                export_nodes: bool = False
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

        db.sql("""
        create or replace table descendants_id as 
        select object_id as id, array_agg(subject_id) as descendants 
        from closure 
        group by object_id
        """)

        db.sql("""
        create or replace table descendants_label as 
        select object_id as id, array_agg(name) as descendants_label 
        from closure 
        join nodes on subject_id = nodes.id
        group by object_id
        """)

    # Get edges table schema to determine which fields are VARCHAR[]
    edges_table_info = db.sql("DESCRIBE edges").fetchall()
    edges_table_types = {col[0]: col[1] for col in edges_table_info}
    edges_column_names = [col[0] for col in edges_table_info]
    
    # Get nodes table schema to check for available columns
    nodes_table_info = db.sql("DESCRIBE nodes").fetchall()
    node_column_names = [col[0] for col in nodes_table_info]
    
    # Build edge joins with proper multivalued field handling
    edge_field_joins = []
    for field in edge_fields:
        is_multivalued = field in multivalued_fields and 'VARCHAR[]' in edges_table_types.get(field, '').upper()
        edge_field_joins.append(edge_joins(field, is_multivalued=is_multivalued))
    
    edge_field_to_label_joins = []
    for field in edge_fields_to_label:
        is_multivalued = field in multivalued_fields and 'VARCHAR[]' in edges_table_types.get(field, '').upper()
        edge_field_to_label_joins.append(edge_joins(field, include_closure_joins=False, is_multivalued=is_multivalued))
    
    edges_query = f"""
    create or replace table denormalized_edges as
    select edges.*, 
           {"".join([edge_columns(field, node_column_names=node_column_names) for field in edge_fields])}
           {"".join([edge_columns(field, include_closure_fields=False, node_column_names=node_column_names) for field in edge_fields_to_label])} 
           {evidence_sum(evidence_fields, edges_column_names)}
           {grouping_key(grouping_fields, edges_column_names)}  
    from edges
        {"".join(edge_field_joins)}
        {"".join(edge_field_to_label_joins)}
    """

    print(edges_query)

    additional_node_constraints = f"where {additional_node_constraints}" if additional_node_constraints else ""
    
    # Get nodes table info to handle multivalued fields in the query
    nodes_table_info = db.sql("DESCRIBE nodes").fetchall()
    nodes_table_column_names = [col[0] for col in nodes_table_info]
    nodes_table_types = {col[0]: col[1] for col in nodes_table_info}
    
    # Create field selections for nodes, converting VARCHAR[] back to pipe-delimited strings
    nodes_field_selections = []
    for field in nodes_table_column_names:
        if field in multivalued_fields and 'VARCHAR[]' in nodes_table_types[field].upper():
            # Convert VARCHAR[] back to pipe-delimited string
            nodes_field_selections.append(f"list_aggregate({field}, 'string_agg', '|') as {field}")
        else:
            # Regular field, use as-is (but need to specify for GROUP BY)
            nodes_field_selections.append(f"nodes.{field}")
    
    nodes_base_fields = ",\n        ".join(nodes_field_selections)
    
    nodes_query = f"""        
    create or replace table denormalized_nodes as
    select {nodes_base_fields}, 
        {"".join([node_columns(node_field) for node_field in node_fields])}
    from nodes
        {node_joins('has_phenotype')}
    {additional_node_constraints}
    group by {", ".join([f"nodes.{field}" for field in nodes_table_column_names])}
    """
    print(nodes_query)


    if not dry_run:

        db.sql(edges_query)

        # Export edges to TSV only if requested
        if export_edges:
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
        
        # Add descendant columns separately to avoid memory issues with large GROUP BY
        print("Adding descendant columns to denormalized_nodes...")
        db.sql("alter table denormalized_nodes add column descendants VARCHAR[]")
        db.sql("alter table denormalized_nodes add column descendants_label VARCHAR[]") 
        db.sql("alter table denormalized_nodes add column descendant_count INTEGER")
        
        db.sql("""
        update denormalized_nodes 
        set descendants = descendants_id.descendants
        from descendants_id 
        where denormalized_nodes.id = descendants_id.id
        """)
        
        db.sql("""
        update denormalized_nodes 
        set descendants_label = descendants_label.descendants_label
        from descendants_label 
        where denormalized_nodes.id = descendants_label.id
        """)
        
        db.sql("""
        update denormalized_nodes 
        set descendant_count = coalesce(array_length(descendants), 0)
        """)
        
        # Export nodes to TSV only if requested
        if export_nodes:
            # Get denormalized_nodes table info to handle array fields in export
            denorm_nodes_table_info = db.sql("DESCRIBE denormalized_nodes").fetchall()
            denorm_nodes_column_names = [col[0] for col in denorm_nodes_table_info]
            denorm_nodes_types = {col[0]: col[1] for col in denorm_nodes_table_info}
            
            # Find all VARCHAR[] fields that need conversion to pipe-delimited strings
            array_field_replacements = [
                f"list_aggregate({field}, 'string_agg', '|') as {field}"
                for field in denorm_nodes_column_names 
                if 'VARCHAR[]' in denorm_nodes_types[field].upper()
            ]
            
            # The descendants fields are already handled by the general VARCHAR[] logic above
            # No need to add them separately
            
            if array_field_replacements:
                nodes_replacements = "REPLACE (\n" + ",\n".join(array_field_replacements) + ")\n"
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
