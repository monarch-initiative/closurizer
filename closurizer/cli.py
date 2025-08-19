import click
from typing import List
from closurizer.closurizer import add_closure


@click.command()
@click.option('--kg', required=False, help='KGX tar.gz archive (mutually exclusive with --input-db)')
@click.option('--input-db', required=False, help='Path to existing DuckDB database with nodes and edges tables (mutually exclusive with --kg)')
@click.option('--database', required=False, default='monarch-kg.duckdb', help='Output database path (default: monarch-kg.duckdb)')
@click.option('--closure', required=True, help='TSV file of closure triples')
@click.option('--nodes-output', required=True, help='file write nodes kgx file with closure fields added')
@click.option('--edges-output', required=True, help='file write edges kgx file with closure fields added')
@click.option('--additional-node-constraints', required=False,
              help='additional where clause constraints to apply to the generation of the denormalized nodes output')
@click.option('--edge-fields', multiple=True, help='edge fields to expand with closure IDs, labels, etc')
@click.option('--edge-fields-to-label', multiple=True, help='edge fields to with category, label, etc but not full closure exansion')
@click.option('--node-fields', multiple=True, help='node fields to expand with closure IDs, labels, etc')
@click.option('--grouping-fields', multiple=True, help='fields to populate a single value grouping_key field')
@click.option('--multivalued-fields', multiple=True, help='fields containing pipe-delimited values to convert to varchar[] arrays in database')
@click.option('--dry-run', is_flag=True, help='A dry run will not write the output file, but will print the SQL query')
def main(kg: str,
         input_db: str,
         database: str,
         closure: str,
         nodes_output: str,
         edges_output: str,
         additional_node_constraints: str = None,
         dry_run: bool = False,
         edge_fields: List[str] = None,
         edge_fields_to_label: List[str] = None,
         node_fields: List[str] = None,
         grouping_fields: List[str] = None,
         multivalued_fields: List[str] = None):
    
    # Validate mutually exclusive input options
    if not kg and not input_db:
        raise click.UsageError("Either --kg or --input-db must be specified")
    if kg and input_db:
        raise click.UsageError("--kg and --input-db are mutually exclusive")
    
    add_closure(kg_archive=kg,
                input_database=input_db,
                database_path=database,
                closure_file=closure,
                edge_fields=edge_fields or ['subject', 'object'],
                edge_fields_to_label=edge_fields_to_label or [],
                node_fields=node_fields or [],
                edges_output_file=edges_output,
                nodes_output_file=nodes_output,
                additional_node_constraints=additional_node_constraints,
                dry_run=dry_run,
                grouping_fields=grouping_fields or ['subject', 'negated', 'predicate', 'object'],
                multivalued_fields=multivalued_fields or ['has_evidence', 'publications', 'in_taxon', 'in_taxon_label'])

if __name__ == "__main__":
    main()
