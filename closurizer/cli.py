import click
from typing import List
from closurizer.closurizer import add_closure


@click.command()
@click.option('--kg', required=True, help='KGX tar.gz archive')
@click.option('--closure', required=True, help='TSV file of closure triples')
@click.option('--nodes-output', required=True, help='file write nodes kgx file with closure fields added')
@click.option('--edges-output', required=True, help='file write edges kgx file with closure fields added')
@click.option('--additional-node-constraints', required=False,
              help='additional where clause constraints to apply to the generation of the denormalized nodes output')
@click.option('--edge-fields', multiple=True, help='edge fields to expand with closure IDs, labels, etc')
@click.option('--edge-fields-to-label', multiple=True, help='edge fields to with category, label, etc but not full closure exansion')
@click.option('--node-fields', multiple=True, help='node fields to expand with closure IDs, labels, etc')
@click.option('--grouping-fields', multiple=True, help='fields to populate a single value grouping_key field')
@click.option('--dry-run', is_flag=True, help='A dry run will not write the output file, but will print the SQL query')
def main(kg: str,
         closure: str,
         nodes_output: str,
         edges_output: str,
         additional_node_constraints: str = None,
         dry_run: bool = False,
         edge_fields: List[str] = None,
         edge_fields_to_label: List[str] = None,
         node_fields: List[str] = None,
         grouping_fields: List[str] = None):
    add_closure(kg_archive=kg,
                closure_file=closure,
                edge_fields=edge_fields,
                edge_fields_to_label=edge_fields_to_label,
                node_fields=node_fields,
                edges_output_file=edges_output,
                nodes_output_file=nodes_output,
                additional_node_constraints=additional_node_constraints,
                dry_run=dry_run,
                grouping_fields=grouping_fields)

if __name__ == "__main__":
    main()
