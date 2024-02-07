import click
from typing import List
from closurizer.closurizer import add_closure


@click.command()
@click.option('--kg', required=True, help='KGX tar.gz archive')
@click.option('--closure', required=True, help='TSV file of closure triples')
@click.option('--nodes-output', required=True, help='file write nodes kgx file with closure fields added')
@click.option('--edges-output', required=True, help='file write edges kgx file with closure fields added')
@click.option('--fields', multiple=True, help='fields to closurize')
@click.option('--grouping-fields', multiple=True, help='fields to populate a single value grouping_key field')
@click.option('--dry-run', is_flag=True, help='A dry run will not write the output file, but will print the SQL query')
def main(kg: str,
         closure: str,
         nodes_output: str,
         edges_output: str,
         dry_run: bool = False,
         fields: List[str] = None,
         grouping_fields: List[str] = None):
    add_closure(kg_archive=kg,
                closure_file=closure,
                fields=fields,
                edges_output_file=edges_output,
                nodes_output_file=nodes_output,
                dry_run=dry_run,
                grouping_fields=grouping_fields)

if __name__ == "__main__":
    main()
