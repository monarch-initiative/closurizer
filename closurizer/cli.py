import click
from typing import List
from closurizer.closurizer import add_closure


@click.command()
@click.option('--kg', required=True, help='KGX tar.gz archive')
@click.option('--closure', required=True, help='TSV file of closure triples')
@click.option('--output', '-o', required=True, help='file write kgx file with closure fields added')
@click.option('--fields', multiple=True, help='fields to closurize')
def main(kg: str,
         closure: str,
         output: str,
         fields: List[str] = None):
    add_closure(kg_archive=kg, closure_file=closure, fields=fields, output_file=output)


if __name__ == "__main__":
    main()
