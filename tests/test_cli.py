import csv

import pytest

from closurizer.cli import main
from tests import INPUT_DIR, OUTPUT_DIR


def test_help(runner):
    """
    Tests help message

    :param runner:
    :return:
    """
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "edges" in result.output


def test_cli_run(runner):
    """
    Tests closurize command

    :param runner:
    :return:
    """
    kg_file = INPUT_DIR / "bundle.tar.gz"
    rg_file = INPUT_DIR / "rg.tsv"
    output_node_file = OUTPUT_DIR / "nodes.csv"
    output_edges_file = OUTPUT_DIR / "edges-denorm.csv"
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    result = runner.invoke(main, ["--kg", kg_file, "--closure", rg_file, "--nodes-output", output_node_file, "--edges-output", output_edges_file])
    if result.exit_code != 0:
        print(result.output)
    assert result.exit_code == 0
    assert output_node_file.exists()
    found = False
    with open(output_edges_file, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
        for row in rows:
            print(row)
            if row["subject"] == "X:4" and row["object"] == "Y:4":
                found = True
                assert set(row["subject_closure"].split("|")) == {"X:4", "X:3", "X:2", "X:1"}
    assert found



