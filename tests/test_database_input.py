import pytest
import tempfile
import duckdb
from pathlib import Path
from click.testing import CliRunner
from closurizer.cli import main


def test_database_input_functionality():
    """Test that closurizer can read from an existing DuckDB database"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create a working database with nodes and edges tables
        working_db_path = temp_path / "working.duckdb"
        working_db = duckdb.connect(str(working_db_path))
        
        # Create test nodes table
        working_db.sql("""
        CREATE TABLE nodes (
            id VARCHAR,
            name VARCHAR,
            category VARCHAR,
            in_taxon VARCHAR,
            in_taxon_label VARCHAR
        )
        """)
        working_db.sql("""
        INSERT INTO nodes VALUES
            ('X:1', 'x1', 'Gene', 'NCBITaxon:9606', 'human'),
            ('X:2', 'x2', 'Gene', 'NCBITaxon:9606', 'human'),
            ('Y:1', 'y1', 'Disease', NULL, NULL)
        """)
        
        # Create test edges table  
        working_db.sql("""
        CREATE TABLE edges (
            subject VARCHAR,
            predicate VARCHAR,
            object VARCHAR,
            has_evidence VARCHAR,
            publications VARCHAR,
            negated BOOLEAN
        )
        """)
        working_db.sql("""
        INSERT INTO edges VALUES
            ('X:1', 'biolink:related_to', 'Y:1', 'ECO:1|ECO:2', 'PMID:1|PMID:2', false),
            ('X:2', 'biolink:related_to', 'Y:1', 'ECO:1', 'PMID:1', false)
        """)
        
        working_db.close()
        
        # Create closure file
        closure_content = """X:1	rdfs:subClassOf	X:2
X:2	rdfs:subClassOf	Y:1"""
        closure_file = temp_path / "closure.tsv"
        closure_file.write_text(closure_content)
        
        # Output files
        nodes_output = temp_path / "nodes_output.tsv"
        edges_output = temp_path / "edges_output.tsv"
        
        # Run closurizer with database input (no --kg means use existing database)
        runner = CliRunner()
        result = runner.invoke(main, [
            "--database", str(working_db_path),
            "--closure", str(closure_file),
            "--nodes-output", str(nodes_output),
            "--edges-output", str(edges_output),
            "--multivalued-fields", "has_evidence",
            "--multivalued-fields", "publications",
            "--multivalued-fields", "in_taxon",
            "--multivalued-fields", "in_taxon_label"
        ])
        
        if result.exit_code != 0:
            print("STDOUT:", result.output)
            if result.exception:
                print("EXCEPTION:", result.exception)
        
        assert result.exit_code == 0, f"Command failed with output: {result.output}"
        
        # Verify output files were created
        assert nodes_output.exists()
        assert edges_output.exists()
        assert working_db_path.exists()
        
        # Verify TSV export contains pipe-delimited strings (this tests the core functionality)
        edges_output_content = edges_output.read_text()
        assert "ECO:1|ECO:2" in edges_output_content
        assert "PMID:1|PMID:2" in edges_output_content
        
        # Basic verification that processing occurred correctly
        nodes_output_content = nodes_output.read_text()
        assert "NCBITaxon:9606" in nodes_output_content
        assert "human" in nodes_output_content


def test_database_input_missing_tables():
    """Test error handling when database is missing required tables"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create a database with only nodes table (missing edges)
        incomplete_db_path = temp_path / "incomplete.duckdb"
        incomplete_db = duckdb.connect(str(incomplete_db_path))
        
        # Create only nodes table
        incomplete_db.sql("""
        CREATE TABLE nodes (id VARCHAR, name VARCHAR, category VARCHAR)
        """)
        incomplete_db.sql("""
        INSERT INTO nodes VALUES ('X:1', 'x1', 'Gene')
        """)
        
        incomplete_db.close()
        
        # Create minimal closure file
        closure_file = temp_path / "closure.tsv"
        closure_file.write_text("X:1\trdfs:subClassOf\tX:1")
        
        # Output files
        nodes_output = temp_path / "nodes_output.tsv"
        edges_output = temp_path / "edges_output.tsv"
        
        # Run closurizer with incomplete database
        runner = CliRunner()
        result = runner.invoke(main, [
            "--database", str(incomplete_db_path),
            "--closure", str(closure_file),
            "--nodes-output", str(nodes_output),
            "--edges-output", str(edges_output)
        ])
        
        assert result.exit_code != 0
        assert "edges" in result.output.lower() or "ERROR" in result.output


def test_missing_database_with_no_archive():
    """Test error handling when no --kg is provided and database doesn't exist"""
    
    runner = CliRunner()
    
    # Test with non-existent database path and no --kg
    result = runner.invoke(main, [
        "--database", "nonexistent.duckdb",
        "--closure", "closure.tsv",
        "--nodes-output", "nodes.tsv", 
        "--edges-output", "edges.tsv"
    ])
    assert result.exit_code != 0
    assert "must be specified or database_path must exist" in result.output or "ERROR" in result.output


def test_custom_database_path():
    """Test that custom database path is used correctly"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create working database  
        working_db_path = temp_path / "my-custom-database.duckdb"
        working_db = duckdb.connect(str(working_db_path))
        
        # Create minimal test data with required fields
        working_db.sql("""
        CREATE TABLE nodes (id VARCHAR, name VARCHAR, category VARCHAR, in_taxon VARCHAR, in_taxon_label VARCHAR)
        """)
        working_db.sql("""INSERT INTO nodes VALUES ('X:1', 'x1', 'Gene', 'NCBITaxon:9606', 'human')""")
        
        working_db.sql("""
        CREATE TABLE edges (subject VARCHAR, predicate VARCHAR, object VARCHAR, has_evidence VARCHAR, publications VARCHAR, negated BOOLEAN)
        """)
        working_db.sql("""INSERT INTO edges VALUES ('X:1', 'biolink:related_to', 'X:1', 'ECO:1', 'PMID:1', false)""")
        
        working_db.close()
        
        # Create closure file
        closure_file = temp_path / "closure.tsv"
        closure_file.write_text("X:1\trdfs:subClassOf\tX:1")
        
        # Output files
        nodes_output = temp_path / "nodes.tsv"
        edges_output = temp_path / "edges.tsv"
        
        # Run with custom database path
        runner = CliRunner()
        result = runner.invoke(main, [
            "--database", str(working_db_path),
            "--closure", str(closure_file),
            "--nodes-output", str(nodes_output),
            "--edges-output", str(edges_output)
        ])
        
        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert working_db_path.exists(), "Custom database path should exist"