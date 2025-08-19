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
        
        # Create an input database with nodes and edges tables
        input_db_path = temp_path / "input.duckdb"
        input_db = duckdb.connect(str(input_db_path))
        
        # Create test nodes table
        input_db.sql("""
        CREATE TABLE nodes (
            id VARCHAR,
            name VARCHAR,
            category VARCHAR,
            in_taxon VARCHAR,
            in_taxon_label VARCHAR
        )
        """)
        input_db.sql("""
        INSERT INTO nodes VALUES
            ('X:1', 'x1', 'Gene', 'NCBITaxon:9606', 'human'),
            ('X:2', 'x2', 'Gene', 'NCBITaxon:9606', 'human'),
            ('Y:1', 'y1', 'Disease', NULL, NULL)
        """)
        
        # Create test edges table  
        input_db.sql("""
        CREATE TABLE edges (
            subject VARCHAR,
            predicate VARCHAR,
            object VARCHAR,
            has_evidence VARCHAR,
            publications VARCHAR,
            negated BOOLEAN
        )
        """)
        input_db.sql("""
        INSERT INTO edges VALUES
            ('X:1', 'biolink:related_to', 'Y:1', 'ECO:1|ECO:2', 'PMID:1|PMID:2', false),
            ('X:2', 'biolink:related_to', 'Y:1', 'ECO:1', 'PMID:1', false)
        """)
        
        input_db.close()
        
        # Create closure file
        closure_content = """X:1	rdfs:subClassOf	X:2
X:2	rdfs:subClassOf	Y:1"""
        closure_file = temp_path / "closure.tsv"
        closure_file.write_text(closure_content)
        
        # Output files
        nodes_output = temp_path / "nodes_output.tsv"
        edges_output = temp_path / "edges_output.tsv"
        output_db_path = temp_path / "output.duckdb"
        
        # Run closurizer with database input
        runner = CliRunner()
        result = runner.invoke(main, [
            "--input-db", str(input_db_path),
            "--database", str(output_db_path),
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
        assert output_db_path.exists()
        
        # Verify TSV export contains pipe-delimited strings (this tests the core functionality)
        edges_output_content = edges_output.read_text()
        assert "ECO:1|ECO:2" in edges_output_content
        assert "PMID:1|PMID:2" in edges_output_content
        
        # Basic verification that processing occurred correctly
        nodes_output_content = nodes_output.read_text()
        assert "NCBITaxon:9606" in nodes_output_content
        assert "human" in nodes_output_content


def test_database_input_missing_tables():
    """Test error handling when input database is missing required tables"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create an input database with only nodes table (missing edges)
        input_db_path = temp_path / "input_incomplete.duckdb"
        input_db = duckdb.connect(str(input_db_path))
        
        # Create only nodes table
        input_db.sql("""
        CREATE TABLE nodes (id VARCHAR, name VARCHAR, category VARCHAR)
        """)
        input_db.sql("""
        INSERT INTO nodes VALUES ('X:1', 'x1', 'Gene')
        """)
        
        input_db.close()
        
        # Create minimal closure file
        closure_file = temp_path / "closure.tsv"
        closure_file.write_text("X:1\trdfs:subClassOf\tX:1")
        
        # Output files
        nodes_output = temp_path / "nodes_output.tsv"
        edges_output = temp_path / "edges_output.tsv"
        
        # Run closurizer with incomplete database
        runner = CliRunner()
        result = runner.invoke(main, [
            "--input-db", str(input_db_path),
            "--closure", str(closure_file),
            "--nodes-output", str(nodes_output),
            "--edges-output", str(edges_output)
        ])
        
        assert result.exit_code != 0
        assert "edges" in result.output.lower() or "ValueError" in str(result.exception)


def test_mutually_exclusive_inputs():
    """Test that --kg and --input-db are mutually exclusive"""
    
    runner = CliRunner()
    
    # Test both options provided
    result = runner.invoke(main, [
        "--kg", "test.tar.gz", 
        "--input-db", "test.duckdb",
        "--closure", "closure.tsv",
        "--nodes-output", "nodes.tsv",
        "--edges-output", "edges.tsv"
    ])
    assert result.exit_code == 2  # Click usage error
    assert "mutually exclusive" in result.output
    
    # Test neither option provided
    result = runner.invoke(main, [
        "--closure", "closure.tsv",
        "--nodes-output", "nodes.tsv", 
        "--edges-output", "edges.tsv"
    ])
    assert result.exit_code == 2  # Click usage error
    assert "Either --kg or --input-db must be specified" in result.output


def test_custom_database_path():
    """Test that custom database path is used correctly"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create input database  
        input_db_path = temp_path / "input.duckdb"
        input_db = duckdb.connect(str(input_db_path))
        
        # Create minimal test data with required fields
        input_db.sql("""
        CREATE TABLE nodes (id VARCHAR, name VARCHAR, category VARCHAR, in_taxon VARCHAR, in_taxon_label VARCHAR)
        """)
        input_db.sql("""INSERT INTO nodes VALUES ('X:1', 'x1', 'Gene', 'NCBITaxon:9606', 'human')""")
        
        input_db.sql("""
        CREATE TABLE edges (subject VARCHAR, predicate VARCHAR, object VARCHAR, has_evidence VARCHAR, publications VARCHAR, negated BOOLEAN)
        """)
        input_db.sql("""INSERT INTO edges VALUES ('X:1', 'biolink:related_to', 'X:1', 'ECO:1', 'PMID:1', false)""")
        
        input_db.close()
        
        # Create closure file
        closure_file = temp_path / "closure.tsv"
        closure_file.write_text("X:1\trdfs:subClassOf\tX:1")
        
        # Custom database path
        custom_db_path = temp_path / "my-custom-database.duckdb"
        
        # Output files
        nodes_output = temp_path / "nodes.tsv"
        edges_output = temp_path / "edges.tsv"
        
        # Run with custom database path
        runner = CliRunner()
        result = runner.invoke(main, [
            "--input-db", str(input_db_path),
            "--database", str(custom_db_path),
            "--closure", str(closure_file),
            "--nodes-output", str(nodes_output),
            "--edges-output", str(edges_output)
        ])
        
        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert custom_db_path.exists(), "Custom database path should be created"