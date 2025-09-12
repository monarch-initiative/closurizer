import pytest
import tempfile
import tarfile
from pathlib import Path
from click.testing import CliRunner
from closurizer.cli import main
import duckdb


def test_multivalued_field_array_conversion():
    """Test that multivalued fields are properly converted to varchar[] arrays in database"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test data with pipe-delimited multivalued fields
        nodes_content = """id	name	category	in_taxon	in_taxon_label
X:1	x1	Gene	NCBITaxon:9606|NCBITaxon:10090	human|mouse
X:2	x2	Gene	NCBITaxon:9606	human
Y:1	y1	Disease		
"""
        
        edges_content = """subject	predicate	object	has_evidence	publications	negated
X:1	biolink:related_to	Y:1	ECO:1|ECO:2	PMID:1|PMID:2|PMID:3	False
X:2	biolink:related_to	Y:1	ECO:1	PMID:1	False
"""
        
        closure_content = """X:1	rdfs:subClassOf	X:2
"""
        
        # Write test files
        nodes_file = temp_path / "test_nodes.tsv"
        edges_file = temp_path / "test_edges.tsv"
        closure_file = temp_path / "closure.tsv"
        
        nodes_file.write_text(nodes_content)
        edges_file.write_text(edges_content)
        closure_file.write_text(closure_content)
        
        # Create tar archive
        archive_file = temp_path / "test_kg.tar.gz"
        with tarfile.open(archive_file, "w:gz") as tar:
            tar.add(nodes_file, arcname="test_nodes.tsv")
            tar.add(edges_file, arcname="test_edges.tsv")
        
        # Output files
        nodes_output = temp_path / "nodes_output.tsv"
        edges_output = temp_path / "edges_output.tsv"
        
        # Run closurizer
        runner = CliRunner()
        result = runner.invoke(main, [
            "--kg", str(archive_file),
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
        
        # Verify database contains arrays
        db = duckdb.connect('monarch-kg.duckdb')
        
        # Check that multivalued fields in edges table are arrays
        edges_schema = {col[0]: col[1] for col in db.sql("DESCRIBE edges").fetchall()}
        assert 'VARCHAR[]' in edges_schema.get('has_evidence', '').upper()
        assert 'VARCHAR[]' in edges_schema.get('publications', '').upper()
        
        # Check that multivalued fields in nodes table are arrays  
        nodes_schema = {col[0]: col[1] for col in db.sql("DESCRIBE nodes").fetchall()}
        assert 'VARCHAR[]' in nodes_schema.get('in_taxon', '').upper()
        assert 'VARCHAR[]' in nodes_schema.get('in_taxon_label', '').upper()
        
        # Verify array contents
        edge_data = db.sql("SELECT has_evidence, publications FROM edges WHERE subject = 'X:1'").fetchone()
        assert edge_data[0] == ['ECO:1', 'ECO:2']  # has_evidence as array
        assert edge_data[1] == ['PMID:1', 'PMID:2', 'PMID:3']  # publications as array
        
        # Single value should still be array with one element
        edge_data_single = db.sql("SELECT has_evidence, publications FROM edges WHERE subject = 'X:2'").fetchone()
        assert edge_data_single[0] == ['ECO:1']
        assert edge_data_single[1] == ['PMID:1']
        
        node_data = db.sql("SELECT in_taxon, in_taxon_label FROM nodes WHERE id = 'X:1'").fetchone()
        assert node_data[0] == ['NCBITaxon:9606', 'NCBITaxon:10090']
        assert node_data[1] == ['human', 'mouse']
        
        # Verify TSV export contains pipe-delimited strings
        edges_output_content = edges_output.read_text()
        assert "ECO:1|ECO:2" in edges_output_content
        assert "PMID:1|PMID:2|PMID:3" in edges_output_content
        
        nodes_output_content = nodes_output.read_text()
        assert "NCBITaxon:9606|NCBITaxon:10090" in nodes_output_content
        assert "human|mouse" in nodes_output_content


def test_backward_compatibility_no_multivalued_fields():
    """Test that the system works when no multivalued fields are specified"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create minimal test data
        nodes_content = """id	name	category	in_taxon	in_taxon_label
X:1	x1	Gene	NCBITaxon:9606	human
"""
        
        edges_content = """subject	predicate	object	has_evidence	publications	negated
X:1	biolink:related_to	X:1	ECO:1	PMID:1	False
"""
        
        closure_content = "X:1\trdfs:subClassOf\tX:1"
        
        # Write test files
        nodes_file = temp_path / "test_nodes.tsv"
        edges_file = temp_path / "test_edges.tsv"
        closure_file = temp_path / "closure.tsv"
        
        nodes_file.write_text(nodes_content)
        edges_file.write_text(edges_content)
        closure_file.write_text(closure_content)
        
        # Create tar archive
        archive_file = temp_path / "test_kg.tar.gz"
        with tarfile.open(archive_file, "w:gz") as tar:
            tar.add(nodes_file, arcname="test_nodes.tsv")
            tar.add(edges_file, arcname="test_edges.tsv")
        
        # Output files
        nodes_output = temp_path / "nodes_output.tsv"
        edges_output = temp_path / "edges_output.tsv"
        
        # Run closurizer without specifying any multivalued fields
        runner = CliRunner()
        result = runner.invoke(main, [
            "--kg", str(archive_file),
            "--closure", str(closure_file),
            "--nodes-output", str(nodes_output),
            "--edges-output", str(edges_output)
            # Don't specify --multivalued-fields at all, use defaults
        ])
        
        assert result.exit_code == 0, f"Command failed with output: {result.output}"
        assert nodes_output.exists()
        assert edges_output.exists()