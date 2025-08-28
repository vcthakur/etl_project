import pandas as pd
import pytest
from extractors.csv_extractor import CSVExtractor
from transformations.basic_cleaning import BasicCleaning
from loaders.csv_loader import CSVLoader

def test_csv_extractor(tmp_path):
    # Create a small temporary CSV
    test_file = tmp_path / "test.csv"
    df_in = pd.DataFrame({
        "id": [1, 2],
        "Name": ["Alice", "Bob"]
    })
    df_in.to_csv(test_file, index=False)

    # Run extractor
    extractor = CSVExtractor({"path": str(test_file)})
    df_out = extractor.extract()

    # Generic checks
    assert not df_out.empty
    assert set(["id", "Name"]).issubset(df_out.columns)

def test_basic_cleaning():
    df = pd.DataFrame({
        "id": [1, None, 3],
        "Name": [" Alice ", "Bob", "Charlie"]
    })
    transformer = BasicCleaning({})
    result = transformer.transform(df)

    # Check required column
    assert "id" in result.columns

    # Check trimming applied
    assert result["Name"].iloc[0] == "Alice"

    # Check nulls dropped (generic, not hardcoded row count)
    assert result["id"].isnull().sum() == 0

def test_basic_cleaning_empty_dataframe():
    df = pd.DataFrame({})
    transformer = BasicCleaning({})
    result = transformer.transform(df)
    assert result.empty

def test_basic_cleaning_no_nulls():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "Name": ["Alice", "Bob", "Charlie"]
    })
    transformer = BasicCleaning({})
    result = transformer.transform(df)

    # No rows dropped
    assert result.shape[0] == df.shape[0]
    # Names remain unchanged
    assert all(result["Name"].str.strip() == df["Name"])

def test_basic_cleaning_multiple_nulls():
    df = pd.DataFrame({
        "id": [1, None, 3, None],
        "Name": [" Alice ", "Bob", "Charlie", "David"]
    })
    transformer = BasicCleaning({})
    result = transformer.transform(df)

    # No null ids should remain
    assert result["id"].isnull().sum() == 0
    # Whitespace should be stripped
    assert all(result["Name"] == result["Name"].str.strip())
