import pytest
import pandas as pd
from src.validate import (
    check_no_empty_dataframe,
    check_required_columns_exist,
    check_no_negative_affinities,
    check_primary_affinity_not_all_null,
    check_affinity_categories_valid,
)


@pytest.fixture
def valid_df():
    return pd.DataFrame({
        "reactant_set_id":      [1, 2, 3],
        "target_name":          ["ProtA", "ProtB", "ProtC"],
        "ligand_smiles":        ["CCO", "CCC", "CCN"],
        "primary_affinity_nm":  [10.0, 50.0, 100.0],
        "primary_affinity_type": ["IC50", "Ki", "Kd"],
        "data_source":          [1, 1, 2],
        "affinity_category":    ["highly_potent", "potent", "moderate"],
    })


def test_no_empty_passes(valid_df):
    assert check_no_empty_dataframe(valid_df) is True

def test_no_empty_fails():
    assert check_no_empty_dataframe(pd.DataFrame()) is False

def test_required_columns_passes(valid_df):
    assert check_required_columns_exist(valid_df) is True

def test_required_columns_fails(valid_df):
    assert check_required_columns_exist(valid_df.drop(columns=["target_name"])) is False

def test_no_negative_passes(valid_df):
    assert check_no_negative_affinities(valid_df) is True

def test_no_negative_fails():
    df = pd.DataFrame({"primary_affinity_nm": [-1.0, 10.0]})
    assert check_no_negative_affinities(df) is False

def test_not_all_null_passes(valid_df):
    assert check_primary_affinity_not_all_null(valid_df) is True

def test_not_all_null_fails():
    df = pd.DataFrame({"primary_affinity_nm": [None, None]})
    assert check_primary_affinity_not_all_null(df) is False

def test_affinity_categories_valid_passes(valid_df):
    assert check_affinity_categories_valid(valid_df) is True

def test_affinity_categories_invalid_fails():
    df = pd.DataFrame({"affinity_category": ["highly_potent", "nonsense"]})
    assert check_affinity_categories_valid(df) is False
