import pytest
import pandas as pd
from src.transform import (
    rename_columns,
    clean_numeric_columns,
    drop_rows_missing_critical_fields,
    remove_outliers,
    add_derived_columns,
)


@pytest.fixture
def raw_sample():
    return pd.DataFrame({
        "activity_id":      [1, 2, 3, 4],
        "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4"],
        "target_chembl_id": ["TCHEMBL1", "TCHEMBL1", "TCHEMBL2", "TCHEMBL2"],
        "assay_chembl_id":  ["ACHEMBL1", "ACHEMBL1", "ACHEMBL2", "ACHEMBL2"],
        "standard_type":    ["IC50", "Ki", "IC50", "Kd"],
        "standard_value":   ["5.0", "500.0", None, "500.0"],
        "standard_units":   ["nM", "nM", "nM", "nM"],
        "pchembl_value":    ["8.3", "6.3", None, "5.0"],
        "target_pref_name": ["ProtA", "ProtA", None, "ProtB"],
        "target_organism":  ["Homo sapiens", "Homo sapiens", "Mus musculus", "Homo sapiens"],
        "molecule_pref_name": ["DrugA", "DrugB", "DrugC", "DrugD"],
        "canonical_smiles": ["CCO", "CCC", "CCN", "CCS"],
        "assay_description": ["Binding assay", "Binding assay", "Binding assay", "Binding assay"],
        "document_chembl_id": ["DOC1", "DOC1", "DOC2", "DOC2"],
        "src_id":           [1, 1, 2, 2],
    })


@pytest.fixture
def renamed_sample(raw_sample):
    return rename_columns(raw_sample)


@pytest.fixture
def cleaned_sample(renamed_sample):
    return clean_numeric_columns(renamed_sample)


def test_rename_columns_produces_correct_names(renamed_sample):
    assert "target_name" in renamed_sample.columns
    assert "affinity_value" in renamed_sample.columns
    assert "ligand_smiles" in renamed_sample.columns
    assert "standard_type" not in renamed_sample.columns


def test_clean_numeric_converts_to_float(cleaned_sample):
    assert cleaned_sample["affinity_value"].dtype == float


def test_drop_rows_removes_null_target(cleaned_sample):
    df = drop_rows_missing_critical_fields(cleaned_sample)
    assert df["target_name"].isna().sum() == 0


def test_drop_rows_removes_null_affinity(cleaned_sample):
    df = drop_rows_missing_critical_fields(cleaned_sample)
    assert df["affinity_value"].isna().sum() == 0


def test_remove_outliers_removes_negative():
    df = pd.DataFrame({
        "target_name": ["A", "B"],
        "affinity_value": [-5.0, 10.0],
    })
    result = remove_outliers(df)
    assert (result["affinity_value"] > 0).all()


def test_remove_outliers_removes_extremely_large():
    df = pd.DataFrame({
        "target_name": ["A", "B"],
        "affinity_value": [1e10, 10.0],
    })
    result = remove_outliers(df)
    assert (result["affinity_value"] <= 1e8).all()


def test_add_derived_columns_highly_potent():
    df = pd.DataFrame({
        "affinity_value": [5.0],
        "affinity_type": ["IC50"],
        "pchembl_value": [8.3],
    })
    result = add_derived_columns(df)
    assert result["affinity_category"].iloc[0] == "highly_potent"
    assert result["primary_affinity_nm"].iloc[0] == 5.0


def test_add_derived_columns_weak():
    df = pd.DataFrame({
        "affinity_value": [50000.0],
        "affinity_type": ["Ki"],
        "pchembl_value": [None],
    })
    result = add_derived_columns(df)
    assert result["affinity_category"].iloc[0] == "weak"
