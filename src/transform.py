import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

PROCESSED_DATA_DIR = Path("data/processed")


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Renaming columns...")
    rename_map = {
        "activity_id":          "reactant_set_id",
        "molecule_chembl_id":   "compound_chembl_id",
        "target_chembl_id":     "target_chembl_id",
        "assay_chembl_id":      "assay_chembl_id",
        "standard_type":        "affinity_type",
        "standard_value":       "affinity_value",
        "standard_units":       "affinity_units",
        "pchembl_value":        "pchembl_value",
        "target_pref_name":     "target_name",
        "target_organism":      "organism",
        "molecule_pref_name":   "compound_name",
        "canonical_smiles":     "ligand_smiles",
        "assay_description":    "assay_description",
        "document_chembl_id":   "document_id",
        "src_id":               "data_source",
    }
    df = df.rename(columns=rename_map)
    logger.info(f"Columns after rename: {list(df.columns)}")
    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning numeric columns...")
    for col in ["affinity_value", "pchembl_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info("Numeric cleaning complete.")
    return df


def drop_rows_missing_critical_fields(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Rows before dropping missing critical fields: {len(df)}")
    df = df.dropna(subset=["target_name", "affinity_value"])
    logger.info(f"Rows after dropping missing critical fields: {len(df)}")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Removing outliers...")
    initial_len = len(df)
    df = df[df["affinity_value"] > 0]
    df = df[df["affinity_value"] <= 1e8]
    logger.info(f"Removed {initial_len - len(df)} outlier rows. Remaining: {len(df)}")
    return df


def clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning string columns...")
    string_cols = ["target_name", "organism", "ligand_smiles", "compound_name", "assay_description"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", None)
            df[col] = df[col].replace("", None)
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding derived columns...")
    df["primary_affinity_nm"] = df["affinity_value"]
    df["primary_affinity_type"] = df["affinity_type"]

    def categorize(val):
        if pd.isna(val):
            return None
        if val <= 10:
            return "highly_potent"
        elif val <= 100:
            return "potent"
        elif val <= 1000:
            return "moderate"
        else:
            return "weak"

    df["affinity_category"] = df["primary_affinity_nm"].apply(categorize)
    df["has_pchembl"] = df["pchembl_value"].notna()
    return df


def save_processed_data(df: pd.DataFrame) -> Path:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DATA_DIR / "chembl_clean.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved processed data to {output_path} ({len(df)} rows)")
    return output_path


def run_transformation(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("=== TRANSFORMATION STEP STARTED ===")
    df = rename_columns(df)
    df = clean_numeric_columns(df)
    df = drop_rows_missing_critical_fields(df)
    df = remove_outliers(df)
    df = clean_string_columns(df)
    df = add_derived_columns(df)
    save_processed_data(df)
    logger.info(f"=== TRANSFORMATION STEP COMPLETE === Final shape: {df.shape}")
    return df
