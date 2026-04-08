import pandas as pd
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("data/raw")
RAW_CSV_PATH = RAW_DATA_DIR / "chembl_raw.csv"


def fetch_chembl_activities(max_records: int = 50000) -> pd.DataFrame:
    from chembl_webresource_client.new_client import new_client
    logger.info(f"Fetching up to {max_records} activity records from ChEMBL API...")
    activity = new_client.activity
    activities = activity.filter(
        assay_type="B",
        standard_type__in=["IC50", "Ki", "Kd", "EC50"],
        standard_relation="=",
    ).only([
        "activity_id", "molecule_chembl_id", "target_chembl_id",
        "assay_chembl_id", "standard_type", "standard_value",
        "standard_units", "pchembl_value", "target_pref_name",
        "target_organism", "molecule_pref_name", "canonical_smiles",
        "assay_description", "document_chembl_id", "src_id",
    ])
    records = []
    for i, record in enumerate(activities):
        records.append(record)
        if (i + 1) % 5000 == 0:
            logger.info(f"  Fetched {i + 1} records so far...")
        if i + 1 >= max_records:
            break
    logger.info(f"Fetched {len(records)} total records.")
    return pd.DataFrame(records)


def save_raw_data(df: pd.DataFrame) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_CSV_PATH, index=False)
    logger.info(f"Saved raw data to {RAW_CSV_PATH} ({len(df)} rows)")
    return RAW_CSV_PATH


def load_raw_data(nrows: int = 50000) -> pd.DataFrame:
    if RAW_CSV_PATH.exists():
        logger.info(f"Loading cached raw data from {RAW_CSV_PATH}...")
        df = pd.read_csv(RAW_CSV_PATH, nrows=nrows)
        logger.info(f"Loaded {len(df)} rows from cache.")
        return df
    df = fetch_chembl_activities(max_records=nrows)
    save_raw_data(df)
    return df


def run_extraction(nrows: int = 50000) -> pd.DataFrame:
    logger.info("=== EXTRACTION STEP STARTED ===")
    df = load_raw_data(nrows=nrows)
    logger.info(f"=== EXTRACTION STEP COMPLETE === Shape: {df.shape}")
    return df


if __name__ == "__main__":
    df = run_extraction()
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")