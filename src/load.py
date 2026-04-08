import pandas as pd
import logging
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_engine():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "bindingdb")
    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "admin123")
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    engine = create_engine(connection_string)
    logger.info(f"Database engine created for {host}:{port}/{name}")
    return engine


def create_schema(engine):
    """
    Creates three normalized tables:
    - targets: unique protein targets
    - compounds: unique small molecules
    - interactions: binding measurements linking targets to compounds
    """
    logger.info("Creating database schema...")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS targets (
                target_id           SERIAL PRIMARY KEY,
                target_name         TEXT NOT NULL,
                organism            TEXT,
                target_chembl_id    TEXT,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS compounds (
                compound_id         SERIAL PRIMARY KEY,
                compound_chembl_id  TEXT,
                compound_name       TEXT,
                ligand_smiles       TEXT,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS interactions (
                interaction_id          SERIAL PRIMARY KEY,
                target_id               INT REFERENCES targets(target_id),
                compound_id             INT REFERENCES compounds(compound_id),
                reactant_set_id         TEXT,
                affinity_type           TEXT,
                affinity_value_nm       FLOAT,
                affinity_units          TEXT,
                pchembl_value           FLOAT,
                affinity_category       TEXT,
                has_pchembl             BOOLEAN,
                assay_chembl_id         TEXT,
                assay_description       TEXT,
                document_id             TEXT,
                data_source             TEXT,
                created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_interactions_target ON interactions(target_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_interactions_affinity ON interactions(affinity_value_nm);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_interactions_category ON interactions(affinity_category);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_targets_name ON targets(target_name);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_compounds_chembl ON compounds(compound_chembl_id);"))
        conn.commit()
    logger.info("Schema created successfully.")


def load_targets(df: pd.DataFrame, engine) -> dict:
    """Loads unique targets, returns name->id map."""
    logger.info("Loading targets...")
    targets_df = df[["target_name", "organism", "target_chembl_id"]].drop_duplicates(
        subset=["target_name"]
    ).dropna(subset=["target_name"]).copy()

    targets_df.to_sql("targets", engine, if_exists="append", index=False, chunksize=500)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT target_id, target_name FROM targets"))
        target_map = {row.target_name: row.target_id for row in result}

    logger.info(f"Loaded {len(targets_df)} unique targets.")
    return target_map


def load_compounds(df: pd.DataFrame, engine) -> dict:
    """Loads unique compounds, returns chembl_id->compound_id map."""
    logger.info("Loading compounds...")
    compounds_df = df[["compound_chembl_id", "compound_name", "ligand_smiles"]].drop_duplicates(
        subset=["compound_chembl_id"]
    ).copy()

    compounds_df.to_sql("compounds", engine, if_exists="append", index=False, chunksize=500)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT compound_id, compound_chembl_id FROM compounds"))
        compound_map = {row.compound_chembl_id: row.compound_id for row in result}

    logger.info(f"Loaded {len(compounds_df)} unique compounds.")
    return compound_map


def load_interactions(df: pd.DataFrame, engine, target_map: dict, compound_map: dict):
    """Loads interactions table linking targets to compounds."""
    logger.info("Loading interactions...")
    interactions_df = df.copy()
    interactions_df["target_id"] = interactions_df["target_name"].map(target_map)
    interactions_df["compound_id"] = interactions_df["compound_chembl_id"].map(compound_map)
    interactions_df = interactions_df.dropna(subset=["target_id", "compound_id"])
    interactions_df["target_id"] = interactions_df["target_id"].astype(int)
    interactions_df["compound_id"] = interactions_df["compound_id"].astype(int)

    cols = [
        "target_id", "compound_id", "reactant_set_id",
        "affinity_type", "primary_affinity_nm", "affinity_units",
        "pchembl_value", "affinity_category", "has_pchembl",
        "assay_chembl_id", "assay_description", "document_id", "data_source"
    ]
    cols = [c for c in cols if c in interactions_df.columns]
    interactions_df = interactions_df[cols]
    interactions_df = interactions_df.rename(columns={"primary_affinity_nm": "affinity_value_nm"})

    interactions_df.to_sql("interactions", engine, if_exists="append", index=False, chunksize=500)
    logger.info(f"Loaded {len(interactions_df)} interaction records.")


def run_loading(df: pd.DataFrame):
    """Master loading function."""
    logger.info("=== LOADING STEP STARTED ===")
    engine = get_engine()
    create_schema(engine)
    target_map = load_targets(df, engine)
    compound_map = load_compounds(df, engine)
    load_interactions(df, engine, target_map, compound_map)
    logger.info("=== LOADING STEP COMPLETE ===")