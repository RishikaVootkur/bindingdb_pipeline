import logging
import os
import pandas as pd
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
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}")


def query_pipeline_summary(engine) -> dict:
    with engine.connect() as conn:
        targets = conn.execute(text("SELECT COUNT(*) FROM targets")).scalar()
        compounds = conn.execute(text("SELECT COUNT(*) FROM compounds")).scalar()
        interactions = conn.execute(text("SELECT COUNT(*) FROM interactions")).scalar()
    summary = {"total_targets": targets, "total_compounds": compounds, "total_interactions": interactions}
    logger.info(f"Pipeline summary: {summary}")
    return summary


def query_top_targets(engine, limit: int = 10) -> pd.DataFrame:
    sql = """
        SELECT
            t.target_name,
            t.organism,
            COUNT(i.interaction_id)       AS interaction_count,
            AVG(i.affinity_value_nm)      AS avg_affinity_nm,
            MIN(i.affinity_value_nm)      AS best_affinity_nm
        FROM targets t
        JOIN interactions i ON t.target_id = i.target_id
        GROUP BY t.target_name, t.organism
        ORDER BY interaction_count DESC
        LIMIT :limit;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(sql), conn, params={"limit": limit})
    logger.info(f"query_top_targets returned {len(result)} rows.")
    return result


def query_potency_distribution(engine) -> pd.DataFrame:
    sql = """
        SELECT
            affinity_category,
            COUNT(*)                      AS count,
            AVG(affinity_value_nm)        AS avg_nm,
            MIN(affinity_value_nm)        AS min_nm
        FROM interactions
        WHERE affinity_value_nm IS NOT NULL
        GROUP BY affinity_category
        ORDER BY min_nm;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(sql), conn)
    logger.info(f"query_potency_distribution returned {len(result)} rows.")
    return result


def query_highly_potent_compounds(engine, threshold_nm: float = 10.0) -> pd.DataFrame:
    sql = """
        SELECT
            c.compound_chembl_id,
            c.compound_name,
            t.target_name,
            i.affinity_type,
            i.affinity_value_nm,
            i.pchembl_value,
            i.affinity_category
        FROM interactions i
        JOIN targets t ON i.target_id = t.target_id
        JOIN compounds c ON i.compound_id = c.compound_id
        WHERE i.affinity_value_nm <= :threshold
        ORDER BY i.affinity_value_nm ASC
        LIMIT 20;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(sql), conn, params={"threshold": threshold_nm})
    logger.info(f"query_highly_potent_compounds: {len(result)} results.")
    return result


def query_affinity_type_breakdown(engine) -> pd.DataFrame:
    sql = """
        SELECT
            affinity_type,
            COUNT(*)                      AS count,
            AVG(affinity_value_nm)        AS mean_nm,
            PERCENTILE_CONT(0.5)
                WITHIN GROUP (ORDER BY affinity_value_nm) AS median_nm
        FROM interactions
        WHERE affinity_value_nm IS NOT NULL
        GROUP BY affinity_type
        ORDER BY count DESC;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(sql), conn)
    logger.info(f"query_affinity_type_breakdown returned {len(result)} rows.")
    return result


def run_queries():
    logger.info("=== QUERIES STEP STARTED ===")
    engine = get_engine()

    print("\n--- Pipeline Summary ---")
    summary = query_pipeline_summary(engine)
    for k, v in summary.items():
        print(f"  {k}: {v:,}")

    print("\n--- Top 10 Targets by Interaction Count ---")
    print(query_top_targets(engine).to_string(index=False))

    print("\n--- Potency Distribution ---")
    print(query_potency_distribution(engine).to_string(index=False))

    print("\n--- Highly Potent Compounds (<=10 nM) ---")
    potent = query_highly_potent_compounds(engine)
    print(f"  Found {len(potent)} results.")
    print(potent.to_string(index=False))

    print("\n--- Affinity Type Breakdown ---")
    print(query_affinity_type_breakdown(engine).to_string(index=False))

    logger.info("=== QUERIES STEP COMPLETE ===")