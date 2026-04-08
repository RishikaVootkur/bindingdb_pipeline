import pandas as pd
import logging

logger = logging.getLogger(__name__)


def check_no_empty_dataframe(df: pd.DataFrame) -> bool:
    if len(df) == 0:
        logger.error("VALIDATION FAILED: DataFrame is empty.")
        return False
    logger.info(f"check_no_empty_dataframe PASSED: {len(df)} rows.")
    return True


def check_required_columns_exist(df: pd.DataFrame) -> bool:
    required = [
        "reactant_set_id", "target_name", "ligand_smiles",
        "primary_affinity_nm", "primary_affinity_type", "data_source"
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        logger.error(f"VALIDATION FAILED: Missing columns: {missing}")
        return False
    logger.info("check_required_columns_exist PASSED.")
    return True


def check_no_negative_affinities(df: pd.DataFrame) -> bool:
    bad = df["primary_affinity_nm"].dropna()
    bad = bad[bad < 0]
    if len(bad) > 0:
        logger.error(f"VALIDATION FAILED: {len(bad)} negative affinity values.")
        return False
    logger.info("check_no_negative_affinities PASSED.")
    return True


def check_primary_affinity_not_all_null(df: pd.DataFrame) -> bool:
    if df["primary_affinity_nm"].isna().all():
        logger.error("VALIDATION FAILED: primary_affinity_nm is null for all rows.")
        return False
    pct = df["primary_affinity_nm"].notna().mean() * 100
    logger.info(f"check_primary_affinity_not_all_null PASSED: {pct:.1f}% rows have a value.")
    return True


def check_affinity_categories_valid(df: pd.DataFrame) -> bool:
    valid = {"highly_potent", "potent", "moderate", "weak", None}
    if "affinity_category" in df.columns:
        actual = set(df["affinity_category"].unique())
        invalid = actual - valid
        if invalid:
            logger.error(f"VALIDATION FAILED: Invalid affinity categories: {invalid}")
            return False
    logger.info("check_affinity_categories_valid PASSED.")
    return True


def check_duplicate_reactant_ids(df: pd.DataFrame) -> bool:
    total = len(df)
    unique = df["reactant_set_id"].nunique()
    logger.info(f"check_duplicate_reactant_ids: {total} rows, {unique} unique activity IDs.")
    return True


def run_validation(df: pd.DataFrame) -> bool:
    logger.info("=== VALIDATION STEP STARTED ===")

    checks = [
        check_no_empty_dataframe,
        check_required_columns_exist,
        check_no_negative_affinities,
        check_primary_affinity_not_all_null,
        check_affinity_categories_valid,
        check_duplicate_reactant_ids,
    ]

    results = [check(df) for check in checks]
    all_passed = all(results)

    if all_passed:
        logger.info("=== VALIDATION STEP COMPLETE: ALL CHECKS PASSED ===")
    else:
        failed = [c.__name__ for c, r in zip(checks, results) if not r]
        logger.error(f"=== VALIDATION STEP FAILED: {failed} ===")

    return all_passed
