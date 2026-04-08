import logging
import sys
from src.extract import run_extraction
from src.transform import run_transformation
from src.validate import run_validation
from src.load import run_loading
from src.queries import run_queries

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline(nrows: int = 50000):
    """
    Orchestrates the full pipeline end to end:
    Extract → Transform → Validate → Load → Query

    nrows controls how many rows to process.
    Default is 50,000 which runs in a few minutes.
    Set to None to process the full dataset (much slower).
    """
    logger.info("========================================")
    logger.info("   BINDINGDB PIPELINE STARTING")
    logger.info("========================================")

    # Step 1: Extract
    raw_df = run_extraction(nrows=nrows)

    # Step 2: Transform
    clean_df = run_transformation(raw_df)

    # Step 3: Validate - stop the pipeline if validation fails
    validation_passed = run_validation(clean_df)
    if not validation_passed:
        logger.error("Pipeline halted due to validation failures.")
        sys.exit(1)

    # Step 4: Load into PostgreSQL
    run_loading(clean_df)

    # Step 5: Run analytical queries
    run_queries()

    logger.info("========================================")
    logger.info("   BINDINGDB PIPELINE COMPLETE")
    logger.info("========================================")


if __name__ == "__main__":
    run_pipeline(nrows=500)
