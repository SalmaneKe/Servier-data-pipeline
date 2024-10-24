# preprocessing/preprocess_drugs.py
import logging

import polars as pl
from pydantic import ValidationError

from servier_data_pipeline.utils.logging_config import setup_logging
from servier_data_pipeline.utils.models import Drug

# Setup logging for this module
setup_logging()
logger = logging.getLogger(__name__)


def clean_drugs_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the drugs.csv data.

    Args:
        df (pl.DataFrame): Raw data from drugs.csv.

    Returns:
        pl.DataFrame: Cleaned data.
    """

    # Validate data with Pydantic
    logger.info("Data cleaning for PubMed data completed. Starting validation...")
    valid_data = []
    for row in df.iter_rows(named=True):
        try:
            drug = Drug.model_validate(dict(atccode=row["atccode"], drug=row["drug"]))
            valid_data.append(drug.model_dump())
        except ValidationError as e:
            logger.error(f"Validation error for row {row.to_dict()}: {e}")

    # Convert validated data back to DataFrame
    cleaned_df = pl.DataFrame(valid_data)
    logger.info(
        f"Validation completed. {len(cleaned_df)} records successfully validated."
    )
    return cleaned_df
