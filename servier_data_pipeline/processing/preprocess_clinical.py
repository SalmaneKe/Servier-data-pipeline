# preprocessing/preprocess_clinical.py
# pylint: disable=missing-module-docstring
import logging

import polars as pl

from servier_data_pipeline.processing.utils import clean_text, parse_dates
from servier_data_pipeline.utils.logging_config import setup_logging
from servier_data_pipeline.utils.models import ClinicalTrialEntry

setup_logging()
logger = logging.getLogger(__name__)


def clean_clinical_trials_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the df.csv data.

    Args:
        df (pl.DataFrame): Raw data from df.csv.

    Returns:
        pl.DataFrame: Cleaned data.
    """

    df = parse_dates(df)
    df = clean_text(df, ["scientific_title", "journal"])

    # Create a new column type
    df = df.with_columns([pl.lit("clinical_trial").alias("type")])

    # Set column order
    df = df.select(["type", "id", "scientific_title", "journal", "date"])

    # Validate data with Pydantic
    logger.info("Data cleaning for PubMed data completed. Starting validation...")
    valid_data = []
    for row in df.iter_rows(named=True):
        try:
            trial = ClinicalTrialEntry.model_validate(
                # pylint: disable=line-too-long
                dict(
                    type=row["type"],
                    id=row["id"],
                    scientific_title=row["scientific_title"],
                    journal=row["journal"],
                    date=row["date"],
                )
            )
            valid_data.append(trial.model_dump())
        except Exception as e:
            print(f"Validation error for row {row}: {e}")

    # Convert validated data back to DataFrame
    cleaned_df = pl.DataFrame(valid_data)
    logger.info(
        f"Validation completed. {len(cleaned_df)} records successfully validated."
    )
    return cleaned_df
