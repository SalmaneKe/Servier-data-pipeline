# preprocessing/preprocess_pubmed.py
import logging

import polars as pl

from servier_data_pipeline.processing.utils import clean_text, parse_dates
from servier_data_pipeline.utils.logging_config import setup_logging
from servier_data_pipeline.utils.models import PubMedEntry

# Setup logging for this module
setup_logging()
logger = logging.getLogger(__name__)


def clean_pubmed_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the pubmed data (both CSV and JSON).

    Args:
        df (pl.DataFrame): Raw data from pubmed.csv or pubmed.json.

    Returns:
        pl.DataFrame: Cleaned data.
    """
    logger.info("Starting cleaning of PubMed data")

    df = clean_text(df, ["title", "journal"])
    df = parse_dates(df)

    # remove row whose both title and journal are null
    df = df.filter(~(df["title"].is_null() & df["journal"].is_null()))

    # rename title column
    df = df.rename({"title": "scientific_title"})

    # added type column
    df = df.with_columns(pl.lit("pubmed").alias("type"))

    # Reorder columns
    df = df.select(["type", "id", "scientific_title", "journal", "date"])
    logger.info("Data cleaning for PubMed data completed. Starting validation...")
    # Validate data with Pydantic
    valid_data = []
    for row in df.iter_rows(named=True):
        try:
            entry = PubMedEntry.model_validate(
                dict(
                    type=row["type"],
                    id=row["id"],
                    scientific_title=row["scientific_title"],
                    journal=row["journal"],
                    date=row["date"],
                )
            )
            valid_data.append(entry.model_dump())
        except Exception as e:
            print(row)
            logger.error(f"Validation error for row {row}: {e}")

    # Convert validated data back to DataFrame
    cleaned_df = pl.DataFrame(valid_data)
    logger.info(
        f"Validation completed. {len(cleaned_df)} records successfully validated."
    )
    return cleaned_df
