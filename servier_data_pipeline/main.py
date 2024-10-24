# main.py
# pylint: disable=missing-module-docstring
import polars as pl

from servier_data_pipeline.config import settings
from servier_data_pipeline.processing.preprocess_clinical import (
    clean_clinical_trials_data,
)
from servier_data_pipeline.processing.preprocess_drugs import clean_drugs_data
from servier_data_pipeline.processing.preprocess_pubmed import clean_pubmed_data
from servier_data_pipeline.utils.data_loader import (
    load_csv_from_gcs,
    load_json_from_gcs,
    upload_to_bigquery,
)


def build_data_pipeline():
    """
    Build and execute a data processing pipeline for handling research-related datasets.

    This function performs the following steps:
    1. Defines file paths for the required datasets stored in Google Cloud Storage (GCS),
       using configuration settings such as project ID, dataset name, and bucket name.
    2. Loads raw data from GCS into DataFrames:
       - Drug data from a CSV file
       - PubMed data from a CSV file and a JSON file
       - Clinical trials data from a CSV file
    3. Cleans the loaded data using dedicated processing functions for each dataset.
    4. Uploads the cleaned data to Google BigQuery for further analysis and storage.

    Raises:
        ValueError: If there are issues with loading data from GCS or uploading to BigQuery.
        FileNotFoundError: If the specified files do not exist in the GCS bucket.
        Any other exceptions that may arise during data processing or uploading.

    Returns:
        None
    """
    # Define file paths using configuration settings
    project_id = settings.project_id
    dataset = settings.dataset
    gcs_drugs_csv = f"gs://{settings.bucket_name}/{settings.drugs_csv_path}"
    gcs_pubmed_csv = f"gs://{settings.bucket_name}/{settings.pubmed_csv_path}"
    gcs_pubmed_json = f"gs://{settings.bucket_name}/{settings.pubmed_json_path}"
    gcs_clinical_trials_csv = (
        f"gs://{settings.bucket_name}/{settings.clinical_trials_csv_path}"
    )

    # # Step 1: Load raw data from GCS
    try:
        drugs_df = load_csv_from_gcs(project_id, gcs_drugs_csv)
        pubmed_csv_df = load_csv_from_gcs(project_id, gcs_pubmed_csv)
        pubmed_json_df = load_json_from_gcs(project_id, gcs_pubmed_json)
        clinical_trials_df = load_csv_from_gcs(project_id, gcs_clinical_trials_csv)
    except Exception as e:
        # Catch any exception, log it, and abort the script
        print(f"Error occurred: {e}")
        raise

    # # Step 2: Clean data using dedicated processing modules
    cleaned_drugs_df = clean_drugs_data(drugs_df)
    cleaned_pubmed_df = clean_pubmed_data(pl.concat([pubmed_csv_df, pubmed_json_df]))
    cleaned_clinical_trials_df = clean_clinical_trials_data(clinical_trials_df)

    # # # Step 3: Upload cleaned data to BigQuery
    upload_to_bigquery(cleaned_drugs_df, "drugs", project_id, dataset)
    upload_to_bigquery(cleaned_pubmed_df, "pubmed", project_id, dataset)
    upload_to_bigquery(
        cleaned_clinical_trials_df, "clinical_trials", project_id, dataset
    )


if __name__ == "__main__":
    try:
        build_data_pipeline()
    except Exception as e:
        print(f"Script aborted due to error: {e}")
        exit(1)  # Optionally exit with a non-zero status code
