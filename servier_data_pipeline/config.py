# config/config.py
# pylint: disable=missing-module-docstring
import os

from pydantic_settings import BaseSettings, SettingsConfigDict

# Define the path to the .env file
DOTENV = os.path.join(os.path.dirname(__file__), ".env")


class Settings(BaseSettings):
    """
    Configuration settings for the application using Pydantic.

    This module defines a `Settings` class that loads configuration
    parameters from a .env file. It includes settings related to
    Google Cloud Platform (GCP) configurations and file paths for
    various CSV and JSON files used in the application.

    Attributes:
        project_id (str): The Google Cloud Project ID.
        dataset (str): The name of the dataset in GCP.
        bucket_name (str): The name of the GCP bucket.
        drugs_csv_path (str): The file path for the drugs CSV in GCS.
        pubmed_csv_path (str): The file path for the PubMed CSV in GCS.
        pubmed_json_path (str): The file path for the PubMed JSON in GCS.
        clinical_trials_csv_path (str): The file path for the clinical trials CSV in GCS.
        output_file1 (str): The path for the first output file.
        output_file2 (str): The path for the second output file.

    Usage:
        settings = Settings()
        print(settings.project_id)
    """

    # GCP  configurations
    project_id: str
    dataset: str
    bucket_name: str

    # File paths in GCS
    drugs_csv_path: str
    pubmed_csv_path: str
    pubmed_json_path: str
    clinical_trials_csv_path: str

    # Output files
    output_file1: str
    output_file2: str

    # Configuration for Pydantic Settings
    model_config = SettingsConfigDict(env_file=DOTENV)


# Optionally, you can create a singleton instance of the settings
settings = Settings()
