# utils/data_loader.py
# pylint: disable=missing-module-docstring
import logging
import os
from typing import Optional, Union

import gcsfs
import polars as pl
import yaml
from google.auth.credentials import Credentials
from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError

# Define a type alias for credentials
CredentialsType = Optional[Union[str, dict, Credentials]]


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_gcp_credentials():
    """
    Retrieve the path to the credentials file from the environment variable
    """

    credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not credentials or not os.path.exists(credentials):
        return ""
    return credentials


def load_gcsfuse(
    project_id: str,
) -> Optional[gcsfs.GCSFileSystem]:
    """
    Load the GCS file system object using GCSFuse.

    Args:
        project_id (str): The Google Cloud project ID.
        credentials (CredentialsType):
            Credentials for accessing GCS. It can be a path to a service account JSON file,
            a dictionary with credentials, or a Google OAuth2 credentials object.

    Returns:
        Optional[gcsfs.GCSFileSystem]: The file system object if initialization is successful,
        otherwise None if an error occurs.
    """

    credentials = get_gcp_credentials()

    try:
        fs = gcsfs.GCSFileSystem(project=project_id, token=credentials)
    except Exception as e:  # Replace with a more specific error if available
        logger.error(f"Error loading GCSFileSystem: {e}")
        raise RuntimeError(f"Failed to initialize GCSFileSystem: {e}")
    else:
        return fs


def load_csv_from_gcs(
    project_id: str,
    gcs_path: str,
) -> Optional[pl.DataFrame]:
    """
    Load a CSV file from GCS and return a DataFrame.

    Args:
        project_id (str): The Google Cloud project ID.
        gcs_path (str): The path to the CSV file in Google Cloud Storage.
        credentials (CredentialsType):
            Credentials for accessing GCS. It can be a path to a service account JSON file,
            a dictionary with credentials, or a Google OAuth2 credentials object.

    Returns:
        Optional[pl.DataFrame]: A DataFrame containing the CSV data if successful,
        otherwise None if an error occurs.

    Raises:
        gcsfs.core.GCSError: If there is an error initializing the GCS file system or accessing the file.
        FileNotFoundError: If the CSV file cannot be found at the specified GCS path.
    """

    fs = load_gcsfuse(project_id)

    try:
        # Open the file and read the CSV
        with fs.open(gcs_path, "rb") as file:
            df = pl.read_csv(file)
    except FileNotFoundError as e:
        logger.error(f"File not found at GCS path '{gcs_path}': {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV file from GCS: {e}")
        return None
    else:
        return df


def load_json_from_gcs(project_id: str, gcs_path: str) -> Optional[pl.DataFrame]:
    """
    Load a JSON file from GCS and return a DataFrame.

    Args:
        project_id (str): The Google Cloud project ID.
        gcs_path (str): The path to the JSON file in Google Cloud Storage.
        credentials (CredentialsType):
            Credentials for accessing GCS. It can be a path to a service account JSON file,
            a dictionary with credentials, or a Google OAuth2 credentials object.

    Returns:
        Optional[pl.DataFrame]: A DataFrame containing the JSON data if successful,
        otherwise None if an error occurs.

    Raises:
        gcsfs.core.GCSError: If there is an error initializing the GCS file system.
        FileNotFoundError: If the JSON file cannot be found at the specified GCS path.
        yaml.YAMLError: If there is an error parsing the JSON content.
    """

    fs = load_gcsfuse(project_id)

    try:
        # Open the JSON file and load it into a DataFrame
        with fs.open(gcs_path, "r") as file:
            data = yaml.safe_load(file)
            df = pl.DataFrame(data)
    except FileNotFoundError as e:
        logger.error(f"File not found at GCS path '{gcs_path}': {e}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing JSON file: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading JSON file from GCS: {e}")
        return None
    else:
        return df


def upload_to_bigquery(
    df: pl.DataFrame, table_name: str, project_id: str, dataset: str
) -> Optional[bool]:
    """
    Upload a DataFrame to a BigQuery table.

    Args:
        df (polars.DataFrame): The DataFrame to upload.
        table_name (str): The name of the target BigQuery table.
        project_id (str): The Google Cloud project ID.
        dataset (str): The BigQuery dataset name.

    Returns:
        Optional[bool]: True if the upload is successful, otherwise None if an error occurs.

    Raises:
        GoogleCloudError: If there is an error during the upload process.
    """
    try:
        # Initialize BigQuery client
        print(project_id)
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{table_name}"

        # Start the upload job
        job = client.load_table_from_dataframe(df.to_pandas(), table_id)
        job.result()  # Wait for the job to complete
    except GoogleCloudError as e:
        logger.error(f"Error uploading data to BigQuery: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during BigQuery upload: {e}")
        return None
    else:
        logger.info(f"Loaded data to {table_id}")
        return True


def save_to_gcs(data: Union[str, bytes], output_path: str) -> Optional[bool]:
    """
    Save JSON data to Google Cloud Storage (GCS).

    Args:
        data (Union[str, bytes]): The JSON data to save. It can be a string or bytes.
        output_path (str): The GCS path where the data will be saved.
                           It should be in the format 'gs://bucket_name/path/to/file.json'.

    Returns:
        Optional[bool]: True if the data is successfully saved, otherwise None if an error occurs.

    Raises:
        GoogleCloudError: If there is an error during the upload process.
    """
    try:
        # Initialize the Google Cloud Storage client
        storage_client = storage.Client()
        bucket_name = output_path.split("/")[2]
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object and upload data
        blob = bucket.blob("/".join(output_path.split("/")[3:]))
        blob.upload_from_string(data, content_type="application/json")
    except GoogleCloudError as e:
        logger.error(f"Error uploading data to GCS: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during GCS upload: {e}")
        return None
    else:
        logger.info(f"Saved data to {output_path}")
        return True
