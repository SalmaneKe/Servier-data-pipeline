# preprocessing/utils.py
from typing import List

import polars as pl


def parse_dates(df: pl.DataFrame, date_formats: List[str] = None) -> pl.DataFrame:
    """
    Parses date strings in a DataFrame using specified date formats and
    coalesces them into a single date column.

    Args:
        df (pl.DataFrame): The DataFrame containing a column of date strings.
        date_formats (List[str], optional): A list of date format strings to try for parsing.
                                             Defaults to ["%F", "%-d %B %Y", "%d/%m/%Y"].

    Returns:
        pl.DataFrame: A DataFrame with the original data and an additional 'date' column
                      containing parsed dates.
    """
    if date_formats is None:
        date_formats = ["%F", "%-d %B %Y", "%d/%m/%Y"]

    # Parse dates in different formats and coalesce them into a single column
    return df.with_columns(
        pl.coalesce(
            [df["date"].str.to_date(fmt, strict=False) for fmt in date_formats]
        ).alias("date")
    )


def remove_escape_sequences(series: pl.Series, pattern: str) -> pl.Series:
    """
    Removes escape sequences from a Polars Series using a specified regular expression pattern.

    Args:
        series (pl.Series): The input Polars Series from which escape sequences will be removed.
        pattern (str): The regular expression pattern used to identify escape sequences.

    Returns:
        pl.Series: A new Polars Series with escape sequences removed.
    """
    return series.str.replace(pattern, "")


def clean_text(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    """
    Cleans specified columns in a DataFrame by removing escape sequences.

    Args:
        df (pl.DataFrame): The DataFrame containing the relevant columns to clean.
        columns (List[str]): A list of column names in the DataFrame from which to remove escape sequences.

    Returns:
        pl.DataFrame: A DataFrame with the specified columns cleaned of escape sequences.
    """
    # Define the regular expression pattern
    regex_pattern = r"(\\x[a-f0-9]+)+"

    # Apply the function to the relevant columns
    for column in columns:
        df = df.with_columns(
            remove_escape_sequences(df[column], regex_pattern).alias(column)
        )

    return df
