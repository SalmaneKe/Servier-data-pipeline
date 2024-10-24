# utils/mdels.py
# pylint: disable=missing-module-docstring
from datetime import date as DateType

from pydantic import BaseModel, Field, field_validator


class Drug(BaseModel):
    """Model for the drugs.csv file."""

    atccode: str = Field(..., description="Unique identifier for the drug")
    drug: str = Field(..., description="Name of the drug")


class PubMedEntry(BaseModel):
    """Model for entries in pubmed.csv and pubmed.json."""

    type: str = Field(..., description="The type of entry (e.g., clinical_trial)")
    id: str = Field(..., description="Unique identifier for the PubMed entry")
    scientific_title: str = Field(..., description="Title of the publication")
    journal: str = Field(..., description="Journal that published the article")
    date: DateType = Field(..., description="Date of publication")

    @classmethod
    @field_validator("date", mode="before")
    def parse_date(cls, value) -> DateType:
        """Parse date from different formats if needed."""
        if isinstance(value, str):
            return DateType.fromisoformat(
                value
            )  # Adjust as necessary for your date formats
        return value


class ClinicalTrialEntry(BaseModel):
    """Model for entries in clinical_trials.csv."""

    type: str = Field(..., description="The type of entry (e.g., clinical_trial)")
    id: str = Field(..., description="Unique identifier for the clinical trial")
    scientific_title: str = Field(
        ..., description="Title of the scientific publication"
    )
    journal: str = Field(..., description="Journal that published the article")
    date: DateType = Field(..., description="Date of publication")

    @classmethod
    @field_validator("date", mode="before")
    def parse_date(cls, value) -> DateType:
        """Parse date from different formats if needed."""
        if isinstance(value, str):
            return DateType.fromisoformat(
                value
            )  # Adjust as necessary for your date formats
        return value
