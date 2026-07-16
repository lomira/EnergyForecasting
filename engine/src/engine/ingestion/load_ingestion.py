"""Simple ingestion module for loading the load data Excel file into the database."""

from pathlib import Path

import pandas as pd
from django.db.models import Max as models_max
from django.db.models import Min as models_min

from engine.models import LoadObservation


def format_load_data(df: pd.DataFrame) -> pd.DataFrame:
    """Read and format the load data from the Excel file."""

    tidy = (
        df.set_index("Date")
        .stack()
        .reset_index()
        .rename(columns={"Date": "datetime", "level_1": "Hour", 0: "load_MW"})
    )
    tidy["Hour"] = tidy["Hour"].str.replace("h", "", regex=False).astype(int)
    tidy["datetime"] = tidy["datetime"] + pd.to_timedelta(tidy["Hour"], unit="h")
    tidy = (
        tidy[["datetime", "load_MW"]]
        .drop_duplicates()
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    if not tidy["datetime"].is_monotonic_increasing:
        raise ValueError("Load timestamps must be chronological")
    if tidy["load_MW"].lt(0).any():
        raise ValueError("Load values must be non-negative")

    return tidy


def add_load_excel_to_db(file_path: Path, sheet_name: str, db_path: Path) -> None:
    """Add the load excel to the SQLite database."""
    excel_file = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
    tidy_load_data = format_load_data(excel_file)

    # Upsert into the Django ORM model
    observations = [
        LoadObservation(datetime=row["datetime"], load_mw=row["load_MW"])
        for row in tidy_load_data.to_dict("records")
    ]
    LoadObservation.objects.bulk_create(
        observations,
        update_conflicts=True,
        unique_fields=["datetime"],
        update_fields=["load_mw"],
    )


def get_load_start_end_dates(db_path: Path) -> tuple:
    agg = LoadObservation.objects.aggregate(
        start=models_min("datetime"), end=models_max("datetime")
    )
    return agg["start"], agg["end"]
