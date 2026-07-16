"""Simple ingestion module for loading the load data into the database."""

import sqlite3
from pathlib import Path

import pandas as pd

from engine.config.config import settings
from engine.data_model.load_model import LoadSchema
from engine.database.manage_conn import add_rows, get_start_end_dates


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

    return tidy


def add_load_excel_to_db(file_path: Path, sheet_name: str, db_path: Path) -> None:
    """Add the load excel to the SQLite database."""
    excel_file = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
    tidy_load_data = format_load_data(excel_file)
    tidy_load_data = LoadSchema.validate(tidy_load_data)
    with sqlite3.connect(str(db_path)) as con:
        add_rows(con, settings.tables.load, tidy_load_data)


def get_load_start_end_dates(db_path: Path) -> tuple:
    with sqlite3.connect(str(db_path)) as con:
        start, end = get_start_end_dates(con, settings.tables.load)
    start, end = pd.to_datetime(start), pd.to_datetime(end)
    return start, end
