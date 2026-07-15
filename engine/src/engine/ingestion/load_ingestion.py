"""Simple ingestion module for loading the load data into the database."""

import sqlite3
from pathlib import Path

import pandas as pd
from engine.data_model.load_model import LoadSchema
from engine.database.manage_conn import add_rows


def format_load_data(df: pd.DataFrame) -> pd.DataFrame:
    """Read and format the load data from the Excel file."""

    # Tidy the data : Put the 1h...24h columns into a single column with the hour as a separate column
    df = df.melt(
        id_vars=["Date"],
        value_vars=[f"{i}h" for i in range(1, 25)],
        var_name="Hour",
        value_name="load_MW",
    )

    # Convert the hour and date into a single datetime column
    df["Hour"] = df["Hour"].str.replace("h", "").astype(int)
    df["datetime"] = df["Date"] + pd.to_timedelta(df["Hour"], unit="h")
    df = df.drop(columns=["Date", "Hour"])
    df = df[["datetime", "load_MW"]]
    df = df.drop_duplicates(subset=["datetime", "load_MW"], keep="first")

    # Sort the data by datetime
    df = df.sort_values(by="datetime").reset_index(drop=True)

    return df


def add_load_excel_to_db(file_path: Path, sheet_name: str, db_path: Path) -> None:
    """Add the load excel to the SQLite database."""
    excel_file = pd.read_excel(file_path, sheet_name="Feuil1", engine="openpyxl")
    tidy_load_data = format_load_data(excel_file)
    tidy_load_data = LoadSchema.validate(tidy_load_data)
    with sqlite3.connect(str(db_path)) as con:
        add_rows(con, "load_time_series", tidy_load_data)
