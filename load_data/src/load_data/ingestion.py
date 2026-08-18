"""Read and validate source load files."""

from pathlib import Path
from typing import cast

import pandas as pd
from loguru import logger

from load_data.store import DB_PATH, _upsert


def format_load_data(data: pd.DataFrame) -> pd.DataFrame:
    """Convert the source's wide daily layout to validated hourly rows."""
    tidy = (
        data.set_index("Date")
        .stack()
        .reset_index()
        .rename(columns={"Date": "datetime", "level_1": "hour", 0: "load_mw"})
    )
    tidy["hour"] = (
        tidy["hour"].astype(str).str.replace("h", "", regex=False).astype(int)
    )
    if not tidy["hour"].between(1, 24).all():
        raise ValueError("Load hours must be between 1 and 24")

    tidy["datetime"] = pd.to_datetime(tidy["datetime"]) + pd.to_timedelta(
        tidy["hour"] - 1, unit="h"
    )
    tidy = (
        tidy[["datetime", "load_mw"]]
        .drop_duplicates(subset="datetime", keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    if tidy["load_mw"].isna().any():
        raise ValueError("Load values must not be missing")
    if tidy["load_mw"].lt(0).any():
        raise ValueError("Load values must be non-negative")
    return tidy


def import_excel(
    file_path: Path,
    sheet_name: str = "Feuil1",
    *,
    db_path: Path = DB_PATH,
) -> int:
    """Import an Excel sheet, updating observations with matching timestamps."""
    tidy = format_load_data(
        pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
    )
    records = [
        (cast(pd.Timestamp, row[0]).isoformat(sep=" "), cast(float, row[1]))
        for row in tidy.itertuples(index=False)
    ]
    _upsert(records, db_path=db_path)
    logger.info(f"Stored {len(records):,.0f} load observations")
    return len(records)
