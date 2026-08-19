import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd
from darts import TimeSeries

from engine.scenario.future_scenario import validate_hourly_scenario
from engine.scenario.store import read_hourly_scenario, save_hourly_scenario


class ScenarioStoreTests(TestCase):
    def test_hourly_scenario_round_trip(self) -> None:
        target_index = pd.date_range("2027-01-01", periods=2, freq="h")
        scenario = TimeSeries.from_dataframe(
            pd.DataFrame(
                {"temperature": [10.0, 11.0], "precipitation": [0.0, 2.0]},
                index=target_index,
            )
        )
        references = {
            "temperature": pd.DatetimeIndex(
                ["2020-01-01 00:00", "2019-01-01 01:00"]
            ),
            "precipitation": pd.DatetimeIndex(
                ["2018-01-01 00:00", "2020-01-01 01:00"]
            ),
        }
        self.assertIsNone(validate_hourly_scenario(scenario, references))

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            scenario_id = save_hourly_scenario(scenario, references, db_path=db_path)
            restored = read_hourly_scenario(
                scenario_id, db_path=db_path
            ).to_dataframe()
            with sqlite3.connect(db_path) as connection:
                stored_references = connection.execute(
                    """
                    SELECT reference_datetime FROM hourly_scenario
                    WHERE scenario_id = ? ORDER BY target_datetime, variable_name
                    """,
                    [scenario_id],
                ).fetchall()

        self.assertTrue(restored.index.equals(target_index.rename("datetime")))
        self.assertEqual(restored["temperature"].tolist(), [10.0, 11.0])
        self.assertEqual(len(stored_references), 4)
