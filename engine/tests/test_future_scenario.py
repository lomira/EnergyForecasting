from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from engine.scenarios import (
    create_hourly_scenario,
    random_future_scenario,
)


class FutureScenarioTests(TestCase):
    def test_random_scenario_without_variables_is_none(self) -> None:
        start = pd.Timestamp("2027-01-01 00:00")
        self.assertIsNone(random_future_scenario(None, start, start))
        self.assertIsNone(random_future_scenario([], start, start))

    @patch("engine.scenarios.read_future_covariates")
    def test_hourly_scenario_uses_each_reference_timestamp(
        self, read_covariates
    ) -> None:
        target_start = pd.Timestamp("2027-01-01 00:00")
        target_end = pd.Timestamp("2027-01-01 03:00")
        temperature_references = pd.DatetimeIndex(
            [
                "2026-01-01 00:00",
                "2025-01-01 01:00",
                "2026-01-01 02:00",
                "2025-01-01 03:00",
            ]
        )
        history_index = temperature_references.sort_values()
        read_covariates.return_value = pd.DataFrame(
            {"Alger_temperature_2m": [20.0, 21.0, 10.0, 12.0]},
            index=history_index,
        )

        scenario = create_hourly_scenario(
            target_start,
            target_end,
            {"Alger_temperature_2m": temperature_references},
        ).to_dataframe()

        self.assertEqual(scenario["Alger_temperature_2m"].tolist(), [10, 20, 12, 21])
        self.assertTrue(
            scenario.index.equals(
                pd.date_range(target_start, target_end, freq="h", name="datetime")
            )
        )

    @patch("engine.scenarios.read_future_covariates")
    def test_hourly_scenario_rejects_null_references(self, read_covariates) -> None:
        reference = pd.Timestamp("2020-01-01 00:00")
        read_covariates.return_value = pd.DataFrame(
            {"temperature": [None]}, index=pd.DatetimeIndex([reference])
        )

        with self.assertRaisesRegex(ValueError, "Null reference"):
            create_hourly_scenario(
                pd.Timestamp("2027-01-01 00:00"),
                pd.Timestamp("2027-01-01 00:00"),
                {"temperature": pd.DatetimeIndex([reference])},
            )

    @patch("engine.scenarios.create_hourly_scenario")
    @patch("engine.scenarios.get_date_range")
    @patch("engine.scenarios.np.random.default_rng")
    def test_random_future_scenario(
        self, default_rng, get_date_range, create_scenario
    ) -> None:
        start = pd.Timestamp("2024-01-01 00:00")
        end = pd.Timestamp("2024-01-01 02:00")
        get_date_range.return_value = (
            pd.Timestamp("2020-01-01 00:00"),
            pd.Timestamp("2020-01-10 23:00"),
        )
        default_rng.return_value.choice.return_value = pd.Timestamp("2020-01-02 00:00")
        expected = object()
        create_scenario.return_value = expected

        scenario = random_future_scenario(["temperature", "holidays"], start, end)

        self.assertIs(scenario, expected)
        references = create_scenario.call_args.args[2]
        expected_references = pd.date_range("2020-01-02 00:00", periods=3, freq="h")
        self.assertTrue(references["temperature"].equals(expected_references))
        self.assertTrue(references["holidays"].equals(expected_references))
