from unittest import TestCase

import pandas as pd
from engine.scenario.future_scenario import random_future_scenario


class FutureScenarioTests(TestCase):
    def test_random_future_scenario(self) -> None:
        start = pd.Timestamp("2024-01-01 00:00")
        end = pd.Timestamp("2024-01-01 02:00")

        scenario = random_future_scenario(
            ["temperature", "holidays"], start, end
        ).to_dataframe()

        self.assertEqual(list(scenario.columns), ["temperature", "holidays"])
        self.assertTrue(
            scenario.index.equals(pd.date_range(start, end, freq="h", name="datetime"))
        )
        self.assertTrue(scenario.ge(0).all().all())
        self.assertTrue(scenario.lt(1).all().all())
