from datetime import datetime

from django.test import TestCase

from engine.ingestion.get_all_covariates import get_all_covariates
from engine.models import Holiday, WeatherObservation


class CovariatesTests(TestCase):
    def test_get_all_covariates_pivots_weather_rows(self) -> None:
        # Seed the EAV weather table + holidays via the ORM.
        WeatherObservation.objects.bulk_create(
            [
                WeatherObservation(
                    datetime=datetime(2024, 1, 1, 0), city="Alger", metric="temperature_2m", value=10.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 1, 0), city="Constantine", metric="temperature_2m", value=11.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 1, 0), city="Alger", metric="relative_humidity_2m", value=60.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 1, 0), city="Constantine", metric="relative_humidity_2m", value=55.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 2, 0), city="Alger", metric="temperature_2m", value=12.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 2, 0), city="Constantine", metric="temperature_2m", value=13.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 2, 0), city="Alger", metric="relative_humidity_2m", value=62.0
                ),
                WeatherObservation(
                    datetime=datetime(2024, 1, 2, 0), city="Constantine", metric="relative_humidity_2m", value=57.0
                ),
            ]
        )
        Holiday.objects.bulk_create(
            [
                Holiday(datetime=datetime(2024, 1, 1, 0), is_holiday=True),
                Holiday(datetime=datetime(2024, 1, 2, 0), is_holiday=False),
            ]
        )

        df = get_all_covariates(datetime(2024, 1, 1), datetime(2024, 1, 2))

        self.assertEqual(
            df.index.tolist(), [datetime(2024, 1, 1), datetime(2024, 1, 2)]
        )
        self.assertIn("Alger_temperature_2m", df.columns)
        self.assertIn("Constantine_temperature_2m", df.columns)
        self.assertIn("holidays", df.columns)
        self.assertEqual(
            df.loc[datetime(2024, 1, 1), "Alger_temperature_2m"], 10.0
        )
        self.assertEqual(
            df.loc[datetime(2024, 1, 1), "Constantine_relative_humidity_2m"],
            55.0,
        )
        self.assertEqual(df.loc[datetime(2024, 1, 2), "holidays"], False)

