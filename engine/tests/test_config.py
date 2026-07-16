"""Tests for the engine's Django settings and migration output."""

from django.conf import settings
from django.test import TestCase


class SettingsTests(TestCase):
    def test_paths_resolve_under_workspace_root(self) -> None:
        self.assertTrue(str(settings.ENGINE_DB_ROOT).endswith("db"))
        self.assertTrue(str(settings.ENGINE_RAW_EXCEL_ROOT).endswith("raw/excel"))
        self.assertTrue(str(settings.ENGINE_DATA_ROOT).endswith("data"))

    def test_villes_are_typed(self) -> None:
        self.assertIn("0", settings.ENGINE_VILLES)
        alger = settings.ENGINE_VILLES["0"]
        self.assertEqual(alger["name"], "Alger")
        self.assertEqual(alger["region"], "Nord")
        self.assertEqual(alger["lat"], 36.73)
        self.assertEqual(alger["lon"], 3.08)
        self.assertEqual(alger["weight"], 2364230)
        self.assertEqual(set(settings.ENGINE_VILLES), {"0", "1", "2"})
        for ville in settings.ENGINE_VILLES.values():
            self.assertIsInstance(ville["lat"], float)
            self.assertIsInstance(ville["lon"], float)

    def test_weather_metrics_and_cache(self) -> None:
        self.assertEqual(
            settings.ENGINE_WEATHER_METRICS,
            [
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "wind_speed_10m",
                "shortwave_radiation",
            ],
        )
        self.assertEqual(settings.ENGINE_WEATHER_PREV_DAYS, 2)
        self.assertTrue(settings.ENGINE_CACHE_METEO)


class MigrationTests(TestCase):
    def test_engine_tables_exist(self) -> None:
        from django.db import connection

        with connection.cursor() as cursor:
            tables = {
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertIn("engine_load", tables)
        self.assertIn("engine_holiday", tables)
        self.assertIn("engine_weather", tables)

    def test_weather_has_unique_constraint(self) -> None:
        from django.db import connection

        with connection.cursor() as cursor:
            table_sql = cursor.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='engine_weather'"
            ).fetchone()[0]
            indexes = cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' "
                "AND tbl_name='engine_weather' AND sql IS NOT NULL"
            ).fetchall()
        # Metric columns are present (named exactly like the Open-Meteo params),
        # e.g. temperature_2m and the previous-day variants like
        # wind_speed_10m_previous_day2. The city is a separate column.
        self.assertIn("temperature_2m", table_sql)
        self.assertIn("wind_speed_10m_previous_day2", table_sql)
        self.assertIn("city", table_sql)
        # The (datetime, city) pair is the unique constraint.
        self.assertTrue(any("datetime" in ix[0] and "city" in ix[0] for ix in indexes))
