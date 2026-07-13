import importlib
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class SettingsConfigTests(unittest.TestCase):
    def test_weather_env_is_loaded_into_settings(self) -> None:
        import engine.config.config as config

        reloaded = importlib.reload(config)

        self.assertIn("0", reloaded.settings.ville)
        self.assertEqual(reloaded.settings.ville["0"].name, "Alger")
        self.assertEqual(reloaded.settings.ville["0"].region, "Nord")
        self.assertEqual(reloaded.settings.ville["0"].lat, 36.73)
        self.assertEqual(reloaded.settings.ville["0"].lon, 3.08)
        self.assertEqual(reloaded.settings.ville["0"].weight, 2_364_230)
        self.assertEqual(reloaded.settings.cache_meteo, "data/.cache_meteo")

    def test_weather_schema_includes_dynamic_metric_columns(self) -> None:
        from engine.data_model.weather_model import build_weather_schema

        schema = build_weather_schema(
            weather_metrics=["temperature_2m", "apparent_temperature"],
            previous_days=2,
        )

        self.assertEqual(schema.__annotations__["datetime"], datetime)
        self.assertEqual(schema.__annotations__["temperature_2m"], float)
        self.assertEqual(schema.__annotations__["temperature_2m_previous_day1"], float)
        self.assertEqual(
            schema.__annotations__["apparent_temperature_previous_day2"], float
        )


if __name__ == "__main__":
    unittest.main()
