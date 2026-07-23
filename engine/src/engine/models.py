"""Django ORM models for the engine's time-series data."""

from functools import cache

from django.db import models


class LoadObservation(models.Model):
    """Hourly electricity load (MW) of the grid."""

    datetime = models.DateTimeField(primary_key=True)
    load_mw = models.FloatField()

    class Meta:
        db_table = "engine_load"
        verbose_name = "load observation"
        verbose_name_plural = "load observations"

    def __str__(self) -> str:
        return f"{self.datetime}: {self.load_mw} MW"


class Holiday(models.Model):
    """Whether a given hour falls on a public holiday in Tunisia."""

    datetime = models.DateTimeField(primary_key=True)
    is_holiday = models.BooleanField()

    class Meta:
        db_table = "engine_holiday"
        verbose_name = "holiday"
        verbose_name_plural = "holidays"

    def __str__(self) -> str:
        return f"{self.datetime}: holiday={self.is_holiday}"


class WeatherObservation(models.Model):
    """Wide weather table: one row per (timestamp, city), one column per metric.

    NAMING CONVENTION : keep every metric column named EXACTLY like the Open-Meteo hourly variable it stores
    Because the ingestion derives the list of API variables to request straight from these metric field names
    """

    datetime = models.DateTimeField()
    city = models.CharField(max_length=64)

    temperature_2m = models.FloatField(null=True, blank=True)
    temperature_2m_previous_day1 = models.FloatField(null=True, blank=True)
    temperature_2m_previous_day2 = models.FloatField(null=True, blank=True)
    relative_humidity_2m = models.FloatField(null=True, blank=True)
    relative_humidity_2m_previous_day1 = models.FloatField(null=True, blank=True)
    relative_humidity_2m_previous_day2 = models.FloatField(null=True, blank=True)
    precipitation = models.FloatField(null=True, blank=True)
    precipitation_previous_day1 = models.FloatField(null=True, blank=True)
    precipitation_previous_day2 = models.FloatField(null=True, blank=True)
    wind_speed_10m = models.FloatField(null=True, blank=True)
    wind_speed_10m_previous_day1 = models.FloatField(null=True, blank=True)
    wind_speed_10m_previous_day2 = models.FloatField(null=True, blank=True)
    shortwave_radiation = models.FloatField(null=True, blank=True)
    shortwave_radiation_previous_day1 = models.FloatField(null=True, blank=True)
    shortwave_radiation_previous_day2 = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "engine_weather"
        verbose_name = "weather observation"
        verbose_name_plural = "weather observations"
        unique_together = [("datetime", "city")]

    def __str__(self) -> str:
        return f"{self.datetime} {self.city}: weather row"


@cache
def weather_api_params() -> list[str]:
    """Open-Meteo hourly variable names, derived from WeatherObservation fields."""
    return [
        f.name
        for f in WeatherObservation._meta.fields
        if isinstance(f, models.FloatField)  # To avoid the PK
    ]
