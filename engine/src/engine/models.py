"""Django ORM models for the engine's time-series data."""

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
    """A single weather metric value for one city at one timestamp (EAV)."""

    # TODO Fix this after because it takes way too much times now

    datetime = models.DateTimeField()
    city = models.CharField(max_length=64)
    metric = models.CharField(max_length=64)
    value = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "engine_weather"
        verbose_name = "weather observation"
        verbose_name_plural = "weather observations"
        unique_together = [("datetime", "city", "metric")]
        indexes = [
            models.Index(fields=["datetime", "city"]),
            models.Index(fields=["metric"]),
        ]

    def __str__(self) -> str:
        return f"{self.datetime} {self.city} {self.metric}={self.value}"
