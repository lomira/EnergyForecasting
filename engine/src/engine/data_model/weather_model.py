from datetime import datetime

import pandera.pandas as pa


def build_weather_column_names(
    weather_metrics: list[str] | None = None,
    previous_days: int | None = None,
) -> list[str]:
    metrics = weather_metrics or []
    suffixes = [""]
    if (previous_days or 0) > 0:
        suffixes += [f"_previous_day{i + 1}" for i in range(previous_days or 0)]

    return [f"{metric}{suffix}" for metric in metrics for suffix in suffixes]


def build_weather_schema(
    weather_metrics: list[str] | None = None,
    previous_days: int | None = None,
):
    """Build a dynamic Pandera-style schema for the weather columns."""
    annotations = {"datetime": datetime, "city": str}

    for column_name in build_weather_column_names(weather_metrics, previous_days):
        annotations[column_name] = float

    return type(
        "WeatherSchema",
        (pa.DataFrameModel,),
        {"__annotations__": annotations, "__module__": __name__},
    )
