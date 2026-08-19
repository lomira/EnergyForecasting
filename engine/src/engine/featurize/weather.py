"""Weather feature engineering."""

import pandas as pd

import weather_data

WEATHER_LAG = 24
WEATHER_WINDOWS = (24, 168)
WEATHER_LOOKBACK = pd.Timedelta(hours=WEATHER_LAG + max(WEATHER_WINDOWS) - 1)


def weather_features(
    from_date: pd.Timestamp, to_date: pd.Timestamp
) -> pd.DataFrame:
    weather = weather_data.read(from_date - WEATHER_LOOKBACK, to_date)
    weights = pd.Series(
        {str(city["name"]): float(city["weight"]) for city in weather_data.CITIES}
    )
    national_averages: dict[str, pd.Series] = {}
    for metric in weather_data.WEATHER_API_PARAMS:
        columns = [f"{city}_{metric}" for city in weights.index]
        national_averages[f"NationalAverage_{metric}"] = (
            weather[columns].astype(float).dot(weights.to_numpy()) / weights.sum()
        )

    features: dict[str, pd.Series] = {}
    for column in weather.columns:
        shifted = weather[column].shift(WEATHER_LAG)
        for window in WEATHER_WINDOWS:
            rolling = shifted.rolling(window, min_periods=window)
            features[f"{column}__roll_mean{window}_lag24"] = rolling.mean()
            features[f"{column}__roll_std{window}_lag24"] = rolling.std()
    city_features = pd.concat(
        [
            weather,
            pd.DataFrame(features, index=weather.index),
        ],
        axis=1,
    )
    return pd.concat(
        [city_features, pd.DataFrame(national_averages, index=weather.index)], axis=1
    ).loc[from_date:to_date]
