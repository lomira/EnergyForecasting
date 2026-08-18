"""Weather feature engineering."""

import pandas as pd

import weather_data


def weather_features(
    from_date: pd.Timestamp, to_date: pd.Timestamp
) -> pd.DataFrame:
    weather = weather_data.read(from_date, to_date)
    features: dict[str, pd.Series] = {}
    for column in weather.columns:
        shifted = weather[column].shift(24)
        for window in (24, 168):
            rolling = shifted.rolling(window, min_periods=window)
            features[f"{column}__roll_mean{window}_lag24"] = rolling.mean()
            features[f"{column}__roll_std{window}_lag24"] = rolling.std()
    return pd.concat(
        [weather, pd.DataFrame(features, index=weather.index)], axis=1
    ).bfill().ffill()
