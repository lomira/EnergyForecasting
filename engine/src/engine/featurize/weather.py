"""Weather feature engineering."""

import pandas as pd

import weather_data


def weather_features(
    from_date: pd.Timestamp, to_date: pd.Timestamp
) -> pd.DataFrame:
    lag = 24
    windows = (24, 168)
    weather = weather_data.read(
        from_date - pd.Timedelta(hours=lag + max(windows) - 1), to_date
    )
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
        shifted = weather[column].shift(lag)
        for window in windows:
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
