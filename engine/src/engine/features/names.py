"""Canonical names of columns in the internal database."""

from enum import StrEnum


class Feature(StrEnum):
    ALGER_PRECIPITATION = "Alger_precipitation"
    CONSTANTINE_PRECIPITATION = "Constantine_precipitation"
    DJELFA_PRECIPITATION = "Djelfa_precipitation"
    ALGER_RELATIVE_HUMIDITY_2M = "Alger_relative_humidity_2m"
    CONSTANTINE_RELATIVE_HUMIDITY_2M = "Constantine_relative_humidity_2m"
    DJELFA_RELATIVE_HUMIDITY_2M = "Djelfa_relative_humidity_2m"
    ALGER_SHORTWAVE_RADIATION = "Alger_shortwave_radiation"
    CONSTANTINE_SHORTWAVE_RADIATION = "Constantine_shortwave_radiation"
    DJELFA_SHORTWAVE_RADIATION = "Djelfa_shortwave_radiation"
    ALGER_TEMPERATURE_2M = "Alger_temperature_2m"
    CONSTANTINE_TEMPERATURE_2M = "Constantine_temperature_2m"
    DJELFA_TEMPERATURE_2M = "Djelfa_temperature_2m"
    ALGER_WIND_SPEED_10M = "Alger_wind_speed_10m"
    CONSTANTINE_WIND_SPEED_10M = "Constantine_wind_speed_10m"
    DJELFA_WIND_SPEED_10M = "Djelfa_wind_speed_10m"

    ALGER_PRECIPITATION_ROLL_MEAN24_LAG24 = "Alger_precipitation__roll_mean24_lag24"
    ALGER_PRECIPITATION_ROLL_STD24_LAG24 = "Alger_precipitation__roll_std24_lag24"
    ALGER_PRECIPITATION_ROLL_MEAN168_LAG24 = "Alger_precipitation__roll_mean168_lag24"
    ALGER_PRECIPITATION_ROLL_STD168_LAG24 = "Alger_precipitation__roll_std168_lag24"
    CONSTANTINE_PRECIPITATION_ROLL_MEAN24_LAG24 = (
        "Constantine_precipitation__roll_mean24_lag24"
    )
    CONSTANTINE_PRECIPITATION_ROLL_STD24_LAG24 = (
        "Constantine_precipitation__roll_std24_lag24"
    )
    CONSTANTINE_PRECIPITATION_ROLL_MEAN168_LAG24 = (
        "Constantine_precipitation__roll_mean168_lag24"
    )
    CONSTANTINE_PRECIPITATION_ROLL_STD168_LAG24 = (
        "Constantine_precipitation__roll_std168_lag24"
    )
    DJELFA_PRECIPITATION_ROLL_MEAN24_LAG24 = "Djelfa_precipitation__roll_mean24_lag24"
    DJELFA_PRECIPITATION_ROLL_STD24_LAG24 = "Djelfa_precipitation__roll_std24_lag24"
    DJELFA_PRECIPITATION_ROLL_MEAN168_LAG24 = "Djelfa_precipitation__roll_mean168_lag24"
    DJELFA_PRECIPITATION_ROLL_STD168_LAG24 = "Djelfa_precipitation__roll_std168_lag24"

    ALGER_RELATIVE_HUMIDITY_2M_ROLL_MEAN24_LAG24 = (
        "Alger_relative_humidity_2m__roll_mean24_lag24"
    )
    ALGER_RELATIVE_HUMIDITY_2M_ROLL_STD24_LAG24 = (
        "Alger_relative_humidity_2m__roll_std24_lag24"
    )
    ALGER_RELATIVE_HUMIDITY_2M_ROLL_MEAN168_LAG24 = (
        "Alger_relative_humidity_2m__roll_mean168_lag24"
    )
    ALGER_RELATIVE_HUMIDITY_2M_ROLL_STD168_LAG24 = (
        "Alger_relative_humidity_2m__roll_std168_lag24"
    )
    CONSTANTINE_RELATIVE_HUMIDITY_2M_ROLL_MEAN24_LAG24 = (
        "Constantine_relative_humidity_2m__roll_mean24_lag24"
    )
    CONSTANTINE_RELATIVE_HUMIDITY_2M_ROLL_STD24_LAG24 = (
        "Constantine_relative_humidity_2m__roll_std24_lag24"
    )
    CONSTANTINE_RELATIVE_HUMIDITY_2M_ROLL_MEAN168_LAG24 = (
        "Constantine_relative_humidity_2m__roll_mean168_lag24"
    )
    CONSTANTINE_RELATIVE_HUMIDITY_2M_ROLL_STD168_LAG24 = (
        "Constantine_relative_humidity_2m__roll_std168_lag24"
    )
    DJELFA_RELATIVE_HUMIDITY_2M_ROLL_MEAN24_LAG24 = (
        "Djelfa_relative_humidity_2m__roll_mean24_lag24"
    )
    DJELFA_RELATIVE_HUMIDITY_2M_ROLL_STD24_LAG24 = (
        "Djelfa_relative_humidity_2m__roll_std24_lag24"
    )
    DJELFA_RELATIVE_HUMIDITY_2M_ROLL_MEAN168_LAG24 = (
        "Djelfa_relative_humidity_2m__roll_mean168_lag24"
    )
    DJELFA_RELATIVE_HUMIDITY_2M_ROLL_STD168_LAG24 = (
        "Djelfa_relative_humidity_2m__roll_std168_lag24"
    )

    ALGER_SHORTWAVE_RADIATION_ROLL_MEAN24_LAG24 = (
        "Alger_shortwave_radiation__roll_mean24_lag24"
    )
    ALGER_SHORTWAVE_RADIATION_ROLL_STD24_LAG24 = (
        "Alger_shortwave_radiation__roll_std24_lag24"
    )
    ALGER_SHORTWAVE_RADIATION_ROLL_MEAN168_LAG24 = (
        "Alger_shortwave_radiation__roll_mean168_lag24"
    )
    ALGER_SHORTWAVE_RADIATION_ROLL_STD168_LAG24 = (
        "Alger_shortwave_radiation__roll_std168_lag24"
    )
    CONSTANTINE_SHORTWAVE_RADIATION_ROLL_MEAN24_LAG24 = (
        "Constantine_shortwave_radiation__roll_mean24_lag24"
    )
    CONSTANTINE_SHORTWAVE_RADIATION_ROLL_STD24_LAG24 = (
        "Constantine_shortwave_radiation__roll_std24_lag24"
    )
    CONSTANTINE_SHORTWAVE_RADIATION_ROLL_MEAN168_LAG24 = (
        "Constantine_shortwave_radiation__roll_mean168_lag24"
    )
    CONSTANTINE_SHORTWAVE_RADIATION_ROLL_STD168_LAG24 = (
        "Constantine_shortwave_radiation__roll_std168_lag24"
    )
    DJELFA_SHORTWAVE_RADIATION_ROLL_MEAN24_LAG24 = (
        "Djelfa_shortwave_radiation__roll_mean24_lag24"
    )
    DJELFA_SHORTWAVE_RADIATION_ROLL_STD24_LAG24 = (
        "Djelfa_shortwave_radiation__roll_std24_lag24"
    )
    DJELFA_SHORTWAVE_RADIATION_ROLL_MEAN168_LAG24 = (
        "Djelfa_shortwave_radiation__roll_mean168_lag24"
    )
    DJELFA_SHORTWAVE_RADIATION_ROLL_STD168_LAG24 = (
        "Djelfa_shortwave_radiation__roll_std168_lag24"
    )

    ALGER_TEMPERATURE_2M_ROLL_MEAN24_LAG24 = "Alger_temperature_2m__roll_mean24_lag24"
    ALGER_TEMPERATURE_2M_ROLL_STD24_LAG24 = "Alger_temperature_2m__roll_std24_lag24"
    ALGER_TEMPERATURE_2M_ROLL_MEAN168_LAG24 = "Alger_temperature_2m__roll_mean168_lag24"
    ALGER_TEMPERATURE_2M_ROLL_STD168_LAG24 = "Alger_temperature_2m__roll_std168_lag24"
    CONSTANTINE_TEMPERATURE_2M_ROLL_MEAN24_LAG24 = (
        "Constantine_temperature_2m__roll_mean24_lag24"
    )
    CONSTANTINE_TEMPERATURE_2M_ROLL_STD24_LAG24 = (
        "Constantine_temperature_2m__roll_std24_lag24"
    )
    CONSTANTINE_TEMPERATURE_2M_ROLL_MEAN168_LAG24 = (
        "Constantine_temperature_2m__roll_mean168_lag24"
    )
    CONSTANTINE_TEMPERATURE_2M_ROLL_STD168_LAG24 = (
        "Constantine_temperature_2m__roll_std168_lag24"
    )
    DJELFA_TEMPERATURE_2M_ROLL_MEAN24_LAG24 = "Djelfa_temperature_2m__roll_mean24_lag24"
    DJELFA_TEMPERATURE_2M_ROLL_STD24_LAG24 = "Djelfa_temperature_2m__roll_std24_lag24"
    DJELFA_TEMPERATURE_2M_ROLL_MEAN168_LAG24 = (
        "Djelfa_temperature_2m__roll_mean168_lag24"
    )
    DJELFA_TEMPERATURE_2M_ROLL_STD168_LAG24 = "Djelfa_temperature_2m__roll_std168_lag24"

    ALGER_WIND_SPEED_10M_ROLL_MEAN24_LAG24 = "Alger_wind_speed_10m__roll_mean24_lag24"
    ALGER_WIND_SPEED_10M_ROLL_STD24_LAG24 = "Alger_wind_speed_10m__roll_std24_lag24"
    ALGER_WIND_SPEED_10M_ROLL_MEAN168_LAG24 = "Alger_wind_speed_10m__roll_mean168_lag24"
    ALGER_WIND_SPEED_10M_ROLL_STD168_LAG24 = "Alger_wind_speed_10m__roll_std168_lag24"
    CONSTANTINE_WIND_SPEED_10M_ROLL_MEAN24_LAG24 = (
        "Constantine_wind_speed_10m__roll_mean24_lag24"
    )
    CONSTANTINE_WIND_SPEED_10M_ROLL_STD24_LAG24 = (
        "Constantine_wind_speed_10m__roll_std24_lag24"
    )
    CONSTANTINE_WIND_SPEED_10M_ROLL_MEAN168_LAG24 = (
        "Constantine_wind_speed_10m__roll_mean168_lag24"
    )
    CONSTANTINE_WIND_SPEED_10M_ROLL_STD168_LAG24 = (
        "Constantine_wind_speed_10m__roll_std168_lag24"
    )
    DJELFA_WIND_SPEED_10M_ROLL_MEAN24_LAG24 = "Djelfa_wind_speed_10m__roll_mean24_lag24"
    DJELFA_WIND_SPEED_10M_ROLL_STD24_LAG24 = "Djelfa_wind_speed_10m__roll_std24_lag24"
    DJELFA_WIND_SPEED_10M_ROLL_MEAN168_LAG24 = (
        "Djelfa_wind_speed_10m__roll_mean168_lag24"
    )
    DJELFA_WIND_SPEED_10M_ROLL_STD168_LAG24 = "Djelfa_wind_speed_10m__roll_std168_lag24"

    HOLIDAYS = "holidays"
    CUSTOM_WEEKDAY_1 = "custom_weekday_1"
    CUSTOM_WEEKDAY_2 = "custom_weekday_2"
    CUSTOM_WEEKDAY_3 = "custom_weekday_3"
    CUSTOM_WEEKDAY_4 = "custom_weekday_4"
