"""Calendar features that can be added (or not) to darts ``add_encoder``."""

import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder


def encode_onehot_custom_weekday(index: pd.DatetimeIndex) -> np.ndarray:
    """Create a custom weekdays pattern

    - Cat 1 => Monday and Friday
    - Cat 2 => T/W/T
    - Cat 3 => Weekend
    """

    conditions = [
        index.dayofweek == 0 | index.dayofweek == 4,  # Monday / Friday
        index.dayofweek >= 1 & index.dayofweek <= 4,  # T/W/T
        index.dayofweek == 5,  # Saturday
        index.dayofweek == 6,  # Sunday
    ]
    choices = ["WeekBorder", "WekkMiddle", "Saturday", "Sunday"]
    categories = np.select(conditions, choices)
    categories_2d = categories.reshape(-1, 1)
    encoder = OneHotEncoder(sparse_output=False, categories=[choices])

    return encoder.fit_transform(categories_2d)
