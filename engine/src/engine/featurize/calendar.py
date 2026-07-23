"""Calendar features that can be added (or not) to darts ``add_encoder``."""

import numpy as np
import pandas as pd


def encode_onehot_custom_weekday(index: pd.DatetimeIndex) -> np.ndarray:
    """Create a custom weekdays pattern

    - Cat 1 => Monday and Friday
    - Cat 2 => T/W/T
    - Cat 3 => Saturday
    - Cat 4 => Sunday

    Returns a 4-column one-hot array.
    """

    # dayofweek: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    dow = index.dayofweek.to_numpy()

    # mapping: Mon/Fri → 0, Tue/Wed/Thu → 1, Sat → 2, Sun → 3
    _WEEKDAY_MAP = np.array([0, 1, 1, 1, 0, 2, 3])
    cat_indices = _WEEKDAY_MAP[dow]

    one_hot = np.zeros((len(index), 4), dtype=np.float64)
    one_hot[np.arange(len(index)), cat_indices] = 1.0
    return one_hot
