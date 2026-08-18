model_hourly: dict[str, dict] = {}


def register_hourly(factory):
    model_hourly[factory.__name__] = factory()
    return factory
