from fastapi import FastAPI

from src.api.data.routers import timeseries_route

app = FastAPI(title="Energy Forecasting API", version="1.0.0", root_path="/api/v1")
app.include_router(timeseries_route.router)


@app.get("/")
async def root():
    return {"message": "Welcome to the Energy Forecasting API"}
