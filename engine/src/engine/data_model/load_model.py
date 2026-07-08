from datetime import datetime

from pydantic import BaseModel, Field


class LoadTimeSeries(BaseModel):
    id: int = Field(description="PRIMARY KEY")
    name: str
    created_at: datetime = Field(default_factory=datetime.now)
