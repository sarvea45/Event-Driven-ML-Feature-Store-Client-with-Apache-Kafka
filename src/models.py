from pydantic import BaseModel, Field
from typing import Dict, Any

class RawEvent(BaseModel):
    event_id: str
    entity_id: str
    action_type: str
    timestamp: str | None = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class FeatureResponse(BaseModel):
    entity_id: str
    features: Dict[str, str]
