import pytest
from pydantic import ValidationError
from src.models import RawEvent
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
import src.main as main

def test_raw_event_validation_success():
    data = {
        "event_id": "123",
        "entity_id": "user_1",
        "action_type": "click"
    }
    event = RawEvent(**data)
    assert event.entity_id == "user_1"
    assert event.action_type == "click"

def test_raw_event_validation_failure():
    data = {
        "event_id": "123"
    }
    with pytest.raises(ValidationError):
        RawEvent(**data)

def test_get_features_found(monkeypatch):
    mock_db = MagicMock()
    mock_db.get_features.return_value = {"user_activity_count": "5", "last_action": "login"}
    monkeypatch.setattr(main, "db_manager", mock_db)
    
    client = TestClient(main.app)
    response = client.get("/features/user_123")
    
    assert response.status_code == 200
    assert response.json() == {
        "entity_id": "user_123",
        "features": {
            "user_activity_count": "5",
            "last_action": "login"
        }
    }

def test_get_features_not_found(monkeypatch):
    mock_db = MagicMock()
    mock_db.get_features.return_value = {}
    monkeypatch.setattr(main, "db_manager", mock_db)
    
    client = TestClient(main.app)
    response = client.get("/features/user_unknown")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()
