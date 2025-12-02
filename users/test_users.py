import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_create_user():
    response = client.post("/users", json={
        "email": "test@example.com",
        "name": "test user",
        "role": "user"
    })
    assert response.status_code == 200
    assert response.json()["email"] == "test@example.com"

def test_get_user_not_found():
    response = client.get("/users/99999")
    assert response.status_code == 404

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

