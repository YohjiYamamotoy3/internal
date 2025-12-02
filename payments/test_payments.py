import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_create_payment():
    response = client.post("/payments", json={
        "user_id": 1,
        "amount": 100.50,
        "currency": "USD",
        "description": "test payment"
    })
    assert response.status_code == 200
    assert response.json()["amount"] == 100.50

def test_get_payment_not_found():
    response = client.get("/payments/99999")
    assert response.status_code == 404

def test_health():
    response = client.get("/health")
    assert response.status_code == 200

