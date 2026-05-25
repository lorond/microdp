import pytest
from fastapi.testclient import TestClient
from kafka.errors import KafkaError

from app.main import app
from app.producer import get_producer


class FakeProducer:
    def __init__(self):
        self.records = []

    def publish(self, records):
        self.records.extend(records)
        return len(records)


class FailingProducer:
    def publish(self, records):
        raise KafkaError("kafka unreachable")


def _valid_event(**overrides):
    base = {
        "event_id": "evt-1",
        "session_id": "sess-1",
        "user_id": "00000000-0000-0000-0000-000000000001",
        "event_type": "click",
        "page": "/",
        "ts": "2026-05-22T12:00:00Z",
        "element_id": "create-transaction",
        "x": 12,
        "y": 24,
        "dwell_ms": 0,
        "payload": {"test": True},
    }
    base.update(overrides)
    return base


@pytest.fixture
def client_with(monkeypatch):
    def _factory(producer):
        app.dependency_overrides[get_producer] = lambda: producer
        return TestClient(app)

    yield _factory
    app.dependency_overrides.clear()


def test_health_returns_topic():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["topic"]


def test_publish_clickstream_batch(client_with):
    fake = FakeProducer()
    client = client_with(fake)

    response = client.post(
        "/api/clickstream/events",
        json={"events": [_valid_event()]},
    )

    assert response.status_code == 202
    assert response.json()["accepted"] == 1
    assert fake.records[0]["event_type"] == "click"


def test_publish_multi_event_batch_preserves_order(client_with):
    fake = FakeProducer()
    client = client_with(fake)

    events = [
        _valid_event(event_id="e1", event_type="page_enter"),
        _valid_event(event_id="e2", event_type="click"),
        _valid_event(event_id="e3", event_type="page_leave"),
    ]
    response = client.post("/api/clickstream/events", json={"events": events})

    assert response.status_code == 202
    assert response.json()["accepted"] == 3
    assert [r["event_id"] for r in fake.records] == ["e1", "e2", "e3"]


def test_rejects_unknown_event_type(client_with):
    client = client_with(FakeProducer())
    response = client.post(
        "/api/clickstream/events",
        json={"events": [_valid_event(event_type="hover")]},
    )
    assert response.status_code == 422


def test_rejects_empty_batch(client_with):
    client = client_with(FakeProducer())
    response = client.post("/api/clickstream/events", json={"events": []})
    assert response.status_code == 422


def test_rejects_negative_dwell(client_with):
    client = client_with(FakeProducer())
    response = client.post(
        "/api/clickstream/events",
        json={"events": [_valid_event(dwell_ms=-5)]},
    )
    assert response.status_code == 422


def test_producer_failure_surfaces_as_503(client_with):
    client = client_with(FailingProducer())
    response = client.post(
        "/api/clickstream/events",
        json={"events": [_valid_event()]},
    )
    assert response.status_code == 503
    assert "Kafka publish failed" in response.json()["detail"]


def test_ts_always_serialized_as_utc_zulu(client_with):
    fake = FakeProducer()
    client = client_with(fake)
    response = client.post(
        "/api/clickstream/events",
        json={"events": [_valid_event(ts="2026-05-22T15:30:00+03:00")]},
    )
    assert response.status_code == 202
    assert fake.records[0]["ts"] == "2026-05-22T12:30:00.000Z"

