from __future__ import annotations

import pytest
from kafka.errors import KafkaError


class FakeFuture:
    def __init__(self, exception: KafkaError | None = None):
        self._exception = exception

    def get(self, timeout=None):
        if self._exception is not None:
            raise self._exception
        return None


class FakeKafkaProducer:
    def __init__(self, fail_topics: set[str] | None = None, **_kwargs):
        self.fail_topics = fail_topics or set()
        self.sends: list[tuple[str, str, dict]] = []

    def send(self, topic, key, value):
        self.sends.append((topic, key, value))
        if topic in self.fail_topics:
            return FakeFuture(exception=KafkaError(f"broker rejected {topic}"))
        return FakeFuture()

    def flush(self, timeout=None):
        return None

    def close(self, timeout=None):
        return None


@pytest.fixture
def producer_factory(monkeypatch):
    fakes: list[FakeKafkaProducer] = []

    def _factory(fail_topics: set[str] | None = None):
        from app import producer as producer_module

        fake = FakeKafkaProducer(fail_topics=fail_topics)
        monkeypatch.setattr(
            producer_module, "KafkaProducer", lambda **kwargs: fake
        )
        instance = producer_module.ClickstreamProducer(
            bootstrap_servers="ignored",
            topic="clickstream.events",
            dlq_topic="clickstream.events.dlq",
        )
        fakes.append(fake)
        return instance, fake

    return _factory


def _record(event_id: str = "evt-1") -> dict:
    return {"event_id": event_id, "session_id": "sess-1", "page": "/"}


def test_publish_happy_path(producer_factory):
    producer, fake = producer_factory()
    accepted = producer.publish([_record("a"), _record("b")])
    assert accepted == 2
    assert [topic for topic, _, _ in fake.sends] == [
        "clickstream.events",
        "clickstream.events",
    ]


def test_publish_routes_failed_records_to_dlq(producer_factory):
    producer, fake = producer_factory(fail_topics={"clickstream.events"})
    producer.publish([_record("a")])

    topics = [topic for topic, _, _ in fake.sends]
    assert topics == ["clickstream.events", "clickstream.events.dlq"]
    dlq_envelope = fake.sends[-1][2]
    assert dlq_envelope["source_topic"] == "clickstream.events"
    assert dlq_envelope["original"]["event_id"] == "a"
    assert "broker rejected" in dlq_envelope["error"]


def test_publish_raises_when_dlq_also_fails(producer_factory):
    producer, _ = producer_factory(
        fail_topics={"clickstream.events", "clickstream.events.dlq"}
    )
    with pytest.raises(KafkaError):
        producer.publish([_record("a")])
