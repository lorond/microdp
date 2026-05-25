import json
import logging

from fastapi import Request
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .settings import settings


logger = logging.getLogger(__name__)


class ClickstreamProducer:
    def __init__(self, bootstrap_servers: str, topic: str, dlq_topic: str):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=50,
            enable_idempotence=True,
            max_in_flight_requests_per_connection=1,
        )

    def publish(self, records: list[dict]) -> int:
        futures = [
            (
                record,
                self._producer.send(
                    self.topic,
                    key=record["session_id"],
                    value=record,
                ),
            )
            for record in records
        ]
        failed = self._collect_failures(futures)
        if failed:
            self._route_to_dlq(failed)
        return len(records)

    def _collect_failures(
        self, futures: list[tuple[dict, "object"]]
    ) -> list[tuple[dict, KafkaError]]:
        failed: list[tuple[dict, KafkaError]] = []
        for record, future in futures:
            try:
                future.get(timeout=10)
            except KafkaError as exc:
                failed.append((record, exc))
        return failed

    def _route_to_dlq(self, failed: list[tuple[dict, KafkaError]]) -> None:
        dlq_futures = []
        for record, error in failed:
            envelope = {"original": record, "error": str(error), "source_topic": self.topic}
            dlq_futures.append(
                (
                    record,
                    self._producer.send(
                        self.dlq_topic,
                        key=record["session_id"],
                        value=envelope,
                    ),
                )
            )

        unrecoverable = self._collect_failures(dlq_futures)
        if unrecoverable:
            for record, error in unrecoverable:
                logger.error(
                    "DLQ publish failed for event_id=%s session_id=%s: %s",
                    record.get("event_id"),
                    record.get("session_id"),
                    error,
                )
            raise KafkaError(
                f"failed to publish {len(unrecoverable)} record(s) to DLQ {self.dlq_topic}"
            )
        logger.warning(
            "routed %d clickstream record(s) to DLQ %s", len(failed), self.dlq_topic
        )

    def close(self) -> None:
        self._producer.close(timeout=5)


def build_producer() -> ClickstreamProducer:
    return ClickstreamProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.clickstream_topic,
        dlq_topic=settings.clickstream_dlq_topic,
    )


def get_producer(request: Request) -> ClickstreamProducer:
    return request.app.state.producer
