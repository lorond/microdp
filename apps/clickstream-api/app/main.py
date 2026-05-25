from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka.errors import KafkaError

from .producer import ClickstreamProducer, build_producer, get_producer
from .schemas import ClickstreamBatch, PublishResponse
from .settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = build_producer()
    app.state.producer = producer
    try:
        yield
    finally:
        producer.close()


app = FastAPI(title="MicroDP Clickstream API", version="0.1.0", lifespan=lifespan)

# DEMO-ONLY: open CORS for the local React UI / curl smoke checks.
# Tighten allow_origins before exposing this API outside the demo stack.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "topic": settings.clickstream_topic}


@app.post("/api/clickstream/events", response_model=PublishResponse, status_code=202)
def publish_events(
    batch: ClickstreamBatch,
    producer: ClickstreamProducer = Depends(get_producer),
) -> PublishResponse:
    records = [event.model_dump(mode="json") for event in batch.events]
    try:
        accepted = producer.publish(records)
    except KafkaError as exc:
        raise HTTPException(status_code=503, detail=f"Kafka publish failed: {exc}") from exc
    return PublishResponse(accepted=accepted, topic=settings.clickstream_topic)
