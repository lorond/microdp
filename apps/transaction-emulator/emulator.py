from __future__ import annotations

import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger("transaction-emulator")


WALLET_API_URL = os.getenv("WALLET_API_URL", "http://localhost:8000")
CLICKSTREAM_API_URL = os.getenv("CLICKSTREAM_API_URL", "http://localhost:8001")
DEMO_USER_IDS = [
    user_id.strip()
    for user_id in os.getenv(
        "DEMO_USER_IDS",
        "00000000-0000-0000-0000-000000000002,00000000-0000-0000-0000-000000000003",
    ).split(",")
    if user_id.strip()
]
try:
    TRANSACTIONS_PER_MINUTE = int(os.getenv("TRANSACTIONS_PER_MINUTE", "6"))
except ValueError:
    logger.warning(
        "TRANSACTIONS_PER_MINUTE=%r is not an int; falling back to 6",
        os.environ.get("TRANSACTIONS_PER_MINUTE"),
    )
    TRANSACTIONS_PER_MINUTE = 6
EMULATOR_ENABLED = os.getenv("EMULATOR_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

MERCHANTS = ["Coffee Lab", "Metro", "Book Shop", "Cloud Market", "Bike Share"]
PAGES = ["/dashboard", "/payments", "/history", "/signals"]
CTA_BY_PAGE = {
    "/dashboard": ["quick-action-pay", "quick-action-deposit"],
    "/payments": ["create-transaction"],
}


@dataclass
class EmulatorSession:
    session_id: str
    user_id: str
    expires_at: float


def new_session() -> EmulatorSession:
    ttl_seconds = random.randint(5 * 60, 10 * 60)
    return EmulatorSession(
        session_id=f"emulator-{uuid.uuid4()}",
        user_id=random.choice(DEMO_USER_IDS),
        expires_at=time.monotonic() + ttl_seconds,
    )


def _build_session() -> requests.Session:
    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=(502, 503, 504),
        allowed_methods=("POST",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP = _build_session()


def post_json(url: str, payload: dict[str, Any]) -> None:
    response = HTTP.post(url, json=payload, timeout=10)
    response.raise_for_status()


TOP_UP_AMOUNT = "500.00"


def _top_up(user_id: str) -> None:
    payload = {
        "type": "deposit",
        "amount": TOP_UP_AMOUNT,
        "currency": "USD",
        "description": "Auto top-up after insufficient funds",
        "metadata": {"source": "transaction-emulator-recovery"},
    }
    post_json(f"{WALLET_API_URL}/api/users/{user_id}/transactions", payload)


def create_transaction(user_id: str, transaction_type: str | None = None) -> None:
    resolved_type = transaction_type or random.choices(
        ["deposit", "payment", "withdrawal"], weights=[2, 7, 1], k=1
    )[0]
    amount = round(random.uniform(3, 75), 2)
    payload = {
        "type": resolved_type,
        "amount": f"{amount:.2f}",
        "currency": "USD",
        "description": "Automated demo transaction",
        "merchant": random.choice(MERCHANTS) if resolved_type == "payment" else None,
        "metadata": {"source": "transaction-emulator"},
    }
    url = f"{WALLET_API_URL}/api/users/{user_id}/transactions"
    try:
        post_json(url, payload)
    except requests.HTTPError as exc:
        response = exc.response
        if response is not None and response.status_code == 409:
            logger.info("user %s: insufficient funds, applying auto top-up", user_id)
            _top_up(user_id)
            return
        raise


def event(
    session: "EmulatorSession",
    event_type: str,
    page: str,
    ts: datetime,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session.session_id,
        "user_id": session.user_id,
        "event_type": event_type,
        "page": page,
        "ts": ts.isoformat(),
        "element_id": extra.get("element_id"),
        "x": extra.get("x"),
        "y": extra.get("y"),
        "dwell_ms": extra.get("dwell_ms", 0),
        "payload": {"source": "transaction-emulator", **extra.get("payload", {})},
    }


def mouse_moves(session: "EmulatorSession", page: str, start_at: datetime) -> tuple[list[dict[str, Any]], datetime]:
    events: list[dict[str, Any]] = []
    ts = start_at
    for _ in range(random.randint(3, 6)):
        ts += timedelta(milliseconds=random.randint(520, 900))
        events.append(
            event(
                session,
                "mouse_move",
                page,
                ts,
                x=random.randint(60, 920),
                y=random.randint(70, 740),
            )
        )
    return events, ts


def planned_pages(target_page: str) -> list[str]:
    optional = [page for page in PAGES if page != target_page]
    journey = random.sample(optional, k=random.randint(2, 3))
    insert_at = random.randint(1, len(journey))
    journey.insert(insert_at, target_page)
    return journey[:4]


def transaction_type_for_cta(element_id: str) -> str:
    if element_id == "quick-action-deposit":
        return "deposit"
    return random.choices(["payment", "withdrawal"], weights=[8, 1], k=1)[0]


def build_journey(session: EmulatorSession) -> tuple[list[dict[str, Any]], str | None]:
    target_page = random.choice(["/dashboard", "/payments"])
    cta_id = random.choice(CTA_BY_PAGE[target_page])
    should_create_transaction = random.random() >= 0.30
    transaction_type = transaction_type_for_cta(cta_id) if should_create_transaction else None
    events: list[dict[str, Any]] = []
    current_ts = datetime.now(UTC)

    for page in planned_pages(target_page):
        current_ts += timedelta(milliseconds=random.randint(150, 900))
        events.append(event(session, "route_change", page, current_ts))
        current_ts += timedelta(milliseconds=random.randint(80, 250))
        enter_ts = current_ts
        events.append(event(session, "page_enter", page, enter_ts))

        if page == target_page:
            move_events, current_ts = mouse_moves(session, page, current_ts)
            events.extend(move_events)
            current_ts += timedelta(milliseconds=random.randint(180, 650))
            events.append(
                event(
                    session,
                    "click",
                    page,
                    current_ts,
                    element_id=cta_id,
                    x=random.randint(160, 840),
                    y=random.randint(120, 680),
                    payload={"will_create_transaction": should_create_transaction},
                )
            )

        dwell_ms = random.randint(3_000, 25_000)
        current_ts = max(current_ts, enter_ts) + timedelta(milliseconds=dwell_ms)
        events.append(
            event(
                session,
                "page_leave",
                page,
                current_ts,
                dwell_ms=int((current_ts - enter_ts).total_seconds() * 1000),
            )
        )

    return events, transaction_type


def emit_clickstream(events: list[dict[str, Any]]) -> None:
    post_json(f"{CLICKSTREAM_API_URL}/api/clickstream/events", {"events": events})


_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    logger.info("received signal %s, stopping after current iteration", signum)
    _running = False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if not EMULATOR_ENABLED:
        logger.info("transaction-emulator disabled via EMULATOR_ENABLED=false")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    base_delay = max(1.0, 60.0 / max(TRANSACTIONS_PER_MINUTE, 1))
    session = new_session()

    while _running:
        try:
            if time.monotonic() >= session.expires_at:
                session = new_session()

            events, transaction_type = build_journey(session)
            emit_clickstream(events)
            if transaction_type:
                create_transaction(session.user_id, transaction_type)
            logger.info(
                "emitted %d clickstream events and %s",
                len(events),
                "one transaction" if transaction_type else "a browse-only visit",
            )
        except Exception:
            logger.exception("emulator iteration failed")

        jitter = random.uniform(0.7, 1.3)
        time.sleep(max(1.0, base_delay * jitter))

    HTTP.close()


if __name__ == "__main__":
    main()
