from __future__ import annotations

import logging
import os
import random
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger("transaction-emulator")


WALLET_API_URL = os.getenv("WALLET_API_URL", "http://localhost:8000")
CLICKSTREAM_API_URL = os.getenv("CLICKSTREAM_API_URL", "http://localhost:8001")

# Единственный пользователь, которого эмулятор НЕ трогает — Demo (...001).
# Его баланс зарезервирован под ручную демонстрацию в UI; всех остальных
# (генерируемых init-скриптом Postgres) эмулятор подтягивает через wallet-api
# /api/users, чтобы код не знал заранее ни одного user-id, кроме Demo.
DEMO_USER_ID = os.getenv("DEMO_USER_ID", "00000000-0000-0000-0000-000000000001")


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        logger.warning("%s=%r is not an int; falling back to %d", name, os.environ.get(name), default)
        return default


# TRANSACTIONS_PER_MINUTE — это целевой темп запуска новых активностей. Реальный
# темп завершённых активностей ниже: его сверху ограничивает MAX_CONCURRENT_ACTIVITIES
# и средняя длительность одной активности (см. AGENTS.md).
TRANSACTIONS_PER_MINUTE = _int_env("TRANSACTIONS_PER_MINUTE", 30)
MAX_CONCURRENT_ACTIVITIES = max(1, _int_env("MAX_CONCURRENT_ACTIVITIES", 4))
# Сколько секунд ждать, пока wallet-api отдаст непустой список (Postgres
# init-скрипт может ещё досевать пользователей, когда эмулятор стартует).
USER_LIST_BOOTSTRAP_TIMEOUT = _int_env("USER_LIST_BOOTSTRAP_TIMEOUT", 120)
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


def new_session_for(user_id: str) -> EmulatorSession:
    return EmulatorSession(
        session_id=f"emulator-{uuid.uuid4()}",
        user_id=user_id,
    )


@dataclass
class JourneyPlan:
    pages: list[str]
    target_page: str
    cta_id: str
    transaction_type: str | None
    mouse_move_count: int


def _build_session() -> requests.Session:
    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=(502, 503, 504),
        allowed_methods=("POST",),
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        # Под нагрузкой ~4 параллельных активностей дефолтные 10 коннектов на host
        # из urllib3 быстро упираются в "Connection pool is full" warning.
        pool_connections=MAX_CONCURRENT_ACTIVITIES * 2,
        pool_maxsize=MAX_CONCURRENT_ACTIVITIES * 2,
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP = _build_session()


def post_json(url: str, payload: dict[str, Any]) -> None:
    response = HTTP.post(url, json=payload, timeout=10)
    response.raise_for_status()


def fetch_users() -> list[dict[str, Any]]:
    response = HTTP.get(f"{WALLET_API_URL}/api/users", timeout=10)
    response.raise_for_status()
    return response.json().get("users", [])


def discover_emulator_users(
    stop_event: threading.Event,
    exclude_user_id: str = DEMO_USER_ID,
    timeout_seconds: int = USER_LIST_BOOTSTRAP_TIMEOUT,
) -> list[str]:
    """Опрашивает wallet-api `/api/users` пока не появится хотя бы один user,
    отличный от `exclude_user_id`, или пока не истечёт timeout. Каждая попытка
    может упасть, если wallet-api ещё не поднялся — это ожидаемо при первом
    `docker compose up`, поэтому исключения просто логируются и ретраятся."""
    deadline = time.monotonic() + timeout_seconds
    while not stop_event.is_set():
        try:
            users = fetch_users()
            pool = [u["id"] for u in users if u["id"] != exclude_user_id]
            if pool:
                return pool
            logger.info(
                "wallet-api returned %d users but all match exclude id %s; retrying",
                len(users),
                exclude_user_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.info("wallet-api /api/users not ready yet: %s", exc)
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Could not discover any non-Demo users via wallet-api within "
                f"{timeout_seconds}s. Did the Postgres init scripts finish?"
            )
        if stop_event.wait(3.0):
            return []
    return []


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


def plan_journey() -> JourneyPlan:
    target_page = random.choice(["/dashboard", "/payments"])
    cta_id = random.choice(CTA_BY_PAGE[target_page])
    should_create_transaction = random.random() >= 0.30
    transaction_type = (
        transaction_type_for_cta(cta_id) if should_create_transaction else None
    )
    return JourneyPlan(
        pages=planned_pages(target_page),
        target_page=target_page,
        cta_id=cta_id,
        transaction_type=transaction_type,
        mouse_move_count=random.randint(3, 6),
    )


def emit_events(events: list[dict[str, Any]]) -> None:
    if not events:
        return
    post_json(f"{CLICKSTREAM_API_URL}/api/clickstream/events", {"events": events})


def _wait(stop_event: threading.Event, seconds: float) -> bool:
    """Sleep up to `seconds`; return True if stop was requested mid-sleep."""
    return stop_event.wait(max(0.0, seconds))


def run_activity(user_id: str, stop_event: threading.Event) -> None:
    """Один независимый act: своя сессия для user'а, реалтайм-стрим событий,
    опциональная транзакция в конце. Между фазами проверяется stop_event,
    чтобы SIGTERM не висел до конца самой длинной dwell-паузы."""
    session = new_session_for(user_id)
    plan = plan_journey()

    for page in plan.pages:
        if _wait(stop_event, random.uniform(0.15, 0.9)):
            return
        emit_events([event(session, "route_change", page, datetime.now(UTC))])
        if _wait(stop_event, random.uniform(0.08, 0.25)):
            return

        enter_ts = datetime.now(UTC)
        emit_events([event(session, "page_enter", page, enter_ts)])

        if page == plan.target_page:
            for _ in range(plan.mouse_move_count):
                if _wait(stop_event, random.uniform(0.4, 0.9)):
                    return
                emit_events(
                    [
                        event(
                            session,
                            "mouse_move",
                            page,
                            datetime.now(UTC),
                            x=random.randint(60, 920),
                            y=random.randint(70, 740),
                        )
                    ]
                )
            if _wait(stop_event, random.uniform(0.18, 0.65)):
                return
            emit_events(
                [
                    event(
                        session,
                        "click",
                        page,
                        datetime.now(UTC),
                        element_id=plan.cta_id,
                        x=random.randint(160, 840),
                        y=random.randint(120, 680),
                        payload={
                            "will_create_transaction": plan.transaction_type is not None,
                        },
                    )
                ]
            )

        # Сравнительно короткий dwell, чтобы активность не висела минутами и слот
        # быстрее освобождался под новую — иначе при cap=4 и больших dwell'ах
        # стабильно работали бы 4 одних и тех же user'а часами.
        if _wait(stop_event, random.uniform(0.6, 4.5)):
            return
        leave_ts = datetime.now(UTC)
        dwell_ms = int((leave_ts - enter_ts).total_seconds() * 1000)
        emit_events(
            [event(session, "page_leave", page, leave_ts, dwell_ms=dwell_ms)]
        )

    if plan.transaction_type and not stop_event.is_set():
        create_transaction(user_id, plan.transaction_type)


_stop_event = threading.Event()


def _handle_signal(signum: int, _frame: Any) -> None:
    logger.info("received signal %s, draining active activities", signum)
    _stop_event.set()


class ActivityScheduler:
    """Пул потоков с ограничением по конкурентности. В каждый момент времени:
       - активных активностей ≤ MAX_CONCURRENT_ACTIVITIES;
       - один user — максимум в одной активности (чтобы пользователи реально
         работали параллельно, а не дублировались в нескольких слотах)."""

    def __init__(
        self,
        users: list[str],
        max_concurrent: int,
        target_launch_per_minute: int,
        stop_event: threading.Event,
    ) -> None:
        if not users:
            raise ValueError("ActivityScheduler requires at least one user")
        self._users = users
        self._max_concurrent = max_concurrent
        self._launch_interval = max(0.5, 60.0 / max(target_launch_per_minute, 1))
        self._stop_event = stop_event
        self._inflight: dict[str, Future] = {}
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(
            max_workers=max_concurrent,
            thread_name_prefix="activity",
        )

    def _free_user(self) -> str | None:
        with self._lock:
            if len(self._inflight) >= self._max_concurrent:
                return None
            busy = set(self._inflight)
            candidates = [u for u in self._users if u not in busy]
        if not candidates:
            return None
        return random.choice(candidates)

    def _launch(self, user_id: str) -> None:
        future = self._pool.submit(run_activity, user_id, self._stop_event)
        with self._lock:
            self._inflight[user_id] = future

        def _cleanup(fut: Future) -> None:
            with self._lock:
                self._inflight.pop(user_id, None)
            try:
                fut.result()
            except Exception:
                logger.exception("activity for user ...%s failed", user_id[-12:])

        future.add_done_callback(_cleanup)
        logger.info(
            "started activity user=...%s inflight=%d/%d",
            user_id[-12:],
            len(self._inflight),
            self._max_concurrent,
        )

    def run(self) -> None:
        while not self._stop_event.is_set():
            user_id = self._free_user()
            if user_id is not None:
                self._launch(user_id)
            jitter = random.uniform(0.6, 1.4)
            self._stop_event.wait(self._launch_interval * jitter)
        with self._lock:
            inflight_count = len(self._inflight)
        logger.info("draining: waiting for %d activities to finish", inflight_count)
        self._pool.shutdown(wait=True, cancel_futures=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not EMULATOR_ENABLED:
        logger.info("transaction-emulator disabled via EMULATOR_ENABLED=false")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        users = discover_emulator_users(_stop_event)
    except RuntimeError as exc:
        logger.error("%s", exc)
        HTTP.close()
        sys.exit(1)
    if _stop_event.is_set():
        HTTP.close()
        sys.exit(0)

    logger.info(
        "starting emulator: users=%d (excluded=%s), max_concurrent=%d, launch_rate=%d/min",
        len(users),
        DEMO_USER_ID,
        MAX_CONCURRENT_ACTIVITIES,
        TRANSACTIONS_PER_MINUTE,
    )
    scheduler = ActivityScheduler(
        users=users,
        max_concurrent=MAX_CONCURRENT_ACTIVITIES,
        target_launch_per_minute=TRANSACTIONS_PER_MINUTE,
        stop_event=_stop_event,
    )
    try:
        scheduler.run()
    finally:
        HTTP.close()


if __name__ == "__main__":
    main()
