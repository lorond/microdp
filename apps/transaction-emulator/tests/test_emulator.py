from __future__ import annotations

import random
import sys
import threading
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import emulator  # noqa: E402


def test_transaction_type_for_cta_deposit_is_deterministic():
    assert emulator.transaction_type_for_cta("quick-action-deposit") == "deposit"


def test_transaction_type_for_cta_pay_returns_debit_kind():
    random.seed(0)
    chosen = {emulator.transaction_type_for_cta("quick-action-pay") for _ in range(50)}
    assert chosen <= {"payment", "withdrawal"}
    assert "payment" in chosen


def test_planned_pages_contains_target_and_fits_within_four():
    random.seed(1)
    for _ in range(20):
        target = random.choice(emulator.PAGES)
        journey = emulator.planned_pages(target)
        assert target in journey
        assert len(journey) <= 4
        assert len(set(journey)) == len(journey)


def test_plan_journey_target_page_consistent_with_cta_and_txn_type():
    random.seed(42)
    for _ in range(20):
        plan = emulator.plan_journey()
        assert plan.target_page in emulator.CTA_BY_PAGE
        assert plan.cta_id in emulator.CTA_BY_PAGE[plan.target_page]
        assert plan.target_page in plan.pages
        if plan.transaction_type is not None:
            if plan.cta_id == "quick-action-deposit":
                assert plan.transaction_type == "deposit"
            else:
                assert plan.transaction_type in {"payment", "withdrawal"}


def test_new_session_for_carries_user_id_and_unique_session_id():
    user_id = "00000000-0000-0000-0000-000000000002"
    s1 = emulator.new_session_for(user_id)
    s2 = emulator.new_session_for(user_id)
    assert s1.user_id == user_id and s2.user_id == user_id
    assert s1.session_id != s2.session_id
    assert s1.session_id.startswith("emulator-")


def test_run_activity_emits_route_change_page_enter_leave_per_page(monkeypatch):
    """run_activity should stream events for every page: route_change → page_enter →
    (… mouse_move/click on target …) → page_leave. Sleeps are squashed to keep
    the test fast."""

    random.seed(7)
    captured: list[dict] = []

    def fake_emit(events):
        captured.extend(events)

    monkeypatch.setattr(emulator, "emit_events", fake_emit)
    monkeypatch.setattr(emulator, "create_transaction", lambda *a, **k: None)
    monkeypatch.setattr(emulator, "_wait", lambda *_a, **_k: False)

    stop = threading.Event()
    emulator.run_activity("00000000-0000-0000-0000-000000000004", stop)

    by_page: dict[str, list[str]] = {}
    for evt in captured:
        by_page.setdefault(evt["page"], []).append(evt["event_type"])

    assert by_page, "run_activity emitted nothing"
    for page, sequence in by_page.items():
        assert sequence[0] == "route_change", page
        assert sequence[1] == "page_enter", page
        assert sequence[-1] == "page_leave", page


def test_run_activity_respects_stop_event_between_phases(monkeypatch):
    """If stop is set before the very first phase, no events should be emitted at all."""

    captured: list[dict] = []
    monkeypatch.setattr(emulator, "emit_events", lambda events: captured.extend(events))
    monkeypatch.setattr(emulator, "create_transaction", lambda *a, **k: None)

    # Make `_wait` always report "stop requested" — first wait at start of first
    # page returns True → run_activity should exit immediately.
    monkeypatch.setattr(emulator, "_wait", lambda *_a, **_k: True)

    stop = threading.Event()
    stop.set()
    emulator.run_activity("00000000-0000-0000-0000-000000000004", stop)

    assert captured == [], "run_activity should not emit when stop is set immediately"


def test_discover_emulator_users_filters_out_demo(monkeypatch):
    excluded = emulator.DEMO_USER_ID
    payload = [
        {"id": excluded, "full_name": "Demo"},
        {"id": "11111111-1111-1111-1111-111111111111", "full_name": "Alice"},
        {"id": "22222222-2222-2222-2222-222222222222", "full_name": "Bob"},
    ]
    monkeypatch.setattr(emulator, "fetch_users", lambda: payload)
    pool = emulator.discover_emulator_users(threading.Event())
    assert excluded not in pool
    assert len(pool) == 2


def test_discover_emulator_users_retries_until_pool_nonempty(monkeypatch):
    """If wallet-api responds before any non-Demo user is seeded, the discovery
    should retry rather than raise immediately."""

    call_log = {"n": 0}

    def flaky_fetch():
        call_log["n"] += 1
        if call_log["n"] < 3:
            return [{"id": emulator.DEMO_USER_ID, "full_name": "Demo"}]
        return [
            {"id": emulator.DEMO_USER_ID, "full_name": "Demo"},
            {"id": "33333333-3333-3333-3333-333333333333", "full_name": "Carla"},
        ]

    monkeypatch.setattr(emulator, "fetch_users", flaky_fetch)
    # Speed up the 3s retry sleep so the test doesn't take that long.
    monkeypatch.setattr(emulator.threading.Event, "wait", lambda self, _t: False)
    pool = emulator.discover_emulator_users(threading.Event(), timeout_seconds=30)
    assert pool == ["33333333-3333-3333-3333-333333333333"]
    assert call_log["n"] >= 3


def test_discover_emulator_users_raises_when_timeout(monkeypatch):
    monkeypatch.setattr(emulator, "fetch_users", lambda: [{"id": emulator.DEMO_USER_ID, "full_name": "Demo"}])
    monkeypatch.setattr(emulator.threading.Event, "wait", lambda self, _t: False)
    monkeypatch.setattr(emulator.time, "monotonic", lambda: 9999999.0)
    with pytest.raises(RuntimeError, match="non-Demo users"):
        emulator.discover_emulator_users(threading.Event(), timeout_seconds=0)


def test_scheduler_does_not_run_same_user_in_two_slots(monkeypatch):
    """At any moment a user should be in at most one in-flight activity, so the
    scheduler can only launch a second activity for the same user after the first
    one has finished."""

    barrier = threading.Event()

    def slow_activity(user_id, stop_event):
        # Holds the slot until barrier is released, simulating an in-flight activity.
        barrier.wait(timeout=5)

    monkeypatch.setattr(emulator, "run_activity", slow_activity)

    stop = threading.Event()
    sched = emulator.ActivityScheduler(
        users=["u1", "u2", "u3", "u4", "u5"],
        max_concurrent=3,
        target_launch_per_minute=600,  # very fast launches
        stop_event=stop,
    )

    runner = threading.Thread(target=sched.run, daemon=True)
    runner.start()
    try:
        # Wait long enough for the scheduler to fill all 3 slots.
        deadline = threading.Event()
        for _ in range(50):
            with sched._lock:  # noqa: SLF001 — internal state check is the point of this test
                inflight = dict(sched._inflight)
            if len(inflight) == 3:
                break
            deadline.wait(0.05)

        with sched._lock:  # noqa: SLF001
            users_inflight = list(sched._inflight.keys())
        assert len(users_inflight) == 3, f"scheduler did not fill slots: {users_inflight}"
        assert len(set(users_inflight)) == 3, (
            f"same user landed in multiple slots: {users_inflight}"
        )
    finally:
        stop.set()
        barrier.set()
        runner.join(timeout=5)
