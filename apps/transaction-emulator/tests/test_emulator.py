from __future__ import annotations

import random
import sys
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


def test_build_journey_emits_route_change_then_page_enter_then_leave_per_page():
    random.seed(42)
    session = emulator.new_session()
    events, _txn_type = emulator.build_journey(session)

    by_page: dict[str, list[str]] = {}
    for event in events:
        by_page.setdefault(event["page"], []).append(event["event_type"])

    for page, sequence in by_page.items():
        assert sequence[0] == "route_change", page
        assert sequence[1] == "page_enter", page
        assert sequence[-1] == "page_leave", page


def test_build_journey_target_page_has_click_event():
    random.seed(7)
    session = emulator.new_session()
    events, txn_type = emulator.build_journey(session)
    if txn_type is None:
        pytest.skip("journey without click — covered by other random seeds")
    click_events = [e for e in events if e["event_type"] == "click"]
    assert click_events, "expected at least one click event in journey with transaction"
    assert all(e["element_id"] for e in click_events)


def test_new_session_carries_demo_user_id():
    session = emulator.new_session()
    assert session.user_id in emulator.DEMO_USER_IDS
    assert session.session_id.startswith("emulator-")
