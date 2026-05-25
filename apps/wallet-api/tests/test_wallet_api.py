from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from fastapi.testclient import TestClient

from app.main import app
from app.repository import direction_for_type, get_repository
from app.schemas import BalanceResponse, TransactionCreate, TransactionResponse, UserResponse


USER_ID = UUID("00000000-0000-0000-0000-000000000001")
ACCOUNT_ID = UUID("10000000-0000-0000-0000-000000000001")


class FakeRepository:
    def __init__(self):
        self.balance = Decimal("100.00")

    def list_users(self):
        return [
            UserResponse(
                id=USER_ID,
                full_name="Demo",
                email="demo@example.com",
                created_at=datetime.now(UTC),
            )
        ]

    def get_balance(self, user_id):
        return BalanceResponse(
            user_id=user_id,
            account_id=ACCOUNT_ID,
            currency="USD",
            opening_balance=Decimal("100.00"),
            current_balance=self.balance,
            updated_at=datetime.now(UTC),
        )

    def list_transactions(self, user_id, limit=50):
        return []

    def create_transaction(self, user_id, payload: TransactionCreate):
        direction = direction_for_type(payload.type)
        self.balance += payload.amount if direction == "credit" else -payload.amount
        return TransactionResponse(
            id=UUID("20000000-0000-0000-0000-000000000001"),
            user_id=user_id,
            account_id=ACCOUNT_ID,
            type=payload.type,
            direction=direction,
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
            merchant=payload.merchant,
            metadata=payload.metadata,
            occurred_at=datetime.now(UTC),
            created_at=datetime.now(UTC),
            balance_after=self.balance,
        )


def test_direction_mapping():
    assert direction_for_type("deposit") == "credit"
    assert direction_for_type("payment") == "debit"


def test_create_transaction_updates_balance_contract():
    fake_repo = FakeRepository()
    app.dependency_overrides[get_repository] = lambda: fake_repo
    client = TestClient(app)

    response = client.post(
        f"/api/users/{USER_ID}/transactions",
        json={"type": "deposit", "amount": "25.00", "currency": "USD"},
    )

    assert response.status_code == 201
    assert response.json()["balance_after"] == "125.00"

    balance = client.get(f"/api/users/{USER_ID}/balance")
    assert balance.status_code == 200
    assert balance.json()["current_balance"] == "125.00"

    app.dependency_overrides.clear()


class UnknownUserRepository:
    def get_balance(self, user_id):
        raise LookupError(f"Account for user {user_id} was not found")

    def list_transactions(self, user_id, limit=50):
        raise LookupError(f"Account for user {user_id} was not found")

    def create_transaction(self, user_id, payload):
        raise LookupError(f"Account for user {user_id} was not found")


def test_list_transactions_unknown_user_returns_404():
    app.dependency_overrides[get_repository] = lambda: UnknownUserRepository()
    client = TestClient(app)
    response = client.get(f"/api/users/{UUID('99999999-9999-9999-9999-999999999999')}/transactions")
    assert response.status_code == 404
    app.dependency_overrides.clear()


def test_get_balance_unknown_user_returns_404():
    app.dependency_overrides[get_repository] = lambda: UnknownUserRepository()
    client = TestClient(app)
    response = client.get(f"/api/users/{UUID('99999999-9999-9999-9999-999999999999')}/balance")
    assert response.status_code == 404
    app.dependency_overrides.clear()


class InsufficientFundsRepository:
    def get_balance(self, user_id):  # pragma: no cover - unused in this test
        raise NotImplementedError

    def list_transactions(self, user_id, limit=50):  # pragma: no cover
        raise NotImplementedError

    def create_transaction(self, user_id, payload):
        from app.repository import InsufficientFunds
        raise InsufficientFunds("Insufficient funds for debit transaction")


def test_list_users_returns_seeded_users():
    fake_repo = FakeRepository()
    app.dependency_overrides[get_repository] = lambda: fake_repo
    client = TestClient(app)

    response = client.get("/api/users")
    assert response.status_code == 200
    payload = response.json()
    assert "users" in payload and len(payload["users"]) >= 1
    assert payload["users"][0]["full_name"] == "Demo"

    app.dependency_overrides.clear()


def test_create_transaction_insufficient_funds_returns_409():
    app.dependency_overrides[get_repository] = lambda: InsufficientFundsRepository()
    client = TestClient(app)
    response = client.post(
        f"/api/users/{USER_ID}/transactions",
        json={"type": "payment", "amount": "999999.00", "currency": "USD"},
    )
    assert response.status_code == 409
    assert "Insufficient funds" in response.json()["detail"]
    app.dependency_overrides.clear()

