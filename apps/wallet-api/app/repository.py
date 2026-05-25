from uuid import UUID

from fastapi import Request
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .schemas import BalanceResponse, TransactionCreate, TransactionResponse
from .settings import settings


CREDIT_TYPES = {"deposit", "transfer_in"}
DEBIT_TYPES = {"withdrawal", "payment", "transfer_out"}


class InsufficientFunds(ValueError):
    pass


def direction_for_type(transaction_type: str) -> str:
    if transaction_type in CREDIT_TYPES:
        return "credit"
    if transaction_type in DEBIT_TYPES:
        return "debit"
    raise ValueError(f"Unsupported transaction type: {transaction_type}")


class WalletRepository:
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def _connect(self):
        return self.pool.connection()

    def get_balance(self, user_id: UUID) -> BalanceResponse:
        with self._connect() as conn:
            account = conn.execute(
                """
                SELECT id AS account_id, user_id, currency, opening_balance,
                       current_balance, updated_at
                FROM accounts
                WHERE user_id = %s
                ORDER BY created_at
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()

        if not account:
            raise LookupError(f"Account for user {user_id} was not found")

        return BalanceResponse(**account)

    def list_transactions(self, user_id: UUID, limit: int = 50) -> list[TransactionResponse]:
        with self._connect() as conn:
            account_exists = conn.execute(
                "SELECT 1 FROM accounts WHERE user_id = %s LIMIT 1",
                (user_id,),
            ).fetchone()
            if not account_exists:
                raise LookupError(f"Account for user {user_id} was not found")
            rows = conn.execute(
                """
                SELECT id, user_id, account_id, type, direction, amount, currency,
                       description, merchant, metadata, balance_after,
                       occurred_at, created_at
                FROM transactions
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (user_id, limit),
            ).fetchall()
        return [TransactionResponse(**row) for row in rows]

    def create_transaction(
        self, user_id: UUID, payload: TransactionCreate
    ) -> TransactionResponse:
        direction = direction_for_type(payload.type)
        currency = payload.currency
        amount = payload.amount

        with self._connect() as conn:
            with conn.transaction():
                account = conn.execute(
                    """
                    SELECT id, currency, current_balance
                    FROM accounts
                    WHERE user_id = %s AND currency = %s
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (user_id, currency),
                ).fetchone()

                if not account:
                    raise LookupError(
                        f"Account for user {user_id} and currency {currency} was not found"
                    )

                balance_before = account["current_balance"]
                signed_amount = amount if direction == "credit" else -amount
                balance_after = balance_before + signed_amount

                if balance_after < 0:
                    raise InsufficientFunds("Insufficient funds for debit transaction")

                conn.execute(
                    """
                    UPDATE accounts
                    SET current_balance = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (balance_after, account["id"]),
                )

                row = conn.execute(
                    """
                    INSERT INTO transactions (
                        user_id, account_id, type, direction, amount, currency,
                        description, merchant, metadata, balance_after
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, user_id, account_id, type, direction, amount, currency,
                              description, merchant, metadata, balance_after,
                              occurred_at, created_at
                    """,
                    (
                        user_id,
                        account["id"],
                        payload.type,
                        direction,
                        amount,
                        currency,
                        payload.description,
                        payload.merchant,
                        Jsonb(payload.metadata),
                        balance_after,
                    ),
                ).fetchone()

        return TransactionResponse(**row)


def build_connection_pool() -> ConnectionPool:
    return ConnectionPool(
        conninfo=settings.database_url,
        kwargs={"row_factory": dict_row},
        min_size=1,
        max_size=10,
        open=False,
    )


def get_repository(request: Request) -> WalletRepository:
    return WalletRepository(request.app.state.db_pool)
