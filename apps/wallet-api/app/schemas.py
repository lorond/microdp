from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


TransactionType = Literal["deposit", "withdrawal", "payment", "transfer_in", "transfer_out"]
Direction = Literal["credit", "debit"]


class BalanceResponse(BaseModel):
    user_id: UUID
    account_id: UUID
    currency: str
    opening_balance: Decimal
    current_balance: Decimal
    updated_at: datetime


class TransactionCreate(BaseModel):
    type: TransactionType
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(default="USD", min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    description: str | None = Field(default=None, max_length=240)
    merchant: str | None = Field(default=None, max_length=120)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TransactionResponse(BaseModel):
    id: UUID
    user_id: UUID
    account_id: UUID
    type: TransactionType
    direction: Direction
    amount: Decimal
    currency: str
    description: str | None
    merchant: str | None
    metadata: dict[str, Any]
    occurred_at: datetime
    created_at: datetime
    balance_after: Decimal | None = None


class TransactionListResponse(BaseModel):
    user_id: UUID
    transactions: list[TransactionResponse]


class UserResponse(BaseModel):
    id: UUID
    full_name: str
    email: str
    created_at: datetime


class UserListResponse(BaseModel):
    users: list[UserResponse]

