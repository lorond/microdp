from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .repository import (
    InsufficientFunds,
    WalletRepository,
    build_connection_pool,
    get_repository,
)
from .schemas import BalanceResponse, TransactionCreate, TransactionListResponse, TransactionResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = build_connection_pool()
    pool.open(wait=True)
    app.state.db_pool = pool
    try:
        yield
    finally:
        pool.close()


app = FastAPI(title="MicroDP Wallet API", version="0.1.0", lifespan=lifespan)

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
    return {"status": "ok"}


@app.get("/api/users/{user_id}/balance", response_model=BalanceResponse)
def get_balance(
    user_id: UUID, repository: WalletRepository = Depends(get_repository)
) -> BalanceResponse:
    try:
        return repository.get_balance(user_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/users/{user_id}/transactions", response_model=TransactionListResponse)
def list_transactions(
    user_id: UUID,
    limit: int = Query(default=50, ge=1, le=100),
    repository: WalletRepository = Depends(get_repository),
) -> TransactionListResponse:
    try:
        transactions = repository.list_transactions(user_id, limit=limit)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TransactionListResponse(user_id=user_id, transactions=transactions)


@app.post("/api/users/{user_id}/transactions", response_model=TransactionResponse, status_code=201)
def create_transaction(
    user_id: UUID,
    payload: TransactionCreate,
    repository: WalletRepository = Depends(get_repository),
) -> TransactionResponse:
    try:
        return repository.create_transaction(user_id, payload)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except InsufficientFunds as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
