import type { Balance, Transaction, TransactionType, User } from "./types";

// Единственный жёстко прописанный ID — пользователь Demo. Используется как
// default-selection в UI до того, как fetchUsers() вернёт реальный список из БД.
// Все остальные пользователи генерируются init-скриптом Postgres со случайными
// UUID и подтягиваются динамически — см. apps/wallet-api/app/main.py:list_users.
export const DEMO_USER_ID = "00000000-0000-0000-0000-000000000001";

export async function fetchUsers(): Promise<User[]> {
  const response = await fetch("/api/users");
  if (!response.ok) {
    throw new Error(`Users request failed: ${response.status}`);
  }
  const payload = await response.json();
  return payload.users;
}

export async function fetchBalance(userId = DEMO_USER_ID): Promise<Balance> {
  const response = await fetch(`/api/users/${userId}/balance`);
  if (!response.ok) {
    throw new Error(`Balance request failed: ${response.status}`);
  }
  return response.json();
}

export async function fetchTransactions(userId = DEMO_USER_ID): Promise<Transaction[]> {
  const response = await fetch(`/api/users/${userId}/transactions?limit=25`);
  if (!response.ok) {
    throw new Error(`Transactions request failed: ${response.status}`);
  }
  const payload = await response.json();
  return payload.transactions;
}

export async function createTransaction(input: {
  userId?: string;
  type: TransactionType;
  amount: string;
  description?: string;
  merchant?: string;
}): Promise<Transaction> {
  const response = await fetch(`/api/users/${input.userId ?? DEMO_USER_ID}/transactions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: input.type,
      amount: input.amount,
      currency: "USD",
      description: input.description,
      merchant: input.merchant,
      metadata: { source: "wallet-ui" }
    })
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail ?? `Create transaction failed: ${response.status}`);
  }

  return response.json();
}
