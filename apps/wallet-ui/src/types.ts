export type User = {
  id: string;
  full_name: string;
  email: string;
  created_at: string;
};

export type TransactionType =
  | "deposit"
  | "withdrawal"
  | "payment"
  | "transfer_in"
  | "transfer_out";

export type Balance = {
  user_id: string;
  account_id: string;
  currency: string;
  opening_balance: string;
  current_balance: string;
  updated_at: string;
};

export type Transaction = {
  id: string;
  user_id: string;
  account_id: string;
  type: TransactionType;
  direction: "credit" | "debit";
  amount: string;
  currency: string;
  description: string | null;
  merchant: string | null;
  metadata: Record<string, unknown>;
  occurred_at: string;
  created_at: string;
  balance_after?: string | null;
};

export type ClickstreamEvent = {
  event_id: string;
  session_id: string;
  user_id: string;
  event_type: "page_enter" | "page_leave" | "route_change" | "click" | "mouse_move";
  page: string;
  ts: string;
  element_id?: string | null;
  x?: number | null;
  y?: number | null;
  dwell_ms?: number | null;
  payload: Record<string, unknown>;
};

