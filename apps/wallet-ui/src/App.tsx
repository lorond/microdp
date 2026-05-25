import { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDownLeft,
  ArrowUpRight,
  CreditCard,
  History,
  LayoutDashboard,
  MousePointerClick,
  RefreshCcw,
  Send,
  UserRound,
  Wallet
} from "lucide-react";
import { createTransaction, DEMO_USER_ID, DEMO_USERS, fetchBalance, fetchTransactions } from "./api";
import { ClickstreamTracker, type ClickstreamSnapshot } from "./clickstream";
import type { Balance, Transaction, TransactionType } from "./types";

type Page = "dashboard" | "payments" | "history" | "signals";

const pageLabels: Record<Page, string> = {
  dashboard: "Dashboard",
  payments: "Payments",
  history: "History",
  signals: "Signals"
};

const pageIcons = {
  dashboard: LayoutDashboard,
  payments: CreditCard,
  history: History,
  signals: MousePointerClick
};

function money(value: string | number | null | undefined): string {
  const parsed = Number(value ?? 0);
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD"
  }).format(parsed);
}

function signedAmount(transaction: Transaction): string {
  const prefix = transaction.direction === "credit" ? "+" : "-";
  return `${prefix}${money(transaction.amount)}`;
}

function emptySignals(): ClickstreamSnapshot {
  return {
    session_id: "",
    session_started_at: null,
    current_page: "",
    page_dwell_ms: 0,
    queue_size: 0,
    last_events: []
  };
}

function duration(valueMs: number): string {
  const seconds = Math.floor(valueMs / 1000);
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes}:${String(remainder).padStart(2, "0")}`;
}

function shortId(value: string): string {
  if (!value) return "pending";
  return value.length > 18 ? `${value.slice(0, 18)}...` : value;
}

export function App() {
  const trackerRef = useRef<ClickstreamTracker | null>(null);
  const userIdRef = useRef(DEMO_USER_ID);
  const [userId, setUserId] = useState(DEMO_USER_ID);
  const [page, setPage] = useState<Page>("dashboard");
  const [balance, setBalance] = useState<Balance | null>(null);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [amount, setAmount] = useState("24.00");
  const [transactionType, setTransactionType] = useState<TransactionType>("payment");
  const [merchant, setMerchant] = useState("Coffee Lab");
  const [status, setStatus] = useState("Ready");
  const [loading, setLoading] = useState(false);
  const [signals, setSignals] = useState<ClickstreamSnapshot>(emptySignals);

  const route = `/${page}`;
  const selectedUser = DEMO_USERS.find((user) => user.id === userId) ?? DEMO_USERS[0];

  useEffect(() => {
    const tracker = new ClickstreamTracker(() => userIdRef.current);
    trackerRef.current = tracker;
    tracker.start();
    return () => tracker.stop();
  }, []);

  useEffect(() => {
    userIdRef.current = userId;
  }, [userId]);

  useEffect(() => {
    trackerRef.current?.enterPage(route);
  }, [route]);

  useEffect(() => {
    void refreshData();
  }, [userId]);

  useEffect(() => {
    const syncSignals = () => {
      const snapshot = trackerRef.current?.getSnapshot();
      if (snapshot) setSignals(snapshot);
    };
    syncSignals();
    const timer = window.setInterval(syncSignals, 1000);
    return () => window.clearInterval(timer);
  }, []);

  const totals = useMemo(() => {
    return transactions.reduce(
      (acc, item) => {
        const amountValue = Number(item.amount);
        if (item.direction === "credit") acc.income += amountValue;
        else acc.spend += amountValue;
        return acc;
      },
      { income: 0, spend: 0 }
    );
  }, [transactions]);

  async function refreshData() {
    setLoading(true);
    try {
      const [nextBalance, nextTransactions] = await Promise.all([
        fetchBalance(userId),
        fetchTransactions(userId)
      ]);
      setBalance(nextBalance);
      setTransactions(nextTransactions);
      setStatus("Synced");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Request failed");
    } finally {
      setLoading(false);
    }
  }

  async function submitTransaction(typeOverride?: TransactionType) {
    setLoading(true);
    const type = typeOverride ?? transactionType;
    try {
      await createTransaction({
        userId,
        type,
        amount,
        description: `${pageLabels[page]} operation`,
        merchant: type === "payment" ? merchant : undefined
      });
      setStatus("Transaction created");
      await refreshData();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Transaction failed");
    } finally {
      setLoading(false);
    }
  }

  function handleTrackedClick(event: React.MouseEvent<HTMLElement>) {
    const target = event.target as HTMLElement;
    const element = target.closest<HTMLElement>("[data-track-id]");
    trackerRef.current?.click(route, element?.dataset.trackId ?? null, event.clientX, event.clientY);
  }

  function handleMouseMove(event: React.MouseEvent<HTMLElement>) {
    trackerRef.current?.mouseMove(route, event.clientX, event.clientY);
  }

  return (
    <main className="app-shell" onClick={handleTrackedClick} onMouseMove={handleMouseMove}>
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">
            <Wallet size={22} />
          </div>
          <div>
            <strong>MicroDP Wallet</strong>
            <span>{selectedUser.label}</span>
          </div>
        </div>

        <nav className="nav">
          {(Object.keys(pageLabels) as Page[]).map((key) => {
            const Icon = pageIcons[key];
            return (
              <button
                key={key}
                className={page === key ? "nav-item active" : "nav-item"}
                onClick={() => setPage(key)}
                data-track-id={`nav-${key}`}
                title={pageLabels[key]}
              >
                <Icon size={18} />
                <span>{pageLabels[key]}</span>
              </button>
            );
          })}
        </nav>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <h1>{pageLabels[page]}</h1>
            <p>{status}</p>
          </div>
          <div className="topbar-actions">
            <div className="user-switcher">
              <UserRound size={16} />
              <select
                value={userId}
                onChange={(event) => setUserId(event.target.value)}
                data-track-id="switch-demo-user"
                aria-label="Demo user"
              >
                {DEMO_USERS.map((user) => (
                  <option key={user.id} value={user.id}>
                    {user.label}
                  </option>
                ))}
              </select>
            </div>
            <button
              className="icon-button"
              onClick={() => void refreshData()}
              data-track-id="refresh-data"
              title="Refresh"
              disabled={loading}
            >
              <RefreshCcw size={18} />
            </button>
          </div>
        </header>

        {page === "dashboard" && (
          <section className="dashboard-grid">
            <div className="metric-card primary">
              <span>Available balance</span>
              <strong>{money(balance?.current_balance)}</strong>
              <small>Account {balance?.account_id.slice(0, 8) ?? "loading"}</small>
            </div>
            <div className="metric-card">
              <span>Credits</span>
              <strong>{money(totals.income)}</strong>
              <small>Last {transactions.length} records</small>
            </div>
            <div className="metric-card">
              <span>Debits</span>
              <strong>{money(totals.spend)}</strong>
              <small>Payments and withdrawals</small>
            </div>

            <div className="panel activity-panel">
              <div className="panel-title">
                <h2>Recent activity</h2>
              </div>
              <TransactionList transactions={transactions.slice(0, 6)} />
            </div>

            <div className="panel quick-actions">
              <div className="panel-title">
                <h2>Quick actions</h2>
              </div>
              <button
                className="action-button credit"
                onClick={() => void submitTransaction("deposit")}
                data-track-id="quick-action-deposit"
                disabled={loading}
              >
                <ArrowDownLeft size={18} />
                <span>Deposit</span>
              </button>
              <button
                className="action-button debit"
                onClick={() => void submitTransaction("payment")}
                data-track-id="quick-action-pay"
                disabled={loading}
              >
                <ArrowUpRight size={18} />
                <span>Pay</span>
              </button>
            </div>
          </section>
        )}

        {page === "payments" && (
          <section className="form-layout">
            <div className="panel payment-form">
              <div className="panel-title">
                <h2>Create transaction</h2>
              </div>
              <label>
                Type
                <select
                  value={transactionType}
                  onChange={(event) => setTransactionType(event.target.value as TransactionType)}
                  data-track-id="transaction-type"
                >
                  <option value="payment">Payment</option>
                  <option value="withdrawal">Withdrawal</option>
                  <option value="deposit">Deposit</option>
                  <option value="transfer_in">Transfer in</option>
                  <option value="transfer_out">Transfer out</option>
                </select>
              </label>
              <label>
                Amount
                <input
                  value={amount}
                  inputMode="decimal"
                  onChange={(event) => setAmount(event.target.value)}
                  data-track-id="transaction-amount"
                />
              </label>
              <label>
                Merchant
                <input
                  value={merchant}
                  onChange={(event) => setMerchant(event.target.value)}
                  data-track-id="transaction-merchant"
                />
              </label>
              <button
                className="submit-button"
                onClick={() => void submitTransaction()}
                data-track-id="create-transaction"
                disabled={loading}
              >
                <Send size={18} />
                <span>Create</span>
              </button>
            </div>
          </section>
        )}

        {page === "history" && (
          <section className="panel full-panel">
            <div className="panel-title">
              <h2>Transactions</h2>
            </div>
            <TransactionList transactions={transactions} />
          </section>
        )}

        {page === "signals" && (
          <section className="signals-grid">
            <div className="metric-card">
              <span>Session</span>
              <strong className="mono-value">{shortId(signals.session_id)}</strong>
              <small>{signals.session_started_at ? new Date(signals.session_started_at).toLocaleTimeString() : "Waiting"}</small>
            </div>
            <div className="metric-card">
              <span>Page dwell</span>
              <strong>{duration(signals.page_dwell_ms)}</strong>
              <small>{signals.current_page || route}</small>
            </div>
            <div className="metric-card">
              <span>Queue</span>
              <strong>{signals.queue_size}</strong>
              <small>Pending batch records</small>
            </div>
            <div className="panel full-panel">
              <div className="panel-title">
                <h2>Recent events</h2>
              </div>
              <SignalEvents events={signals.last_events} />
            </div>
          </section>
        )}
      </section>
    </main>
  );
}

function SignalEvents({ events }: { events: ClickstreamSnapshot["last_events"] }) {
  if (events.length === 0) {
    return <div className="empty-state">No events yet</div>;
  }

  return (
    <div className="signal-events">
      {events.map((event) => (
        <div className="signal-event-row" key={event.event_id}>
          <strong>{event.event_type}</strong>
          <span>{event.page}</span>
          <span>{event.element_id ?? new Date(event.ts).toLocaleTimeString()}</span>
        </div>
      ))}
    </div>
  );
}

function TransactionList({ transactions }: { transactions: Transaction[] }) {
  if (transactions.length === 0) {
    return <div className="empty-state">No transactions yet</div>;
  }

  return (
    <div className="transaction-list">
      {transactions.map((transaction) => (
        <div className="transaction-row" key={transaction.id}>
          <div className={transaction.direction === "credit" ? "txn-icon credit" : "txn-icon debit"}>
            {transaction.direction === "credit" ? (
              <ArrowDownLeft size={16} />
            ) : (
              <ArrowUpRight size={16} />
            )}
          </div>
          <div className="txn-main">
            <strong>{transaction.merchant || transaction.description || transaction.type}</strong>
            <span>{new Date(transaction.created_at).toLocaleString()}</span>
          </div>
          <strong className={transaction.direction === "credit" ? "amount credit-text" : "amount debit-text"}>
            {signedAmount(transaction)}
          </strong>
        </div>
      ))}
    </div>
  );
}
