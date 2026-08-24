# Telegram

The bot uses long polling and accepts commands only when both chat and optional sender user are
allowlisted through `TELEGRAM_ALLOWED_CHAT_IDS` and `TELEGRAM_ALLOWED_USER_IDS`.

Supported commands are `/status`, `/today`, `/all`, `/open`, `/last`, `/health`, `/pause`,
`/resume`, `/config`, `/rejections`, and `/day YYYY-MM-DD`.

Telegram cannot change risk limits, secrets, strategy configuration, drawdown state, or runtime
mode. Unauthorized commands are ignored and logged without exposing IDs or system state.

Trade and report notifications use PostgreSQL transactional outbox. Delivery is marked only
after Telegram returns a message ID, and daily-report delivery updates the report row in the same
transaction. Known transient failures retry with bounded backoff and Telegram outages do not stop
event collection. If the process dies after sending but before storing the message ID, the expired
lease becomes `UNCERTAIN` instead of being resent, because Telegram provides no idempotency key
that can prove exactly-once delivery. Permanent failures and ten exhausted attempts become `DEAD`.

An operator must reconcile each `UNCERTAIN` row against Telegram before changing its state. After
confirming the actual outcome, run exactly one of:

```bash
python scripts/reconcile_outbox.py EVENT_ID delivered --telegram-message-id MESSAGE_ID
python scripts/reconcile_outbox.py EVENT_ID retry
python scripts/reconcile_outbox.py EVENT_ID dead
```

Use `retry` only after confirming that Telegram did not deliver the message. The command locks the
row and refuses any state other than `UNCERTAIN`; marking a daily report delivered updates its
stored report metadata in the same transaction.
