.PHONY: install test lint typecheck migrate run replay verify-data audit-no-live

install:
	python -m pip install -e ".[dev]"

test:
	python -m pytest -q

lint:
	python -m ruff check .

typecheck:
	python -m mypy src

migrate:
	python -m alembic upgrade head

run:
	python -m sniper_bot.main --config configs/default.yaml

replay:
	python -m sniper_bot.main --config configs/default.yaml --replay-data data/raw --replay-speed max

verify-data:
	python scripts/verify_data.py

audit-no-live:
	python scripts/audit_no_live.py
