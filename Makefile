.PHONY: up down migrate dev worker test chat history psql logs

SESSION ?= default
MSG     ?= hello

up:
	docker compose up -d

down:
	docker compose down

migrate:
	uv run alembic upgrade head

dev:
	uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

worker:
	uv run python -m worker.main

test:
	uv run pytest -v

chat:
	uv run python cli.py chat --session $(SESSION) "$(MSG)"

history:
	uv run python cli.py history $(SESSION)

psql:
	docker compose exec postgres psql -U app -d chatdb

logs:
	docker compose logs -f
