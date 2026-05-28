.PHONY: up down build migrate dev worker persistence test chat history psql logs

SESSION ?= default
MSG     ?= hello

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

migrate:
	docker compose exec api python -m alembic upgrade head

dev:
	docker compose up api --no-deps

worker:
	docker compose up worker --no-deps

persistence:
	docker compose up persistence --no-deps

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
