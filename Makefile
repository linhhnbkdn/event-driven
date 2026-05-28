.PHONY: up down migrate dev worker persistence test chat history psql logs

SESSION ?= default
MSG     ?= hello

up:
	docker compose up -d

down:
	docker compose down

migrate:
	uv run alembic upgrade head

dev:
	uv run python run_api.py

worker:
	uv run python run_worker.py

persistence:
	uv run python run_persistence.py

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
