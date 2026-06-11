.PHONY: up down build migrate dev worker persistence test chat history psql logs \
        prod-up prod-down prod-build prod-migrate prod-logs prod-locust-scale

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

prod-build:
	docker compose -f docker-compose.prod.yaml build

prod-up:
	docker compose -f docker-compose.prod.yaml up -d

prod-down:
	docker compose -f docker-compose.prod.yaml down

prod-migrate:
	docker compose -f docker-compose.prod.yaml exec api uv run alembic upgrade head

prod-logs:
	docker compose -f docker-compose.prod.yaml logs -f

LOCUST_WORKERS ?= 4
prod-locust-scale:
	docker compose -f docker-compose.prod.yaml up -d --scale locust-worker=$(LOCUST_WORKERS)
