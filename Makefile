.PHONY: help install migrate dev-http dev-worker dev-worker-2 dev-scheduler \
        test test-integration test-all lint format health

help:  ## 列出所有可用目标
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36mmake %-18s\033[0m %s\n", $$1, $$2}'

install:  ## 安装依赖(uv sync)
	uv sync

migrate:  ## 跑数据库迁移(alembic upgrade head)
	uv run alembic upgrade head

dev-http:  ## 启 HTTP API 进程(:8000, --reload)
	uv run uvicorn app.entrypoints.http.app:app --reload --port 8000

dev-worker:  ## 启 Worker 进程(:8001)
	uv run uvicorn app.entrypoints.worker:app --port 8001

dev-worker-2:  ## 启 Worker 进程(:8001,--workers 2 双进程)
	uv run uvicorn app.entrypoints.worker:app --port 8001 --workers 2

dev-scheduler:  ## 启 Scheduler 进程(:8002)
	uv run uvicorn app.entrypoints.scheduler:app --port 8002

test:  ## 跑 unit 测试(默认,零基础设施依赖)
	uv run pytest

test-integration:  ## 跑 integration 测试(需真实 PG/Redis/RabbitMQ)
	uv run pytest -m integration

test-all:  ## 跑全部测试(含 contract / e2e)
	uv run pytest -m ""

lint:  ## ruff check + import-linter
	uv run ruff check .
	uv run lint-imports

format:  ## ruff format
	uv run ruff format .

health:  ## HTTP 健康检查(curl :8000/health)
	curl -s localhost:8000/health
