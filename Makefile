all: deps lint test

uv:
	@which uv >/dev/null 2>&1 || { \
		echo "❌ uv is not installed"; \
		exit 1;\
	}

deps: uv
	@uv sync --all-extras

format:
	@uv run ruff format asyncpg_lock tests example
	@uv run ruff check --fix asyncpg_lock tests example

pyright:
	@uv run pyright

lint: format pyright

test:
	@uv run pytest -vv --rootdir tests .
