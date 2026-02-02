.PHONY: help install test lint format format-check validate

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'


install: ## Install project dependencies
	@echo "Installing dependencies..."
	curl -LsSf https://astral.sh/uv/install.sh | sh || true
	uv sync
	@echo "Dependencies installed"

test: ## Run all tests
	@echo "Running tests..."
	uv run pytest tests/ -v
	@echo "Tests passed"

test-fast: ## Run fast tests only (skip slow)
	@echo "Running fast tests..."
	uv run pytest tests/ -v -m "not slow"

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	uv run pytest tests/ --cov=src --cov-report=html --cov-report=term
	@echo "Coverage report: htmlcov/index.html"

lint: ## Lint code with ruff
	@echo "Linting code..."
	uv run ruff check src/ tests/
	@echo "Linting passed"

format: ## Format code with black
	@echo "Formatting code..."
	uv run black src/ tests/
	@echo "Code formatted"

format-check: ## Check code formatting
	@echo "Checking code format..."
	uv run black --check src/ tests/

validate: format lint test ## Run format, lint, and tests
	@echo "All validations passed"