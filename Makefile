.PHONY: help build-cli fmt lint lint-all test test-all py-build py-dev py-lint py-test py-typecheck setup-py setup-maturin clean

PY ?= python3
MATURIN ?= $(PY) -m maturin

help:
	@echo "Common targets:"
	@echo "  build-cli     - Build the Rust CLI (deltasort)"
	@echo "  fmt           - Run cargo fmt on the workspace"
	@echo "  lint          - Run cargo clippy with -D warnings"
	@echo "  test          - Run all Rust tests (workspace)"
	@echo "  test-all      - Run Rust tests then Python tests"
	@echo "  py-build      - Build the Python wheel (PyO3 via maturin)"
	@echo "  py-dev        - Develop-install the Python native module into current env"
	@echo "  setup-maturin - Install maturin (and patchelf on Linux)"
	@echo "  setup-py      - Install Python test/runtime deps (pytest, deltalake, pandas, pyarrow)"
	@echo "  py-test       - Run Python tests (expects native module installed via py-dev)"
	@echo "  py-typecheck  - Run mypy against the Python package"
	@echo "  clean         - Clean cargo artifacts"

build-cli:
	cargo build -p sorter-cli

fmt:
	cargo fmt --all
	cd python && $(PY) -m ruff format

lint:
	cargo clippy --workspace -- -D warnings
	cargo fmt --all --check

test:
	cargo test --workspace

# Runs Rust tests, then Python tests. Expects native module installed via `make py-dev`.
test-all:
	$(MAKE) test
	$(MAKE) py-test

lint-all:
	$(MAKE) lint
	$(MAKE) py-lint

py-build:
	$(MATURIN) build -m python/Cargo.toml

py-dev:
	$(MATURIN) develop -m python/Cargo.toml

py-lint:
	cd python && $(PY) -m ruff check
	cd python && $(PY) -m ruff format --check
	$(MAKE) py-typecheck

py-typecheck:
	cd python && $(PY) -m mypy pysrc

setup-maturin:
	$(PY) -m pip install -U "maturin[patchelf]"

setup-py:
	$(PY) -m pip install -U pytest hypothesis deltalake pandas pyarrow ruff mypy

py-test: py-dev
	pytest -q python/tests

clean:
	cargo clean
