.PHONY: help build-cli fmt lint test py-build py-dev py-test setup-py setup-maturin clean

PY ?= python3
MATURIN ?= $(PY) -m maturin

help:
	@echo "Common targets:"
	@echo "  build-cli     - Build the Rust CLI (deltasort)"
	@echo "  fmt           - Run cargo fmt on the workspace"
	@echo "  lint          - Run cargo clippy with -D warnings"
	@echo "  test          - Run Rust library tests (sorter-core)"
	@echo "  py-build      - Build the Python wheel (PyO3 via maturin)"
	@echo "  py-dev        - Develop-install the Python native module into current env"
	@echo "  setup-maturin - Install maturin (and patchelf on Linux)"
	@echo "  setup-py      - Install Python test/runtime deps (pytest, deltalake, pandas, pyarrow)"
	@echo "  py-test       - Run Python tests (expects native module installed via py-dev)"
	@echo "  clean         - Clean cargo artifacts"

build-cli:
	cargo build -p sorter-cli

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace -- -D warnings

test:
	cargo test -p sorter-core --lib

py-build:
	$(MATURIN) build -m crates/sorter-py/Cargo.toml

py-dev:
	$(MATURIN) develop -m crates/sorter-py/Cargo.toml

setup-maturin:
	$(PY) -m pip install -U "maturin[patchelf]"

setup-py:
	$(PY) -m pip install -U pytest deltalake pandas pyarrow

py-test:
	pytest -q python/tests

clean:
	cargo clean
