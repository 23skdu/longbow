.PHONY: all build build-adbc test test-adbc test-adbc-python test-adbc-so clean install-commit-hook install-hooks accuracy-gate oracle-gate test-coverage test-fuzz pre-commit test-docker

all: build build-adbc

build:
	go build ./...

build-adbc:
	@echo "Building ADBC driver as C-shared library..."
	go build -buildmode=c-shared -o liblongbow_adbc.so ./cmd/adbc

test:
	go test -v ./...

test-adbc:
	go test -v -cover ./internal/adbc/...

test-adbc-python: build-adbc
	python3 scripts/verify_driver.py

test-adbc-so: build-adbc
	python3 tests/adbc/test_driver_so.py

clean:
	rm -f liblongbow_adbc.so liblongbow_adbc.h


# Observability conformance (docs/roadmap.md Items 18-20, 24).
# Regenerates the metric inventory the conformance gate checks against. Run this
# after adding, renaming or removing a Prometheus metric, and commit the result
# alongside the change - the gate fails if the two disagree.
observability-inventory:
	go run ./tools/metrics-inventory > internal/observability/testdata/registered_metrics.tsv

# Fails if an alert, dashboard panel or docs/metrics.md entry references a
# metric family that is not registered. This is the check that catches alerts
# wired to metrics that do not exist, which are permanently silent because an
# expression over an unknown series is empty rather than an error.
observability-check: observability-inventory
	go test ./internal/observability/ -count=1

# Parse-checks rules.yml and runs the promtool unit tests. Needs promtool on
# PATH, or PROMTOOL=/path/to/promtool. Install from a Prometheus release archive.
observability-rules:
	grafana/tests/test-rules.sh

# Pre-commit hook installation and quality gates
install-commit-hook:
	@echo "Installing pre-commit hook..."
	@mkdir -p .git/hooks
	@ln -sf ../../scripts/pre-commit.sh .git/hooks/pre-commit || cp scripts/pre-commit.sh .git/hooks/pre-commit
	@chmod +x scripts/pre-commit.sh .git/hooks/pre-commit
	@echo "Pre-commit hook installed successfully into .git/hooks/pre-commit."

install-hooks: install-commit-hook

accuracy-gate:
	@echo "Running Accuracy Gate (numerical precision across 15 vector dtypes)..."
	go test -v ./internal/simd/ -run='^TestSIMDAccuracy'

oracle-gate:
	@echo "Running Oracle Gate (ground-truth parity vs reference oracles)..."
	go test -v ./internal/simd/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'

test-coverage:
	@echo "Verifying 100% Package Test Coverage Gate..."
	@UNTESTED=$$(go test -list '.*' ./... 2>&1 | grep '\[no test files\]' || true); \
	if [ -n "$$UNTESTED" ]; then \
		echo "Found packages without test files:"; \
		echo "$$UNTESTED"; \
		exit 1; \
	fi
	@echo "All packages have active test coverage."
	go test -v -cover ./...

test-fuzz:
	@echo "Running fuzz tests..."
	go test -run='^Fuzz' ./...

pre-commit:
	@./scripts/pre-commit.sh

test-docker:
	@echo "Running Dockerfile unit tests (Go & Python)..."
	go test -v ./tests/docker/...
	python3 tests/docker/test_dockerfiles.py
