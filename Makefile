.PHONY: all build build-adbc test test-adbc clean

all: build build-adbc

build:
	go build ./...

build-adbc:
	@echo "Building ADBC driver as C-shared library..."
	go build -buildmode=c-shared -o liblongbow_adbc.so ./cmd/adbc

test:
	go test -v ./...

test-adbc:
	go test -v ./internal/adbc/...

test-adbc-python: build-adbc
	python3 tests/adbc/verify_driver.py

clean:
	rm -f liblongbow_adbc.so liblongbow_adbc.h