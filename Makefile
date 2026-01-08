.PHONY: test test-unit test-race test-integration test-bench bench lint lint-fix coverage coverage-all clean install-tools install-hooks all help check ci

.DEFAULT_GOAL := help

# Default target
all: test lint

# Display help
help:
	@echo "ASTQL Development Commands"
	@echo "=========================="
	@echo ""
	@echo "Testing & Quality:"
	@echo "  make test             - Run unit tests (fast)"
	@echo "  make test-unit        - Run unit tests (alias for test)"
	@echo "  make test-race        - Run unit tests with race detector"
	@echo "  make test-integration - Run integration tests (requires Docker)"
	@echo "  make test-all         - Run all tests including integration"
	@echo "  make test-bench       - Run benchmarks (alias for bench)"
	@echo "  make bench            - Run benchmarks"
	@echo "  make lint             - Run golangci-lint"
	@echo "  make lint-fix         - Run golangci-lint with auto-fix"
	@echo "  make coverage         - Generate coverage report (unit tests)"
	@echo "  make coverage-all     - Generate coverage report (all tests, requires Docker)"
	@echo "  make check            - Run lint + tests with race detector (pre-commit)"
	@echo "  make ci               - Full CI simulation locally (requires Docker)"
	@echo ""
	@echo "Other:"
	@echo "  make install-tools    - Install required development tools"
	@echo "  make install-hooks    - Install git pre-commit hook"
	@echo "  make clean            - Clean generated files"
	@echo "  make all              - Run tests and lint (default)"

# Run unit tests only (skip integration tests)
test:
	@echo "Running unit tests..."
	@go test -v -short ./...

# Alias for test (checklist compliance)
test-unit: test

# Run unit tests with race detector
test-race:
	@echo "Running unit tests with race detector..."
	@go test -v -race -short ./...

# Run integration tests only (requires Docker)
test-integration:
	@echo "Running integration tests..."
	@go test -v ./testing/integration/...

# Run all tests including integration
test-all:
	@echo "Running all tests..."
	@go test -v ./...

# Run benchmarks (exclude integration test directory)
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=1s -short ./...

# Alias for bench (checklist compliance)
test-bench: bench

# Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run --config=.golangci.yml --timeout=5m

# Run linters with auto-fix
lint-fix:
	@echo "Running linters with auto-fix..."
	@golangci-lint run --config=.golangci.yml --fix

# Generate coverage report (unit tests only)
coverage:
	@echo "Generating coverage report..."
	@go test -short -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report generated: coverage.html"

# Generate coverage report including integration tests (requires Docker)
coverage-all:
	@echo "Generating full coverage report (including integration tests)..."
	@echo "=== Unit tests ==="
	@go test -short -coverprofile=unit.out -covermode=atomic -coverpkg=./... ./...
	@echo "Unit coverage:"
	@go tool cover -func=unit.out | tail -1
	@echo ""
	@echo "=== Integration tests ==="
	@cd testing/integration && go test -coverprofile=integration.out -covermode=atomic \
		-coverpkg=github.com/zoobzio/astql/... ./...
	@echo "Integration coverage:"
	@go tool cover -func=testing/integration/integration.out | tail -1
	@echo ""
	@echo "=== Merging profiles ==="
	@go run github.com/wadey/gocovmerge@latest unit.out testing/integration/integration.out > coverage.out
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Combined coverage:"
	@go tool cover -func=coverage.out | tail -1
	@echo "Full coverage report generated: coverage.html"

# Clean generated files
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete
	@go clean -cache

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

# Install git pre-commit hook
install-hooks:
	@echo "Installing git hooks..."
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make check' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed!"

# Quick check - run tests with race detector and lint (pre-commit)
check: lint test-race
	@echo "All checks passed!"

# CI simulation - full CI pipeline locally (requires Docker)
ci: clean lint test-race test-integration coverage-all bench
	@echo "CI simulation complete!"