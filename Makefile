.PHONY: test test-race test-integration bench lint lint-fix coverage clean install-tools all help check ci

# Default target
all: test lint

# Display help
help:
	@echo "ASTQL Development Commands"
	@echo "=========================="
	@echo ""
	@echo "Testing & Quality:"
	@echo "  make test             - Run unit tests (fast)"
	@echo "  make test-race        - Run unit tests with race detector"
	@echo "  make test-integration - Run integration tests (requires Docker)"
	@echo "  make test-all         - Run all tests including integration"
	@echo "  make bench            - Run benchmarks"
	@echo "  make lint             - Run golangci-lint"
	@echo "  make lint-fix         - Run golangci-lint with auto-fix"
	@echo "  make coverage         - Generate coverage report (HTML)"
	@echo "  make check            - Run tests and lint (quick check)"
	@echo ""
	@echo "Other:"
	@echo "  make install-tools    - Install required development tools"
	@echo "  make clean            - Clean generated files"
	@echo "  make all              - Run tests and lint (default)"
	@echo "  make ci               - Full CI simulation"

# Run unit tests only (skip integration tests)
test:
	@echo "Running unit tests..."
	@go test -v -short ./...

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
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Quick check - run tests and lint
check: test lint
	@echo "All checks passed!"

# CI simulation - what CI runs
ci: clean lint test-race coverage bench
	@echo "CI simulation complete!"