.PHONY: build build-only run deps test lint lint-fix fmt clean clean-all

BINARY_NAME=dronc
DIST_DIR=dist

build: deps build-only

build-only:
	@echo "Building..."
	@mkdir -p $(DIST_DIR)
	@go build -o $(DIST_DIR)/$(BINARY_NAME) cmd/main.go

run: build
	@echo "Running..."
	@./$(DIST_DIR)/$(BINARY_NAME)

deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

test:
	@echo "Running tests..."
	@go test -v ./...

lint:
	@echo "Running linter..."
	@golangci-lint run

lint-fix:
	@echo "Running linter with auto-fix..."
	@golangci-lint run --fix

fmt:
	@echo "Formatting code..."
	@gofmt -w .
	@goimports -w .

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(DIST_DIR)

clean-all: clean
	@echo "Cleaning module cache..."
	@go clean -modcache
