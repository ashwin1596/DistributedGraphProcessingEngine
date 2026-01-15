.PHONY: all build worker coordinator clean run-workers run-demo stop-workers help

# Variables
NUM_WORKERS ?= 3
BASE_PORT ?= 50051
GRAPH_FILE ?= data/sample_graph.txt
SOURCE_NODE ?= 0

all: build

# Build all binaries
build:
	@echo "Building worker..."
	@mkdir -p bin
	go build -o bin/worker ./worker/worker.go
	@echo "Building coordinator..."
	go build -o bin/coordinator ./coordinator/coordinator.go
	@echo "Build complete!"

# Build individual components
worker:
	@mkdir -p bin
	go build -o bin/worker ./worker/worker.go

coordinator:
	@mkdir -p bin
	go build -o bin/coordinator ./coordinator/coordinator.go

# Run workers in background
run-workers: build
	@echo "Starting $(NUM_WORKERS) workers..."
	@for i in $$(seq 0 $$(($(NUM_WORKERS) - 1))); do \
		PORT=$$(($(BASE_PORT) + $$i)); \
		./bin/worker --id $$i --workers $(NUM_WORKERS) --port $$PORT & \
		echo "  Worker $$i started on port $$PORT"; \
	done
	@sleep 2
	@echo "Workers ready!"

# Run full demo
run-demo: build
	@echo "=================================================="
	@echo "Distributed Graph Processing Demo (Go)"
	@echo "=================================================="
	@echo "Starting $(NUM_WORKERS) workers..."
	@for i in $$(seq 0 $$(($(NUM_WORKERS) - 1))); do \
		PORT=$$(($(BASE_PORT) + $$i)); \
		./bin/worker --id $$i --workers $(NUM_WORKERS) --port $$PORT & \
		echo "  Worker $$i started on port $$PORT"; \
	done
	@sleep 2
	@echo ""
	@echo "Running coordinator..."
	@./bin/coordinator --workers $(NUM_WORKERS) --base-port $(BASE_PORT) \
	                  --graph $(GRAPH_FILE) --source $(SOURCE_NODE) || true
	@echo ""
	@echo "Stopping workers..."
	@pkill -f "bin/worker" 2>/dev/null || true
	@echo "Done!"

# Run with generated graph
run-generated: build
	@echo "Running with generated 100-node graph..."
	@for i in $$(seq 0 $$(($(NUM_WORKERS) - 1))); do \
		PORT=$$(($(BASE_PORT) + $$i)); \
		./bin/worker --id $$i --workers $(NUM_WORKERS) --port $$PORT & \
	done
	@sleep 2
	@./bin/coordinator --workers $(NUM_WORKERS) --base-port $(BASE_PORT) --nodes 100 || true
	@pkill -f "bin/worker" 2>/dev/null || true

# Stop all workers
stop-workers:
	@echo "Stopping workers..."
	@pkill -f "bin/worker" 2>/dev/null || true
	@echo "Done!"

# Clean build artifacts
clean:
	rm -rf bin/

# Show help
help:
	@echo "Distributed Graph Processing - Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make              Build everything"
	@echo "  make build        Build worker and coordinator"
	@echo "  make run-demo     Run full demo with sample graph"
	@echo "  make run-generated Run with 100-node generated graph"
	@echo "  make run-workers  Start workers in background"
	@echo "  make stop-workers Stop background workers"
	@echo "  make clean        Remove build artifacts"
	@echo ""
	@echo "Variables:"
	@echo "  NUM_WORKERS=$(NUM_WORKERS)  Number of workers"
	@echo "  BASE_PORT=$(BASE_PORT)    Starting port"
	@echo "  GRAPH_FILE=$(GRAPH_FILE)"
	@echo "  SOURCE_NODE=$(SOURCE_NODE)      BFS source"
