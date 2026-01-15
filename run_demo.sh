#!/bin/bash
# Run distributed graph demo locally

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_WORKERS=${1:-3}
BASE_PORT=50051

echo "=================================================="
echo "Distributed Graph Processing Demo (Go)"
echo "=================================================="

# Build binaries
echo "Building..."
mkdir -p bin
go build -o bin/worker ./worker/worker.go
go build -o bin/coordinator ./coordinator/coordinator.go

# Start workers in background
echo "Starting $NUM_WORKERS workers..."
WORKER_PIDS=()

for i in $(seq 0 $((NUM_WORKERS - 1))); do
    PORT=$((BASE_PORT + i))
    ./bin/worker --id $i --workers $NUM_WORKERS --port $PORT &
    WORKER_PIDS+=($!)
    echo "  Worker $i started on port $PORT (PID: ${WORKER_PIDS[-1]})"
done

# Wait for workers to start
sleep 2

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down workers..."
    for pid in "${WORKER_PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    exit 0
}

trap cleanup SIGINT SIGTERM

# Run coordinator
echo ""
echo "Starting coordinator..."
./bin/coordinator --workers $NUM_WORKERS --base-port $BASE_PORT \
                  --graph data/sample_graph.txt --source 0

# Cleanup
cleanup
