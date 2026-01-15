# Distributed Partitioned Graph Processing

A Pregel-lite implementation for distributed graph algorithms (BFS, Connected Components) across multiple worker nodes with hash-based vertex partitioning.

**Zero external dependencies** - uses only Go's standard library (`net/rpc` with JSON-RPC).

## Architecture

```
              ┌─────────────┐
              │ Coordinator │
              │  (Master)   │
              └─────┬───────┘
                    │
     ┌──────────────┼──────────────┐
     │              │              │
┌────▼────┐    ┌────▼────┐    ┌────▼────┐
│ Worker  │    │ Worker  │    │ Worker  │
│ Node 0  │    │ Node 1  │    │ Node 2  │
└─────────┘    └─────────┘    └─────────┘
```

### Components

**Coordinator (Master)**
- Assigns partitions using hash-based vertex partitioning
- Initiates and controls algorithm execution  
- Tracks convergence/termination via superstep model
- Collects and aggregates results

**Workers**
- Store local graph partitions
- Execute local BFS/CC computation
- Exchange boundary messages with peer workers via JSON-RPC
- Track ghost nodes (remote neighbors)

## Algorithms

### Distributed BFS
Superstep-based breadth-first search:

1. **Initialization**: Source node owner marks distance = 0
2. **Expansion**: Each worker processes frontier, queues remote messages
3. **Message Exchange**: Workers send `(node_id, distance)` to owners
4. **Termination**: Coordinator detects when all frontiers empty

### Connected Components (Label Propagation)
Distributed label propagation:

1. **Initialization**: Each node's label = node_id
2. **Propagation**: Update label = min(neighbor labels)
3. **Message Exchange**: Send updated labels to remote neighbors
4. **Convergence**: Stop when no labels change

## Project Structure

```
distributed_graph/
├── common/
│   └── types.go            # Shared data structures
├── coordinator/
│   └── coordinator.go      # Master node
├── worker/
│   └── worker.go           # Worker node
├── data/
│   └── sample_graph.txt    # Test graph
├── bin/                    # Built binaries
├── Makefile                # Build automation
├── run_demo.sh             # Quick demo script
└── go.mod
```

## Quick Start

### Prerequisites

- Go 1.21+ (no external dependencies needed)

### Build & Run

**Option A: Using Make**
```bash
make run-demo
```

**Option B: Using shell script**
```bash
chmod +x run_demo.sh
./run_demo.sh
```

**Option C: Manual startup**
```bash
# Build
make build

# Terminal 1-3: Start workers
./bin/worker --id 0 --workers 3 --port 50051
./bin/worker --id 1 --workers 3 --port 50052
./bin/worker --id 2 --workers 3 --port 50053

# Terminal 4: Run coordinator
./bin/coordinator --workers 3 --base-port 50051 --graph data/sample_graph.txt
```

## Usage

### Coordinator Options

```
--workers     Number of worker nodes (default: 3)
--base-port   Starting port for workers (default: 50051)
--graph       Path to edge list file (optional)
--nodes       Nodes for generated graph (default: 100)
--source      BFS source node (default: 0)
```

### Worker Options

```
--id          Worker ID (0 to num_workers-1)
--workers     Total number of workers
--port        Port to listen on
```

### Make Targets

```bash
make              # Build everything
make build        # Build worker and coordinator
make run-demo     # Run full demo with sample graph
make run-generated # Run with 100-node generated graph
make run-workers  # Start workers in background
make stop-workers # Stop background workers
make clean        # Remove build artifacts
```

## Key Design Decisions

### Hash-Based Partitioning
```go
partition := nodeID % numWorkers
```
- Simple and deterministic
- No preprocessing required
- Easy to explain in interviews

### Ghost Nodes
Workers track remote neighbors as "ghost nodes":
```go
type LocalGraphPartition struct {
    nodes      map[int64]bool           // Owned vertices
    adjacency  map[int64][]int64        // Outgoing edges
    ghostNodes map[int64]bool           // Remote neighbors
}
```

### JSON-RPC Communication
Uses Go's built-in `net/rpc` with JSON encoding for inter-node communication:
```go
client := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
client.Call("WorkerService.SendMessages", batch, &resp)
```

### Concurrency
Go's goroutines and sync primitives handle concurrent message processing:
```go
type WorkerService struct {
    incomingMessages []common.Message
    msgMu            sync.Mutex
    // ...
}
```

## Performance Metrics

The system tracks:
- **Messages per iteration**: Cross-partition communication
- **Iterations to converge**: Algorithm efficiency
- **Cross-partition edge ratio**: Partition quality

Example output:
```
==================================================
PERFORMANCE METRICS
==================================================
Total iterations: 7
Total messages exchanged: 15
Algorithm time: 89.234ms
Cross-partition edges (ghost nodes): 21

Per-worker statistics:
  Worker 0: 9 nodes, 7 ghosts, 5 sent, 5 recv
  Worker 1: 7 nodes, 7 ghosts, 5 sent, 4 recv
  Worker 2: 7 nodes, 7 ghosts, 5 sent, 6 recv
==================================================
```

## Fault Tolerance (Conceptual)

This implementation includes minimal fault tolerance:
- Stateless coordinator (restartable)
- Workers can checkpoint state per N iterations

Production systems would add:
- Worker heartbeats
- State replication
- Automatic recovery

## Tech Stack

- **Go 1.21+** - Workers and coordinator
- **net/rpc + JSON-RPC** - Inter-node communication (standard library)
- **No external dependencies** - Pure Go implementation

## Interview Talking Points

1. **Partitioned State**: Hash-based vertex partitioning, ghost nodes
2. **Boundary Communication**: JSON-RPC message batching between workers
3. **Termination Detection**: Coordinator aggregates convergence signals
4. **Superstep Model**: Pregel-style BSP execution
5. **Concurrency**: Go channels/mutexes for thread-safe message handling
6. **Metrics Collection**: Cross-partition ratio, message volume

## Extending to gRPC

To use gRPC instead of JSON-RPC (for production):

1. Add proto definitions in `proto/graph.proto`
2. Generate Go code: `protoc --go_out=. --go-grpc_out=. proto/graph.proto`
3. Replace `net/rpc` calls with gRPC client/server
4. Add `google.golang.org/grpc` dependency

The core algorithms and data structures remain identical.

## References

- [Pregel Paper](https://research.google/pubs/pub36726/)
- [SNAP Datasets](https://snap.stanford.edu/data/)
