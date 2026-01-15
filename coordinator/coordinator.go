package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"distributed_graph/common"
)

// GraphLoader handles loading and partitioning graph data
type GraphLoader struct{}

func (g *GraphLoader) LoadEdgeList(filepath string) ([]common.Edge, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var edges []common.Edge
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) >= 2 {
			source, err1 := strconv.ParseInt(parts[0], 10, 64)
			target, err2 := strconv.ParseInt(parts[1], 10, 64)
			if err1 == nil && err2 == nil {
				edges = append(edges, common.Edge{Source: source, Target: target})
			}
		}
	}

	return edges, scanner.Err()
}

func (g *GraphLoader) PartitionEdges(edges []common.Edge, numWorkers int64) map[int64][]common.Edge {
	partitions := make(map[int64][]common.Edge)

	for _, edge := range edges {
		owner := edge.Source % numWorkers
		partitions[owner] = append(partitions[owner], edge)
	}

	return partitions
}

func (g *GraphLoader) GenerateSampleGraph(numNodes, edgesPerNode int) []common.Edge {
	var edges []common.Edge
	rand.Seed(time.Now().UnixNano())

	for node := 0; node < numNodes; node++ {
		targets := make(map[int]bool)
		for i := 0; i < edgesPerNode && len(targets) < numNodes-1; i++ {
			target := rand.Intn(numNodes)
			if target != node && !targets[target] {
				targets[target] = true
				edges = append(edges, common.Edge{Source: int64(node), Target: int64(target)})
			}
		}
	}

	return edges
}

// Coordinator orchestrates distributed graph processing
type Coordinator struct {
	numWorkers    int64
	workerClients map[int64]*rpc.Client
	workerAddrs   map[int64]string

	totalIterations int64
	totalMessages   int64
	algorithmTime   time.Duration
}

func NewCoordinator(numWorkers int64) *Coordinator {
	return &Coordinator{
		numWorkers:    numWorkers,
		workerClients: make(map[int64]*rpc.Client),
		workerAddrs:   make(map[int64]string),
	}
}

func (c *Coordinator) RegisterWorker(workerID int64, address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to worker %d: %v", workerID, err)
	}

	client := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
	c.workerClients[workerID] = client
	c.workerAddrs[workerID] = address

	log.Printf("[Coordinator] Registered worker %d at %s", workerID, address)
	return nil
}

func (c *Coordinator) Close() {
	for _, client := range c.workerClients {
		client.Close()
	}
}

func (c *Coordinator) BroadcastWorkerList() {
	var workers []common.WorkerInfo
	for wid, addr := range c.workerAddrs {
		workers = append(workers, common.WorkerInfo{WorkerID: wid, Address: addr})
	}

	workerList := &common.WorkerList{Workers: workers}

	for workerID, client := range c.workerClients {
		var resp common.Ack
		err := client.Call("WorkerService.SetWorkerList", workerList, &resp)
		if err != nil {
			log.Printf("[Coordinator] Failed to send worker list to %d: %v", workerID, err)
		} else {
			log.Printf("[Coordinator] Sent worker list to worker %d", workerID)
		}
	}
}

func (c *Coordinator) LoadGraph(edges []common.Edge) {
	loader := &GraphLoader{}
	partitions := loader.PartitionEdges(edges, c.numWorkers)

	totalNodes := make(map[int64]bool)
	var totalEdges int64

	for workerID := int64(0); workerID < c.numWorkers; workerID++ {
		client, exists := c.workerClients[workerID]
		if !exists {
			log.Printf("[Coordinator] Warning: Worker %d not registered", workerID)
			continue
		}

		workerEdges := partitions[workerID]
		for _, e := range workerEdges {
			totalNodes[e.Source] = true
			totalNodes[e.Target] = true
		}

		partitionData := &common.PartitionData{
			WorkerID: workerID,
			Edges:    workerEdges,
		}

		var result common.LoadResult
		err := client.Call("WorkerService.LoadPartition", partitionData, &result)
		if err != nil {
			log.Printf("[Coordinator] Failed to load partition to worker %d: %v", workerID, err)
			continue
		}

		log.Printf("[Coordinator] Worker %d: %d nodes, %d edges",
			workerID, result.NodesLoaded, result.EdgesLoaded)
		totalEdges += result.EdgesLoaded
	}

	log.Printf("[Coordinator] Total graph: %d nodes, %d edges", len(totalNodes), totalEdges)
	c.BroadcastWorkerList()
}

func (c *Coordinator) RunBFS(sourceNode int64, maxIterations int) map[int64]int64 {
	log.Printf("\n[Coordinator] Starting BFS from node %d", sourceNode)
	startTime := time.Now()

	// Initialize BFS on all workers
	for workerID, client := range c.workerClients {
		var resp common.Ack
		err := client.Call("WorkerService.StartBFS", &common.StartBFSRequest{SourceNode: sourceNode}, &resp)
		if err != nil {
			log.Printf("[Coordinator] Failed to start BFS on worker %d: %v", workerID, err)
		}
	}

	// Superstep loop
	var iteration int64
	var totalMessages int64

	for int(iteration) < maxIterations {
		var results []common.IterationResult

		for workerID, client := range c.workerClients {
			var result common.IterationResult
			err := client.Call("WorkerService.RunIteration", &common.IterationCommand{Iteration: iteration}, &result)
			if err != nil {
				log.Printf("[Coordinator] Worker %d iteration failed: %v", workerID, err)
				continue
			}
			results = append(results, result)
			totalMessages += result.MessagesSent
		}

		// Check termination
		var totalActive int64
		anyChanged := false
		for _, r := range results {
			totalActive += r.ActiveNodes
			if r.Changed {
				anyChanged = true
			}
		}

		log.Printf("[Coordinator] BFS iteration %d: active_nodes=%d, changed=%v",
			iteration, totalActive, anyChanged)

		if totalActive == 0 && !anyChanged {
			log.Printf("[Coordinator] BFS converged after %d iterations", iteration+1)
			break
		}

		iteration++
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(startTime)
	c.totalIterations = iteration + 1
	c.totalMessages = totalMessages
	c.algorithmTime = elapsed

	allResults := c.collectResults()

	log.Printf("[Coordinator] BFS complete: %d nodes reached", len(allResults))
	log.Printf("[Coordinator] Metrics: %d iterations, %d messages, %v",
		c.totalIterations, c.totalMessages, elapsed)

	return allResults
}

func (c *Coordinator) RunConnectedComponents(maxIterations int) map[int64]int64 {
	log.Printf("\n[Coordinator] Starting Connected Components")
	startTime := time.Now()

	// Initialize CC on all workers
	for workerID, client := range c.workerClients {
		var resp common.Ack
		err := client.Call("WorkerService.StartCC", &common.StartCCRequest{}, &resp)
		if err != nil {
			log.Printf("[Coordinator] Failed to start CC on worker %d: %v", workerID, err)
		}
	}

	// Superstep loop
	var iteration int64
	var totalMessages int64

	for int(iteration) < maxIterations {
		var results []common.IterationResult

		for workerID, client := range c.workerClients {
			var result common.IterationResult
			err := client.Call("WorkerService.RunIteration", &common.IterationCommand{Iteration: iteration}, &result)
			if err != nil {
				log.Printf("[Coordinator] Worker %d iteration failed: %v", workerID, err)
				continue
			}
			results = append(results, result)
			totalMessages += result.MessagesSent
		}

		// Check convergence
		anyChanged := false
		for _, r := range results {
			if r.Changed {
				anyChanged = true
			}
		}

		log.Printf("[Coordinator] CC iteration %d: changed=%v", iteration, anyChanged)

		if !anyChanged && iteration > 0 {
			log.Printf("[Coordinator] CC converged after %d iterations", iteration+1)
			break
		}

		iteration++
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(startTime)
	c.totalIterations = iteration + 1
	c.totalMessages = totalMessages
	c.algorithmTime = elapsed

	allResults := c.collectResults()

	uniqueLabels := make(map[int64]bool)
	for _, label := range allResults {
		uniqueLabels[label] = true
	}

	log.Printf("[Coordinator] CC complete: %d components found", len(uniqueLabels))
	log.Printf("[Coordinator] Metrics: %d iterations, %d messages, %v",
		c.totalIterations, c.totalMessages, elapsed)

	return allResults
}

func (c *Coordinator) collectResults() map[int64]int64 {
	allResults := make(map[int64]int64)

	for workerID, client := range c.workerClients {
		var response common.ResultsResponse
		err := client.Call("WorkerService.GetResults", &common.ResultsRequest{WorkerID: workerID}, &response)
		if err != nil {
			log.Printf("[Coordinator] Failed to get results from worker %d: %v", workerID, err)
			continue
		}

		for _, result := range response.Results {
			allResults[result.NodeID] = result.Value
		}
	}

	return allResults
}

func (c *Coordinator) GetStats() map[int64]map[string]int64 {
	stats := make(map[int64]map[string]int64)

	for workerID, client := range c.workerClients {
		var workerStats common.WorkerStats
		err := client.Call("WorkerService.GetStats", &common.Empty{}, &workerStats)
		if err != nil {
			log.Printf("[Coordinator] Failed to get stats from worker %d: %v", workerID, err)
			continue
		}

		stats[workerID] = map[string]int64{
			"local_nodes":       workerStats.LocalNodes,
			"ghost_nodes":       workerStats.GhostNodes,
			"messages_sent":     workerStats.MessagesSent,
			"messages_received": workerStats.MessagesReceived,
		}
	}

	return stats
}

func (c *Coordinator) ResetWorkers() {
	for workerID, client := range c.workerClients {
		var resp common.Ack
		err := client.Call("WorkerService.Reset", &common.Empty{}, &resp)
		if err != nil {
			log.Printf("[Coordinator] Failed to reset worker %d: %v", workerID, err)
		}
	}
}

func (c *Coordinator) PrintMetrics() {
	fmt.Println("\n==================================================")
	fmt.Println("PERFORMANCE METRICS")
	fmt.Println("==================================================")
	fmt.Printf("Total iterations: %d\n", c.totalIterations)
	fmt.Printf("Total messages exchanged: %d\n", c.totalMessages)
	fmt.Printf("Algorithm time: %v\n", c.algorithmTime)

	stats := c.GetStats()
	var totalGhosts int64
	for _, s := range stats {
		totalGhosts += s["ghost_nodes"]
	}
	fmt.Printf("Cross-partition edges (ghost nodes): %d\n", totalGhosts)

	fmt.Println("\nPer-worker statistics:")
	for workerID := int64(0); workerID < c.numWorkers; workerID++ {
		s := stats[workerID]
		fmt.Printf("  Worker %d: %d nodes, %d ghosts, %d sent, %d recv\n",
			workerID, s["local_nodes"], s["ghost_nodes"], s["messages_sent"], s["messages_received"])
	}
	fmt.Println("==================================================")
}

func main() {
	numWorkers := flag.Int64("workers", 3, "Number of workers")
	basePort := flag.Int("base-port", 50051, "Base port for workers")
	graphFile := flag.String("graph", "", "Path to edge list file")
	numNodes := flag.Int("nodes", 100, "Nodes for generated graph")
	sourceNode := flag.Int64("source", 0, "BFS source node")
	flag.Parse()

	coord := NewCoordinator(*numWorkers)
	defer coord.Close()

	// Register workers
	for i := int64(0); i < *numWorkers; i++ {
		addr := fmt.Sprintf("localhost:%d", *basePort+int(i))
		if err := coord.RegisterWorker(i, addr); err != nil {
			log.Fatalf("Failed to register worker %d: %v", i, err)
		}
	}

	// Load graph
	loader := &GraphLoader{}
	var edges []common.Edge
	var err error

	if *graphFile != "" {
		edges, err = loader.LoadEdgeList(*graphFile)
		if err != nil {
			log.Fatalf("Failed to load graph: %v", err)
		}
		log.Printf("[Main] Loaded %d edges from %s", len(edges), *graphFile)
	} else {
		edges = loader.GenerateSampleGraph(*numNodes, 5)
		log.Printf("[Main] Generated graph with %d nodes", *numNodes)
	}

	coord.LoadGraph(edges)

	// Run BFS
	fmt.Println("\n==================================================")
	bfsResults := coord.RunBFS(*sourceNode, 100)
	coord.PrintMetrics()

	// Show sample BFS results
	fmt.Println("\nSample BFS distances (first 10):")
	var sortedNodes []int64
	for node := range bfsResults {
		sortedNodes = append(sortedNodes, node)
	}
	sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i] < sortedNodes[j] })

	count := 0
	for _, node := range sortedNodes {
		if count >= 10 {
			break
		}
		fmt.Printf("  Node %d: distance %d\n", node, bfsResults[node])
		count++
	}

	// Reset and run Connected Components
	coord.ResetWorkers()
	time.Sleep(500 * time.Millisecond)

	coord.LoadGraph(edges)

	fmt.Println("\n==================================================")
	ccResults := coord.RunConnectedComponents(100)
	coord.PrintMetrics()

	// Show component sizes
	componentSizes := make(map[int64]int)
	for _, label := range ccResults {
		componentSizes[label]++
	}

	fmt.Printf("\nComponent sizes (top 5): ")
	type kv struct {
		Key   int64
		Value int
	}
	var sorted []kv
	for k, v := range componentSizes {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Value > sorted[j].Value })

	for i, item := range sorted {
		if i >= 5 {
			break
		}
		fmt.Printf("%d:%d ", item.Key, item.Value)
	}
	fmt.Println()
}
