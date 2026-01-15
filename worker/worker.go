package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"distributed_graph/common"
)

// LocalGraphPartition stores local vertices and edges, tracks ghost nodes
type LocalGraphPartition struct {
	workerID   int64
	numWorkers int64
	nodes      map[int64]bool
	adjacency  map[int64][]int64
	ghostNodes map[int64]bool
	mu         sync.RWMutex
}

func NewLocalGraphPartition(workerID, numWorkers int64) *LocalGraphPartition {
	return &LocalGraphPartition{
		workerID:   workerID,
		numWorkers: numWorkers,
		nodes:      make(map[int64]bool),
		adjacency:  make(map[int64][]int64),
		ghostNodes: make(map[int64]bool),
	}
}

func (p *LocalGraphPartition) AddEdge(source, target int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.nodes[source] = true
	p.adjacency[source] = append(p.adjacency[source], target)

	targetOwner := p.GetOwner(target)
	if targetOwner != p.workerID {
		p.ghostNodes[target] = true
	} else {
		p.nodes[target] = true
	}
}

func (p *LocalGraphPartition) GetOwner(nodeID int64) int64 {
	return nodeID % p.numWorkers
}

func (p *LocalGraphPartition) GetNeighbors(nodeID int64) []int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.adjacency[nodeID]
}

func (p *LocalGraphPartition) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodes = make(map[int64]bool)
	p.adjacency = make(map[int64][]int64)
	p.ghostNodes = make(map[int64]bool)
}

// BFSState holds state for distributed BFS
type BFSState struct {
	distances       map[int64]int64
	currentFrontier map[int64]bool
	nextFrontier    map[int64]bool
	visited         map[int64]bool
	mu              sync.RWMutex
}

func NewBFSState() *BFSState {
	return &BFSState{
		distances:       make(map[int64]int64),
		currentFrontier: make(map[int64]bool),
		nextFrontier:    make(map[int64]bool),
		visited:         make(map[int64]bool),
	}
}

func (s *BFSState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.distances = make(map[int64]int64)
	s.currentFrontier = make(map[int64]bool)
	s.nextFrontier = make(map[int64]bool)
	s.visited = make(map[int64]bool)
}

// CCState holds state for connected components
type CCState struct {
	labels  map[int64]int64
	changed bool
	mu      sync.RWMutex
}

func NewCCState() *CCState {
	return &CCState{
		labels: make(map[int64]int64),
	}
}

func (s *CCState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.labels = make(map[int64]int64)
	s.changed = false
}

// WorkerService implements the RPC service
type WorkerService struct {
	workerID   int64
	numWorkers int64
	port       int

	partition *LocalGraphPartition
	bfsState  *BFSState
	ccState   *CCState

	incomingMessages []common.Message
	msgMu            sync.Mutex

	workerClients map[int64]*rpc.Client
	workerAddrs   map[int64]string
	clientMu      sync.RWMutex

	messagesSent     int64
	messagesReceived int64
	currentIteration int64
	algorithm        string
}

func NewWorkerService(workerID, numWorkers int64, port int) *WorkerService {
	return &WorkerService{
		workerID:      workerID,
		numWorkers:    numWorkers,
		port:          port,
		partition:     NewLocalGraphPartition(workerID, numWorkers),
		bfsState:      NewBFSState(),
		ccState:       NewCCState(),
		workerClients: make(map[int64]*rpc.Client),
		workerAddrs:   make(map[int64]string),
	}
}

func (s *WorkerService) LoadPartition(req *common.PartitionData, resp *common.LoadResult) error {
	s.partition.Clear()

	for _, edge := range req.Edges {
		s.partition.AddEdge(edge.Source, edge.Target)
	}

	edgeCount := int64(0)
	s.partition.mu.RLock()
	for _, neighbors := range s.partition.adjacency {
		edgeCount += int64(len(neighbors))
	}
	nodeCount := int64(len(s.partition.nodes))
	ghostCount := int64(len(s.partition.ghostNodes))
	s.partition.mu.RUnlock()

	log.Printf("[Worker %d] Loaded %d nodes, %d edges, %d ghost nodes",
		s.workerID, nodeCount, edgeCount, ghostCount)

	resp.Success = true
	resp.NodesLoaded = nodeCount
	resp.EdgesLoaded = edgeCount
	return nil
}

func (s *WorkerService) SetWorkerList(req *common.WorkerList, resp *common.Ack) error {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	s.workerAddrs = make(map[int64]string)
	s.workerClients = make(map[int64]*rpc.Client)

	for _, worker := range req.Workers {
		if worker.WorkerID != s.workerID {
			s.workerAddrs[worker.WorkerID] = worker.Address
		}
	}

	log.Printf("[Worker %d] Received worker list: %v", s.workerID, s.workerAddrs)
	resp.Success = true
	return nil
}

func (s *WorkerService) connectToPeer(workerID int64) *rpc.Client {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if client, exists := s.workerClients[workerID]; exists {
		return client
	}

	addr, exists := s.workerAddrs[workerID]
	if !exists {
		return nil
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("[Worker %d] Failed to connect to worker %d: %v", s.workerID, workerID, err)
		return nil
	}

	client := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
	s.workerClients[workerID] = client
	return client
}

func (s *WorkerService) SendMessages(req *common.MessageBatch, resp *common.Ack) error {
	s.msgMu.Lock()
	s.incomingMessages = append(s.incomingMessages, req.Messages...)
	s.messagesReceived += int64(len(req.Messages))
	s.msgMu.Unlock()

	resp.Success = true
	resp.MessagesReceived = int64(len(req.Messages))
	return nil
}

func (s *WorkerService) sendMessagesToWorker(targetWorker int64, messages []common.Message) {
	if len(messages) == 0 {
		return
	}

	client := s.connectToPeer(targetWorker)
	if client == nil {
		return
	}

	batch := &common.MessageBatch{
		Messages:  messages,
		Iteration: s.currentIteration,
	}

	var resp common.Ack
	err := client.Call("WorkerService.SendMessages", batch, &resp)
	if err != nil {
		log.Printf("[Worker %d] Failed to send to worker %d: %v", s.workerID, targetWorker, err)
		return
	}

	s.messagesSent += int64(len(messages))
}

func (s *WorkerService) StartBFS(req *common.StartBFSRequest, resp *common.Ack) error {
	s.algorithm = "bfs"
	s.bfsState.Reset()
	s.messagesSent = 0
	s.messagesReceived = 0
	s.currentIteration = 0

	source := req.SourceNode
	sourceOwner := s.partition.GetOwner(source)

	if sourceOwner == s.workerID {
		s.bfsState.mu.Lock()
		s.bfsState.distances[source] = 0
		s.bfsState.visited[source] = true
		s.bfsState.currentFrontier[source] = true
		s.bfsState.mu.Unlock()
		log.Printf("[Worker %d] BFS source %d is local, starting frontier", s.workerID, source)
	} else {
		log.Printf("[Worker %d] BFS source %d owned by worker %d", s.workerID, source, sourceOwner)
	}

	resp.Success = true
	return nil
}

func (s *WorkerService) StartCC(req *common.StartCCRequest, resp *common.Ack) error {
	s.algorithm = "cc"
	s.ccState.Reset()
	s.messagesSent = 0
	s.messagesReceived = 0
	s.currentIteration = 0

	s.ccState.mu.Lock()
	s.partition.mu.RLock()
	for node := range s.partition.nodes {
		s.ccState.labels[node] = node
	}
	s.partition.mu.RUnlock()
	s.ccState.mu.Unlock()

	log.Printf("[Worker %d] CC initialized with %d labels", s.workerID, len(s.ccState.labels))
	resp.Success = true
	return nil
}

func (s *WorkerService) RunIteration(req *common.IterationCommand, resp *common.IterationResult) error {
	s.currentIteration = req.Iteration

	switch s.algorithm {
	case "bfs":
		return s.runBFSIteration(resp)
	case "cc":
		return s.runCCIteration(resp)
	default:
		resp.WorkerID = s.workerID
		resp.Iteration = s.currentIteration
		resp.ActiveNodes = 0
		resp.MessagesSent = 0
		resp.Changed = false
		return nil
	}
}

func (s *WorkerService) runBFSIteration(resp *common.IterationResult) error {
	// Process incoming messages
	s.msgMu.Lock()
	for _, msg := range s.incomingMessages {
		s.bfsState.mu.Lock()
		if !s.bfsState.visited[msg.NodeID] {
			s.bfsState.visited[msg.NodeID] = true
			s.bfsState.distances[msg.NodeID] = msg.Value
			s.bfsState.currentFrontier[msg.NodeID] = true
		}
		s.bfsState.mu.Unlock()
	}
	s.incomingMessages = nil
	s.msgMu.Unlock()

	// Expand frontier
	s.bfsState.mu.Lock()
	s.bfsState.nextFrontier = make(map[int64]bool)
	s.bfsState.mu.Unlock()

	outgoing := make(map[int64][]common.Message)
	messagesThisIter := int64(0)

	s.bfsState.mu.RLock()
	frontier := make([]int64, 0, len(s.bfsState.currentFrontier))
	for node := range s.bfsState.currentFrontier {
		frontier = append(frontier, node)
	}
	s.bfsState.mu.RUnlock()

	for _, node := range frontier {
		s.bfsState.mu.RLock()
		currentDist := s.bfsState.distances[node]
		s.bfsState.mu.RUnlock()

		for _, neighbor := range s.partition.GetNeighbors(node) {
			neighborOwner := s.partition.GetOwner(neighbor)

			if neighborOwner == s.workerID {
				s.bfsState.mu.Lock()
				if !s.bfsState.visited[neighbor] {
					s.bfsState.visited[neighbor] = true
					s.bfsState.distances[neighbor] = currentDist + 1
					s.bfsState.nextFrontier[neighbor] = true
				}
				s.bfsState.mu.Unlock()
			} else {
				msg := common.Message{
					NodeID:       neighbor,
					Value:        currentDist + 1,
					SourceWorker: s.workerID,
				}
				outgoing[neighborOwner] = append(outgoing[neighborOwner], msg)
				messagesThisIter++
			}
		}
	}

	// Send messages
	for targetWorker, messages := range outgoing {
		s.sendMessagesToWorker(targetWorker, messages)
	}

	// Swap frontiers
	s.bfsState.mu.Lock()
	s.bfsState.currentFrontier = s.bfsState.nextFrontier
	active := int64(len(s.bfsState.currentFrontier))
	s.bfsState.mu.Unlock()

	log.Printf("[Worker %d] BFS iter %d: active=%d, sent=%d",
		s.workerID, s.currentIteration, active, messagesThisIter)

	resp.WorkerID = s.workerID
	resp.Iteration = s.currentIteration
	resp.ActiveNodes = active
	resp.MessagesSent = messagesThisIter
	resp.Changed = active > 0 || messagesThisIter > 0
	return nil
}

func (s *WorkerService) runCCIteration(resp *common.IterationResult) error {
	// Process incoming label updates
	ghostLabels := make(map[int64]int64)

	s.msgMu.Lock()
	for _, msg := range s.incomingMessages {
		if existing, ok := ghostLabels[msg.NodeID]; !ok || msg.Value < existing {
			ghostLabels[msg.NodeID] = msg.Value
		}
	}
	s.incomingMessages = nil
	s.msgMu.Unlock()

	// Update labels
	changed := false
	outgoing := make(map[int64][]common.Message)
	messagesThisIter := int64(0)

	s.partition.mu.RLock()
	nodes := make([]int64, 0, len(s.partition.nodes))
	for node := range s.partition.nodes {
		nodes = append(nodes, node)
	}
	s.partition.mu.RUnlock()

	for _, node := range nodes {
		s.ccState.mu.RLock()
		oldLabel := s.ccState.labels[node]
		s.ccState.mu.RUnlock()

		newLabel := oldLabel

		for _, neighbor := range s.partition.GetNeighbors(node) {
			neighborOwner := s.partition.GetOwner(neighbor)

			var neighborLabel int64
			if neighborOwner == s.workerID {
				s.ccState.mu.RLock()
				neighborLabel = s.ccState.labels[neighbor]
				s.ccState.mu.RUnlock()
			} else {
				if label, ok := ghostLabels[neighbor]; ok {
					neighborLabel = label
				} else {
					neighborLabel = neighbor
				}
			}

			if neighborLabel < newLabel {
				newLabel = neighborLabel
			}
		}

		if newLabel < oldLabel {
			s.ccState.mu.Lock()
			s.ccState.labels[node] = newLabel
			s.ccState.mu.Unlock()
			changed = true

			for _, neighbor := range s.partition.GetNeighbors(node) {
				neighborOwner := s.partition.GetOwner(neighbor)
				if neighborOwner != s.workerID {
					msg := common.Message{
						NodeID:       node,
						Value:        newLabel,
						SourceWorker: s.workerID,
					}
					outgoing[neighborOwner] = append(outgoing[neighborOwner], msg)
					messagesThisIter++
				}
			}
		}
	}

	// Send initial labels on first iteration
	if s.currentIteration == 0 {
		for _, node := range nodes {
			s.ccState.mu.RLock()
			label := s.ccState.labels[node]
			s.ccState.mu.RUnlock()

			for _, neighbor := range s.partition.GetNeighbors(node) {
				neighborOwner := s.partition.GetOwner(neighbor)
				if neighborOwner != s.workerID {
					msg := common.Message{
						NodeID:       node,
						Value:        label,
						SourceWorker: s.workerID,
					}
					outgoing[neighborOwner] = append(outgoing[neighborOwner], msg)
					messagesThisIter++
				}
			}
		}
	}

	// Send messages
	for targetWorker, messages := range outgoing {
		s.sendMessagesToWorker(targetWorker, messages)
	}

	s.ccState.mu.Lock()
	s.ccState.changed = changed
	s.ccState.mu.Unlock()

	log.Printf("[Worker %d] CC iter %d: changed=%v, sent=%d",
		s.workerID, s.currentIteration, changed, messagesThisIter)

	resp.WorkerID = s.workerID
	resp.Iteration = s.currentIteration
	resp.ActiveNodes = boolToInt64(changed)
	resp.MessagesSent = messagesThisIter
	resp.Changed = changed
	return nil
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func (s *WorkerService) GetStats(req *common.Empty, resp *common.WorkerStats) error {
	s.partition.mu.RLock()
	localNodes := int64(len(s.partition.nodes))
	ghostNodes := int64(len(s.partition.ghostNodes))
	s.partition.mu.RUnlock()

	var frontierSize int64
	if s.algorithm == "bfs" {
		s.bfsState.mu.RLock()
		frontierSize = int64(len(s.bfsState.currentFrontier))
		s.bfsState.mu.RUnlock()
	}

	s.ccState.mu.RLock()
	converged := !s.ccState.changed
	s.ccState.mu.RUnlock()

	if s.algorithm == "bfs" {
		converged = frontierSize == 0
	}

	resp.WorkerID = s.workerID
	resp.LocalNodes = localNodes
	resp.GhostNodes = ghostNodes
	resp.ActiveFrontierSize = frontierSize
	resp.MessagesSent = s.messagesSent
	resp.MessagesReceived = s.messagesReceived
	resp.Converged = converged
	return nil
}

func (s *WorkerService) GetResults(req *common.ResultsRequest, resp *common.ResultsResponse) error {
	switch s.algorithm {
	case "bfs":
		s.bfsState.mu.RLock()
		for node, dist := range s.bfsState.distances {
			resp.Results = append(resp.Results, common.NodeResult{NodeID: node, Value: dist})
		}
		s.bfsState.mu.RUnlock()
	case "cc":
		s.ccState.mu.RLock()
		for node, label := range s.ccState.labels {
			resp.Results = append(resp.Results, common.NodeResult{NodeID: node, Value: label})
		}
		s.ccState.mu.RUnlock()
	}
	return nil
}

func (s *WorkerService) Reset(req *common.Empty, resp *common.Ack) error {
	s.bfsState.Reset()
	s.ccState.Reset()
	s.messagesSent = 0
	s.messagesReceived = 0
	s.currentIteration = 0
	s.algorithm = ""

	s.msgMu.Lock()
	s.incomingMessages = nil
	s.msgMu.Unlock()

	log.Printf("[Worker %d] Reset complete", s.workerID)
	resp.Success = true
	return nil
}

func main() {
	workerID := flag.Int64("id", 0, "Worker ID")
	numWorkers := flag.Int64("workers", 3, "Total number of workers")
	port := flag.Int("port", 50051, "Port to listen on")
	flag.Parse()

	service := NewWorkerService(*workerID, *numWorkers, *port)
	rpc.Register(service)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("[Worker %d] Server started on port %d", *workerID, *port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go rpc.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}
