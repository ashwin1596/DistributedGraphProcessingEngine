package common

// Message for sending BFS/label updates between workers
type Message struct {
	NodeID       int64
	Value        int64 // distance for BFS, label for connected components
	SourceWorker int64
}

// MessageBatch is a batch of messages
type MessageBatch struct {
	Messages  []Message
	Iteration int64
}

// Ack is an acknowledgment response
type Ack struct {
	Success          bool
	MessagesReceived int64
}

// Empty is an empty request
type Empty struct{}

// WorkerStats contains worker statistics
type WorkerStats struct {
	WorkerID           int64
	LocalNodes         int64
	GhostNodes         int64
	ActiveFrontierSize int64
	MessagesSent       int64
	MessagesReceived   int64
	Converged          bool
}

// StartBFSRequest initializes BFS
type StartBFSRequest struct {
	SourceNode int64
}

// StartCCRequest initializes Connected Components
type StartCCRequest struct{}

// IterationCommand triggers an iteration
type IterationCommand struct {
	Iteration int64
}

// IterationResult is the result of an iteration
type IterationResult struct {
	WorkerID     int64
	Iteration    int64
	ActiveNodes  int64
	MessagesSent int64
	Changed      bool
}

// Edge represents a graph edge
type Edge struct {
	Source int64
	Target int64
}

// PartitionData contains partition data for loading
type PartitionData struct {
	WorkerID int64
	Edges    []Edge
}

// LoadResult is the result of loading a partition
type LoadResult struct {
	Success     bool
	NodesLoaded int64
	EdgesLoaded int64
}

// WorkerInfo contains worker information
type WorkerInfo struct {
	WorkerID int64
	Address  string
}

// WorkerList is a list of workers
type WorkerList struct {
	Workers []WorkerInfo
}

// NodeResult contains a node's result value
type NodeResult struct {
	NodeID int64
	Value  int64
}

// ResultsRequest requests results
type ResultsRequest struct {
	WorkerID int64
}

// ResultsResponse contains results
type ResultsResponse struct {
	Results []NodeResult
}
