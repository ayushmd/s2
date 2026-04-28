package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type FileMetadata struct {
	// Path        string `json:"path"`
	Identifier  string `json:"identifier"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	FileType    string `json:"filetype"`
	Compression string `json:"compression,omitempty"`
	Size        int    `json:"size"`
	Timestamp   int64  `json:"timestamp"`
	Blueprint
}

type FileOperation struct {
	Type string `json:"type"` // create, delete
	FileMetadata
}

type FileStore struct {
	raft *raft.Raft
	mu   sync.RWMutex
	// meta    map[string]FileMetadata
	store   *DataStorage
	metrics map[string]Metrics

	ln net.Listener

	localNodeID   string
	localHTTPAddr string
}

func (fm FileMetadata) Encode() ([]byte, error) {
	return json.Marshal(fm)
}

func (fm *FileMetadata) Decode(data []byte) error {
	return json.Unmarshal(data, fm)
}

func NewFileStore(metastore string) *FileStore {
	return &FileStore{
		mu:      sync.RWMutex{},
		metrics: map[string]Metrics{},
		store:   NewDataStorage(metastore),
	}
}

func (fs *FileStore) StartRaft(nodeID, raftAddr, dataDir, master string) error {
	bootstrap := master == ""
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 2

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve address: %v", err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create transport: %v", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("failed to create log store: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "stable.db"))
	if err != nil {
		return fmt.Errorf("failed to create stable store: %v", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %v", err)
	}

	raftInstance, err := raft.NewRaft(config, fs, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft: %v", err)
	}

	fs.raft = raftInstance
	fs.localNodeID = nodeID

	go fs.GatherMetrics()
	if bootstrap {
		// go fs.listDb()
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		future := raftInstance.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
	} else {
		err := addVoter(master, nodeID, raftAddr)
		fmt.Println("Add Voter Error:", err)
	}
	return nil
}

func (fs *FileStore) SetLocalHTTPAddr(addr string) {
	fs.localHTTPAddr = addr
}

func normalizeStorageEndpoint(addr string) string {
	addr = strings.TrimSpace(addr)
	lower := strings.ToLower(addr)
	lower = strings.TrimPrefix(lower, "http://")
	lower = strings.TrimPrefix(lower, "https://")
	return strings.TrimRight(lower, "/")
}

func (fs *FileStore) isLocalStorageEndpoint(addr string) bool {
	ep := normalizeStorageEndpoint(addr)
	if ep == "" {
		return false
	}
	if fs.localNodeID != "" && ep == strings.ToLower(fs.localNodeID) {
		return true
	}
	if fs.localHTTPAddr == "" {
		return false
	}
	self := normalizeStorageEndpoint(fs.localHTTPAddr)
	if ep == self {
		return true
	}
	if strings.HasPrefix(self, ":") {
		port := self[1:]
		if port != "" && strings.HasSuffix(ep, ":"+port) {
			return true
		}
	}
	return false
}

func (fs *FileStore) deleteFileLocal(addr, path string) {
	if fs.isLocalStorageEndpoint(addr) {
		filePath := filepath.Join(storageDir, path)
		fmt.Println("Local delete (no HTTP):", filePath)
		if err := os.RemoveAll(filePath); err != nil {
			fmt.Println("Error local delete:", err)
		}
		return
	}
	if err := deleteFile(addr, path); err != nil {
		fmt.Println("Error remote delete:", err)
	}
}

func (fs *FileStore) Apply(log *raft.Log) interface{} {
	var op FileOperation
	if err := json.Unmarshal(log.Data, &op); err != nil {
		fmt.Printf("Failed to unmarshal log entry: %v", err)
		return nil
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	path := filepath.Join(op.Bucket, op.Key)
	fmt.Println("Applying operation:", op.Type, "=", op)
	switch op.Type {
	case "create":
		fmt.Println("Applying create operation for", path, op.FileMetadata)
		// fs.meta[path] = op.FileMetadata
		er := fs.store.UpsertKeyAsync(op.FileMetadata)
		if er != nil {
			fmt.Println("Error: ", er)
		}
		er = fs.store.InsertIdentifierAsync(op.FileMetadata)
		if er != nil {
			fmt.Println("Error: ", er)
		}
		deletes, er := fs.store.DeleteOldIdentifiers(op.FileMetadata)
		if er != nil {
			fmt.Println("Error: ", er)
		}
		for _, d := range deletes {
			delpath := filepath.Join(d.Bucket, d.Key, d.Identifier)
			uniqueEndpoints := make(map[string]struct{})
			for _, v := range d.Store {
				for _, block := range v {
					uniqueEndpoints[block.Addr] = struct{}{}
				}
			}
			for url := range uniqueEndpoints {
				u := url
				go fs.deleteFileLocal(u, delpath)
			}
		}
		return nil
	case "delete":
		// delete(fs.meta, path)
		err := fs.store.DeleteKey(op.Bucket, op.Key)
		if err != nil {
			fmt.Println("Error: ", err)
		}
		return nil
	default:
		fmt.Printf("Unknown operation type: %s", op.Type)
		return nil
	}
}

func (fs *FileStore) Snapshot() (raft.FSMSnapshot, error) {
	return &FileStoreSnapshot{}, nil
}

// Restore restores the state from a snapshot
func (fs *FileStore) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	// For simplicity, we don't implement snapshot restore
	// In a production system, you would restore the file system state
	return nil
}

// func (fs *FileStore) applyCreateFile(op FileOperation) interface{} {
// 	dir := filepath.Dir(op.Path)
// 	if err := os.MkdirAll(dir, 0755); err != nil {
// 		return fmt.Errorf("failed to create directory: %v", err)
// 	}

// 	file, err := os.Create(op.Path)
// 	if err != nil {
// 		return fmt.Errorf("failed to create file: %v", err)
// 	}
// 	defer file.Close()

// 	var contentToWrite []byte
// 	if op.Compression != "" {
// 		contentToWrite, err = compressData(op.Content)
// 		if err != nil {
// 			return fmt.Errorf("failed to compress content: %v", err)
// 		}
// 	} else {
// 		contentToWrite = []byte(op.Content)
// 	}

// 	if _, err := file.Write(contentToWrite); err != nil {
// 		return fmt.Errorf("failed to write content: %v", err)
// 	}

// 	return fmt.Sprintf("File created: %s", op.Path)
// }

// func (fs *FileStore) applyDeleteFile(op FileOperation) interface{} {
// 	if _, err := os.Stat(op.Path); os.IsNotExist(err) {
// 		return fmt.Errorf("file not found: %s", op.Path)
// 	}

// 	if err := os.Remove(op.Path); err != nil {
// 		return fmt.Errorf("failed to delete file: %v", err)
// 	}

// 	return fmt.Sprintf("File deleted: %s", op.Path)
// }

func (fs *FileStore) SubmitOperation(op FileOperation) error {
	if fs.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	op.Timestamp = time.Now().Unix()
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %v", err)
	}

	future := fs.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply operation: %v", err)
	}

	return nil
}

func (fs *FileStore) GetLeader() (string, string) {
	addr, id := fs.raft.LeaderWithID()
	return string(addr), string(id)
}

func (fs *FileStore) IsLeader() bool {
	return fs.raft.State() == raft.Leader
}

func (fs *FileStore) GetState() string {
	return fs.raft.State().String()
}

func (fs *FileStore) AddVoter(serverID, address string) error {
	server := raft.Server{
		ID:      raft.ServerID(serverID),
		Address: raft.ServerAddress(address),
	}

	future := fs.raft.AddVoter(server.ID, server.Address, 0, 0)
	return future.Error()
}

type FileStoreSnapshot struct{}

func (f *FileStoreSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

// Release releases the snapshot
func (f *FileStoreSnapshot) Release() {}

type BlockStore struct {
	ln net.Listener
}

func (bs *BlockStore) PutObject(bucket, key, filetype string, n int, data io.Reader) error {
	return nil
}

// var (
// 	nodeID    *string
// 	raftAddr  *string
// 	httpAddr  *string
// 	dataDir   *string
// 	bootstrap *bool
// )

// func main() {
// 	// Command line flags
// 	nodeID = flag.String("node", "node1", "Node ID for this instance")
// 	raftAddr = flag.String("raft-addr", "127.0.0.1:7000", "Raft server address")
// 	httpAddr = flag.String("http-addr", "127.0.0.1:8080", "HTTP server address")
// 	dataDir = flag.String("data-dir", "./data", "Data directory for Raft")
// 	bootstrap = flag.Bool("bootstrap", false, "Bootstrap the cluster")
// 	flag.Parse()

// 	// Create data directory
// 	if err := os.MkdirAll(*dataDir, 0755); err != nil {
// 		log.Fatalf("Failed to create data directory: %v", err)
// 	}

// 	bs := BlockStore{}
// 	// Initialize file store
// 	fileStore = NewFileStore()

// 	// Start Raft consensus
// 	if err := fileStore.StartRaft(*nodeID, *raftAddr, *dataDir, *bootstrap); err != nil {
// 		log.Fatalf("Failed to start Raft: %v", err)
// 	}

// 	// Wait for Raft to be ready
// 	time.Sleep(2 * time.Second)

// 	r := mux.NewRouter()

// 	// API routes
// 	r.HandleFunc("/{bucket}/{file}", bs.createFile).Methods("PUT")
// 	// r.HandleFunc("/{bucket}/{file}", getFile).Methods("GET")
// 	// r.HandleFunc("/{bucket}/{file}", deleteFile).Methods("DELETE")

// 	// api := r.PathPrefix("/v1/").Subrouter()
// 	// // Cluster management routes
// 	// api.HandleFunc("/cluster/status", getClusterStatus).Methods("GET")
// 	// api.HandleFunc("/cluster/leader", getLeader).Methods("GET")
// 	// api.HandleFunc("/cluster/add", addNode).Methods("POST")

// 	// Health check endpoint
// 	r.HandleFunc("/health", healthCheck).Methods("GET")

// 	fmt.Printf("Server starting on %s (Raft: %s)\n", *httpAddr, *raftAddr)
// 	fmt.Println("Available endpoints:")
// 	fmt.Println("  POST   /api/v1/files - Create a file")
// 	fmt.Println("  GET    /api/v1/files - Get file content")
// 	fmt.Println("  DELETE /api/v1/files - Delete a file")
// 	fmt.Println("  GET    /api/v1/cluster/status - Get cluster status")
// 	fmt.Println("  GET    /api/v1/cluster/leader - Get leader info")
// 	fmt.Println("  POST   /api/v1/cluster/add - Add node to cluster")
// 	fmt.Println("  GET    /health - Health check")

// 	log.Fatal(http.ListenAndServe(*httpAddr, r))
// }

// compressData compresses the given data using gzip
func compressData(data string) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write([]byte(data))
	if err != nil {
		return nil, err
	}
	writer.Close()
	return buf.Bytes(), nil
}

// decompressData decompresses the given gzip data
func decompressData(data []byte) (string, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// createFile handles file creation
func (bs *BlockStore) createFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	file := vars["file"]
	n := r.Header["Content-Length"]
	length, _ := strconv.Atoi(n[0])

	filetype := r.Header["Content-Type"]

	w.Header().Set("Content-Type", "application/json")

	bs.PutObject(bucket, file, strings.Join(filetype, ""), length, r.Body)

	response := FileResponse{
		Success: true,
		Message: fmt.Sprintf("File created successfully"),
	}

	json.NewEncoder(w).Encode(response)
}

// getFile handles file reading
// func getFile(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	bucket := vars["bucket"]
// 	file := vars["file"]
// 	w.Header().Set("Content-Type", "application/json")

// 	path := r.URL.Query().Get("path")
// 	compression := r.URL.Query().Get("compression") == "true"

// 	if path == "" {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "Path parameter is required",
// 		})
// 		return
// 	}

// 	// Security check: prevent directory traversal
// 	if strings.Contains(path, "..") {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "Invalid path: directory traversal not allowed",
// 		})
// 		return
// 	}

// 	// Check if file exists
// 	if _, err := os.Stat(path); os.IsNotExist(err) {
// 		w.WriteHeader(http.StatusNotFound)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "File not found",
// 		})
// 		return
// 	}

// 	// Read file content
// 	content, err := os.ReadFile(path)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   fmt.Sprintf("Failed to read file: %v", err),
// 		})
// 		return
// 	}

// 	// Handle decompression if requested
// 	var finalContent string
// 	if compression {
// 		// Try to decompress the content
// 		decompressed, err := decompressData(content)
// 		if err != nil {
// 			w.WriteHeader(http.StatusInternalServerError)
// 			json.NewEncoder(w).Encode(ErrorResponse{
// 				Success: false,
// 				Error:   fmt.Sprintf("Failed to decompress file: %v", err),
// 			})
// 			return
// 		}
// 		finalContent = decompressed
// 	} else {
// 		finalContent = string(content)
// 	}

// 	response := FileResponse{
// 		Success: true,
// 		Message: "File read successfully",
// 		Data:    finalContent,
// 	}

// 	if compression {
// 		response.Compression = true
// 		response.Message += " (decompressed)"
// 	}

// 	json.NewEncoder(w).Encode(response)
// }

// // deleteFile handles file deletion
// func deleteFile(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	bucket := vars["bucket"]
// 	file := vars["file"]

// 	w.Header().Set("Content-Type", "application/json")

// 	path := r.URL.Query().Get("path")
// 	if path == "" {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "Path parameter is required",
// 		})
// 		return
// 	}

// 	// Security check: prevent directory traversal
// 	if strings.Contains(path, "..") {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "Invalid path: directory traversal not allowed",
// 		})
// 		return
// 	}

// 	// Check if file exists
// 	if _, err := os.Stat(path); os.IsNotExist(err) {
// 		w.WriteHeader(http.StatusNotFound)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "File not found",
// 		})
// 		return
// 	}

// 	// Submit delete operation to Raft cluster
// 	op := FileOperation{
// 		Type: "delete",
// 		Path: path,
// 	}

// 	if err := fileStore.SubmitOperation(op); err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   fmt.Sprintf("Failed to submit delete operation to cluster: %v", err),
// 		})
// 		return
// 	}

// 	json.NewEncoder(w).Encode(FileResponse{
// 		Success: true,
// 		Message: fmt.Sprintf("File deleted successfully: %s", path),
// 	})
// }

// // getClusterStatus returns the current cluster status
// func getClusterStatus(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")

// 	status := map[string]interface{}{
// 		"state":     fileStore.GetState(),
// 		"leader":    fileStore.GetLeader(),
// 		"is_leader": fileStore.IsLeader(),
// 	}

// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		"success": true,
// 		"data":    status,
// 	})
// }

// // getLeader returns the current leader information
// func getLeader(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")

// 	leader := fileStore.GetLeader()
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		"success":   true,
// 		"leader":    leader,
// 		"is_leader": fileStore.IsLeader(),
// 	})
// }

// // AddNodeRequest represents a request to add a node to the cluster
// type AddNodeRequest struct {
// 	NodeID  string `json:"node_id"`
// 	Address string `json:"address"`
// }

// // addNode adds a new node to the cluster
// func addNode(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")

// 	var req AddNodeRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "Invalid JSON format",
// 		})
// 		return
// 	}

// 	if req.NodeID == "" || req.Address == "" {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   "Node ID and address are required",
// 		})
// 		return
// 	}

// 	if err := fileStore.AddVoter(req.NodeID, req.Address); err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Success: false,
// 			Error:   fmt.Sprintf("Failed to add node: %v", err),
// 		})
// 		return
// 	}

// 	json.NewEncoder(w).Encode(FileResponse{
// 		Success: true,
// 		Message: fmt.Sprintf("Node %s added to cluster", req.NodeID),
// 	})
// }
