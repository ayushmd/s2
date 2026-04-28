package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var (
	nodeID      *string
	raftAddr    *string
	httpAddr    *string
	dataDir     *string
	master      *string
	storageDir  string
	metadataDir string
)

const ReplicationFactor = 2
const BlockSize = 128 * 1024 * 1024
const WindowSize = 16 * 1024 * 1024
const WindowPerBlock = BlockSize / WindowSize
const MaxWorker = 20
const MetricsInterval = 10 // seconds

func main() {
	// Command line flags
	nodeID = flag.String("node", "node1", "Node ID for this instance")
	raftAddr = flag.String("raft-addr", ":7000", "Raft server address")
	httpAddr = flag.String("http-addr", ":8080", "HTTP server address")
	dataDir = flag.String("dir", "", "Storing data location")
	master = flag.String("master", "", "Bootstrap the cluster")
	flag.Parse()
	if *dataDir == "" {
		dataDir = nodeID
	}

	// Create data directory
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
	storageDir = filepath.Join(*dataDir, "storage")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
	metadataDir = filepath.Join(*dataDir, "metadata")
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	fs := NewFileStore(metadataDir)
	err := fs.StartRaft(*nodeID, *raftAddr, *dataDir, *master)
	fmt.Println(err)
	fs.SetLocalHTTPAddr(*httpAddr)
	fs.StartApi(*httpAddr)
}
