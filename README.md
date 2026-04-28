# Distributed File API Server

A Go-based REST API server with Raft consensus for distributed file management across multiple nodes.

## Features

- **Distributed File Management**: Multi-node file operations with Raft consensus
- **Create files**: Create files at specified paths with content
- **Read files**: Retrieve file content
- **Delete files**: Remove files from the filesystem
- **Compression**: Optional gzip compression for file storage and retrieval
- **Cluster Management**: Add/remove nodes, check cluster status
- **Leader Election**: Automatic leader election and failover
- **Data Consistency**: Strong consistency across all nodes
- **Security**: Protection against directory traversal attacks
- **Health check**: Server health monitoring endpoint

## API Endpoints

### Health Check
```
GET /health
```
Returns server health status.

### Create File
```
POST /api/v1/files
Content-Type: application/json

{
  "path": "path/to/file.txt",
  "content": "File content here",
  "compression": true
}
```

### Read File
```
GET /api/v1/files?path=path/to/file.txt&compression=true
```

### Delete File
```
DELETE /api/v1/files?path=path/to/file.txt
```

### Cluster Management
```
GET    /api/v1/cluster/status - Get cluster status
GET    /api/v1/cluster/leader - Get leader information  
POST   /api/v1/cluster/add   - Add node to cluster
```

## Getting Started

### Prerequisites
- Go 1.21 or higher

### Installation

1. Clone or download this project
2. Install dependencies:
   ```bash
   go mod tidy
   ```

### Single Node Setup

Run a single node (for development):
```bash
go run . -node-id=node1 -http-addr=127.0.0.1:8080 -raft-addr=127.0.0.1:7000 -data-dir=./data -bootstrap=true
```

### Multi-Node Cluster Setup

#### Option 1: Quick Start (3-node cluster)
```bash
./start-cluster.sh
```

#### Option 2: Manual Setup

**Start the bootstrap node (leader):**
```bash
go run . -node-id=node1 -http-addr=127.0.0.1:8080 -raft-addr=127.0.0.1:7000 -data-dir=./data/node1 -bootstrap=true
```

**Start additional nodes:**
```bash
# Node 2
go run . -node-id=node2 -http-addr=127.0.0.1:8081 -raft-addr=127.0.0.1:7001 -data-dir=./data/node2

# Node 3  
go run . -node-id=node3 -http-addr=127.0.0.1:8082 -raft-addr=127.0.0.1:7002 -data-dir=./data/node3
```

**Add nodes to cluster:**
```bash
curl -X POST http://127.0.0.1:8080/api/v1/cluster/add \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node2", "address": "127.0.0.1:7001"}'

curl -X POST http://127.0.0.1:8080/api/v1/cluster/add \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node3", "address": "127.0.0.1:7002"}'
```

### Usage Examples

#### Create a file
```bash
curl -X POST http://localhost:8080/api/v1/files \
  -H "Content-Type: application/json" \
  -d '{"path": "test.txt", "content": "Hello, World!"}'
```

#### Create a compressed file
```bash
curl -X POST http://localhost:8080/api/v1/files \
  -H "Content-Type: application/json" \
  -d '{"path": "test.txt", "content": "Hello, World!", "compression": true}'
```

#### Read a file
```bash
curl "http://localhost:8080/api/v1/files?path=test.txt"
```

#### Read a compressed file
```bash
curl "http://localhost:8080/api/v1/files?path=test.txt&compression=true"
```

#### Delete a file
```bash
curl -X DELETE "http://localhost:8080/api/v1/files?path=test.txt"
```

#### Cluster management
```bash
# Check cluster status
curl http://127.0.0.1:8080/api/v1/cluster/status

# Get leader information
curl http://127.0.0.1:8080/api/v1/cluster/leader

# Add a new node to cluster
curl -X POST http://127.0.0.1:8080/api/v1/cluster/add \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node4", "address": "127.0.0.1:7003"}'
```

#### Health check
```bash
curl http://localhost:8080/health
```

## Response Format

All responses follow this JSON format:

**Success Response:**
```json
{
  "success": true,
  "message": "Operation completed successfully",
  "data": "file content (for read operations)",
  "compression": true
}
```

**Error Response:**
```json
{
  "success": false,
  "error": "Error description"
}
```

## Security Features

- **Directory Traversal Protection**: Prevents access to files outside the intended scope
- **Path Validation**: Ensures file paths are properly formatted
- **Error Handling**: Comprehensive error messages for debugging

## Compression Features

- **Gzip Compression**: Files can be stored and retrieved with gzip compression
- **Automatic Decompression**: Compressed files are automatically decompressed when reading
- **Compression Flag**: Use `compression: true` in requests to enable compression
- **Query Parameter**: Use `?compression=true` in GET requests to decompress files

## Distributed Features

- **Raft Consensus**: All file operations are replicated across all nodes using Raft consensus
- **Leader Election**: Automatic leader election ensures high availability
- **Strong Consistency**: All nodes maintain identical file system state
- **Fault Tolerance**: Cluster continues operating even if some nodes fail
- **Dynamic Membership**: Add or remove nodes from the cluster at runtime

## Project Structure

```
.
├── main.go              # Main application file
├── raft.go              # Raft consensus implementation
├── go.mod               # Go module definition
├── go.sum               # Go module checksums
├── cluster-config.json  # Cluster configuration
├── start-cluster.sh     # Cluster startup script
└── README.md            # This file
```

## Dependencies

- `github.com/gorilla/mux` - HTTP router and URL matcher
- `github.com/hashicorp/raft` - Raft consensus algorithm
- `github.com/hashicorp/raft-boltdb` - BoltDB backend for Raft
