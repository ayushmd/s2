#!/bin/bash

# Start a 3-node Raft cluster for distributed file management

echo "Starting 3-node Raft cluster..."

# Create data directories
# mkdir -p ./data/node1
# mkdir -p ./data/node2  
# mkdir -p ./data/node3
rm -rf data1
rm -rf data2
rm -rf data3

# Start node 1 (bootstrap)
echo "Starting node 1 (bootstrap leader)..."
go run . -node='127.0.0.1:8000' -http-addr='127.0.0.1:8000' -raft-addr='127.0.0.1:7000' -dir=./data1 &
NODE1_PID=$!

# Wait for node 1 to start
sleep 3

# Start node 2
echo "Starting node 2..."
go run . -node='127.0.0.1:8001' -http-addr='127.0.0.1:8001' -raft-addr='127.0.0.1:7001' -dir=./data2 -master='127.0.0.1:8000' &
NODE2_PID=$!

# Wait for node 2 to start
sleep 1

# Start node 3
echo "Starting node 3..."
go run . -node='127.0.0.1:8002' -http-addr='127.0.0.1:8002' -raft-addr='127.0.0.1:7002' -dir=./data3 -master='127.0.0.1:8000' &
NODE3_PID=$!

# Wait for all nodes to start
sleep 3

echo "Cluster started!"
echo "Node 1: http://127.0.0.1:8000 (Leader)"
echo "Node 2: http://127.0.0.1:8001"
echo "Node 3: http://127.0.0.1:8002"
echo ""
echo "Test the cluster:"
echo "curl http://127.0.0.1:8080/api/v1/cluster/status"
echo ""
echo "Press Ctrl+C to stop all nodes"

# Function to cleanup on exit
cleanup() {
    echo "Stopping cluster..."
    kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
    exit
}

# Set trap to cleanup on script exit
trap cleanup SIGINT SIGTERM

# Wait for user to stop
wait
