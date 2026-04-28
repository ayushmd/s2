.PHONY: clean run worker start dev

# --- Cleanup ---
clean:
	rm -rf data1 data2 data3

# --- Run master manually ---
run:
	rm -rf data1
	go run . \
		--node 127.0.0.1:7000 \
		--raft-addr 127.0.0.1:7001 \
		--http-addr :7000 \
		--dir data1

# --- Run worker manually ---
worker:
	rm -rf data2
	go run . \
		--node 127.0.0.1:8000 \
		--raft-addr 127.0.0.1:8001 \
		--http-addr :8000 \
		--dir data2 \
		--master 127.0.0.1:7000


worker2:
	rm -rf data3
	go run . \
		--node 127.0.0.1:9000 \
		--raft-addr 127.0.0.1:9001 \
		--http-addr :9000 \
		--dir data3 \
		--master 127.0.0.1:7000

# --- Clean and run both master and worker ---
start: clean
	@echo "🚀 Starting master and worker..."
	@trap 'echo "\n🧹 Stopping nodes..."; kill $$(jobs -p) 2>/dev/null || true' INT TERM EXIT; \
	go run . \
		--node 127.0.0.1:8000 \
		--raft-addr 127.0.0.1:7000 \
		--http-addr :8000 \
		--dir data1 > logs/master.log 2>&1 & \
	sleep 2; \
	go run . \
		--node 127.0.0.1:9000 \
		--raft-addr 127.0.0.1:9001 \
		--http-addr :9000 \
		--dir data2 \
		--master 127.0.0.1:8000 > logs/worker.log 2>&1 & \
	tail -f logs/master.log
	wait

# --- Development mode: watches and re-runs `make start` on changes ---
dev:
	@echo "🔁 Watching for changes..."
	air -c .air.toml
