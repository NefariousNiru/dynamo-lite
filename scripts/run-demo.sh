# file: scripts/run-demo.sh
#!/usr/bin/env bash
set -euo pipefail

# Resolve project root (script is in scripts/, go one level up)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[demo] Building server jar…"

# Try to build a fat jar for the server module if available
if ./gradlew :server:clean :server:shadowJar >/dev/null 2>&1; then
  JAR_PATH="$(ls server/build/libs/*-all.jar 2>/dev/null | head -n1)"
else
  # Fallback: plain server jar
  ./gradlew :server:clean :server:jar
  JAR_PATH="$(ls server/build/libs/*.jar 2>/dev/null | head -n1)"
fi

if [[ -z "${JAR_PATH:-}" || ! -f "$JAR_PATH" ]]; then
  echo "[demo] ERROR: Could not find built jar under server/build/libs"
  ls -R server/build || true
  exit 1
fi

echo "[demo] Using jar: $JAR_PATH"

# Cluster config path
CLUSTER_CFG="$ROOT_DIR/config/cluster-3nodes.json"
if [[ ! -f "$CLUSTER_CFG" ]]; then
  echo "[demo] ERROR: Cluster config not found at $CLUSTER_CFG"
  exit 1
fi

# Per-node data directories
mkdir -p data/node-a/wal data/node-a/snap
mkdir -p data/node-b/wal data/node-b/snap
mkdir -p data/node-c/wal data/node-c/snap

echo "[demo] Starting 3 nodes…"
echo "  - gRPC ports: 50051 (a), 50052 (b), 50053 (c) from cluster-3nodes.json"
echo "  - HTTP ports: 8080 (a), 8081 (b), 8082 (c) from CLI"

# Optional: enable auth for the demo if you want
# export DYNLITE_AUTH_TOKEN="demo-secret-token"

# Node A
java -jar "$JAR_PATH" \
  --node-id node-a \
  --http-port 8080 \
  --wal ./data/node-a/wal \
  --snap ./data/node-a/snap \
  --dedupe-ttl-seconds 600 \
  --cluster-config "$CLUSTER_CFG" \
  > logs-node-a.out 2>&1 &

PID_A=$!
echo "  node-a -> PID $PID_A (HTTP :8080, gRPC :50051)"

# Node B
java -jar "$JAR_PATH" \
  --node-id node-b \
  --http-port 8081 \
  --wal ./data/node-b/wal \
  --snap ./data/node-b/snap \
  --dedupe-ttl-seconds 600 \
  --cluster-config "$CLUSTER_CFG" \
  > logs-node-b.out 2>&1 &

PID_B=$!
echo "  node-b -> PID $PID_B (HTTP :8081, gRPC :50052)"

# Node C
java -jar "$JAR_PATH" \
  --node-id node-c \
  --http-port 8082 \
  --wal ./data/node-c/wal \
  --snap ./data/node-c/snap \
  --dedupe-ttl-seconds 600 \
  --cluster-config "$CLUSTER_CFG" \
  > logs-node-c.out 2>&1 &

PID_C=$!
echo "  node-c -> PID $PID_C (HTTP :8082, gRPC :50053)"

echo
echo "[demo] All nodes started."
echo "       Try:"
echo "         curl -X PUT http://localhost:8080/kv/foo -d '{\"valueBase64\":\"YmFy\",\"nodeId\":\"client-1\"}' -H 'Content-Type: application/json'"
echo "         curl http://localhost:8081/kv/foo"
echo
echo "[demo] To stop them: kill $PID_A $PID_B $PID_C"
