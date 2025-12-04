# file: bench/scripts/netem-add-latency.sh
#!/usr/bin/env bash
set -euo pipefail

IFACE="${1:-eth0}"
DELAY_MS="${2:-200}"
JITTER_MS="${3:-50}"
LOSS_PCT="${4:-0}"

echo "Adding netem on ${IFACE}: delay=${DELAY_MS}ms jitter=${JITTER_MS}ms loss=${LOSS_PCT}%"

sudo tc qdisc add dev "${IFACE}" root netem \
  delay "${DELAY_MS}"ms "${JITTER_MS}"ms \
  loss "${LOSS_PCT}"%
