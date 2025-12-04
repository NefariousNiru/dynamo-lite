# file: bench/scripts/netem-clear.sh
#!/usr/bin/env bash
set -euo pipefail

IFACE="${1:-eth0}"

echo "Clearing netem on ${IFACE}"
sudo tc qdisc del dev "${IFACE}" root || true
