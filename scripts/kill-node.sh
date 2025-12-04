# file: bench/scripts/kill-node.sh
#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <nodeId-pattern>"
  exit 1
fi

NODE_PATTERN="$1"

# This is crude: it kills any Java process whose command line contains both
# io.dynlite.server.Main and the given nodeId.
PIDS=$(ps aux | grep 'io.dynlite.server.Main' | grep "${NODE_PATTERN}" | grep -v grep | awk '{print $2}')

if [[ -z "${PIDS}" ]]; then
  echo "No matching dynlite node for pattern: ${NODE_PATTERN}"
  exit 0
fi

echo "Killing PIDs: ${PIDS}"
kill ${PIDS}
