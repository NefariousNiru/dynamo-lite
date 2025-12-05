# file: src/client.py
from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from src.config import NodeConfig, CONFIG


@dataclass
class KvReadResult:
    found: bool
    value: Optional[bytes]
    clock: Dict[str, int]


class DynLiteHttpClient:
    """
    Thin HTTP client for talking to a single DynLite node.
    """

    def __init__(self, node: NodeConfig, timeout: float = 5.0) -> None:
        self.node = node
        self._client = httpx.Client(base_url=node.base_url, timeout=timeout)

    # -------------- basic KV operations --------------

    def put(
        self,
        key: str,
        value: bytes,
        coord_node_id: Optional[str] = None,
    ) -> None:
        value_b64 = base64.b64encode(value).decode("ascii")
        payload: Dict[str, Any] = {"valueBase64": value_b64}
        if coord_node_id:
            # WebServer's PutRequest expects nodeId
            payload["nodeId"] = coord_node_id

        resp = self._client.put(f"/kv/{key}", json=payload)
        resp.raise_for_status()

    def delete(self, key: str, coord_node_id: Optional[str] = None) -> None:
        """
        Logically delete a key (tombstone).

        WebServer's DeleteRequest expects a JSON body with nodeId/opId; we only
        care about nodeId here.
        """
        payload: Dict[str, Any] = {}
        if coord_node_id:
            payload["nodeId"] = coord_node_id

        if payload:
            body = json.dumps(payload).encode("utf-8")
            resp = self._client.request(
                "DELETE",
                f"/kv/{key}",
                content=body,
                headers={"Content-Type": "application/json"},
            )
        else:
            resp = self._client.request("DELETE", f"/kv/{key}")

        resp.raise_for_status()

    def get(self, key: str) -> KvReadResult:
        """
        Read the current value for key from this node.

        Semantics:
          - 200: found or not (found=false if no value).
          - 404: not found (we translate to found=False, no exception).
        """
        resp = self._client.get(f"/kv/{key}")

        if resp.status_code == 404:
            # WebServer sends { "found": false } with 404 when key is absent.
            return KvReadResult(found=False, value=None, clock={})

        resp.raise_for_status()
        data = resp.json()

        found = bool(data.get("found", False))
        if not found:
            return KvReadResult(found=False, value=None, clock={})

        value_b64 = data.get("valueBase64")
        if value_b64 is None:
            return KvReadResult(found=False, value=None, clock={})

        try:
            raw = base64.b64decode(value_b64)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to decode Base64 value for key={key!r}") from exc

        # Java DTO uses 'vectorClock'
        clock_raw = data.get("vectorClock", {}) or {}
        clock = {k: int(v) for k, v in clock_raw.items()}
        return KvReadResult(found=True, value=raw, clock=clock)

    # -------------- admin / debug operations --------------

    def fetch_merkle_snapshot(
        self,
        start_token: int = -2**63,
        end_token: int = 2**63 - 1,
        leaf_count: int = 1024,
    ) -> Dict[str, Any]:
        params = {
            "startToken": str(start_token),
            "endToken": str(end_token),
            "leafCount": str(leaf_count),
        }
        resp = self._client.get("/admin/anti-entropy/merkle-snapshot", params=params)
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self._client.close()


def build_cluster_clients() -> Dict[str, DynLiteHttpClient]:
    clients: Dict[str, DynLiteHttpClient] = {}
    for node in CONFIG.cluster.nodes:
        clients[node.name] = DynLiteHttpClient(node)
    return clients
