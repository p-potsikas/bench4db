import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import psutil

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


HOST = os.environ.get("BENCH4DB_AGENT_HOST", "0.0.0.0")
PORT = int(os.environ.get("BENCH4DB_AGENT_PORT", "8765"))
PG_DSN = os.environ.get("BENCH4DB_PG_DSN", "").strip()
AGENT_TOKEN = os.environ.get("BENCH4DB_AGENT_TOKEN", "").strip()


def _pg_metrics():
    if not PG_DSN or psycopg is None:
        return {"enabled": False}
    try:
        with psycopg.connect(PG_DSN, connect_timeout=3) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit
                    FROM pg_stat_database
                    WHERE datname = current_database()
                    """
                )
                row = cursor.fetchone()
        if not row:
            return {"enabled": False}
        numbackends, xact_commit, xact_rollback, blks_read, blks_hit = row
        total_blocks = (blks_read or 0) + (blks_hit or 0)
        hit_ratio = ((blks_hit or 0) / total_blocks) if total_blocks else 0.0
        return {
            "enabled": True,
            "connections": int(numbackends or 0),
            "xact_commit": int(xact_commit or 0),
            "xact_rollback": int(xact_rollback or 0),
            "blks_read": int(blks_read or 0),
            "blks_hit": int(blks_hit or 0),
            "cache_hit_ratio": hit_ratio,
        }
    except Exception as error:
        return {"enabled": False, "error": str(error)}


def collect_metrics():
    virtual_memory = psutil.virtual_memory()
    disk_io = psutil.disk_io_counters()
    network_io = psutil.net_io_counters()
    return {
        "host": {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "ram_used_mb": virtual_memory.used / (1024 * 1024),
            "ram_percent": virtual_memory.percent,
            "disk_read_bytes": getattr(disk_io, "read_bytes", 0),
            "disk_write_bytes": getattr(disk_io, "write_bytes", 0),
            "network_rx_bytes": getattr(network_io, "bytes_recv", 0),
            "network_tx_bytes": getattr(network_io, "bytes_sent", 0),
        },
        "postgresql": _pg_metrics(),
    }


class MetricsHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _is_authorized(self):
        if not AGENT_TOKEN:
            return True
        return self.headers.get("X-Bench4DB-Token", "") == AGENT_TOKEN

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json({"status": "ok"})
            return
        if not self._is_authorized():
            self._send_json({"error": "unauthorized"}, status=401)
            return
        if parsed.path == "/metrics":
            self._send_json(collect_metrics())
            return
        self._send_json({"error": "not_found"}, status=404)

    def log_message(self, format, *args):  # noqa: A003
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), MetricsHandler)
    print(f"bench4db agent listening on http://{HOST}:{PORT}")
    server.serve_forever()
