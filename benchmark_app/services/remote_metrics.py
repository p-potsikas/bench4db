import json
from urllib.error import URLError
from urllib.request import Request, urlopen


class RemoteMetricsSampler:
    def __init__(self, agent_url, token=""):
        self.agent_url = (agent_url or "").strip().rstrip("/")
        self.token = (token or "").strip()
        self.last_error = ""
        self._last_host = None

    @classmethod
    def from_workload_config(cls, workload_config):
        telemetry = workload_config.get("telemetry", {})
        return cls(
            telemetry.get("agent_url", ""),
            telemetry.get("agent_token", ""),
        )

    def enabled(self):
        return bool(self.agent_url)

    def sample(self, sample_interval):
        if not self.enabled():
            return {}
        request = Request(f"{self.agent_url}/metrics")
        if self.token:
            request.add_header("X-Bench4DB-Token", self.token)
        try:
            with urlopen(request, timeout=3) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (URLError, OSError, ValueError) as error:
            self.last_error = str(error)
            return {
                "remote_agent_ok": False,
                "remote_agent_error": self.last_error,
            }

        host = payload.get("host", {})
        postgresql = payload.get("postgresql", {})
        if self._last_host is None:
            self._last_host = host
            return {
                "remote_agent_ok": True,
                "server_cpu_percent": float(host.get("cpu_percent", 0.0)),
                "server_ram_mb": float(host.get("ram_used_mb", 0.0)),
                "server_ram_percent": float(host.get("ram_percent", 0.0)),
                "server_disk_read_mbps": 0.0,
                "server_disk_write_mbps": 0.0,
                "server_network_rx_mbps": 0.0,
                "server_network_tx_mbps": 0.0,
                "server_pg_connections": int(postgresql.get("connections", 0)) if postgresql.get("enabled") else 0,
                "remote_agent_error": "",
            }

        disk_read_bytes = max(int(host.get("disk_read_bytes", 0)) - int(self._last_host.get("disk_read_bytes", 0)), 0)
        disk_write_bytes = max(int(host.get("disk_write_bytes", 0)) - int(self._last_host.get("disk_write_bytes", 0)), 0)
        network_rx_bytes = max(int(host.get("network_rx_bytes", 0)) - int(self._last_host.get("network_rx_bytes", 0)), 0)
        network_tx_bytes = max(int(host.get("network_tx_bytes", 0)) - int(self._last_host.get("network_tx_bytes", 0)), 0)
        self._last_host = host

        divisor = max(sample_interval, 1e-9) * 1024 * 1024
        self.last_error = ""
        return {
            "remote_agent_ok": True,
            "remote_agent_error": "",
            "server_cpu_percent": float(host.get("cpu_percent", 0.0)),
            "server_ram_mb": float(host.get("ram_used_mb", 0.0)),
            "server_ram_percent": float(host.get("ram_percent", 0.0)),
            "server_disk_read_mbps": disk_read_bytes / divisor,
            "server_disk_write_mbps": disk_write_bytes / divisor,
            "server_network_rx_mbps": network_rx_bytes / divisor,
            "server_network_tx_mbps": network_tx_bytes / divisor,
            "server_pg_connections": int(postgresql.get("connections", 0)) if postgresql.get("enabled") else 0,
        }
