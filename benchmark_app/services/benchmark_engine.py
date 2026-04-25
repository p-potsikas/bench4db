import time
import json

import psutil

from benchmark_app.services.workload import build_random_payload, percentile, validate_identifier


def run_relational_benchmark(connection, paramstyle, workload_config, stop_event, metrics_queue, metrics_sampler=None):
    target_name = validate_identifier(workload_config["workload"]["target_object"], "table")
    read_ratio = int(workload_config["workload"]["read_ratio_percent"])
    keyspace_size = max(1, int(workload_config["execution"]["record_count"]))
    duration_seconds = max(1, int(workload_config["execution"]["duration_seconds"]))
    max_qps = max(0, int(workload_config["execution"]["max_qps"]))
    started_at = time.perf_counter()
    total_latency = 0.0
    latency_samples_ms = []
    errors = 0
    op_index = 0
    read_ops = 0
    write_ops = 0
    db_tx_bytes_total = 0
    db_rx_bytes_total = 0
    last_db_tx_bytes = 0
    last_db_rx_bytes = 0
    process = psutil.Process()
    process.cpu_percent(None)
    last_sample_time = started_at
    last_net_counters = psutil.net_io_counters()
    last_disk_counters = psutil.disk_io_counters()

    while True:
        elapsed_before = time.perf_counter() - started_at
        if stop_event.is_set() or elapsed_before >= duration_seconds:
            break
        op_index += 1

        op_started = time.perf_counter()
        try:
            record_id = ((op_index - 1) % keyspace_size) + 1
            if ((op_index - 1) % 100) < read_ratio:
                sent_bytes, received_bytes = _execute_read(connection, paramstyle, target_name, record_id)
                read_ops += 1
            else:
                payload = build_random_payload(workload_config)
                sent_bytes, received_bytes = _execute_write(connection, paramstyle, target_name, record_id, payload)
                write_ops += 1
            db_tx_bytes_total += sent_bytes
            db_rx_bytes_total += received_bytes
            if hasattr(connection, "commit"):
                connection.commit()
        except Exception:
            errors += 1
            if hasattr(connection, "rollback"):
                connection.rollback()

        op_latency_ms = (time.perf_counter() - op_started) * 1000
        total_latency += op_latency_ms / 1000
        latency_samples_ms.append(op_latency_ms)

        if max_qps > 0:
            target_elapsed = op_index / max_qps
            actual_elapsed = time.perf_counter() - started_at
            sleep_time = target_elapsed - actual_elapsed
            if sleep_time > 0:
                time.sleep(min(sleep_time, 0.2))

        if op_index == 1 or op_index % 25 == 0:
            sampled_at = time.perf_counter()
            elapsed = max(sampled_at - started_at, 1e-9)
            sample_interval = max(sampled_at - last_sample_time, 1e-9)
            cpu_percent = process.cpu_percent(None)
            ram_mb = process.memory_info().rss / (1024 * 1024)
            remaining_seconds = max(duration_seconds - elapsed, 0.0)
            net_counters = psutil.net_io_counters()
            disk_counters = psutil.disk_io_counters()
            network_rx_bytes = max(net_counters.bytes_recv - last_net_counters.bytes_recv, 0)
            network_tx_bytes = max(net_counters.bytes_sent - last_net_counters.bytes_sent, 0)
            disk_read_bytes = max(disk_counters.read_bytes - last_disk_counters.read_bytes, 0)
            disk_write_bytes = max(disk_counters.write_bytes - last_disk_counters.write_bytes, 0)
            db_rx_sample_bytes = max(db_rx_bytes_total - last_db_rx_bytes, 0)
            db_tx_sample_bytes = max(db_tx_bytes_total - last_db_tx_bytes, 0)
            extra_metrics = metrics_sampler(sample_interval) if metrics_sampler is not None else {}
            last_sample_time = sampled_at
            last_net_counters = net_counters
            last_disk_counters = disk_counters
            last_db_rx_bytes = db_rx_bytes_total
            last_db_tx_bytes = db_tx_bytes_total
            metrics_queue.put(
                {
                    "type": "metric",
                    "progress": min((elapsed / duration_seconds) * 100, 100),
                    "ops": op_index,
                    "read_ops": read_ops,
                    "write_ops": write_ops,
                    "errors": errors,
                    "throughput": op_index / elapsed,
                    "latency_ms": (total_latency / op_index) * 1000 if op_index else 0.0,
                    "p50_ms": percentile(latency_samples_ms, 50),
                    "p95_ms": percentile(latency_samples_ms, 95),
                    "p99_ms": percentile(latency_samples_ms, 99),
                    "cpu_percent": cpu_percent,
                    "ram_mb": ram_mb,
                    "remaining_seconds": remaining_seconds,
                    "network_rx_bytes": network_rx_bytes,
                    "network_tx_bytes": network_tx_bytes,
                    "network_rx_mbps": (network_rx_bytes / (1024 * 1024)) / sample_interval,
                    "network_tx_mbps": (network_tx_bytes / (1024 * 1024)) / sample_interval,
                    "db_rx_bytes": db_rx_sample_bytes,
                    "db_tx_bytes": db_tx_sample_bytes,
                    "db_rx_mbps": (db_rx_sample_bytes / (1024 * 1024)) / sample_interval,
                    "db_tx_mbps": (db_tx_sample_bytes / (1024 * 1024)) / sample_interval,
                    "disk_read_bytes": disk_read_bytes,
                    "disk_write_bytes": disk_write_bytes,
                    "disk_read_mbps": (disk_read_bytes / (1024 * 1024)) / sample_interval,
                    "disk_write_mbps": (disk_write_bytes / (1024 * 1024)) / sample_interval,
                    "status": "Benchmark running...",
                    **extra_metrics,
                }
            )

    completed_ops = op_index
    elapsed = max(time.perf_counter() - started_at, 1e-9)
    status = "Benchmark stopped." if stop_event.is_set() else "Benchmark completed."
    cpu_percent = process.cpu_percent(None)
    ram_mb = process.memory_info().rss / (1024 * 1024)
    sampled_at = time.perf_counter()
    sample_interval = max(sampled_at - last_sample_time, 1e-9)
    net_counters = psutil.net_io_counters()
    disk_counters = psutil.disk_io_counters()
    network_rx_bytes = max(net_counters.bytes_recv - last_net_counters.bytes_recv, 0)
    network_tx_bytes = max(net_counters.bytes_sent - last_net_counters.bytes_sent, 0)
    disk_read_bytes = max(disk_counters.read_bytes - last_disk_counters.read_bytes, 0)
    disk_write_bytes = max(disk_counters.write_bytes - last_disk_counters.write_bytes, 0)
    db_rx_sample_bytes = max(db_rx_bytes_total - last_db_rx_bytes, 0)
    db_tx_sample_bytes = max(db_tx_bytes_total - last_db_tx_bytes, 0)
    extra_metrics = metrics_sampler(sample_interval) if metrics_sampler is not None else {}
    metrics_queue.put(
        {
            "type": "done",
            "progress": 100 if not stop_event.is_set() else min((elapsed / duration_seconds) * 100, 100),
            "ops": completed_ops,
            "read_ops": read_ops,
            "write_ops": write_ops,
            "errors": errors,
            "throughput": completed_ops / elapsed if completed_ops else 0.0,
            "latency_ms": (total_latency / completed_ops) * 1000 if completed_ops else 0.0,
            "p50_ms": percentile(latency_samples_ms, 50),
            "p95_ms": percentile(latency_samples_ms, 95),
            "p99_ms": percentile(latency_samples_ms, 99),
            "cpu_percent": cpu_percent,
            "ram_mb": ram_mb,
            "remaining_seconds": 0.0,
            "network_rx_bytes": network_rx_bytes,
            "network_tx_bytes": network_tx_bytes,
            "network_rx_mbps": (network_rx_bytes / (1024 * 1024)) / sample_interval,
            "network_tx_mbps": (network_tx_bytes / (1024 * 1024)) / sample_interval,
            "db_rx_bytes": db_rx_sample_bytes,
            "db_tx_bytes": db_tx_sample_bytes,
            "db_rx_mbps": (db_rx_sample_bytes / (1024 * 1024)) / sample_interval,
            "db_tx_mbps": (db_tx_sample_bytes / (1024 * 1024)) / sample_interval,
            "disk_read_bytes": disk_read_bytes,
            "disk_write_bytes": disk_write_bytes,
            "disk_read_mbps": (disk_read_bytes / (1024 * 1024)) / sample_interval,
            "disk_write_mbps": (disk_write_bytes / (1024 * 1024)) / sample_interval,
            "status": status,
            "message": f"{status} Completed {completed_ops} operations in {elapsed:.2f}s over a {duration_seconds}s run window.",
            **extra_metrics,
        }
    )


def _execute_read(connection, paramstyle, target_name, record_id):
    query, params = _build_select_query(paramstyle, target_name, record_id)
    cursor = connection.cursor()
    try:
        cursor.execute(query, params)
        row = cursor.fetchone()
        sent_bytes = _estimate_wire_bytes(query) + _estimate_wire_bytes(params)
        received_bytes = _estimate_wire_bytes(row)
        return sent_bytes, received_bytes
    finally:
        cursor.close()


def _execute_write(connection, paramstyle, target_name, record_id, payload):
    query, params = _build_upsert_query(paramstyle, target_name, record_id, payload)
    cursor = connection.cursor()
    try:
        cursor.execute(query, params)
        sent_bytes = _estimate_wire_bytes(query) + _estimate_wire_bytes(params)
        return sent_bytes, 0
    finally:
        cursor.close()


def _build_select_query(paramstyle, target_name, record_id):
    if paramstyle == "qmark":
        return f"SELECT id, payload FROM {target_name} WHERE id = ?", (record_id,)
    if paramstyle == "pyformat":
        return f"SELECT id, payload FROM {target_name} WHERE id = %s", (record_id,)
    return f"SELECT id, payload FROM `{target_name}` WHERE id = %s", (record_id,)


def _build_upsert_query(paramstyle, target_name, record_id, payload):
    if paramstyle == "qmark":
        return (
            f"INSERT INTO {target_name} (id, tenant_id, payload, updated_at) "
            f"VALUES (?, ?, ?, CURRENT_TIMESTAMP) "
            f"ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP",
            (record_id, "tenant-a", payload),
        )
    if paramstyle == "pyformat":
        return (
            f"INSERT INTO {target_name} (id, tenant_id, payload) VALUES (%s, %s, %s) "
            f"ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()",
            (record_id, "tenant-a", payload),
        )
    return (
        f"INSERT INTO `{target_name}` (id, tenant_id, payload) VALUES (%s, %s, %s) "
        f"ON DUPLICATE KEY UPDATE payload = VALUES(payload), updated_at = CURRENT_TIMESTAMP",
        (record_id, "tenant-a", payload),
    )


def _estimate_wire_bytes(value):
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, (int, float, bool)):
        return len(str(value).encode("utf-8"))
    if isinstance(value, (list, tuple)):
        return sum(_estimate_wire_bytes(item) for item in value)
    if isinstance(value, dict):
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return len(str(value).encode("utf-8"))
