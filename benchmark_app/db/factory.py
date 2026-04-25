from benchmark_app.db import postgresql


def test_connection(config):
    return postgresql.test_connection(config)


def prepare_target(connection_config, target_name):
    return postgresql.prepare_target(connection_config, target_name)


def run_benchmark(connection_config, workload_config, stop_event, metrics_queue):
    return postgresql.run_benchmark(connection_config, workload_config, stop_event, metrics_queue)
