import psycopg

from benchmark_app.services.benchmark_engine import run_relational_benchmark
from benchmark_app.services.remote_metrics import RemoteMetricsSampler
from benchmark_app.services.workload import format_connection_target, validate_identifier


def test_connection(config):
    connect_kwargs = {
        "host": config["host"],
        "port": int(config["port"]),
        "dbname": config["database_name"],
        "user": config["username"] or None,
        "password": config["password"] or None,
        "connect_timeout": 5,
    }
    if config["ssl_enabled"]:
        connect_kwargs["sslmode"] = "require"

    with psycopg.connect(**connect_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()

    return (
        "Connection established successfully.\n\n"
        "Database type: PostgreSQL\n"
        f"Endpoint: {format_connection_target(config)}\n"
        f"Database: {config['database_name']}"
    )


def prepare_target(connection_config, target_name):
    target = validate_identifier(target_name, "table")
    connect_kwargs = {
        "host": connection_config["host"],
        "port": int(connection_config["port"]),
        "dbname": connection_config["database_name"],
        "user": connection_config["username"] or None,
        "password": connection_config["password"] or None,
        "connect_timeout": 5,
    }
    if connection_config["ssl_enabled"]:
        connect_kwargs["sslmode"] = "require"

    sql = f"""
    CREATE TABLE IF NOT EXISTS {target} (
        id BIGINT PRIMARY KEY,
        tenant_id VARCHAR(64),
        payload TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """

    with psycopg.connect(**connect_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()

    return f"Target table '{target}' is ready for benchmark inserts."


def run_benchmark(connection_config, workload_config, stop_event, metrics_queue):
    connect_kwargs = {
        "host": connection_config["host"],
        "port": int(connection_config["port"]),
        "dbname": connection_config["database_name"],
        "user": connection_config["username"] or None,
        "password": connection_config["password"] or None,
        "connect_timeout": 5,
    }
    if connection_config["ssl_enabled"]:
        connect_kwargs["sslmode"] = "require"

    prepare_target(connection_config, workload_config["workload"]["target_object"])
    metrics_queue.put(
        {
            "type": "log",
            "message": f"Target table '{workload_config['workload']['target_object']}' is ready.",
        }
    )
    remote_sampler = RemoteMetricsSampler.from_workload_config(workload_config)
    if remote_sampler.enabled():
        metrics_queue.put(
            {
                "type": "log",
                "message": f"Remote metrics agent configured: {remote_sampler.agent_url}",
            }
        )
    with psycopg.connect(**connect_kwargs) as connection:
        run_relational_benchmark(
            connection,
            "pyformat",
            workload_config,
            stop_event,
            metrics_queue,
            metrics_sampler=remote_sampler.sample if remote_sampler.enabled() else None,
        )
