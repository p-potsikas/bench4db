import json
import random
import re
import string


def format_connection_target(config):
    host = config.get("host") or "localhost"
    port = config.get("port")
    return f"{host}:{port}" if port else host


def validate_identifier(identifier, kind):
    value = identifier.strip()
    if not value:
        raise ValueError(f"Enter a {kind} name first.")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            f"Invalid {kind} name '{value}'. Use only letters, numbers, and underscores, "
            "starting with a letter or underscore."
        )
    return value


def parse_scale_plan(scale_plan):
    try:
        values = [int(part.strip()) for part in scale_plan.split(",") if part.strip()]
    except ValueError:
        return []
    return [value for value in values if value > 0]


def estimate_size_gb(data_size_mb):
    try:
        size_value = float(data_size_mb)
    except ValueError:
        return ""
    if size_value <= 0:
        return ""
    return f"{size_value / 1024:.2f}"


def estimate_field_size_bytes(data_size_mb, record_count, field_count):
    try:
        total_data_mb = float(data_size_mb)
        total_records = int(record_count)
        total_fields = int(field_count)
    except ValueError:
        return 0

    if total_data_mb <= 0 or total_records <= 0 or total_fields <= 0:
        return 0

    total_bytes = int(total_data_mb * 1024 * 1024)
    return max(1, total_bytes // (total_records * total_fields))


def build_random_payload(workload_config):
    field_count = int(workload_config["workload"]["field_count"])
    field_size_bytes = max(
        1,
        estimate_field_size_bytes(
            workload_config["execution"]["data_size_mb"],
            workload_config["execution"]["record_count"],
            workload_config["workload"]["field_count"],
        ),
    )
    alphabet = string.ascii_letters + string.digits
    payload = {
        f"field_{index + 1}": "".join(random.choices(alphabet, k=field_size_bytes))
        for index in range(field_count)
    }
    return json.dumps(payload, separators=(",", ":"))


def percentile(values, percentile_value):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((percentile_value / 100) * (len(ordered) - 1)))))
    return ordered[index]
