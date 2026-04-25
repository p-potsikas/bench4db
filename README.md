# 🧪 bench4db

<p>
  <strong>An experimental benchmarking tool for databases</strong><br>
  <span style="color:#1f6feb;">Current focus:</span> PostgreSQL<br>
  <span style="color:#1d6b43;">Planned expansion:</span> SQL, NoSQL, and NewSQL systems
</p>

This project is an experimental database benchmarking tool.

Its current first version is focused on PostgreSQL. The broader goal is to expand it over time to additional database systems, including SQL, NoSQL, and NewSQL platforms.

It is being developed as a supporting research utility in the context of my doctoral dissertation.

## 🚀 Current Scope

At this stage, the application provides a desktop UI for configuring and running PostgreSQL benchmark workloads.

The current implementation supports:

- PostgreSQL connection setup
- connection testing
- workload configuration
- automatic target table preparation
- benchmark execution
- live metrics
- charts
- report generation
- raw metric export

## 🧭 How It Works

The application currently follows a three-step flow in a single window.

### 1. 🔌 Connection Setup

In the first step, the user configures the PostgreSQL connection.

The UI collects:

- connection name
- host or IP
- port
- database name
- username
- password
- SSL/TLS option
- topology information
- optional extra nodes for cluster-style setups

The user can also run a connection test before moving to the next step.

### 2. ⚙️ Workload Setup

In the second step, the user defines the benchmark workload.

The UI currently allows configuration of:

- workload name
- workload type
- target table
- fields per record
- read ratio
- write ratio
- batch size
- client threads
- duration
- ramp-up time
- record count
- dataset size in MB
- estimated dataset size in GB
- key distribution
- max QPS
- replication factor
- node scale plan
- failure scenario
- cost-related fields
- consistency level

The user can also prepare the target table from the UI.

### 3. 📈 Run Benchmark

In the third step, the benchmark is executed and monitored live.

The run view provides:

- start and stop actions
- live progress
- run log
- results panel
- report button
- raw TXT export
- chart export

## 🏗️ PostgreSQL Target Preparation

Before running the workload, the tool prepares the benchmark target if needed.

In the current implementation, it creates the target table automatically when it does not already exist.

The current PostgreSQL table structure includes:

- `id`
- `tenant_id`
- `payload`
- `created_at`
- `updated_at`

This schema is part of the current first implementation and may evolve in later revisions.

## 🔄 Benchmark Behavior

The benchmark is duration-based.

For each operation:

- a read operation executes a primary-key lookup
- a write operation executes an upsert

The read/write pattern is controlled by the configured read ratio and write ratio.

The tool cycles through record IDs from `1` up to the configured record count.

The generated write data is random alphanumeric content derived from:

- total data size
- record count
- field count

## 📊 What the Tool Measures

During execution, the application displays live metrics in the UI.

The current metrics include:

- total completed operations
- read operations
- write operations
- errors
- throughput
- average latency
- p50 latency
- p95 latency
- p99 latency
- CPU usage
- RAM usage
- DB traffic (estimated)
- disk I/O
- remaining time

## 🌐 What “DB Traffic” Means

The `DB Traffic` metric is an estimated benchmark-to-database traffic metric.

It is not a full packet-capture measurement of the operating system or the network interface.

Instead, it estimates:

- bytes sent by the benchmark client to the database
- bytes returned by the database to the benchmark client

This was designed intentionally so the metric can later be extended to additional database systems without depending on platform-specific packet capture tooling.

## 📝 Results and Outputs

After or during a run, the tool provides:

- live metric values in the run screen
- charts for throughput, latency, CPU/RAM, DB traffic, and disk I/O
- a popup report with average values from the run
- raw metric export as TXT
- chart export as PNG

The report currently summarizes average values for:

- throughput
- latency
- p50 / p95 / p99 latency
- CPU
- RAM
- estimated DB traffic
- disk I/O

It also includes the final:

- total operations
- read operations
- write operations
- error count

## 🎓 Intended Research Use

This tool is being developed as a supporting benchmarking and observation utility for my doctoral research.

Its role is to help structure repeatable experiments, compare system behavior, and collect practical performance indicators through a controlled UI-driven workflow.

The current release should be considered a first experimental version centered on PostgreSQL, with future planned expansion toward:

- additional SQL databases
- NoSQL databases
- NewSQL databases

## ⬇️ Getting the Project

You can clone the repository from GitHub with:

```bash
git clone https://github.com/p-potsikas/bench4db.git
cd bench4db
```

## ▶️ Running the Application

From the project root:

```bash
python app.py
```

If you are using the project virtual environment:

```bash
.venv/bin/python app.py
```
