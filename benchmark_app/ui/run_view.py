import json
import queue
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import psutil
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from benchmark_app.constants import PREVIEW_BG, PREVIEW_FG
from benchmark_app.db.factory import run_benchmark
from benchmark_app.services.workload import format_connection_target


class RunBenchmarkView(ttk.Frame):
    def __init__(self, parent, connection_config, workload_config, on_back=None):
        super().__init__(parent, style="InnerCard.TFrame")
        self.connection_config = connection_config
        self.workload_config = workload_config
        self.on_back = on_back

        self.progress_var = tk.DoubleVar(value=0.0)
        self.status_var = tk.StringVar(value="Ready to run benchmark.")
        self.ops_var = tk.StringVar(value="0")
        self.read_ops_var = tk.StringVar(value="0")
        self.write_ops_var = tk.StringVar(value="0")
        self.errors_var = tk.StringVar(value="0")
        self.throughput_var = tk.StringVar(value="0 ops/sec")
        self.latency_var = tk.StringVar(value="0.00 ms")
        self.p50_var = tk.StringVar(value="0.00 ms")
        self.p95_var = tk.StringVar(value="0.00 ms")
        self.p99_var = tk.StringVar(value="0.00 ms")
        self.cpu_var = tk.StringVar(value="0.00 %")
        self.ram_var = tk.StringVar(value="0.00 MB")
        self.network_var = tk.StringVar(value="DB Rx 0.00 MB/s | DB Tx 0.00 MB/s")
        self.disk_var = tk.StringVar(value="Read 0.00 MB/s | Write 0.00 MB/s")
        self.remaining_var = tk.StringVar(value="0.00 s")

        self.log_text = None
        self.summary_container = None
        self.summary_toggle_button = None
        self.summary_visible = True
        self.results_card = None
        self.results_visible = False
        self.metrics_history = []
        self.results_text = None
        self.chart_figure = None
        self.chart_canvas = None
        self._stop_event = threading.Event()
        self._metrics_queue = queue.Queue()
        self._worker_thread = None
        self._runner_active = False
        self._process = psutil.Process()

        self._build_layout()
        self._refresh_summary()

    def _build_layout(self):
        root = ttk.Frame(self, style="InnerCard.TFrame")
        root.pack(fill="both", expand=True)

        hero = ttk.Frame(root, style="Hero.TFrame", padding=22)
        hero.pack(fill="x", pady=(0, 18))

        title_block = ttk.Frame(hero, style="InnerCard.TFrame")
        title_block.pack(fill="x")

        ttk.Label(title_block, text="Run Benchmark", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            title_block,
            text="Execute the configured workload and monitor live metrics.",
            style="Subheader.TLabel",
        ).pack(anchor="w", pady=(6, 0))

        self._build_action_buttons(root)

        top = ttk.Frame(root)
        top.pack(fill="both", expand=True)

        left_panel = ttk.Frame(top, style="Card.TFrame", padding=20)
        left_panel.pack(side="left", fill="both", expand=True, padx=(0, 15))

        right_panel = ttk.Frame(top, style="Card.TFrame", padding=20)
        right_panel.pack(side="right", fill="both", expand=True, padx=(15, 0))

        self._build_summary_panel(left_panel)
        self._build_metrics_panel(right_panel)
        self._build_log_panel(root)
        self._build_results_panel(root)

    def _build_summary_panel(self, parent):
        header = ttk.Frame(parent, style="InnerCard.TFrame")
        header.pack(fill="x", pady=(0, 10))
        ttk.Label(header, text="Execution Summary", style="Section.TLabel").pack(side="left")
        self.summary_toggle_button = ttk.Button(header, text="Hide Config", command=self._toggle_summary)
        self.summary_toggle_button.pack(side="right")
        ttk.Label(
            parent,
            text="Review the selected connection and workload before starting the run.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(0, 16))

        self.summary_container = ttk.Frame(parent, style="InnerCard.TFrame")
        self.summary_container.pack(fill="both", expand=True)
        self.summary_text = tk.Text(
            self.summary_container,
            height=16,
            wrap="word",
            font=("SFMono-Regular", 10),
            bg=PREVIEW_BG,
            fg=PREVIEW_FG,
            insertbackground="#ffffff",
            relief="flat",
            padx=16,
            pady=16,
        )
        self.summary_text.pack(fill="both", expand=True)
        self.summary_text.configure(state="disabled")

    def _build_metrics_panel(self, parent):
        ttk.Label(parent, text="Live Metrics", style="Section.TLabel").pack(anchor="w", pady=(0, 10))
        ttk.Label(
            parent,
            text="Track throughput, latency, errors, and benchmark progress while the run is active.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(0, 16))

        metrics = ttk.Frame(parent, style="InnerCard.TFrame")
        metrics.pack(fill="x")

        self._metric_row(metrics, "Status", ttk.Label(metrics, textvariable=self.status_var, style="Card.TLabel"))
        self._metric_row(metrics, "Completed operations", ttk.Label(metrics, textvariable=self.ops_var, style="Card.TLabel"))
        self._metric_row(metrics, "Read ops", ttk.Label(metrics, textvariable=self.read_ops_var, style="Card.TLabel"))
        self._metric_row(metrics, "Write ops", ttk.Label(metrics, textvariable=self.write_ops_var, style="Card.TLabel"))
        self._metric_row(metrics, "Errors", ttk.Label(metrics, textvariable=self.errors_var, style="Card.TLabel"))
        self._metric_row(metrics, "Throughput", ttk.Label(metrics, textvariable=self.throughput_var, style="Card.TLabel"))
        self._metric_row(metrics, "Avg latency", ttk.Label(metrics, textvariable=self.latency_var, style="Card.TLabel"))
        self._metric_row(metrics, "p50 latency", ttk.Label(metrics, textvariable=self.p50_var, style="Card.TLabel"))
        self._metric_row(metrics, "p95 latency", ttk.Label(metrics, textvariable=self.p95_var, style="Card.TLabel"))
        self._metric_row(metrics, "p99 latency", ttk.Label(metrics, textvariable=self.p99_var, style="Card.TLabel"))
        self._metric_row(metrics, "CPU", ttk.Label(metrics, textvariable=self.cpu_var, style="Card.TLabel"))
        self._metric_row(metrics, "RAM", ttk.Label(metrics, textvariable=self.ram_var, style="Card.TLabel"))
        self._metric_row(metrics, "DB Traffic", ttk.Label(metrics, textvariable=self.network_var, style="Card.TLabel"))
        self._metric_row(metrics, "Disk I/O", ttk.Label(metrics, textvariable=self.disk_var, style="Card.TLabel"))
        self._metric_row(metrics, "Remaining time", ttk.Label(metrics, textvariable=self.remaining_var, style="Card.TLabel"))

        progress_card = ttk.Frame(parent, style="InnerCard.TFrame")
        progress_card.pack(fill="x", pady=(18, 0))
        ttk.Label(progress_card, text="Progress", style="Field.TLabel").pack(anchor="w")
        ttk.Progressbar(
            progress_card,
            variable=self.progress_var,
            maximum=100,
            style="Benchmark.Horizontal.TProgressbar",
        ).pack(fill="x", pady=(8, 0))

    def _build_log_panel(self, parent):
        card = ttk.Frame(parent, style="Card.TFrame", padding=20)
        card.pack(fill="x", pady=(18, 0))
        header = ttk.Frame(card, style="InnerCard.TFrame")
        header.pack(fill="x", pady=(0, 8))
        ttk.Label(header, text="Run Log", style="Section.TLabel").pack(side="left")
        ttk.Label(header, text="Execution events", style="Hint.TLabel").pack(side="right")

        self.log_text = tk.Text(
            card,
            height=10,
            wrap="word",
            font=("SFMono-Regular", 10),
            bg=PREVIEW_BG,
            fg=PREVIEW_FG,
            insertbackground="#ffffff",
            relief="flat",
            padx=16,
            pady=16,
        )
        self.log_text.pack(fill="x")
        self.log_text.configure(state="disabled")
        self._append_log("Benchmark runner initialized.")

    def _build_results_panel(self, parent):
        self.results_card = ttk.Frame(parent, style="Card.TFrame", padding=20)
        header = ttk.Frame(self.results_card, style="InnerCard.TFrame")
        header.pack(fill="x", pady=(0, 8))
        ttk.Label(header, text="Results", style="Section.TLabel").pack(side="left")
        ttk.Label(header, text="Charts and final metrics", style="Hint.TLabel").pack(side="right")

        self.results_text = tk.Text(
            self.results_card,
            height=8,
            wrap="word",
            font=("SFMono-Regular", 10),
            bg=PREVIEW_BG,
            fg=PREVIEW_FG,
            insertbackground="#ffffff",
            relief="flat",
            padx=16,
            pady=16,
        )
        self.results_text.pack(fill="x", pady=(0, 12))
        self.results_text.configure(state="disabled")

        self.chart_figure = Figure(figsize=(7, 3.2), dpi=100)
        self.chart_canvas = FigureCanvasTkAgg(self.chart_figure, master=self.results_card)
        self.chart_canvas.get_tk_widget().pack(fill="both", expand=True)

    def _build_action_buttons(self, parent):
        buttons = ttk.Frame(parent, style="Card.TFrame", padding=18)
        buttons.pack(fill="x", pady=(0, 18))
        left_actions = ttk.Frame(buttons, style="InnerCard.TFrame")
        left_actions.pack(side="left", fill="x", expand=True)
        right_actions = ttk.Frame(buttons, style="InnerCard.TFrame")
        right_actions.pack(side="right")

        ttk.Button(left_actions, text="Report", command=self._show_report).pack(side="left")
        ttk.Button(left_actions, text="Export Raw TXT", command=self._export_raw_txt).pack(side="left", padx=(8, 0))
        ttk.Button(left_actions, text="Save Chart PNG", command=self._save_chart_png).pack(side="left", padx=(8, 0))
        ttk.Button(right_actions, text="Back to Workload", command=self._go_back).pack(side="right")
        ttk.Button(right_actions, text="Stop Benchmark", command=self._stop_benchmark).pack(side="right", padx=(0, 8))
        ttk.Button(right_actions, text="Start Benchmark", style="Primary.TButton", command=self._start_benchmark).pack(
            side="right", padx=(0, 8)
        )

    def _metric_row(self, parent, label, widget):
        row = len(parent.grid_slaves()) // 2
        ttk.Label(parent, text=label, style="Field.TLabel").grid(row=row, column=0, sticky="w", pady=8, padx=(0, 12))
        widget.grid(row=row, column=1, sticky="w", pady=8)
        parent.columnconfigure(1, weight=1)

    def _refresh_summary(self):
        payload = {
            "connection": {
                "name": self.connection_config.get("connection_name"),
                "database_type": self.connection_config.get("database_type"),
                "target": format_connection_target(self.connection_config),
                "database_name": self.connection_config.get("database_name"),
            },
            "workload": self.workload_config,
        }
        formatted = json.dumps(payload, indent=2, ensure_ascii=False)
        self.summary_text.configure(state="normal")
        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert(tk.END, formatted)
        self.summary_text.configure(state="disabled")

    def _append_log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _start_benchmark(self):
        if self._runner_active:
            messagebox.showinfo("Benchmark already running", "A benchmark run is already in progress.", parent=self)
            return

        self._stop_event.clear()
        self.progress_var.set(0)
        self.ops_var.set("0")
        self.read_ops_var.set("0")
        self.write_ops_var.set("0")
        self.errors_var.set("0")
        self.throughput_var.set("0 ops/sec")
        self.latency_var.set("0.00 ms")
        self.p50_var.set("0.00 ms")
        self.p95_var.set("0.00 ms")
        self.p99_var.set("0.00 ms")
        self.cpu_var.set("0.00 %")
        self.ram_var.set("0.00 MB")
        self.network_var.set("DB Rx 0.00 MB/s | DB Tx 0.00 MB/s")
        self.disk_var.set("Read 0.00 MB/s | Write 0.00 MB/s")
        self.remaining_var.set(f"{float(self.workload_config['execution']['duration_seconds']):.2f} s")
        self.status_var.set("Starting benchmark...")
        self.metrics_history = []
        self._hide_results_panel()
        self._process.cpu_percent(None)
        self._append_log("Benchmark started.")

        self._runner_active = True
        self._worker_thread = threading.Thread(target=self._run_benchmark_worker, daemon=True)
        self._worker_thread.start()
        self.after(100, self._poll_metrics_queue)

    def _stop_benchmark(self):
        if not self._runner_active:
            messagebox.showinfo("No active benchmark", "There is no benchmark run to stop.", parent=self)
            return
        self._stop_event.set()
        self.status_var.set("Stopping benchmark...")
        self._append_log("Stop requested by user.")

    def _poll_metrics_queue(self):
        while True:
            try:
                item = self._metrics_queue.get_nowait()
            except queue.Empty:
                break

            event_type = item.get("type")
            if event_type == "metric":
                self.metrics_history.append(item)
                self.progress_var.set(item["progress"])
                self.ops_var.set(str(item["ops"]))
                self.read_ops_var.set(str(item.get("read_ops", 0)))
                self.write_ops_var.set(str(item.get("write_ops", 0)))
                self.errors_var.set(str(item["errors"]))
                self.throughput_var.set(f"{item['throughput']:.2f} ops/sec")
                self.latency_var.set(f"{item['latency_ms']:.2f} ms")
                self.p50_var.set(f"{item['p50_ms']:.2f} ms")
                self.p95_var.set(f"{item['p95_ms']:.2f} ms")
                self.p99_var.set(f"{item['p99_ms']:.2f} ms")
                self.cpu_var.set(f"{item['cpu_percent']:.2f} %")
                self.ram_var.set(f"{item['ram_mb']:.2f} MB")
                self.network_var.set(self._format_network(item))
                self.disk_var.set(self._format_disk(item))
                self.remaining_var.set(f"{item['remaining_seconds']:.2f} s")
                self.status_var.set(item["status"])
            elif event_type == "log":
                self._append_log(item["message"])
            elif event_type == "done":
                self.metrics_history.append(item)
                self._runner_active = False
                self.progress_var.set(item["progress"])
                self.ops_var.set(str(item["ops"]))
                self.read_ops_var.set(str(item.get("read_ops", 0)))
                self.write_ops_var.set(str(item.get("write_ops", 0)))
                self.errors_var.set(str(item["errors"]))
                self.throughput_var.set(f"{item['throughput']:.2f} ops/sec")
                self.latency_var.set(f"{item['latency_ms']:.2f} ms")
                self.p50_var.set(f"{item['p50_ms']:.2f} ms")
                self.p95_var.set(f"{item['p95_ms']:.2f} ms")
                self.p99_var.set(f"{item['p99_ms']:.2f} ms")
                self.cpu_var.set(f"{item['cpu_percent']:.2f} %")
                self.ram_var.set(f"{item['ram_mb']:.2f} MB")
                self.network_var.set(self._format_network(item))
                self.disk_var.set(self._format_disk(item))
                self.remaining_var.set("0.00 s")
                self.status_var.set(item["status"])
                self._append_log(item["message"])
                self._show_results_panel(item)
            elif event_type == "error":
                self._runner_active = False
                self.status_var.set("Benchmark failed.")
                self._append_log(item["message"])
                messagebox.showerror("Benchmark failed", item["message"], parent=self)

        if self._runner_active:
            self.after(150, self._poll_metrics_queue)

    def _run_benchmark_worker(self):
        try:
            run_benchmark(self.connection_config, self.workload_config, self._stop_event, self._metrics_queue)
        except Exception as error:
            self._metrics_queue.put({"type": "error", "message": str(error)})

    def _toggle_summary(self):
        if self.summary_container is None or self.summary_toggle_button is None:
            return
        if self.summary_visible:
            self.summary_container.pack_forget()
            self.summary_toggle_button.configure(text="Show Config")
        else:
            self.summary_container.pack(fill="both", expand=True)
            self.summary_toggle_button.configure(text="Hide Config")
        self.summary_visible = not self.summary_visible

    def _show_results_panel(self, final_item):
        self.results_text.configure(state="normal")
        self.results_text.delete("1.0", tk.END)
        self.results_text.insert(
            tk.END,
            (
                f"Status: {final_item['status']}\n"
                f"Operations: {final_item['ops']}\n"
                f"Read ops: {final_item.get('read_ops', 0)}\n"
                f"Write ops: {final_item.get('write_ops', 0)}\n"
                f"Errors: {final_item['errors']}\n"
                f"Throughput: {final_item['throughput']:.2f} ops/sec\n"
                f"Avg latency: {final_item['latency_ms']:.2f} ms\n"
                f"p50/p95/p99: {final_item['p50_ms']:.2f} / {final_item['p95_ms']:.2f} / {final_item['p99_ms']:.2f} ms\n"
                f"CPU/RAM: {final_item['cpu_percent']:.2f}% / {final_item['ram_mb']:.2f} MB\n"
                f"DB Traffic: {self._format_network(final_item)}\n"
                f"Disk I/O: {self._format_disk(final_item)}"
            ),
        )
        self.results_text.configure(state="disabled")
        if not self.results_visible:
            self.results_card.pack(fill="x", pady=(18, 0))
            self.results_visible = True
        self._draw_charts()

    def _hide_results_panel(self):
        if self.results_card is not None and self.results_visible:
            self.results_card.pack_forget()
            self.results_visible = False

    def _draw_charts(self):
        metric_items = [item for item in self.metrics_history if item["type"] in {"metric", "done"}]
        if not metric_items:
            return
        xs = list(range(1, len(metric_items) + 1))
        throughput = [item["throughput"] for item in metric_items]
        p95 = [item["p95_ms"] for item in metric_items]
        cpu = [item["cpu_percent"] for item in metric_items]
        ram = [item["ram_mb"] for item in metric_items]
        network_rx = [item.get("db_rx_mbps", 0.0) for item in metric_items]
        network_tx = [item.get("db_tx_mbps", 0.0) for item in metric_items]
        disk_read = [item.get("disk_read_mbps", 0.0) for item in metric_items]
        disk_write = [item.get("disk_write_mbps", 0.0) for item in metric_items]

        self.chart_figure.clear()
        ax1 = self.chart_figure.add_subplot(221)
        ax2 = self.chart_figure.add_subplot(222)
        ax3 = self.chart_figure.add_subplot(223)
        ax4 = self.chart_figure.add_subplot(224)
        ax1.plot(xs, throughput, label="Throughput", color="#1f6feb")
        ax1.plot(xs, p95, label="p95 latency", color="#b42318")
        ax1.set_title("Throughput and p95 Latency")
        ax1.set_xlabel("Metric sample")
        ax1.set_ylabel("Ops/sec and ms")
        ax1.legend(loc="upper right")
        ax1.grid(True, alpha=0.2)
        ax2.plot(xs, cpu, label="CPU %", color="#1d6b43")
        ax2.plot(xs, ram, label="RAM MB", color="#5d7288")
        ax2.set_title("CPU and RAM")
        ax2.set_xlabel("Metric sample")
        ax2.set_ylabel("CPU % and MB")
        ax2.legend(loc="upper right")
        ax2.grid(True, alpha=0.2)
        ax3.plot(xs, network_rx, label="DB RX MB/s", color="#2563eb")
        ax3.plot(xs, network_tx, label="DB TX MB/s", color="#0f766e")
        ax3.set_title("DB Traffic")
        ax3.set_xlabel("Metric sample")
        ax3.set_ylabel("MB/s")
        ax3.legend(loc="upper right")
        ax3.grid(True, alpha=0.2)
        ax4.plot(xs, disk_read, label="Read MB/s", color="#b45309")
        ax4.plot(xs, disk_write, label="Write MB/s", color="#be123c")
        ax4.set_title("Disk I/O")
        ax4.set_xlabel("Metric sample")
        ax4.set_ylabel("MB/s")
        ax4.legend(loc="upper right")
        ax4.grid(True, alpha=0.2)
        self.chart_figure.tight_layout()
        self.chart_canvas.draw()

    def _format_network(self, item):
        return f"DB Rx {item.get('db_rx_mbps', 0.0):.2f} MB/s | DB Tx {item.get('db_tx_mbps', 0.0):.2f} MB/s"

    def _format_disk(self, item):
        return f"Read {item.get('disk_read_mbps', 0.0):.2f} MB/s | Write {item.get('disk_write_mbps', 0.0):.2f} MB/s"

    def _show_report(self):
        metric_items = [item for item in self.metrics_history if item["type"] in {"metric", "done"}]
        if not metric_items:
            messagebox.showinfo("No report", "Run a benchmark first to generate a report.", parent=self)
            return

        averages = {
            "throughput": sum(item["throughput"] for item in metric_items) / len(metric_items),
            "latency_ms": sum(item["latency_ms"] for item in metric_items) / len(metric_items),
            "p50_ms": sum(item["p50_ms"] for item in metric_items) / len(metric_items),
            "p95_ms": sum(item["p95_ms"] for item in metric_items) / len(metric_items),
            "p99_ms": sum(item["p99_ms"] for item in metric_items) / len(metric_items),
            "cpu_percent": sum(item["cpu_percent"] for item in metric_items) / len(metric_items),
            "ram_mb": sum(item["ram_mb"] for item in metric_items) / len(metric_items),
            "network_rx_mbps": sum(item.get("db_rx_mbps", 0.0) for item in metric_items) / len(metric_items),
            "network_tx_mbps": sum(item.get("db_tx_mbps", 0.0) for item in metric_items) / len(metric_items),
            "disk_read_mbps": sum(item.get("disk_read_mbps", 0.0) for item in metric_items) / len(metric_items),
            "disk_write_mbps": sum(item.get("disk_write_mbps", 0.0) for item in metric_items) / len(metric_items),
        }
        final_item = metric_items[-1]
        report_text = (
            f"Status: {final_item['status']}\n"
            f"Completed operations: {final_item['ops']}\n"
            f"Read ops: {final_item.get('read_ops', 0)}\n"
            f"Write ops: {final_item.get('write_ops', 0)}\n"
            f"Errors: {final_item['errors']}\n\n"
            f"Average throughput: {averages['throughput']:.2f} ops/sec\n"
            f"Average latency: {averages['latency_ms']:.2f} ms\n"
            f"Average p50 latency: {averages['p50_ms']:.2f} ms\n"
            f"Average p95 latency: {averages['p95_ms']:.2f} ms\n"
            f"Average p99 latency: {averages['p99_ms']:.2f} ms\n"
            f"Average CPU: {averages['cpu_percent']:.2f} %\n"
            f"Average RAM: {averages['ram_mb']:.2f} MB\n"
            f"Average DB traffic: Rx {averages['network_rx_mbps']:.2f} MB/s | Tx {averages['network_tx_mbps']:.2f} MB/s\n"
            f"Average disk I/O: Read {averages['disk_read_mbps']:.2f} MB/s | Write {averages['disk_write_mbps']:.2f} MB/s"
        )
        messagebox.showinfo("Benchmark Report", report_text, parent=self)

    def _export_raw_txt(self):
        if not self.metrics_history:
            messagebox.showinfo("No data", "Run a benchmark first to export raw metrics.", parent=self)
            return
        file_path = filedialog.asksaveasfilename(
            title="Export raw benchmark data",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            parent=self,
        )
        if not file_path:
            return
        with open(file_path, "w", encoding="utf-8") as file:
            for item in self.metrics_history:
                file.write(json.dumps(item, ensure_ascii=False) + "\n")
        messagebox.showinfo("Export complete", f"Raw benchmark data was saved to:\n{file_path}", parent=self)

    def _save_chart_png(self):
        if not self.metrics_history:
            messagebox.showinfo("No chart", "Run a benchmark first to generate charts.", parent=self)
            return
        file_path = filedialog.asksaveasfilename(
            title="Save benchmark chart",
            defaultextension=".png",
            filetypes=[("PNG files", "*.png"), ("All files", "*.*")],
            parent=self,
        )
        if not file_path:
            return
        self.chart_figure.savefig(file_path, dpi=160)
        messagebox.showinfo("Saved", f"Chart image was saved to:\n{file_path}", parent=self)

    def _go_back(self):
        if self._runner_active:
            messagebox.showwarning("Benchmark running", "Stop the benchmark before going back.", parent=self)
            return
        if self.on_back is not None:
            self.on_back()
