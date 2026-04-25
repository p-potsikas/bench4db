import json
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from benchmark_app.constants import PREVIEW_BG, PREVIEW_FG
from benchmark_app.db.factory import prepare_target
from benchmark_app.services.workload import (
    estimate_field_size_bytes,
    estimate_size_gb,
    format_connection_target,
    parse_scale_plan,
    validate_identifier,
)


class WorkloadSetupView(ttk.Frame):
    def __init__(self, parent, connection_config, on_back=None, on_next=None, initial_config=None):
        super().__init__(parent, style="InnerCard.TFrame")
        self.connection_config = connection_config
        self.on_back = on_back
        self.on_next = on_next
        self.initial_config = initial_config or {}
        self.preview_text = None
        self.preview_container = None
        self.preview_toggle_button = None
        self.preview_visible = False

        self._build_layout()
        self._bind_preview_updates()
        self._update_estimated_size()
        self._refresh_preview()

    def _build_layout(self):
        root = ttk.Frame(self, style="InnerCard.TFrame")
        root.pack(fill="both", expand=True)

        hero = ttk.Frame(root, style="Hero.TFrame", padding=22)
        hero.pack(fill="x", pady=(0, 18))

        title_block = ttk.Frame(hero, style="InnerCard.TFrame")
        title_block.pack(fill="x")

        ttk.Label(title_block, text="Workload Setup", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            title_block,
            text="Configure the benchmark profile, concurrency, runtime, and request mix.",
            style="Subheader.TLabel",
        ).pack(anchor="w", pady=(6, 0))

        self._build_action_buttons(root)

        main = ttk.Frame(root)
        main.pack(fill="both", expand=True)

        left_panel = ttk.Frame(main, style="Card.TFrame", padding=20)
        left_panel.pack(side="left", fill="both", expand=True, padx=(0, 15))

        right_panel = ttk.Frame(main, style="Card.TFrame", padding=20)
        right_panel.pack(side="right", fill="both", expand=True, padx=(15, 0))

        self._build_workload_form(left_panel)
        self._build_execution_form(right_panel)
        self._build_preview_panel(root)

    def _build_workload_form(self, parent):
        ttk.Label(parent, text="Workload Profile", style="Section.TLabel").pack(anchor="w", pady=(0, 10))
        ttk.Label(
            parent,
            text="Choose the benchmark model and define the request distribution.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(0, 16))

        form = ttk.Frame(parent, style="InnerCard.TFrame")
        form.pack(fill="x")

        self.workload_name_var = tk.StringVar(value="mixed-oltp")
        self.workload_type_var = tk.StringVar(value="Read / Write Mix")
        self.target_table_var = tk.StringVar(value="benchmark_items")
        self.field_count_var = tk.StringVar(value="8")
        self.read_ratio_var = tk.StringVar(value="70")
        self.write_ratio_var = tk.StringVar(value="30")
        self.batch_size_var = tk.StringVar(value="100")

        workload_data = self.initial_config.get("workload", {})
        self.workload_name_var.set(workload_data.get("name", self.workload_name_var.get()))
        self.workload_type_var.set(workload_data.get("type", self.workload_type_var.get()))
        self.target_table_var.set(workload_data.get("target_object", self.target_table_var.get()))
        self.field_count_var.set(str(workload_data.get("field_count", self.field_count_var.get())))
        self.read_ratio_var.set(str(workload_data.get("read_ratio_percent", self.read_ratio_var.get())))
        self.write_ratio_var.set(str(workload_data.get("write_ratio_percent", self.write_ratio_var.get())))
        self.batch_size_var.set(str(workload_data.get("batch_size", self.batch_size_var.get())))

        self._add_labeled_widget(form, "Workload name", ttk.Entry(form, textvariable=self.workload_name_var))
        self._add_labeled_widget(
            form,
            "Workload type",
            ttk.Combobox(
                form,
                textvariable=self.workload_type_var,
                values=["Read Only", "Write Only", "Read / Write Mix", "Scan Heavy", "Custom"],
                state="readonly",
            ),
        )
        self._add_labeled_widget(form, "Target table / collection", ttk.Entry(form, textvariable=self.target_table_var))
        self._add_labeled_widget(form, "Fields per record (max 100)", ttk.Entry(form, textvariable=self.field_count_var))
        self._add_labeled_widget(form, "Read ratio (%)", ttk.Entry(form, textvariable=self.read_ratio_var))
        self._add_labeled_widget(form, "Write ratio (%)", ttk.Entry(form, textvariable=self.write_ratio_var))
        self._add_labeled_widget(form, "Batch size", ttk.Entry(form, textvariable=self.batch_size_var))

    def _build_execution_form(self, parent):
        ttk.Label(parent, text="Execution Settings", style="Section.TLabel").pack(anchor="w", pady=(0, 10))
        ttk.Label(
            parent,
            text="Tune concurrency, duration, warmup, scale, and cost assumptions for the run.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(0, 16))

        form = ttk.Frame(parent, style="InnerCard.TFrame")
        form.pack(fill="x")

        self.client_threads_var = tk.StringVar(value="16")
        self.duration_seconds_var = tk.StringVar(value="120")
        self.ramp_up_seconds_var = tk.StringVar(value="10")
        self.record_count_var = tk.StringVar(value="100000")
        self.data_size_mb_var = tk.StringVar(value="1024")
        self.estimated_size_gb_var = tk.StringVar(value="1.00")
        self.key_distribution_var = tk.StringVar(value="Uniform")
        self.max_qps_var = tk.StringVar(value="0")
        self.consistency_level_var = tk.StringVar(value="Default")
        self.replication_factor_var = tk.StringVar(value="3")
        self.node_scale_plan_var = tk.StringVar(value="5,30,70")
        self.failure_scenario_var = tk.StringVar(value="None")
        self.cost_per_gb_var = tk.StringVar(value="0.00")
        self.cost_per_kops_var = tk.StringVar(value="0.00")
        self.avg_power_per_node_w_var = tk.StringVar(value="0")
        self.remote_agent_url_var = tk.StringVar(value="")
        self.remote_agent_token_var = tk.StringVar(value="")

        execution_data = self.initial_config.get("execution", {})
        telemetry_data = self.initial_config.get("telemetry", {})
        self.client_threads_var.set(str(execution_data.get("client_threads", self.client_threads_var.get())))
        self.duration_seconds_var.set(str(execution_data.get("duration_seconds", self.duration_seconds_var.get())))
        self.ramp_up_seconds_var.set(str(execution_data.get("ramp_up_seconds", self.ramp_up_seconds_var.get())))
        self.record_count_var.set(str(execution_data.get("record_count", self.record_count_var.get())))
        self.data_size_mb_var.set(str(execution_data.get("data_size_mb", self.data_size_mb_var.get())))
        self.key_distribution_var.set(execution_data.get("key_distribution", self.key_distribution_var.get()))
        self.max_qps_var.set(str(execution_data.get("max_qps", self.max_qps_var.get())))
        self.consistency_level_var.set(execution_data.get("consistency_level", self.consistency_level_var.get()))
        self.replication_factor_var.set(str(execution_data.get("replication_factor", self.replication_factor_var.get())))
        self.node_scale_plan_var.set(execution_data.get("node_scale_plan", self.node_scale_plan_var.get()))
        self.failure_scenario_var.set(execution_data.get("failure_scenario", self.failure_scenario_var.get()))
        self.cost_per_gb_var.set(str(execution_data.get("cost_per_gb", self.cost_per_gb_var.get())))
        self.cost_per_kops_var.set(str(execution_data.get("cost_per_1000_ops_sec", self.cost_per_kops_var.get())))
        self.avg_power_per_node_w_var.set(str(execution_data.get("avg_power_per_node_watts", self.avg_power_per_node_w_var.get())))
        self.remote_agent_url_var.set(telemetry_data.get("agent_url", self.remote_agent_url_var.get()))
        self.remote_agent_token_var.set(telemetry_data.get("agent_token", self.remote_agent_token_var.get()))

        self._add_labeled_widget(form, "Client threads", ttk.Entry(form, textvariable=self.client_threads_var))
        self._add_labeled_widget(form, "Duration (seconds)", ttk.Entry(form, textvariable=self.duration_seconds_var))
        self._add_labeled_widget(form, "Ramp-up (seconds)", ttk.Entry(form, textvariable=self.ramp_up_seconds_var))
        self._add_labeled_widget(form, "Record count", ttk.Entry(form, textvariable=self.record_count_var))
        self._add_labeled_widget(form, "Data size (MB)", ttk.Entry(form, textvariable=self.data_size_mb_var))
        self._add_labeled_widget(form, "Estimated size (GB)", ttk.Label(form, textvariable=self.estimated_size_gb_var, style="Card.TLabel"))
        self._add_labeled_widget(form, "Replication factor", ttk.Entry(form, textvariable=self.replication_factor_var))
        self._add_labeled_widget(
            form,
            "Key distribution",
            ttk.Combobox(
                form,
                textvariable=self.key_distribution_var,
                values=["Uniform", "Zipfian", "Latest", "Hotspot", "Custom"],
                state="readonly",
            ),
        )
        self._add_labeled_widget(form, "Max QPS (0 = unlimited)", ttk.Entry(form, textvariable=self.max_qps_var))
        self._add_labeled_widget(form, "Node scale plan", ttk.Entry(form, textvariable=self.node_scale_plan_var))
        self._add_labeled_widget(
            form,
            "Failure scenario",
            ttk.Combobox(
                form,
                textvariable=self.failure_scenario_var,
                values=["None", "Node Down", "Node Rejoin", "Rolling Restart"],
                state="readonly",
            ),
        )
        self._add_labeled_widget(form, "Cost per GB", ttk.Entry(form, textvariable=self.cost_per_gb_var))
        self._add_labeled_widget(form, "Cost per 1,000 ops/sec", ttk.Entry(form, textvariable=self.cost_per_kops_var))
        self._add_labeled_widget(form, "Avg power per node (W)", ttk.Entry(form, textvariable=self.avg_power_per_node_w_var))
        self._add_labeled_widget(form, "Remote agent URL", ttk.Entry(form, textvariable=self.remote_agent_url_var))
        self._add_labeled_widget(form, "Remote agent token", ttk.Entry(form, textvariable=self.remote_agent_token_var, show="*"))
        self._add_labeled_widget(
            form,
            "Consistency level",
            ttk.Combobox(
                form,
                textvariable=self.consistency_level_var,
                values=["Default", "Strong", "Eventual", "Quorum", "Local Quorum"],
                state="readonly",
            ),
        )

    def _build_preview_panel(self, parent):
        preview_card = ttk.Frame(parent, style="Card.TFrame", padding=20)
        preview_card.pack(fill="x", pady=(18, 0))

        header_row = ttk.Frame(preview_card, style="InnerCard.TFrame")
        header_row.pack(fill="x", pady=(0, 8))

        ttk.Label(header_row, text="Workload Preview", style="Section.TLabel").pack(side="left")
        self.preview_toggle_button = ttk.Button(header_row, text="Show Config", command=self._toggle_preview)
        self.preview_toggle_button.pack(side="right")

        self.preview_container = ttk.Frame(preview_card, style="InnerCard.TFrame")
        self.preview_text = tk.Text(
            self.preview_container,
            height=12,
            wrap="word",
            font=("SFMono-Regular", 11),
            bg=PREVIEW_BG,
            fg=PREVIEW_FG,
            insertbackground="#ffffff",
            relief="flat",
            padx=16,
            pady=16,
        )
        self.preview_text.pack(fill="x")
        self.preview_text.configure(state="disabled")

    def _build_action_buttons(self, parent):
        buttons = ttk.Frame(parent, style="Card.TFrame", padding=18)
        buttons.pack(fill="x", pady=(0, 18))

        left_actions = ttk.Frame(buttons, style="InnerCard.TFrame")
        left_actions.pack(side="left", fill="x", expand=True)
        right_actions = ttk.Frame(buttons, style="InnerCard.TFrame")
        right_actions.pack(side="right")

        ttk.Button(left_actions, text="Refresh Preview", command=self._refresh_preview).pack(side="left")
        ttk.Button(left_actions, text="Validate Workload", command=self._validate_workload).pack(side="left", padx=(8, 0))
        ttk.Button(left_actions, text="Prepare Target", command=self._prepare_target).pack(side="left", padx=(8, 0))
        ttk.Button(left_actions, text="Save Workload as JSON", command=self._save_workload).pack(side="left", padx=(8, 0))
        ttk.Button(right_actions, text="Back to Connection", command=self._go_back).pack(side="right")
        ttk.Button(
            right_actions,
            text="Next: Run Benchmark",
            style="Primary.TButton",
            command=self._next_step,
        ).pack(side="right", padx=(0, 8))

    def _add_labeled_widget(self, parent, label, widget):
        row = len(parent.grid_slaves()) // 2
        ttk.Label(parent, text=label, style="Field.TLabel").grid(
            row=row,
            column=0,
            sticky="w",
            padx=(0, 12),
            pady=8,
        )
        widget.grid(row=row, column=1, sticky="ew", pady=8)
        parent.columnconfigure(1, weight=1)

    def _bind_preview_updates(self):
        tracked_vars = [
            self.workload_name_var,
            self.workload_type_var,
            self.target_table_var,
            self.field_count_var,
            self.read_ratio_var,
            self.write_ratio_var,
            self.batch_size_var,
            self.client_threads_var,
            self.duration_seconds_var,
            self.ramp_up_seconds_var,
            self.record_count_var,
            self.data_size_mb_var,
            self.key_distribution_var,
            self.max_qps_var,
            self.consistency_level_var,
            self.replication_factor_var,
            self.node_scale_plan_var,
            self.failure_scenario_var,
            self.cost_per_gb_var,
            self.cost_per_kops_var,
            self.avg_power_per_node_w_var,
            self.remote_agent_url_var,
            self.remote_agent_token_var,
        ]
        for variable in tracked_vars:
            variable.trace_add("write", lambda *_args: self._on_workload_field_change())

    def _on_workload_field_change(self):
        self._update_estimated_size()
        self._refresh_preview()

    def _get_workload_config(self):
        data_size_mb = self.data_size_mb_var.get().strip()
        estimated_size_gb = estimate_size_gb(data_size_mb)
        estimated_field_size_bytes = estimate_field_size_bytes(
            data_size_mb=data_size_mb,
            record_count=self.record_count_var.get().strip(),
            field_count=self.field_count_var.get().strip(),
        )

        return {
            "connection": {
                "connection_name": self.connection_config.get("connection_name"),
                "database_type": "PostgreSQL",
                "target": format_connection_target(self.connection_config),
            },
            "workload": {
                "name": self.workload_name_var.get().strip(),
                "type": self.workload_type_var.get(),
                "target_object": self.target_table_var.get().strip(),
                "field_count": self.field_count_var.get().strip(),
                "read_ratio_percent": self.read_ratio_var.get().strip(),
                "write_ratio_percent": self.write_ratio_var.get().strip(),
                "batch_size": self.batch_size_var.get().strip(),
            },
            "execution": {
                "client_threads": self.client_threads_var.get().strip(),
                "duration_seconds": self.duration_seconds_var.get().strip(),
                "ramp_up_seconds": self.ramp_up_seconds_var.get().strip(),
                "record_count": self.record_count_var.get().strip(),
                "data_size_mb": data_size_mb,
                "estimated_size_gb": estimated_size_gb,
                "estimated_field_size_bytes": str(estimated_field_size_bytes) if estimated_field_size_bytes else "",
                "key_distribution": self.key_distribution_var.get(),
                "max_qps": self.max_qps_var.get().strip(),
                "replication_factor": self.replication_factor_var.get().strip(),
                "node_scale_plan": self.node_scale_plan_var.get().strip(),
                "failure_scenario": self.failure_scenario_var.get(),
                "cost_per_gb": self.cost_per_gb_var.get().strip(),
                "cost_per_1000_ops_sec": self.cost_per_kops_var.get().strip(),
                "avg_power_per_node_watts": self.avg_power_per_node_w_var.get().strip(),
                "consistency_level": self.consistency_level_var.get(),
                "requested_metrics": [
                    "throughput",
                    "p50_latency",
                    "p95_latency",
                    "p99_latency",
                    "cpu_ram",
                    "disk_io",
                    "db_traffic",
                    "read_write_ops",
                    "remaining_time",
                    "cost_efficiency",
                ],
            },
            "telemetry": {
                "agent_url": self.remote_agent_url_var.get().strip(),
                "agent_token": self.remote_agent_token_var.get().strip(),
            },
        }

    def _refresh_preview(self):
        if self.preview_text is None:
            return
        formatted = json.dumps(self._get_workload_config(), indent=2, ensure_ascii=False)
        self.preview_text.configure(state="normal")
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert(tk.END, formatted)
        self.preview_text.configure(state="disabled")

    def _validate_workload(self):
        config = self._get_workload_config()

        if not config["workload"]["name"]:
            messagebox.showwarning("Validation error", "Enter a workload name.", parent=self)
            return False
        if not config["workload"]["target_object"]:
            messagebox.showwarning("Validation error", "Enter a target table.", parent=self)
            return False

        try:
            read_ratio = int(config["workload"]["read_ratio_percent"])
            write_ratio = int(config["workload"]["write_ratio_percent"])
            field_count = int(config["workload"]["field_count"])
            threads = int(config["execution"]["client_threads"])
            duration = int(config["execution"]["duration_seconds"])
            ramp_up = int(config["execution"]["ramp_up_seconds"])
            batch_size = int(config["workload"]["batch_size"])
            record_count = int(config["execution"]["record_count"])
            max_qps = int(config["execution"]["max_qps"])
            data_size_mb = float(config["execution"]["data_size_mb"])
            replication_factor = int(config["execution"]["replication_factor"])
            cost_per_gb = float(config["execution"]["cost_per_gb"])
            cost_per_kops = float(config["execution"]["cost_per_1000_ops_sec"])
            avg_power_per_node = float(config["execution"]["avg_power_per_node_watts"])
        except ValueError:
            messagebox.showwarning(
                "Validation error",
                "Numeric workload fields must contain valid numbers. Data size may use decimals.",
                parent=self,
            )
            return False

        if read_ratio < 0 or write_ratio < 0 or read_ratio + write_ratio != 100:
            messagebox.showwarning("Validation error", "Read ratio and write ratio must add up to 100.", parent=self)
            return False
        if field_count <= 0 or field_count > 100:
            messagebox.showwarning("Validation error", "Fields per record must be between 1 and 100.", parent=self)
            return False
        if threads <= 0 or duration <= 0 or ramp_up < 0 or batch_size <= 0 or record_count <= 0:
            messagebox.showwarning("Validation error", "Execution values must be greater than zero, except ramp-up which may be zero.", parent=self)
            return False
        if data_size_mb <= 0:
            messagebox.showwarning("Validation error", "Data size in MB must be greater than zero.", parent=self)
            return False
        if replication_factor <= 0:
            messagebox.showwarning("Validation error", "Replication factor must be greater than zero.", parent=self)
            return False
        if max_qps < 0:
            messagebox.showwarning("Validation error", "Max QPS cannot be negative.", parent=self)
            return False
        if cost_per_gb < 0 or cost_per_kops < 0 or avg_power_per_node < 0:
            messagebox.showwarning("Validation error", "Cost and power fields cannot be negative.", parent=self)
            return False
        if not parse_scale_plan(config["execution"]["node_scale_plan"]):
            messagebox.showwarning("Validation error", "Node scale plan must be comma-separated integers, e.g. 5,30,70.", parent=self)
            return False

        return True

    def _save_workload(self):
        if not self._validate_workload():
            return

        file_path = filedialog.asksaveasfilename(
            title="Save workload configuration",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            parent=self,
        )
        if not file_path:
            return

        try:
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(self._get_workload_config(), file, indent=2, ensure_ascii=False)
            messagebox.showinfo("Saved", f"The workload configuration was saved to:\n{file_path}", parent=self)
        except OSError as error:
            messagebox.showerror("Save error", f"Could not save the file:\n{error}", parent=self)

    def _go_back(self):
        if self.on_back is not None:
            self.on_back()

    def _next_step(self):
        if not self._validate_workload():
            return
        if self.on_next is not None:
            self.on_next(self._get_workload_config())

    def _prepare_target(self):
        if not self._validate_workload():
            return

        target_name = validate_identifier(self.target_table_var.get().strip(), "target object")

        try:
            message = prepare_target(self.connection_config, target_name)
        except Exception as error:
            messagebox.showerror(
                "Prepare target failed",
                f"Database type: PostgreSQL\n"
                f"Target object: {target_name}\n\n"
                f"Error: {error}",
                parent=self,
            )
            return

        messagebox.showinfo("Prepare target", message, parent=self)

    def _update_estimated_size(self):
        self.estimated_size_gb_var.set(estimate_size_gb(self.data_size_mb_var.get().strip()))

    def _toggle_preview(self):
        if self.preview_container is None or self.preview_toggle_button is None:
            return
        if self.preview_visible:
            self.preview_container.pack_forget()
            self.preview_toggle_button.configure(text="Show Config")
        else:
            self.preview_container.pack(fill="x")
            self.preview_toggle_button.configure(text="Hide Config")
        self.preview_visible = not self.preview_visible
