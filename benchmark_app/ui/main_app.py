import json
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from benchmark_app.constants import (
    ACCENT,
    ACCENT_DARK,
    APP_BG,
    CARD_BG,
    CARD_BORDER,
    ERROR_BG,
    ERROR_FG,
    POSTGRESQL_DEFAULT_PORT,
    PREVIEW_BG,
    PREVIEW_FG,
    SUCCESS_BG,
    SUCCESS_FG,
    TEXT_MUTED,
    TEXT_PRIMARY,
    WARNING_BG,
    WARNING_FG,
)
from benchmark_app.db.factory import test_connection
from benchmark_app.services.workload import format_connection_target
from benchmark_app.ui.run_view import RunBenchmarkView
from benchmark_app.ui.workload_view import WorkloadSetupView


class DatabaseBenchmarkUI(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Database Benchmark Tool - Connection Setup")
        self.geometry("1120x760")
        self.minsize(980, 680)
        self.configure(bg=APP_BG)

        self.nodes = []
        self.preview_text = None
        self.preview_container = None
        self.preview_toggle_button = None
        self.preview_visible = False
        self.connection_status_label = None
        self.connection_status_detail_label = None
        self.scroll_canvas = None
        self.scroll_container = None
        self.hero_subtitle_label = None
        self.hero_badge_label = None
        self.view_container = None
        self.connection_view = None
        self.workload_view = None
        self.run_view = None
        self.context_menu = None
        self._context_widget = None

        self._configure_style()
        self._build_layout()
        self._setup_context_menu()
        self._bind_live_preview()

    def _configure_style(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("TFrame", background=APP_BG)
        style.configure("Card.TFrame", background=CARD_BG, relief="solid", borderwidth=1)
        style.configure("InnerCard.TFrame", background=CARD_BG)
        style.configure("Hero.TFrame", background=CARD_BG, relief="solid", borderwidth=1)
        style.configure("TLabel", background=APP_BG, foreground=TEXT_PRIMARY, font=("Helvetica", 10))
        style.configure("Card.TLabel", background=CARD_BG, foreground=TEXT_PRIMARY, font=("Helvetica", 10))
        style.configure("Header.TLabel", background=CARD_BG, font=("Helvetica", 22, "bold"), foreground=TEXT_PRIMARY)
        style.configure("Subheader.TLabel", background=CARD_BG, font=("Helvetica", 10), foreground=TEXT_MUTED)
        style.configure("Section.TLabel", background=CARD_BG, font=("Helvetica", 12, "bold"), foreground=TEXT_PRIMARY)
        style.configure("Field.TLabel", background=CARD_BG, font=("Helvetica", 10, "bold"), foreground=TEXT_MUTED)
        style.configure("Hint.TLabel", background=CARD_BG, font=("Helvetica", 9), foreground=TEXT_MUTED)
        style.configure("Badge.TLabel", background=SUCCESS_BG, foreground=SUCCESS_FG, font=("Helvetica", 9, "bold"), padding=(10, 5))
        style.configure(
            "TEntry",
            fieldbackground="#ffffff",
            foreground=TEXT_PRIMARY,
            bordercolor=CARD_BORDER,
            lightcolor=CARD_BORDER,
            darkcolor=CARD_BORDER,
            insertcolor=TEXT_PRIMARY,
            padding=8,
        )
        style.map("TEntry", bordercolor=[("focus", ACCENT)], lightcolor=[("focus", ACCENT)], darkcolor=[("focus", ACCENT)])
        style.configure(
            "TCombobox",
            fieldbackground="#ffffff",
            foreground=TEXT_PRIMARY,
            bordercolor=CARD_BORDER,
            lightcolor=CARD_BORDER,
            darkcolor=CARD_BORDER,
            padding=8,
        )
        style.map(
            "TCombobox",
            bordercolor=[("focus", ACCENT)],
            lightcolor=[("focus", ACCENT)],
            darkcolor=[("focus", ACCENT)],
            fieldbackground=[("readonly", "#ffffff")],
        )
        style.configure("TButton", font=("Helvetica", 10), padding=(12, 8), background="#ffffff")
        style.configure(
            "Primary.TButton",
            font=("Helvetica", 10, "bold"),
            padding=(14, 10),
            background=ACCENT,
            foreground="#ffffff",
            bordercolor=ACCENT,
        )
        style.map("Primary.TButton", background=[("active", ACCENT_DARK)], bordercolor=[("active", ACCENT_DARK)])
        style.configure(
            "Benchmark.Horizontal.TProgressbar",
            troughcolor="#e5e7eb",
            bordercolor="#e5e7eb",
            background=ACCENT,
            lightcolor=ACCENT,
            darkcolor=ACCENT,
        )
        style.configure("TCheckbutton", background=CARD_BG, foreground=TEXT_PRIMARY)
        style.configure("TRadiobutton", background=CARD_BG, foreground=TEXT_PRIMARY)

    def _build_layout(self):
        outer = ttk.Frame(self)
        outer.pack(fill="both", expand=True)

        self.scroll_canvas = tk.Canvas(outer, bg=APP_BG, highlightthickness=0, borderwidth=0)
        self.scroll_canvas.pack(side="left", fill="both", expand=True)

        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=self.scroll_canvas.yview)
        scrollbar.pack(side="right", fill="y")
        self.scroll_canvas.configure(yscrollcommand=scrollbar.set)

        root = ttk.Frame(self.scroll_canvas, padding=24)
        self.scroll_container = root
        canvas_window = self.scroll_canvas.create_window((0, 0), window=root, anchor="nw")

        root.bind("<Configure>", lambda _event: self.scroll_canvas.configure(scrollregion=self.scroll_canvas.bbox("all")))
        self.scroll_canvas.bind("<Configure>", lambda event: self.scroll_canvas.itemconfigure(canvas_window, width=event.width))
        self._bind_mousewheel(root)

        hero = ttk.Frame(root, style="Hero.TFrame", padding=22)
        hero.pack(fill="x", pady=(0, 18))

        hero_top = ttk.Frame(hero, style="InnerCard.TFrame")
        hero_top.pack(fill="x")

        title_block = ttk.Frame(hero_top, style="InnerCard.TFrame")
        title_block.pack(side="left", fill="x", expand=True)

        ttk.Label(title_block, text="Database Benchmark Tool", style="Header.TLabel").pack(anchor="w")
        self.hero_subtitle_label = ttk.Label(title_block, text="Configure Database before running a benchmark.", style="Subheader.TLabel")
        self.hero_subtitle_label.pack(anchor="w", pady=(6, 0))

        self.hero_badge_label = ttk.Label(hero_top, text="Connection Setup", style="Badge.TLabel")
        self.hero_badge_label.pack(side="right", anchor="n")

        self.view_container = ttk.Frame(root)
        self.view_container.pack(fill="both", expand=True)
        self._build_connection_view()

    def _bind_mousewheel(self, root):
        widgets = [self, self.scroll_canvas, root]
        for widget in widgets:
            widget.bind_all("<MouseWheel>", self._on_mousewheel, add="+")
            widget.bind_all("<Button-4>", self._on_mousewheel_linux, add="+")
            widget.bind_all("<Button-5>", self._on_mousewheel_linux, add="+")
        self._install_scroll_bindings(root)

    def _on_mousewheel(self, event):
        if self.scroll_canvas is None:
            return
        if event.delta == 0:
            return
        if sys.platform == "darwin":
            delta = -1 if event.delta > 0 else 1
        else:
            delta = int(-event.delta / 120)
            if delta == 0:
                delta = -1 if event.delta > 0 else 1
        if delta:
            self.scroll_canvas.yview_scroll(delta, "units")
        return "break"

    def _on_mousewheel_linux(self, event):
        if self.scroll_canvas is None:
            return
        if event.num == 4:
            self.scroll_canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            self.scroll_canvas.yview_scroll(1, "units")
        return "break"

    def _install_scroll_bindings(self, widget):
        for sequence, handler in (
            ("<MouseWheel>", self._on_mousewheel),
            ("<Button-4>", self._on_mousewheel_linux),
            ("<Button-5>", self._on_mousewheel_linux),
        ):
            widget.bind(sequence, handler, add="+")
        for child in widget.winfo_children():
            self._install_scroll_bindings(child)

    def _setup_context_menu(self):
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="Cut", command=self._context_cut)
        self.context_menu.add_command(label="Copy", command=self._context_copy)
        self.context_menu.add_command(label="Paste", command=self._context_paste)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Select All", command=self._context_select_all)

        for widget_class in ("Entry", "TEntry", "Text", "Listbox"):
            self.bind_class(widget_class, "<Button-2>", self._show_context_menu, add="+")
            self.bind_class(widget_class, "<Button-3>", self._show_context_menu, add="+")
            self.bind_class(widget_class, "<Control-Button-1>", self._show_context_menu, add="+")

    def _show_context_menu(self, event):
        if self.context_menu is None:
            return
        self._context_widget = event.widget
        try:
            event.widget.focus_set()
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()

    def _context_cut(self):
        if self._context_widget is None:
            return
        self._context_widget.event_generate("<<Cut>>")

    def _context_copy(self):
        if self._context_widget is None:
            return
        widget_class = self._context_widget.winfo_class()
        if widget_class == "Listbox":
            selection = self._context_widget.curselection()
            if not selection:
                return
            values = [self._context_widget.get(index) for index in selection]
            self.clipboard_clear()
            self.clipboard_append("\n".join(values))
            return
        self._context_widget.event_generate("<<Copy>>")

    def _context_paste(self):
        if self._context_widget is None:
            return
        if self._context_widget.winfo_class() == "Listbox":
            return
        self._context_widget.event_generate("<<Paste>>")

    def _context_select_all(self):
        if self._context_widget is None:
            return
        widget_class = self._context_widget.winfo_class()
        if widget_class == "Text":
            self._context_widget.tag_add("sel", "1.0", "end-1c")
            self._context_widget.mark_set("insert", "1.0")
            self._context_widget.see("insert")
        elif widget_class in {"Entry", "TEntry"}:
            self._context_widget.select_range(0, tk.END)
            self._context_widget.icursor(tk.END)
        elif widget_class == "Listbox":
            self._context_widget.selection_set(0, tk.END)

    def _build_connection_view(self):
        self.connection_view = ttk.Frame(self.view_container)
        self.connection_view.pack(fill="both", expand=True)

        self._build_action_buttons(self.connection_view)

        main = ttk.Frame(self.connection_view)
        main.pack(fill="both", expand=True)

        left_panel = ttk.Frame(main, style="Card.TFrame", padding=20)
        left_panel.pack(side="left", fill="both", expand=True, padx=(0, 15))

        right_panel = ttk.Frame(main, style="Card.TFrame", padding=20)
        right_panel.pack(side="right", fill="both", expand=True, padx=(15, 0))

        self._build_connection_form(left_panel)
        self._build_nodes_panel(right_panel)
        self._build_preview_panel(self.connection_view)
        self._install_scroll_bindings(self.connection_view)

    def _show_connection_view(self):
        if self.workload_view is not None:
            self.workload_view.pack_forget()
        if self.run_view is not None:
            self.run_view.pack_forget()
        if self.connection_view is not None:
            self.connection_view.pack(fill="both", expand=True)
        self._update_header_for_connection()
        self._scroll_to_top()

    def _show_workload_view(self, connection_config, initial_workload_config=None):
        if self.connection_view is not None:
            self.connection_view.pack_forget()
        if self.run_view is not None:
            self.run_view.pack_forget()
        if self.workload_view is not None:
            self.workload_view.destroy()

        self.workload_view = WorkloadSetupView(
            self.view_container,
            connection_config,
            on_back=self._show_connection_view,
            on_next=self._show_run_view,
            initial_config=initial_workload_config,
        )
        self.workload_view.pack(fill="both", expand=True)
        self._install_scroll_bindings(self.workload_view)
        self._update_header_for_workload()
        self._scroll_to_top()

    def _show_run_view(self, workload_config):
        if self.connection_view is not None:
            self.connection_view.pack_forget()
        if self.workload_view is not None:
            self.workload_view.pack_forget()
        if self.run_view is not None:
            self.run_view.destroy()

        self.run_view = RunBenchmarkView(
            self.view_container,
            self._get_config(),
            workload_config,
            on_back=lambda: self._show_workload_view(self._get_config(), workload_config),
        )
        self.run_view.pack(fill="both", expand=True)
        self._install_scroll_bindings(self.run_view)
        self._update_header_for_run()
        self._scroll_to_top()

    def _update_header_for_connection(self):
        if self.hero_subtitle_label is not None:
            self.hero_subtitle_label.configure(text="Configure Database before running a benchmark.")
        if self.hero_badge_label is not None:
            self.hero_badge_label.configure(text="Connection Setup")

    def _update_header_for_workload(self):
        if self.hero_subtitle_label is not None:
            self.hero_subtitle_label.configure(text="Configure the benchmark profile, concurrency, runtime, and request mix.")
        if self.hero_badge_label is not None:
            self.hero_badge_label.configure(text="Workload Setup")

    def _update_header_for_run(self):
        if self.hero_subtitle_label is not None:
            self.hero_subtitle_label.configure(text="Run the configured workload and monitor benchmark metrics live.")
        if self.hero_badge_label is not None:
            self.hero_badge_label.configure(text="Run Benchmark")

    def _scroll_to_top(self):
        if self.scroll_canvas is not None:
            self.scroll_canvas.yview_moveto(0)

    def _build_connection_form(self, parent):
        section = ttk.Label(parent, text="Connection Details", style="Section.TLabel")
        section.pack(anchor="w", pady=(0, 10))
        ttk.Label(
            parent,
            text="Define the PostgreSQL target, endpoint, credentials, and topology for the benchmark run.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(0, 16))

        form = ttk.Frame(parent, style="InnerCard.TFrame")
        form.pack(fill="x")

        self.db_type_var = tk.StringVar(value="PostgreSQL")
        self.host_var = tk.StringVar(value="localhost")
        self.port_var = tk.StringVar(value=POSTGRESQL_DEFAULT_PORT)
        self.database_name_var = tk.StringVar()
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.ssl_enabled_var = tk.BooleanVar(value=False)
        self.connection_name_var = tk.StringVar(value="local-postgresql")
        self.cluster_mode_var = tk.StringVar(value="single")

        self._add_labeled_widget(form, "Connection name", ttk.Entry(form, textvariable=self.connection_name_var))
        self._add_labeled_widget(form, "Database type", ttk.Label(form, text="PostgreSQL", style="Card.TLabel"))
        self._add_labeled_widget(form, "Host / IP", ttk.Entry(form, textvariable=self.host_var))
        self._add_labeled_widget(form, "Port", ttk.Entry(form, textvariable=self.port_var))
        self._add_labeled_widget(form, "Database", ttk.Entry(form, textvariable=self.database_name_var))
        self._add_labeled_widget(form, "Username", ttk.Entry(form, textvariable=self.username_var))
        self._add_labeled_widget(form, "Password", ttk.Entry(form, textvariable=self.password_var, show="*"))

        ssl_checkbox = ttk.Checkbutton(form, text="Enable SSL / TLS", variable=self.ssl_enabled_var)
        ssl_checkbox.grid(row=7, column=1, sticky="w", pady=8)

        mode_frame = ttk.Frame(form)
        ttk.Radiobutton(mode_frame, text="Single node", value="single", variable=self.cluster_mode_var, command=self._on_cluster_mode_change).pack(side="left")
        ttk.Radiobutton(mode_frame, text="Cluster / Multi-node", value="cluster", variable=self.cluster_mode_var, command=self._on_cluster_mode_change).pack(side="left", padx=(12, 0))
        self._add_labeled_widget(form, "Topology", mode_frame)

        for column in range(2):
            form.columnconfigure(column, weight=1)

    def _build_nodes_panel(self, parent):
        section = ttk.Label(parent, text="Nodes / Cluster Topology", style="Section.TLabel")
        section.pack(anchor="w", pady=(0, 10))
        ttk.Label(
            parent,
            text="Use the primary host as the first node and add extra nodes when benchmarking a cluster.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(0, 16))

        input_frame = ttk.Frame(parent, style="InnerCard.TFrame")
        input_frame.pack(fill="x")

        self.node_host_var = tk.StringVar(value="localhost")
        self.node_port_var = tk.StringVar(value=self.port_var.get())

        ttk.Label(input_frame, text="Node host / IP").grid(row=0, column=0, sticky="w")
        ttk.Entry(input_frame, textvariable=self.node_host_var).grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(4, 8))
        ttk.Label(input_frame, text="Node port").grid(row=0, column=1, sticky="w")
        ttk.Entry(input_frame, textvariable=self.node_port_var, width=12).grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(4, 8))

        self.add_node_button = ttk.Button(input_frame, text="Add node", command=self._add_node)
        self.add_node_button.grid(row=1, column=2, sticky="ew", pady=(4, 8))
        self.node_input_frame = input_frame

        input_frame.columnconfigure(0, weight=1)

        self.nodes_listbox = tk.Listbox(parent, height=12, activestyle="dotbox", font=("Arial", 10))
        self.nodes_listbox.pack(fill="both", expand=True, pady=(5, 8))

        node_buttons = ttk.Frame(parent, style="InnerCard.TFrame")
        node_buttons.pack(fill="x")
        self.remove_node_button = ttk.Button(node_buttons, text="Remove selected", command=self._remove_selected_node)
        self.remove_node_button.pack(side="left")
        self.clear_nodes_button = ttk.Button(node_buttons, text="Clear nodes", command=self._clear_nodes)
        self.clear_nodes_button.pack(side="left", padx=(8, 0))

        ttk.Label(
            parent,
            text="For a single-node database, the primary host is enough.\nFor a cluster, add the additional node addresses here.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(10, 0))

        self._add_default_node()
        self._toggle_node_controls()

    def _build_preview_panel(self, parent):
        preview_card = ttk.Frame(parent, style="Card.TFrame", padding=20)
        preview_card.pack(fill="x", pady=(18, 0))
        header_row = ttk.Frame(preview_card, style="InnerCard.TFrame")
        header_row.pack(fill="x", pady=(0, 8))
        ttk.Label(header_row, text="Connection Preview", style="Section.TLabel").pack(side="left")
        self.preview_toggle_button = ttk.Button(header_row, text="Show Config", command=self._toggle_preview)
        self.preview_toggle_button.pack(side="right")

        self.preview_container = ttk.Frame(preview_card, style="InnerCard.TFrame")
        self.preview_text = tk.Text(
            self.preview_container,
            height=10,
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
        self._refresh_preview()

    def _build_action_buttons(self, parent):
        buttons = ttk.Frame(parent, style="Card.TFrame", padding=18)
        buttons.pack(fill="x", pady=(0, 18))

        left_actions = ttk.Frame(buttons, style="InnerCard.TFrame")
        left_actions.pack(side="left", fill="x", expand=True)
        right_actions = ttk.Frame(buttons, style="InnerCard.TFrame")
        right_actions.pack(side="right")

        ttk.Button(left_actions, text="Preview config", command=self._refresh_preview).pack(side="left")
        ttk.Button(left_actions, text="Test Connection", command=self._test_connection).pack(side="left", padx=(8, 0))
        ttk.Button(left_actions, text="Save Config as JSON", command=self._save_config).pack(side="left", padx=(8, 0))
        ttk.Button(left_actions, text="Validate Form", command=self._validate_form).pack(side="left", padx=(8, 0))

        status_panel = ttk.Frame(right_actions, style="InnerCard.TFrame")
        status_panel.pack(side="left", padx=(0, 18))
        self.connection_status_label = tk.Label(
            status_panel,
            text="Not Tested",
            bg=WARNING_BG,
            fg=WARNING_FG,
            font=("Helvetica", 9, "bold"),
            padx=10,
            pady=5,
        )
        self.connection_status_label.pack(anchor="e")
        self.connection_status_detail_label = ttk.Label(
            status_panel,
            text="Run a connection test to verify access.",
            style="Hint.TLabel",
        )
        self.connection_status_detail_label.pack(anchor="e", pady=(6, 0))

        ttk.Button(
            right_actions,
            text="Next: Workload Setup",
            style="Primary.TButton",
            command=self._next_step,
        ).pack(side="right")

    def _add_labeled_widget(self, parent, label, widget):
        row = len(parent.grid_slaves()) // 2
        ttk.Label(parent, text=label, style="Field.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 12), pady=8)
        widget.grid(row=row, column=1, sticky="ew", pady=8)
        parent.columnconfigure(1, weight=1)

    def _bind_live_preview(self):
        tracked_vars = [
            self.connection_name_var,
            self.host_var,
            self.port_var,
            self.database_name_var,
            self.username_var,
            self.password_var,
            self.ssl_enabled_var,
            self.cluster_mode_var,
        ]
        for variable in tracked_vars:
            variable.trace_add("write", self._on_form_field_change)

    def _on_form_field_change(self, *_args):
        self._mark_connection_status_idle()
        self._sync_primary_node()
        self._refresh_preview()

    def _on_cluster_mode_change(self):
        self._sync_primary_node(force_single=True)
        self._toggle_node_controls()
        self._refresh_preview()

    def _add_default_node(self):
        if not self.nodes:
            self.nodes.append({"host": self.host_var.get() or "localhost", "port": self.port_var.get()})
            self._render_nodes()
            self._refresh_preview()

    def _sync_primary_node(self, force_single=False):
        primary_node = {"host": self.host_var.get().strip() or "localhost", "port": self.port_var.get().strip()}
        is_single = self.cluster_mode_var.get() == "single"

        if is_single or force_single:
            self.nodes = [primary_node]
        elif self.nodes:
            self.nodes[0] = primary_node
        else:
            self.nodes.append(primary_node)
        self._render_nodes()

    def _toggle_node_controls(self):
        state = "disabled" if self.cluster_mode_var.get() == "single" else "normal"

        for child in self.node_input_frame.winfo_children():
            try:
                child.configure(state=state)
            except tk.TclError:
                pass

        self.nodes_listbox.configure(state=state)
        self.add_node_button.configure(state=state)
        self.remove_node_button.configure(state=state)
        self.clear_nodes_button.configure(state=state)
        self.nodes_listbox.configure(
            bg="#edf2f7" if state == "disabled" else "#ffffff",
            fg=TEXT_MUTED if state == "disabled" else TEXT_PRIMARY,
            disabledforeground=TEXT_MUTED,
        )

    def _add_node(self):
        host = self.node_host_var.get().strip()
        port = self.node_port_var.get().strip()
        if not host:
            messagebox.showwarning("Missing node host", "Enter a host or IP address for the node.")
            return

        node = {"host": host, "port": port}
        primary_node = {"host": self.host_var.get().strip() or "localhost", "port": self.port_var.get().strip()}
        if node == primary_node:
            messagebox.showinfo("Primary node", "The primary host is already included as the main node.")
            return
        if node in self.nodes:
            messagebox.showinfo("Duplicate node", "This node is already in the list.")
            return

        self.nodes.append(node)
        self._mark_connection_status_idle()
        self._render_nodes()
        self._refresh_preview()

    def _remove_selected_node(self):
        selection = self.nodes_listbox.curselection()
        if not selection:
            messagebox.showinfo("No selection", "Select a node first.")
            return
        del self.nodes[selection[0]]
        self._mark_connection_status_idle()
        self._render_nodes()
        self._refresh_preview()

    def _clear_nodes(self):
        self.nodes = []
        if self.cluster_mode_var.get() == "single":
            self._add_default_node()
        self._mark_connection_status_idle()
        self._render_nodes()
        self._refresh_preview()

    def _render_nodes(self):
        self.nodes_listbox.delete(0, tk.END)
        self.nodes_listbox.configure(
            bg="#ffffff",
            fg=TEXT_PRIMARY,
            selectbackground=ACCENT,
            selectforeground="#ffffff",
            borderwidth=0,
            highlightthickness=1,
            highlightbackground=CARD_BORDER,
            highlightcolor=ACCENT,
        )
        for node in self.nodes:
            text = f"{node['host']}:{node['port']}" if node.get("port") else node["host"]
            self.nodes_listbox.insert(tk.END, text)

    def _get_config(self):
        return {
            "connection_name": self.connection_name_var.get().strip(),
            "database_type": self.db_type_var.get(),
            "host": self.host_var.get().strip(),
            "port": self.port_var.get().strip(),
            "database_name": self.database_name_var.get().strip(),
            "username": self.username_var.get().strip(),
            "password": self.password_var.get(),
            "ssl_enabled": self.ssl_enabled_var.get(),
            "topology": self.cluster_mode_var.get(),
            "nodes": self.nodes,
        }

    def _refresh_preview(self):
        config = dict(self._get_config())
        if config.get("password"):
            config["password"] = "********"
        formatted = json.dumps(config, indent=2, ensure_ascii=False)
        if self.preview_text is None:
            return
        self.preview_text.configure(state="normal")
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert(tk.END, formatted)
        self.preview_text.configure(state="disabled")

    def _save_config(self):
        if not self._validate_form(show_success=False):
            return
        config = self._get_config()
        file_path = filedialog.asksaveasfilename(
            title="Save database connection config",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not file_path:
            return
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(config, file, indent=2, ensure_ascii=False)
            messagebox.showinfo("Saved", f"The configuration was saved to:\n{file_path}")
        except OSError as error:
            messagebox.showerror("Save error", f"Could not save the file:\n{error}")

    def _validate_form(self, show_success=True):
        config = self._get_config()
        if not config["connection_name"]:
            messagebox.showwarning("Validation error", "Enter a connection name.")
            return False
        if not config["host"]:
            messagebox.showwarning("Validation error", "Enter a host or IP address.")
            return False
        if not config["port"]:
            messagebox.showwarning("Validation error", "Enter a port.")
            return False
        if config["topology"] == "cluster" and len(config["nodes"]) < 2:
            messagebox.showwarning("Validation error", "Cluster mode requires at least 2 nodes.")
            return False
        if not config["database_name"]:
            messagebox.showwarning("Validation error", "Enter a database name.")
            return False
        self._refresh_preview()
        return True

    def _test_connection(self):
        if not self._validate_form(show_success=False):
            return

        config = self._get_config()
        try:
            test_message = test_connection(config)
        except Exception as error:
            self._set_connection_status("Failed", f"Last test failed for {format_connection_target(config)}.", ERROR_BG, ERROR_FG)
            messagebox.showerror(
                "Connection test failed",
                f"Database type: PostgreSQL\nTarget: {format_connection_target(config)}\n\nError: {error}",
            )
            return

        self._set_connection_status("Connected", f"Last test succeeded for {format_connection_target(config)}.", SUCCESS_BG, SUCCESS_FG)
        messagebox.showinfo("Connection test successful", test_message)

    def _mark_connection_status_idle(self):
        self._set_connection_status(
            "Not Tested",
            "Configuration changed. Run the connection test again.",
            WARNING_BG,
            WARNING_FG,
        )

    def _set_connection_status(self, status_text, detail_text, bg, fg):
        if self.connection_status_label is None or self.connection_status_detail_label is None:
            return
        self.connection_status_label.configure(text=status_text, bg=bg, fg=fg)
        self.connection_status_detail_label.configure(text=detail_text)

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

    def _next_step(self):
        if not self._validate_form(show_success=False):
            return
        self._show_workload_view(self._get_config())
