import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import sys
import datetime
from multiprocessing import cpu_count

# 嘗試導入 tkcalendar
try:
    from tkcalendar import DateEntry
except ImportError:
    DateEntry = None

# 導入專案依賴
from config import APP_NAME_TC, APP_VERSION, CONFIG_FILE
from utils import log_error, save_config, ARCHIVE_SUPPORT_ENABLED, QR_SCAN_ENABLED
# 假設 tooltip.py 與 settings_window.py 在同一目錄 (gui/)
from .tooltip import Tooltip

class SettingsGUI(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.config = master.config.copy()
        self.plugin_ui_vars = {} 

        self.enable_extract_count_limit_var = tk.BooleanVar()
        self.extract_count_var = tk.StringVar()
        self.worker_processes_var = tk.StringVar()
        self.similarity_threshold_var = tk.DoubleVar()
        self.qr_resize_var = tk.StringVar()
        
        self.mode_key_map_to_internal = {"mutual_comparison": "mutual_comparison", "ad_comparison": "ad_comparison", "qr_detection": "qr_detection"}
        for plugin_id in self.master.plugin_manager: self.mode_key_map_to_internal[plugin_id] = plugin_id
        self.mode_key_map_from_internal = {v: k for k, v in self.mode_key_map_to_internal.items()}
        
        initial_mode = self.config.get('comparison_mode', 'ad_comparison')
        initial_mode_key = self.mode_key_map_from_internal.get(initial_mode, initial_mode)
        self.comparison_mode_var = tk.StringVar(value=initial_mode_key)
        
        self.enable_inter_folder_only_var = tk.BooleanVar()
        self.enable_ad_cross_comparison_var = tk.BooleanVar()
        self.enable_qr_hybrid_var = tk.BooleanVar()
        self.enable_time_filter_var = tk.BooleanVar()
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        self.folder_time_mode_var = tk.StringVar()
        self.page_size_var = tk.StringVar()
        self.enable_color_filter_var = tk.BooleanVar()
        self.enable_archive_scan_var = tk.BooleanVar()
        
        self.title(f"{APP_NAME_TC} v{APP_VERSION} - 設定"); self.geometry("700x900"); self.resizable(False, False)
        self.transient(master); self.grab_set(); self.protocol("WM_DELETE_WINDOW", self.destroy)
        
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        main_tab = ttk.Frame(notebook)
        notebook.add(main_tab, text="主設定")
        main_frame = ttk.Frame(main_tab, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_columnconfigure(1, weight=1)

        preproc_tab = ttk.Frame(notebook)
        notebook.add(preproc_tab, text="擴充功能 (前置處理)")
        
        self._preprocessor_host = preproc_tab

        # 1. 建立主設定頁面
        self._create_widgets(main_frame)

        # 2. 【關鍵修復】在此處明確初始化擴充功能頁面
        # 確保外掛的變數 (ui_vars) 在載入設定值之前就已經建立好
        try:
            self._init_plugin_slot_structures()
            self._place_preprocessor_plugins()
        except Exception as e:
            print(f"[ERROR] 初始化擴充功能介面失敗: {e}")

        self._setup_bindings()
        self._load_settings_into_gui()
        self._update_all_ui_states()
        
    def _update_all_ui_states(self, *args):
        mode = self.comparison_mode_var.get()
        is_plugin_mode = mode in self.master.plugin_manager
        if hasattr(self, 'plugin_frames'):
            for frame in self.plugin_frames.values(): frame.grid_forget()
        if is_plugin_mode and hasattr(self, 'plugin_frames') and mode in self.plugin_frames:
            self.plugin_frames[mode].grid(row=self.plugin_frame_row, column=0, columnspan=2, sticky="ew", pady=5, padx=5)
        is_ad_mode = (mode == "ad_comparison")
        is_hybrid_qr = (mode == "qr_detection" and self.enable_qr_hybrid_var.get())
        is_cross_comp = (mode == "mutual_comparison" and self.enable_ad_cross_comparison_var.get())
        ad_folder_state = tk.NORMAL if (is_ad_mode or is_hybrid_qr or is_cross_comp) else tk.DISABLED
        self.ad_folder_entry.config(state=ad_folder_state)
        self.ad_folder_button.config(state=ad_folder_state)
        self.qr_hybrid_cb.config(state=tk.NORMAL if mode == "qr_detection" and QR_SCAN_ENABLED else tk.DISABLED)
        is_mutual = (mode == "mutual_comparison")
        self.inter_folder_only_cb.config(state=tk.NORMAL if is_mutual else tk.DISABLED)
        self.ad_cross_comparison_cb.config(state=tk.NORMAL if is_mutual else tk.DISABLED)

    def _toggle_time_filter_fields(self, *args):
        state = tk.NORMAL if self.enable_time_filter_var.get() else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)

    def _init_plugin_slot_structures(self, slot_count: int = 6):
        if hasattr(self, "_preproc_stack_container") and self._preproc_stack_container.winfo_exists():
            return
        host_parent = getattr(self, "_preprocessor_host", self)
        host_parent.grid_rowconfigure(1, weight=1)
        host_parent.grid_columnconfigure(0, weight=1)
        self._preproc_stack_container = ttk.Frame(host_parent)
        self._preproc_stack_container.grid(row=0, column=0, sticky="new")
        self._plugin_slot_frames = {}
        self._plugin_slot_count = 0
        
    def _place_preprocessor_plugins(self):
        preprocessor_plugins = {
            pid: p for pid, p in self.master.plugin_manager.items()
            if p.get_plugin_type() == 'preprocessor'
        }
        if not preprocessor_plugins:
            return

        self._plugin_blocks = {}

        items_sorted = sorted(
            preprocessor_plugins.items(),
            key=lambda kv: (getattr(kv[1], 'get_slot_order', lambda: 999)(), kv[0])
        )

        for plugin_id, plugin in items_sorted:
            holder = ttk.LabelFrame(
                self._preproc_stack_container,
                text=getattr(plugin, 'get_tab_label', plugin.get_name)() or plugin.get_name(),
                padding="10"
            )
            
            prefers_inner = getattr(plugin, 'plugin_prefers_inner_enable', lambda: False)()
            
            enable_key = f"enable_{plugin_id}"
            self.plugin_ui_vars.setdefault(enable_key, tk.BooleanVar(value=self.config.get(enable_key, False)))
            enable_var = self.plugin_ui_vars[enable_key]
            
            inner_content_frame = ttk.Frame(holder)
            
            if not prefers_inner:
                cb = ttk.Checkbutton(holder, text=f"啟用 {plugin.get_name()}", variable=enable_var)
                desc = getattr(plugin, "get_description", lambda: "")() or ""
                if desc: Tooltip(cb, desc)
                cb.pack(anchor="w", fill="x", pady=(0, 6))

                inner_content_frame.pack(fill="x", expand=True)
                
                def create_outer_toggle(var, h):
                    def _toggle():
                        if var.get():
                            if not h.winfo_manager(): h.pack(fill="x", expand=True, pady=6, padx=5)
                        else:
                            h.pack_forget()
                    return _toggle
                
                toggle_func = create_outer_toggle(enable_var, holder)
                enable_var.trace_add("write", lambda *args, f=toggle_func: f())
                toggle_func()
            else:
                holder.pack(fill="x", expand=True, pady=6, padx=5)
                inner_content_frame.pack(fill="x", expand=True)

            get_frame_func = getattr(plugin, "get_settings_frame", getattr(plugin, "create_settings_frame", None))
            if get_frame_func:
                try:
                    plugin_frame = get_frame_func(inner_content_frame, self.config, self.plugin_ui_vars)
                except Exception as e:
                    # 這裡 log_error 可能還不能用，因為主視窗還沒完全建立，改用 print
                    print(f"[ERROR] 外掛 '{plugin.get_id()}' UI 載入失敗: {e}")
            
            self._plugin_blocks[plugin_id] = {'holder': holder, 'inner': inner_content_frame, 'enable_var': enable_var}

    def _create_widgets(self, frame: ttk.Frame):
        row_idx = 0
        
        path_frame = ttk.LabelFrame(frame, text="路徑設定", padding="10")
        path_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5)
        path_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(path_frame, text="根掃描資料夾:").grid(row=0, column=0, sticky="w", pady=2)
        self.root_scan_folder_entry = ttk.Entry(path_frame)
        self.root_scan_folder_entry.grid(row=0, column=1, sticky="ew", padx=5)
        ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.root_scan_folder_entry)).grid(row=0, column=2)
        ttk.Label(path_frame, text="廣告圖片資料夾:").grid(row=1, column=0, sticky="w", pady=2)
        self.ad_folder_entry = ttk.Entry(path_frame)
        self.ad_folder_entry.grid(row=1, column=1, sticky="ew", padx=5)
        self.ad_folder_button = ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.ad_folder_entry))
        self.ad_folder_button.grid(row=1, column=2)
        self.archive_scan_cb = ttk.Checkbutton(path_frame, text="啟用壓縮檔掃描 (ZIP/CBZ/RAR/CBR)", variable=self.enable_archive_scan_var)
        self.archive_scan_cb.grid(row=2, column=0, columnspan=3, sticky="w", pady=5)
        if not ARCHIVE_SUPPORT_ENABLED: self.archive_scan_cb.config(text="啟用壓縮檔掃描 (未找到 archive_handler.py)", state=tk.DISABLED)
        row_idx += 1
        
        basic_settings_frame = ttk.LabelFrame(frame, text="基本與性能設定", padding="10")
        basic_settings_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5)
        basic_settings_frame.grid_columnconfigure(1, weight=1)
        self.extract_count_limit_cb = ttk.Checkbutton(basic_settings_frame, text="啟用圖片抽取數量限制", variable=self.enable_extract_count_limit_var)
        self.extract_count_limit_cb.grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2)
        self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5)
        self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="(從每個資料夾樹末尾提取N張)").grid(row=1, column=2, sticky="w")
        ttk.Label(basic_settings_frame, text="工作進程數:").grid(row=3, column=0, sticky="w", pady=2)
        ttk.Spinbox(basic_settings_frame, from_=0, to=cpu_count(), textvariable=self.worker_processes_var, width=5).grid(row=3, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="(0=自動)").grid(row=3, column=2, sticky="w")
        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=4, column=0, sticky="w", pady=2)
        ttk.Scale(basic_settings_frame, from_=80, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label).grid(row=4, column=1, sticky="w", padx=5)
        self.threshold_label = ttk.Label(basic_settings_frame, text=""); self.threshold_label.grid(row=4, column=2, sticky="w")
        ttk.Checkbutton(basic_settings_frame, text="啟用顏色過濾閘 (建議開啟)", variable=self.enable_color_filter_var).grid(row=5, column=0, columnspan=3, sticky="w", pady=5)
        ttk.Label(basic_settings_frame, text="QR 檢測縮放尺寸:").grid(row=6, column=0, sticky="w", pady=2)
        ttk.Spinbox(basic_settings_frame, from_=400, to=1600, increment=200, textvariable=self.qr_resize_var, width=5).grid(row=6, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="px").grid(row=6, column=2, sticky="w")
        ttk.Label(basic_settings_frame, text="每頁顯示數量:").grid(row=7, column=0, sticky="w", pady=2)
        self.page_size_combo = ttk.Combobox(basic_settings_frame, textvariable=self.page_size_var, values=['50', '100', '200', '500', '顯示全部'], width=10); self.page_size_combo.grid(row=7, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (換行分隔):").grid(row=8, column=0, sticky="w", pady=2)
        self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=3); self.excluded_folders_text.grid(row=8, column=1, columnspan=2, sticky="ew", padx=5)
        row_idx += 1

        mode_plugins = {pid: p for pid, p in self.master.plugin_manager.items() if p.get_plugin_type() == 'mode'}
        
        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10"); mode_frame.grid(row=row_idx, column=0, sticky="nsew", pady=5, padx=5)
        ttk.Label(mode_frame, text="核心功能:").pack(anchor="w", pady=(0, 2))
        ttk.Radiobutton(mode_frame, text="廣告比對", variable=self.comparison_mode_var, value="ad_comparison", command=self._update_all_ui_states).pack(anchor="w", padx=10)
        ttk.Radiobutton(mode_frame, text="互相比對", variable=self.comparison_mode_var, value="mutual_comparison", command=self._update_all_ui_states).pack(anchor="w", padx=10)
        self.inter_folder_only_cb = ttk.Checkbutton(mode_frame, text="僅比對不同資料夾的圖片", variable=self.enable_inter_folder_only_var, command=self._update_all_ui_states); self.inter_folder_only_cb.pack(anchor="w", padx=30)
        self.ad_cross_comparison_cb = ttk.Checkbutton(mode_frame, text="[BETA] 智慧標記與廣告庫相似的羣組", variable=self.enable_ad_cross_comparison_var, command=self._update_all_ui_states); self.ad_cross_comparison_cb.pack(anchor="w", padx=30)
        self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測", variable=self.comparison_mode_var, value="qr_detection", command=self._update_all_ui_states); self.qr_mode_radiobutton.pack(anchor="w", padx=10, pady=(8,0))
        self.qr_hybrid_cb = ttk.Checkbutton(mode_frame, text="啟用廣告庫快速匹配", variable=self.enable_qr_hybrid_var, command=self._update_all_ui_states); self.qr_hybrid_cb.pack(anchor="w", padx=30)
        if not QR_SCAN_ENABLED: self.qr_mode_radiobutton.config(state=tk.DISABLED); self.qr_hybrid_cb.config(state=tk.DISABLED)

        if mode_plugins:
            ttk.Separator(mode_frame).pack(fill='x', pady=10)
            ttk.Label(mode_frame, text="外掛模式:").pack(anchor="w", pady=(0, 2))
            for plugin_id, plugin in mode_plugins.items():
                rb = ttk.Radiobutton(mode_frame, text=plugin.get_name(), variable=self.comparison_mode_var, value=plugin_id, command=self._update_all_ui_states)
                rb.pack(anchor="w", padx=10)
                if plugin.get_description(): Tooltip(rb, plugin.get_description())
        
        cache_time_frame = ttk.LabelFrame(frame, text="快取與篩選", padding="10")
        cache_time_frame.grid(row=row_idx, column=1, sticky="nsew", pady=5, padx=5)
        ttk.Button(cache_time_frame, text="清理圖片快取 (回收桶)", command=self._clear_image_cache).pack(anchor="w", pady=2)
        ttk.Button(cache_time_frame, text="清理資料夾快取 (回收桶)", command=self._clear_folder_cache).pack(anchor="w", pady=2)
        ttk.Separator(cache_time_frame, orient='horizontal').pack(fill='x', pady=5)
        time_mode_frame = ttk.Frame(cache_time_frame); time_mode_frame.pack(anchor='w', pady=(5, 5))
        ttk.Label(time_mode_frame, text="時間篩選基準:").pack(side=tk.LEFT)
        self.time_mode_combo = ttk.Combobox(time_mode_frame, textvariable=self.folder_time_mode_var, values=['修改時間', '建立時間'], width=12, state="readonly")
        self.time_mode_combo.pack(side=tk.LEFT, padx=5)
        Tooltip(self.time_mode_combo, "選擇判斷資料夾是否過期的時間基準。\n修改時間 (mtime): 資料夾內容變更時更新，適合增量掃描。\n建立時間 (ctime): 資料夾被建立時的時間。")
        self.time_filter_cb = ttk.Checkbutton(cache_time_frame, text="啟用資料夾時間篩選", variable=self.enable_time_filter_var)
        self.time_filter_cb.pack(anchor="w")
        time_inputs_frame = ttk.Frame(cache_time_frame); time_inputs_frame.pack(anchor='w', padx=20)
        ttk.Label(time_inputs_frame, text="從:").grid(row=0, column=0, sticky="w")
        if DateEntry: self.start_date_entry = DateEntry(time_inputs_frame, textvariable=self.start_date_var, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern='y-mm-dd')
        else: self.start_date_entry = ttk.Entry(time_inputs_frame, textvariable=self.start_date_var, width=15)
        self.start_date_entry.grid(row=0, column=1, sticky="ew")
        ttk.Label(time_inputs_frame, text="到:").grid(row=1, column=0, sticky="w")
        if DateEntry: self.end_date_entry = DateEntry(time_inputs_frame, textvariable=self.end_date_var, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern='y-mm-dd')
        else: self.end_date_entry = ttk.Entry(time_inputs_frame, textvariable=self.end_date_var, width=15)
        self.end_date_entry.grid(row=1, column=1, sticky="ew")
        row_idx += 1
        
        self.plugin_frames = {}
        if mode_plugins:
            for plugin_id, plugin in mode_plugins.items():
                plugin_settings_container = ttk.LabelFrame(frame, text=f"{plugin.get_name()} 專屬設定", padding="10")
                get_frame_func = getattr(plugin, "get_settings_frame", None)
                if get_frame_func and get_frame_func(plugin_settings_container, self.config, self.plugin_ui_vars):
                    self.plugin_frames[plugin_id] = plugin_settings_container

        self.plugin_frame_row = row_idx
        row_idx += 1
        
        button_frame = ttk.Frame(frame, padding="10")
        button_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=10)
        ttk.Button(button_frame, text="保存並關閉", command=self._save_and_close).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT)

    def _clear_image_cache(self): pass
    def _clear_folder_cache(self): pass

    def _load_settings_into_gui(self):
        self.root_scan_folder_entry.insert(0, self.config.get('root_scan_folder', ''))
        self.ad_folder_entry.insert(0, self.config.get('ad_folder_path', ''))
        self.extract_count_var.set(str(self.config.get('extract_count', 8)))
        self.worker_processes_var.set(str(self.config.get('worker_processes', 0)))
        self.excluded_folders_text.insert(tk.END, "\n".join(self.config.get('excluded_folders', [])))
        self.similarity_threshold_var.set(self.config.get('similarity_threshold', 95.0))
        self._update_threshold_label(self.similarity_threshold_var.get())
        self.enable_extract_count_limit_var.set(self.config.get('enable_extract_count_limit', True))
        today = datetime.date.today()
        self.start_date_var.set(self.config.get('start_date_filter') or today.replace(month=1, day=1).strftime("%Y-%m-%d"))
        self.end_date_var.set(self.config.get('end_date_filter') or today.strftime("%Y-%m-%d"))
        self.enable_time_filter_var.set(self.config.get('enable_time_filter', False))
        self.enable_qr_hybrid_var.set(self.config.get('enable_qr_hybrid_mode', True))
        self.qr_resize_var.set(str(self.config.get('qr_resize_size', 800)))
        self.enable_inter_folder_only_var.set(self.config.get('enable_inter_folder_only', True))
        self.enable_ad_cross_comparison_var.set(self.config.get('enable_ad_cross_comparison', True))
        self.enable_color_filter_var.set(self.config.get('enable_color_filter', True))
        self.page_size_var.set(str(self.config.get('page_size', 'all')))
        self.enable_archive_scan_var.set(self.config.get('enable_archive_scan', True))
        mode = self.config.get('folder_time_mode', 'mtime')
        self.folder_time_mode_var.set('建立時間' if mode == 'ctime' else '修改時間')
        
    def _setup_bindings(self):
        self.comparison_mode_var.trace_add("write", self._update_all_ui_states)
        self.enable_time_filter_var.trace_add("write", self._toggle_time_filter_fields)
        self.enable_ad_cross_comparison_var.trace_add("write", self._update_all_ui_states)
        self.enable_qr_hybrid_var.trace_add("write", self._update_all_ui_states)
        
    def _browse_folder(self, entry: ttk.Entry):
        folder = filedialog.askdirectory(parent=self)
        if folder: entry.delete(0, tk.END); entry.insert(0, folder)
        
    def _update_threshold_label(self, val: float): self.threshold_label.config(text=f"{float(val):.0f}%")
    
    def _save_and_close(self):
        if self._save_settings(): self.destroy()
        
    def _save_settings(self) -> bool:
        try:
            raw_mode = self.comparison_mode_var.get()
            comparison_mode = self.mode_key_map_to_internal.get(raw_mode, "mutual_comparison")
            mode_text = self.folder_time_mode_var.get()
            folder_time_mode = 'ctime' if mode_text == '建立時間' else 'mtime'
            config = {
                'root_scan_folder': self.root_scan_folder_entry.get().strip(),
                'ad_folder_path': self.ad_folder_entry.get().strip(),
                'extract_count': int(self.extract_count_var.get()),
                'worker_processes': int(self.worker_processes_var.get()),
                'enable_extract_count_limit': self.enable_extract_count_limit_var.get(),
                'excluded_folders': [f.strip() for f in self.excluded_folders_text.get("1.0", tk.END).splitlines() if f.strip()],
                'similarity_threshold': float(self.similarity_threshold_var.get()),
                'comparison_mode': comparison_mode,
                'enable_time_filter': self.enable_time_filter_var.get(),
                'start_date_filter': self.start_date_var.get(),
                'end_date_filter': self.end_date_var.get(),
                'enable_qr_hybrid_mode': self.enable_qr_hybrid_var.get(),
                'qr_resize_size': int(self.qr_resize_var.get()),
                'enable_inter_folder_only': self.enable_inter_folder_only_var.get(),
                'enable_ad_cross_comparison': self.enable_ad_cross_comparison_var.get(),
                'page_size': self.page_size_var.get().strip(),
                'enable_archive_scan': self.enable_archive_scan_var.get(),
                'enable_color_filter': self.enable_color_filter_var.get(),
                'folder_time_mode': folder_time_mode,
            }
            
            for var_key, var_obj in self.plugin_ui_vars.items():
                config_key = var_key
                if isinstance(var_obj, tk.BooleanVar): config[config_key] = var_obj.get()
                elif isinstance(var_obj, (tk.StringVar, tk.DoubleVar, tk.IntVar)): config[config_key] = var_obj.get()

            for plugin in self.master.plugin_manager.values():
                if hasattr(plugin, 'save_settings'):
                    config = plugin.save_settings(config, self.plugin_ui_vars)

            if not os.path.isdir(config['root_scan_folder']): messagebox.showerror("錯誤", "根掃描資料夾無效！", parent=self); return False
            if config['enable_time_filter']:
                try: 
                    if config['start_date_filter']: datetime.datetime.strptime(config['start_date_filter'], "%Y-%m-%d")
                    if config['end_date_filter']: datetime.datetime.strptime(config['end_date_filter'], "%Y-%m-%d")
                except ValueError: messagebox.showerror("錯誤", "日期格式不正確，請使用 YYYY-MM-DD。", parent=self); return False
            
            self.master.config.update(config)
            save_config(self.master.config, CONFIG_FILE)
            return True
        except ValueError as e: messagebox.showerror("錯誤", f"數字格式無效: {e}", parent=self); return False
        except Exception as e: 
            log_error(f"保存設定時出錯: {e}", True)
            messagebox.showerror("儲存失敗", f"保存設定時發生未知錯誤:\n{e}", parent=self)
            return False