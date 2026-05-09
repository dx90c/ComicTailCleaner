# ======================================================================
# 檔案：gui/settings_window.py
# 版本：1.3 (UI連動優化：外掛模式自動禁用主提取設定，Checkbox 即時連動)
# ======================================================================
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import sys
import datetime
from multiprocessing import cpu_count
try:
    import send2trash
except ImportError:
    send2trash = None

try: from tkcalendar import DateEntry
except ImportError: DateEntry = None

from config import APP_NAME_TC, APP_VERSION, CONFIG_FILE, DATA_DIR, CACHE_DIR
from utils import log_error, save_config, ARCHIVE_SUPPORT_ENABLED, QR_SCAN_ENABLED, _sanitize_path_for_filename
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
        self.hash_resolution_var = tk.StringVar(value="128")
        
        self.mode_key_map_to_internal = {"mutual_comparison": "mutual_comparison", "ad_comparison": "ad_comparison", "qr_detection": "qr_detection"}
        for plugin_id in self.master.plugin_manager: self.mode_key_map_to_internal[plugin_id] = plugin_id
        self.mode_key_map_from_internal = {v: k for k, v in self.mode_key_map_to_internal.items()}
        
        initial_mode = self.config.get('comparison_mode', 'ad_comparison')
        initial_mode_key = self.mode_key_map_from_internal.get(initial_mode, initial_mode)
        self.comparison_mode_var = tk.StringVar(value=initial_mode_key)
        
        self.enable_inter_folder_only_var = tk.BooleanVar()
        self.enable_ad_cross_comparison_var = tk.BooleanVar()
        self.enable_qr_hybrid_var = tk.BooleanVar()
        self.qr_resize_var = tk.StringVar(value="800")
        self.enable_qr_color_filter_var = tk.BooleanVar(value=False)
        self.enable_time_filter_var = tk.BooleanVar()
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        self.folder_time_mode_var = tk.StringVar()
        self.page_size_var = tk.StringVar()
        self.enable_color_filter_var = tk.BooleanVar()
        self.enable_whash_var = tk.BooleanVar()
        self.enable_archive_scan_var = tk.BooleanVar()
        self.enable_everything_mft_scan_var = tk.BooleanVar()
        self.enable_targeted_search_var = tk.BooleanVar()  # v-MOD: 尋親模式
        self.enable_rotation_matching_var = tk.BooleanVar()  # v-MOD: 旋轉容差比對
        self.enable_image_preprocess_var  = tk.BooleanVar()  # v-MOD: 圖像前處理加強
        
        self.title(f"{APP_NAME_TC} v{APP_VERSION} - 設定"); self.geometry("700x900"); self.resizable(False, False)
        self.transient(master); self.grab_set(); self.protocol("WM_DELETE_WINDOW", self.destroy)
        
        # 底部按鈕區 (固定)
        button_frame = ttk.Frame(self, padding="10")
        button_frame.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Button(button_frame, text="保存並關閉", command=self._save_and_close).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="套用", command=self._apply_settings).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT)
        
        # 分頁區
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)

        main_tab = ttk.Frame(self.notebook)
        self.notebook.add(main_tab, text="主設定")
        
        main_frame = ttk.Frame(main_tab, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_columnconfigure(1, weight=1)

        preproc_tab = ttk.Frame(self.notebook)
        self.notebook.add(preproc_tab, text="擴充功能 (前置處理)")
        
        self._preprocessor_host = preproc_tab
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        self._create_widgets(main_frame)

        try:
            self._init_plugin_slot_structures()
            self._place_preprocessor_plugins()
            self._place_secondary_mode_plugins()  # v-MOD: EMM 輔助掃描模式放到第二頁
        except Exception as e:
            print(f"[ERROR] 初始化擴充功能介面失敗: {e}")

        self._load_settings_into_gui()
        self._setup_bindings()
        
        # 初始化完成後，手動觸發一次狀態更新
        self._update_all_ui_states()

    def _on_tab_changed(self, event):
        tab_id = self.notebook.select()
        tab_text = self.notebook.tab(tab_id, "text")
        self.title(f"{APP_NAME_TC} - 設定 [{tab_text}]")
        
    def _update_all_ui_states(self, *args):
        mode = self.comparison_mode_var.get()
        is_plugin_mode = mode in self.master.plugin_manager
        
        # 切換外掛設定面板
        if hasattr(self, 'plugin_frames'):
            for frame in self.plugin_frames.values(): frame.grid_forget()
        if is_plugin_mode and hasattr(self, 'plugin_frames') and mode in self.plugin_frames:
            self.plugin_frames[mode].grid(row=self.plugin_frame_row, column=0, columnspan=2, sticky="ew", pady=5, padx=5)
        
        is_ad_mode = (mode == "ad_comparison")
        is_hybrid_qr = (mode == "qr_detection" and self.enable_qr_hybrid_var.get())
        is_cross_comp = (mode == "mutual_comparison" and self.enable_ad_cross_comparison_var.get())
        is_targeted = is_ad_mode and self.enable_targeted_search_var.get()
        
        ad_folder_state = tk.NORMAL if (is_ad_mode or is_hybrid_qr or is_cross_comp) else tk.DISABLED
        self.ad_folder_entry.config(state=ad_folder_state)
        self.ad_folder_button.config(state=ad_folder_state)
        
        # v-MOD: 根據尋親模式動態更換路徑標籤文字
        if hasattr(self, 'root_scan_folder_label') and hasattr(self, 'ad_folder_label'):
            if is_targeted:
                self.root_scan_folder_label.config(text="子本資料夾 (搜尋範圍):")
                self.ad_folder_label.config(text="親本資料夾 (比對基準):")
            else:
                self.root_scan_folder_label.config(text="根掃描資料夾:")
                self.ad_folder_label.config(text="廣告圖片資料夾:")
        
        # 尋親模式子選項只在廣告比對模式下可用
        if hasattr(self, 'targeted_search_cb'):
            self.targeted_search_cb.config(state=tk.NORMAL if is_ad_mode else tk.DISABLED)
        
        # v-MOD: 尋親模式啟用時，相似度滑塊無意義，鎖定並視覺提示
        if hasattr(self, 'threshold_scale'):
            self.threshold_scale.config(state=tk.DISABLED if is_targeted else tk.NORMAL)
        
        # v-MOD: 尋親模式 → 旋轉容差 & 圖片前處理 自動勾選並鎖灰；退出時還原
        if hasattr(self, 'rotation_matching_cb') and hasattr(self, 'image_preprocess_cb'):
            if is_targeted:
                # 進入：先記住原始狀態，再強制開啟並鎖定
                if not getattr(self, '_targeted_was_active', False):
                    self._saved_rotation = self.enable_rotation_matching_var.get()
                    self._saved_preprocess = self.enable_image_preprocess_var.get()
                    self._targeted_was_active = True
                self.enable_rotation_matching_var.set(True)
                self.enable_image_preprocess_var.set(True)
                self.rotation_matching_cb.config(state=tk.DISABLED)
                self.image_preprocess_cb.config(state=tk.DISABLED)
            else:
                # 退出：還原到進入前的狀態
                if getattr(self, '_targeted_was_active', False):
                    self.enable_rotation_matching_var.set(getattr(self, '_saved_rotation', False))
                    self.enable_image_preprocess_var.set(getattr(self, '_saved_preprocess', False))
                    self._targeted_was_active = False
                self.rotation_matching_cb.config(state=tk.NORMAL)
                self.image_preprocess_cb.config(state=tk.NORMAL)
        
        if hasattr(self, 'qr_hybrid_cb'):
            self.qr_hybrid_cb.config(state=tk.NORMAL if mode == "qr_detection" and QR_SCAN_ENABLED else tk.DISABLED)
        if hasattr(self, 'qr_color_cb'):
            self.qr_color_cb.config(state=tk.NORMAL if mode == "qr_detection" and QR_SCAN_ENABLED else tk.DISABLED)
        
        is_mutual = (mode == "mutual_comparison")
        self.inter_folder_only_cb.config(state=tk.NORMAL if is_mutual else tk.DISABLED)
        self.ad_cross_comparison_cb.config(state=tk.NORMAL if is_mutual else tk.DISABLED)

        # === v-MOD: 外掛模式與數量限制連動邏輯 ===
        if is_plugin_mode:
            # 外掛模式下，強制禁用主設定的提取數量 (因為外掛有自己的)
            self.extract_count_limit_cb.config(state=tk.DISABLED)
            self.extract_count_spinbox.config(state=tk.DISABLED)
        else:
            # 內建模式下，恢復可用
            self.extract_count_limit_cb.config(state=tk.NORMAL)
            # 輸入框狀態取決於 Checkbox 是否勾選
            is_limit_checked = self.enable_extract_count_limit_var.get()
            self.extract_count_spinbox.config(state=tk.NORMAL if is_limit_checked else tk.DISABLED)
        # === v-MOD END ===

    def _toggle_time_filter_fields(self, *args):
        state = tk.NORMAL if self.enable_time_filter_var.get() else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)

    def _init_plugin_slot_structures(self, slot_count: int = 6):
        if hasattr(self, "_preproc_stack_container") and self._preproc_stack_container.winfo_exists(): return
        host_parent = getattr(self, "_preprocessor_host", self)
        host_parent.grid_rowconfigure(1, weight=1)
        host_parent.grid_columnconfigure(0, weight=1)
        self._preproc_stack_container = ttk.Frame(host_parent)
        self._preproc_stack_container.grid(row=0, column=0, sticky="new")
        self._plugin_slot_frames = {}
        self._plugin_slot_count = 0
        
    def _place_preprocessor_plugins(self):
        preprocessor_plugins = {pid: p for pid, p in self.master.plugin_manager.items() if p.get_plugin_type() == 'preprocessor'}
        if not preprocessor_plugins: return

        self._plugin_blocks = {}
        items_sorted = sorted(preprocessor_plugins.items(), key=lambda kv: (getattr(kv[1], 'get_slot_order', lambda: 999)(), kv[0]))

        for plugin_id, plugin in items_sorted:
            holder = ttk.LabelFrame(self._preproc_stack_container, text=getattr(plugin, 'get_tab_label', plugin.get_name)() or plugin.get_name(), padding="10")
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
                        else: h.pack_forget()
                    return _toggle
                toggle_func = create_outer_toggle(enable_var, holder)
                enable_var.trace_add("write", lambda *args, f=toggle_func: f())
                toggle_func()
            else:
                holder.pack(fill="x", expand=True, pady=6, padx=5)
                inner_content_frame.pack(fill="x", expand=True)

            get_frame_func = getattr(plugin, "get_settings_frame", getattr(plugin, "create_settings_frame", None))
            if get_frame_func:
                try: plugin_frame = get_frame_func(inner_content_frame, self.config, self.plugin_ui_vars)
                except Exception as e: print(f"[ERROR] 外掛 '{plugin.get_id()}' UI 載入失敗: {e}")
            
            self._plugin_blocks[plugin_id] = {'holder': holder, 'inner': inner_content_frame, 'enable_var': enable_var}

    def _place_secondary_mode_plugins(self):
        """將 get_plugin_type()=='secondary_mode' 的外掛，以 RadioButton 形式放到第二頁。"""
        secondary_plugins = {
            pid: p for pid, p in self.master.plugin_manager.items()
            if p.get_plugin_type() == 'secondary_mode'
        }
        if not secondary_plugins:
            return

        holder = ttk.LabelFrame(
            self._preproc_stack_container,
            text="EMM 輔助掃描模式",
            padding="10"
        )
        holder.pack(fill="x", expand=True, pady=6, padx=5)
        
        ttk.Label(
            holder,
            text="選擇後回到主畫面，按「開始執行」即可使用：",
            foreground="#555555"
        ).pack(anchor="w", pady=(0, 6))

        for plugin_id, plugin in secondary_plugins.items():
            rb = ttk.Radiobutton(
                holder,
                text=plugin.get_name(),
                variable=self.comparison_mode_var,
                value=plugin_id,
                command=self._update_all_ui_states
            )
            rb.pack(anchor="w", padx=10, pady=2)
            desc = plugin.get_description()
            if desc:
                Tooltip(rb, desc)

    def _create_widgets(self, frame: ttk.Frame):
        row_idx = 0
        path_frame = ttk.LabelFrame(frame, text="路徑設定", padding="10")
        path_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5)
        path_frame.grid_columnconfigure(1, weight=1)
        # v-MOD: 儲存 Label 物件以便動態修改文字
        self.root_scan_folder_label = ttk.Label(path_frame, text="根掃描資料夾:")
        self.root_scan_folder_label.grid(row=0, column=0, sticky="w", pady=2)
        self.root_scan_folder_entry = ttk.Entry(path_frame)
        self.root_scan_folder_entry.grid(row=0, column=1, sticky="ew", padx=5)
        ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.root_scan_folder_entry)).grid(row=0, column=2)
        self.ad_folder_label = ttk.Label(path_frame, text="廣告圖片資料夾:")
        self.ad_folder_label.grid(row=1, column=0, sticky="w", pady=2)
        self.ad_folder_entry = ttk.Entry(path_frame)
        self.ad_folder_entry.grid(row=1, column=1, sticky="ew", padx=5)
        self.ad_folder_button = ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.ad_folder_entry))
        self.ad_folder_button.grid(row=1, column=2)
        self.archive_scan_cb = ttk.Checkbutton(path_frame, text="啟用壓縮檔掃描 (ZIP/CBZ/RAR/CBR)", variable=self.enable_archive_scan_var)
        self.archive_scan_cb.grid(row=2, column=0, columnspan=3, sticky="w", pady=5)
        if not ARCHIVE_SUPPORT_ENABLED: self.archive_scan_cb.config(text="啟用壓縮檔掃描 (未找到 archive_handler.py)", state=tk.DISABLED)
        self.everything_scan_cb = ttk.Checkbutton(path_frame, text="⚡ 啟用 Everything SDK 秒搜 (如未安裝將自動下載 100KB 外掛)", variable=self.enable_everything_mft_scan_var)
        self.everything_scan_cb.grid(row=3, column=0, columnspan=3, sticky="w", pady=(0, 5))
        Tooltip(self.everything_scan_cb, "借用 Everything 軟體的系統服務，以 0.1 秒極速獲取全硬碟 MFT 變更檔，強烈建議啟用。\n需確保您的右下角任務列有運行 Everything 軟體。")
        row_idx += 1
        
        basic_settings_frame = ttk.LabelFrame(frame, text="基本與性能設定", padding="10")
        basic_settings_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5)
        basic_settings_frame.grid_columnconfigure(1, weight=1)
        
        # --- v-MOD: 綁定 command 以即時更新 UI ---
        self.extract_count_limit_cb = ttk.Checkbutton(
            basic_settings_frame, 
            text="啟用圖片抽取數量限制", 
            variable=self.enable_extract_count_limit_var,
            command=self._update_all_ui_states # 勾選時觸發狀態檢查
        )
        # --- v-MOD END ---
        
        self.extract_count_limit_cb.grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2)
        self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5)
        self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="(從每個資料夾樹末尾提取N張)").grid(row=1, column=2, sticky="w")
        ttk.Label(basic_settings_frame, text="工作進程數:").grid(row=3, column=0, sticky="w", pady=2)
        worker_row = ttk.Frame(basic_settings_frame)
        worker_row.grid(row=3, column=1, columnspan=2, sticky="w", padx=5)
        ttk.Spinbox(worker_row, from_=0, to=cpu_count(), textvariable=self.worker_processes_var, width=5).pack(side=tk.LEFT)
        ttk.Label(worker_row, text="(0=自動)").pack(side=tk.LEFT, padx=(4, 20))
        ttk.Label(worker_row, text="圖片指紋基準:").pack(side=tk.LEFT)
        self.hash_res_combo = ttk.Combobox(
            worker_row,
            textvariable=self.hash_resolution_var,
            values=["標準 (64-bit)"],
            width=13, state="readonly"
        )
        self.hash_res_combo.pack(side=tk.LEFT, padx=(4, 0))
        Tooltip(self.hash_res_combo, "目前比對固定使用 64-bit (8x8) pHash 以確保 LSH 索引效能與相容性。\n資料庫仍會同步計算並存儲 256/1024-bit 數據供未來擴充使用。")
        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=4, column=0, sticky="w", pady=2)
        self.threshold_scale = ttk.Scale(basic_settings_frame, from_=70, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label)
        self.threshold_scale.grid(row=4, column=1, sticky="w", padx=5)
        self.threshold_label = ttk.Label(basic_settings_frame, text=""); self.threshold_label.grid(row=4, column=2, sticky="w")
        color_filter_frame = ttk.Frame(basic_settings_frame)
        color_filter_frame.grid(row=5, column=0, columnspan=3, sticky="w", pady=5)
        ttk.Checkbutton(color_filter_frame, text="啟用顏色過濾閘 (建議開啟)", variable=self.enable_color_filter_var).pack(side="left")
        ttk.Checkbutton(color_filter_frame, text="啟用 wHash 覆核 (關閉=僅看pHash)", variable=self.enable_whash_var).pack(side="left", padx=15)
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
        # --- 廣告比對 + 尋親模式 (同一行) ---
        ad_row = ttk.Frame(mode_frame); ad_row.pack(anchor="w", padx=10)
        ttk.Radiobutton(ad_row, text="廣告比對", variable=self.comparison_mode_var, value="ad_comparison", command=self._update_all_ui_states).pack(side=tk.LEFT)
        self.targeted_search_cb = ttk.Checkbutton(
            ad_row, text="🔍 尋親模式",
            variable=self.enable_targeted_search_var,
            command=self._update_all_ui_states
        )
        self.targeted_search_cb.pack(side=tk.LEFT, padx=(10, 0))
        Tooltip(self.targeted_search_cb, "假設廣告資料夾裡的每張圖，在掃描範圍內一定存在對應圖。\n啟用後為每張廣告找出最佳配對，\n並自動開啟旋轉容差與圖片前處理。\n\n親本資料夾 = 廣告圖片資料夾 (比對基準)\n子本資料夾 = 根掃描資料夾 (搜尋範圍)")
        # --- 旋轉容差 + 圖片前處理 (同一行，縮排在廣告比對下方) ---
        enhance_row = ttk.Frame(mode_frame); enhance_row.pack(anchor="w", padx=30)
        self.rotation_matching_cb = ttk.Checkbutton(
            enhance_row, text="🔄 旋轉容差",
            variable=self.enable_rotation_matching_var,
        )
        self.rotation_matching_cb.pack(side=tk.LEFT)
        Tooltip(self.rotation_matching_cb, "對每張圖計算 0°/90°/180°/270° 四角度指紋。\n適用：同一本書橫掃/直掃不一致。\n尋親模式時自動開啟並鎖定。")
        self.image_preprocess_cb = ttk.Checkbutton(
            enhance_row, text="✂️ 圖片前處理",
            variable=self.enable_image_preprocess_var,
        )
        self.image_preprocess_cb.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.image_preprocess_cb, "自動裁切白邊 + 對比度拉平。\n適用：黑白掃描 vs 彩色廣告相似度偏低時。\n尋親模式時自動開啟並鎖定。")
        # --- 互相比對 ---
        ttk.Radiobutton(mode_frame, text="互相比對", variable=self.comparison_mode_var, value="mutual_comparison", command=self._update_all_ui_states).pack(anchor="w", padx=10)
        self.inter_folder_only_cb = ttk.Checkbutton(mode_frame, text="僅比對不同資料夾的圖片", variable=self.enable_inter_folder_only_var, command=self._update_all_ui_states); self.inter_folder_only_cb.pack(anchor="w", padx=30)
        self.ad_cross_comparison_cb = ttk.Checkbutton(mode_frame, text="[BETA] 智慧標記與廣告庫相似的羣組", variable=self.enable_ad_cross_comparison_var, command=self._update_all_ui_states); self.ad_cross_comparison_cb.pack(anchor="w", padx=30)
        # --- QR ---
        self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測", variable=self.comparison_mode_var, value="qr_detection", command=self._update_all_ui_states)
        self.qr_mode_radiobutton.pack(anchor="w", padx=10, pady=(8,0))
        
        qr_sub_frame = ttk.Frame(mode_frame); qr_sub_frame.pack(anchor="w", padx=30)
        self.qr_hybrid_cb = ttk.Checkbutton(qr_sub_frame, text="啟用廣告庫快速匹配", variable=self.enable_qr_hybrid_var, command=self._update_all_ui_states)
        self.qr_hybrid_cb.pack(anchor="w")
        
        self.qr_color_cb = ttk.Checkbutton(qr_sub_frame, text="✅ 僅掃描彩色圖片 (極快過濾黑白內頁)", variable=self.enable_qr_color_filter_var, command=self._update_all_ui_states)
        self.qr_color_cb.pack(anchor="w", pady=(2, 0))
        
        if not QR_SCAN_ENABLED: 
            self.qr_mode_radiobutton.config(state=tk.DISABLED)
            self.qr_hybrid_cb.config(state=tk.DISABLED)
            self.qr_color_cb.config(state=tk.DISABLED)

        if mode_plugins:
            ttk.Separator(mode_frame).pack(fill='x', pady=10)
            ttk.Label(mode_frame, text="外掛模式:").pack(anchor="w", pady=(0, 2))
            for plugin_id, plugin in mode_plugins.items():
                rb = ttk.Radiobutton(mode_frame, text=plugin.get_name(), variable=self.comparison_mode_var, value=plugin_id, command=self._update_all_ui_states)
                rb.pack(anchor="w", padx=10)
                if plugin.get_description(): Tooltip(rb, plugin.get_description())
        
        cache_time_frame = ttk.LabelFrame(frame, text="快取與篩選", padding="10")
        cache_time_frame.grid(row=row_idx, column=1, sticky="nsew", pady=5, padx=5)
        ttk.Button(cache_time_frame, text="移除根目錄圖片快取 (回收桶)", command=self._clear_image_cache).pack(anchor="w", pady=2)
        ttk.Button(cache_time_frame, text="移除廣告庫圖片快取 (回收桶)", command=self._clear_ad_cache).pack(anchor="w", pady=2)
        ttk.Button(cache_time_frame, text="重建資料夾掃描索引", command=self._clear_folder_cache).pack(anchor="w", pady=2)
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
        
        # v-MOD: [今日] 按鈕
        self.today_btn = ttk.Button(time_inputs_frame, text="今日", width=4, command=self._set_today)
        self.today_btn.grid(row=1, column=2, padx=(5, 0))
        Tooltip(self.today_btn, "將結束日期重設為今天")
        
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
        
    def _build_scanned_cache_db_path(self, folder_path):
        folder_path = (folder_path or "").strip()
        if not folder_path:
            return None
        cache_name = _sanitize_path_for_filename(folder_path)
        if not cache_name:
            return None
        return os.path.join(CACHE_DIR, f"scanned_hashes_cache_{cache_name}.db")

    def _delete_sqlite_family(self, db_path):
        removed = 0
        for suffix in ("", "-wal", "-shm"):
            target = db_path + suffix
            if os.path.exists(target):
                if send2trash is not None:
                    send2trash.send2trash(target)
                else:
                    os.remove(target)
                removed += 1
        return removed

    def _clear_image_cache(self):
        from tkinter import messagebox

        root_path = self.root_scan_folder_entry.get().strip()
        db_path = self._build_scanned_cache_db_path(root_path)
        if not db_path:
            messagebox.showinfo("提示", "請先設定根掃描資料夾，才能將對應的圖片快取移至回收桶。")
            return
        if not os.path.exists(db_path):
            messagebox.showinfo("提示", f"目前根目錄尚無圖片快取：\n{root_path}")
            return
        
        if not messagebox.askyesno("警告", "⚠️ 警告：清理此快取將導致下次掃描時【全量重新計算特徵】，耗時將顯著增加！\n\n除非您更換了硬碟或懷疑快取損毀，否則不建議手動清理。\n\n您確定要繼續將其移至回收桶嗎？"):
            return
            
        try:
            removed = self._delete_sqlite_family(db_path)
            action = "移至回收桶" if send2trash is not None else "刪除"
            messagebox.showinfo("完成", f"已將目前根目錄的圖片快取{action}。\n路徑：{root_path}\n處理檔案數：{removed}")
        except OSError as e:
            messagebox.showerror("錯誤", f"處理目前根目錄圖片快取失敗：{e}")

    def _clear_folder_cache(self):
        import os, glob
        from tkinter import messagebox
        data_dir = CACHE_DIR
        if not os.path.exists(data_dir):
            messagebox.showinfo("提示", "目前沒有任何快取需要清理。")
            return
            
        files = glob.glob(os.path.join(data_dir, 'folder_state_cache_*.db'))
        if not files:
            messagebox.showinfo("提示", "目前沒有任何資料夾快取需要清理。")
            return

        if not messagebox.askyesno("警告", "⚠️ 警告：清理此快取將導致下次掃描時【全量重新計算特徵】，耗時將顯著增加！\n\n除非您更換了硬碟或懷疑快取損毀，否則不建議手動清理。\n\n您確定要繼續將其移至回收桶嗎？"):
            return

        count = 0
        for f in files:
            try: 
                removed = self._delete_sqlite_family(f)
                if removed > 0:
                    count += 1
            except OSError as e:
                print(f"無法刪除 {f}: {e}")
                
        if hasattr(self.master, 'folder_cache') and isinstance(self.master.folder_cache, dict):
            self.master.folder_cache.clear()
            
        action = "移至回收桶" if send2trash is not None else "刪除"
        messagebox.showinfo("重建完成", f"已將 {count} 份資料夾狀態快取{action}。\n下次掃描時，系統會重新建立資料夾掃描索引。")

    def _clear_ad_cache(self):
        from tkinter import messagebox

        ad_path = self.ad_folder_entry.get().strip()
        db_path = self._build_scanned_cache_db_path(ad_path)
        if not db_path:
            messagebox.showinfo("提示", "請先設定廣告圖片資料夾，才能將對應的快取移至回收桶。")
            return
        if not os.path.exists(db_path):
            messagebox.showinfo("提示", f"目前廣告資料夾尚無快取：\n{ad_path}")
            return
            
        if not messagebox.askyesno("警告", "⚠️ 警告：清理此快取將導致下次掃描時【全量重新計算特徵】，耗時將顯著增加！\n\n除非您更換了硬碟或懷疑快取損毀，否則不建議手動清理。\n\n您確定要繼續將其移至回收桶嗎？"):
            return
        try:
            removed = self._delete_sqlite_family(db_path)
            action = "移至回收桶" if send2trash is not None else "刪除"
            messagebox.showinfo("完成", f"已將目前廣告資料夾的快取{action}。\n路徑：{ad_path}\n處理檔案數：{removed}")
        except OSError as e:
            messagebox.showerror("錯誤", f"處理目前廣告資料夾快取失敗：{e}")

    def _set_today(self):
        """將結束日期設為今天"""
        self.end_date_var.set(datetime.date.today().strftime("%Y-%m-%d"))

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
        # --- v-MOD START: Session 首次開啟自動填入最近範圍 ---
        is_first_open = getattr(self.master, '_settings_first_open', False)
        
        if is_first_open:
            # 參考 maintenance_workflow 的邏輯：兩個月前的 1 號
            t_month, t_year = today.month - 2, today.year
            if t_month < 1: t_month += 12; t_year -= 1
            try: start_date_str = datetime.date(t_year, t_month, 1).strftime("%Y-%m-%d")
            except ValueError: start_date_str = "2025-10-01"
            
            self.start_date_var.set(start_date_str)
            self.end_date_var.set(today.strftime("%Y-%m-%d"))
            
            # 標記為已處理，下次打開就讀取 config
            self.master._settings_first_open = False
        else:
            self.start_date_var.set(self.config.get('start_date_filter') or today.replace(month=1, day=1).strftime("%Y-%m-%d"))
            self.end_date_var.set(self.config.get('end_date_filter') or today.strftime("%Y-%m-%d"))
        # --- v-MOD END ---
        self.enable_time_filter_var.set(self.config.get('enable_time_filter', False))
        self.enable_qr_hybrid_var.set(self.config.get('enable_qr_hybrid_mode', True))
        self.enable_qr_color_filter_var.set(self.config.get('enable_qr_color_filter', False))
        
        mode = self.config.get('folder_time_mode', 'mtime')
        self.folder_time_mode_var.set('建立時間' if mode == 'ctime' else '修改時間')
        self.enable_inter_folder_only_var.set(self.config.get('enable_inter_folder_only', True))
        self.enable_ad_cross_comparison_var.set(self.config.get('enable_ad_cross_comparison', True))
        self.enable_color_filter_var.set(self.config.get('enable_color_filter', True))
        self.enable_whash_var.set(self.config.get('enable_whash', True))
        self.page_size_var.set(str(self.config.get('page_size', 'all')))
        self.enable_archive_scan_var.set(self.config.get('enable_archive_scan', True))
        self.enable_everything_mft_scan_var.set(self.config.get('enable_everything_mft_scan', True))
        self.enable_targeted_search_var.set(self.config.get('enable_targeted_search', False))  # v-MOD
        self.enable_rotation_matching_var.set(self.config.get('enable_rotation_matching', False))  # v-MOD
        self.enable_image_preprocess_var.set(self.config.get('enable_image_preprocess', False))   # v-MOD
        # 圖片指紋基準 (HASH-CONFIG-01 UI 修正：目前固定顯示 64-bit)
        self.hash_resolution_var.set("標準 (64-bit)")
        
    def _setup_bindings(self):
        self.comparison_mode_var.trace_add("write", self._update_all_ui_states)
        self.enable_time_filter_var.trace_add("write", self._toggle_time_filter_fields)
        self.enable_ad_cross_comparison_var.trace_add("write", self._update_all_ui_states)
        self.enable_qr_hybrid_var.trace_add("write", self._update_all_ui_states)
        
    def _browse_folder(self, entry: ttk.Entry):
        folder = filedialog.askdirectory(parent=self)
        if folder: entry.delete(0, tk.END); entry.insert(0, folder)
        
    def _update_threshold_label(self, val: float): self.threshold_label.config(text=f"{float(val):.0f}%")
    
    def _apply_settings(self):
        """保存設定但不關閉視窗"""
        if self._save_settings(show_msg=False):
            messagebox.showinfo("成功", "設定已套用！", parent=self)

    def _save_and_close(self):
        if self._save_settings(): self.destroy()
        
    def _save_settings(self, show_msg=True) -> bool:
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
                'enable_qr_color_filter': self.enable_qr_color_filter_var.get(),
                'qr_resize_size': int(self.qr_resize_var.get()),
                'enable_inter_folder_only': self.enable_inter_folder_only_var.get(),
                'enable_ad_cross_comparison': self.enable_ad_cross_comparison_var.get(),
                'page_size': self.page_size_var.get().strip(),
                'enable_archive_scan': self.enable_archive_scan_var.get(),
                'enable_everything_mft_scan': self.enable_everything_mft_scan_var.get(),
                'enable_color_filter': self.enable_color_filter_var.get(),
                'enable_whash': self.enable_whash_var.get(),
                'folder_time_mode': folder_time_mode,
                'enable_targeted_search': self.enable_targeted_search_var.get(),  # v-MOD
                'enable_rotation_matching': self.enable_rotation_matching_var.get(),  # v-MOD
                'enable_image_preprocess': self.enable_image_preprocess_var.get(),   # v-MOD
                'hash_resolution': {"極速 (32px)": 32, "標準 (128px)": 128, "精準 (512px)": 512, "標準 (64-bit)": 128}.get(self.hash_resolution_var.get(), 128),
            }
            
            for var_key, var_obj in self.plugin_ui_vars.items():
                config_key = var_key
                if isinstance(var_obj, tk.BooleanVar): config[config_key] = var_obj.get()
                elif isinstance(var_obj, (tk.StringVar, tk.DoubleVar, tk.IntVar)): config[config_key] = var_obj.get()

            for plugin in self.master.plugin_manager.values():
                if hasattr(plugin, 'save_settings'):
                    config = plugin.save_settings(config, self.plugin_ui_vars)

            if not os.path.isdir(config['root_scan_folder']): 
                if show_msg: messagebox.showerror("錯誤", "根掃描資料夾無效！", parent=self)
                return False
                
            if config['enable_time_filter']:
                try: 
                    if config['start_date_filter']: datetime.datetime.strptime(config['start_date_filter'], "%Y-%m-%d")
                    if config['end_date_filter']: datetime.datetime.strptime(config['end_date_filter'], "%Y-%m-%d")
                except ValueError: 
                    if show_msg: messagebox.showerror("錯誤", "日期格式不正確，請使用 YYYY-MM-DD。", parent=self)
                    return False
            
            self.master.config.update(config)
            save_config(self.master.config, CONFIG_FILE)
            return True
        except ValueError as e: 
            if show_msg: messagebox.showerror("錯誤", f"數字格式無效: {e}", parent=self)
            return False
        except Exception as e: 
            log_error(f"保存設定時出錯: {e}", True)
            if show_msg: messagebox.showerror("儲存失敗", f"保存設定時發生未知錯誤:\n{e}", parent=self)
            return False
