# ======================================================================
# 檔案名稱：gui/main_window.py
# 版本：1.9.3 (模組化拆分版 + 補回生命週期鉤子)
# ======================================================================
import os
import sys
import json
import hashlib
import datetime
import threading
import time
import shutil
import tkinter as tk
import importlib.util
from tkinter import ttk, filedialog, messagebox, font
from multiprocessing import cpu_count
from queue import Queue, Empty
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Optional, Tuple, List, Dict

try:
    from PIL import Image, ImageTk, ImageOps, ImageDraw, ImageChops
except ImportError:
    Image = ImageTk = ImageOps = ImageDraw = ImageChops = None
try:
    import send2trash
except ImportError:
    send2trash = None
    
from config import CONFIG_FILE, default_config, APP_NAME_TC, APP_VERSION, CACHE_DIR, CONFIG_DIR, DATA_DIR
import utils
from utils import (log_info, log_error, log_warning, log_performance, save_config, load_config, 
                   _is_virtual_path, _parse_virtual_path, _open_folder, _reveal_in_explorer, _get_file_stat, 
                   _open_image_from_any_path, 
                   ARCHIVE_SUPPORT_ENABLED, QR_SCAN_ENABLED, VPATH_SEPARATOR)


try:
    from plugins.base_plugin import BasePlugin
except ImportError:
    BasePlugin = None 

# from core_engine import ImageComparisonEngine (已移除，此處未直接使用)
# from processors.comparison_processor import ComparisonProcessor (已改為延遲載入)
# from processors.qr_processor import QrProcessor (已改為延遲載入)
from processors.base_processor import BaseProcessor 
from processors.scanner import ScannedImageCacheManager, FolderStateCacheManager

# 注意：請確保 core 資料夾下有 selection_strategies.py，否則這行會報錯
# 如果沒有該檔案，請暫時註解掉這行以及 _select_suggested_smart 中的相關邏輯
# import core.selection_strategies as selection_strategies (已改為延遲載入)
from core.undo_manager import UndoManager

try:
    from multiprocessing import cpu_count
except ImportError:
    def cpu_count(): return 4

# --- 導入拆分後的模組 ---
# from .settings_window import SettingsGUI (已改為延遲載入)
from .tooltip import Tooltip

class MainWindow(tk.Tk):
    def __init__(self, *args, **kwargs):
        self._startup_t0 = time.perf_counter()
        super().__init__(*args, **kwargs)
        if Image is None or ImageTk is None: messagebox.showerror("缺少核心依賴", "Pillow 函式庫未安裝或無法載入，程式無法運行。"); self.destroy(); return
        
        # 1. 初始化外掛管理器
        self.plugin_manager = {}
        
        # 2. 先以預設配置讀取基本設定 (外掛配置稍後合併)
        self.config = load_config(CONFIG_FILE, default_config.copy())

        self.pil_img_target = None; self.pil_img_compare = None; self.img_tk_target = None; self.img_tk_compare = None; self._after_id = None
        self.all_found_items, self.all_file_data = [], {}; self.sorted_groups = []
        self.selected_files, self.banned_groups = set(), set(); self.protected_paths = set()
        self.child_to_parent, self.parent_to_children, self.item_to_path = {}, defaultdict(list), {}
        self.scan_thread = None; self.cancel_event, self.pause_event = threading.Event(), threading.Event()
        self.scan_queue, self.preview_queue = Queue(), Queue(); self.executor = ThreadPoolExecutor(max_workers=2)
        self.sort_by_column = 'count'; self.sort_direction_is_ascending = False; self._preview_delay = 150
        self.scan_start_time, self.final_status_text = None, ""; self._widgets_initialized = False; self.is_paused = False; self.is_closing = False
        self._settings_first_open = True  # v-MOD: 紀錄 session 內是否首次開啟設定
        self.processor_instance: Optional[BaseProcessor] = None
        self.current_target_path: Optional[str] = None
        self.current_compare_path: Optional[str] = None
        self._last_target_path: Optional[str] = None
        self._last_compare_path: Optional[str] = None
        self._last_target_src_size: Optional[Tuple[int, int]] = None
        self._last_compare_src_size: Optional[Tuple[int, int]] = None
        self._qr_style = dict(color=(0, 255, 0), alpha=90, outline_thickness=None)
        self._initial_ui_started = False
        self._heavy_widgets_finished = False
        self.preprocessor_results = [] # 新增：存放前置處理器的結果摘要
        
        self._setup_main_window()

    def _load_plugins(self):
        if BasePlugin is None: return
        self.plugin_manager = {}
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            # 修正路徑：因為 gui/main_window.py 在 gui 資料夾內，所以要往上兩層才能找到 plugins
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        plugins_dir = os.path.join(base_path, "plugins")
        if not os.path.isdir(plugins_dir):
            return
        
        for plugin_name in os.listdir(plugins_dir):
            plugin_path = os.path.join(plugins_dir, plugin_name)
            if os.path.isdir(plugin_path) and os.path.isfile(os.path.join(plugin_path, "processor.py")):
                try:
                    processor_path = os.path.join(plugin_path, "processor.py")
                    spec = importlib.util.spec_from_file_location(f"plugins.{plugin_name}.processor", processor_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    for attr in dir(module):
                        cls = getattr(module, attr)
                        if isinstance(cls, type) and issubclass(cls, BasePlugin) and cls is not BasePlugin:
                            instance = cls()
                            self.plugin_manager[instance.get_id()] = instance
                except Exception as e:
                    log_error(f"[Plugin load] Failed to load plugin '{plugin_name}' from {processor_path}: {e}", include_traceback=True)
                    print(f"加載外掛 '{plugin_name}' 失敗: {e}")

    def deiconify(self):
        super().deiconify()
        if not self._widgets_initialized and not self._initial_ui_started:
            self._initial_ui_started = True
            self._init_widgets()
            self.update_idletasks()
            log_info(f"[啟動] Real UI shell painted in {time.perf_counter() - self._startup_t0:.2f}s")
            self.after_idle(self._finish_initial_ui)

    def _finish_initial_ui(self):
        if not self._heavy_widgets_finished:
            t0 = time.perf_counter()
            self._create_checkbox_icons()
            self._setup_tree_tooltip()
            self._create_context_menu()
            self._heavy_widgets_finished = True
            log_info(f"[啟動] Deferred UI helpers initialized in {time.perf_counter() - t0:.2f}s")

        self._check_queues()
        log_info(f"[啟動] GUI widgets initialized in {time.perf_counter() - self._startup_t0:.2f}s")

        # 延遲觸發外掛載入
        self.after(50, self._start_async_plugin_load)
            
    def _start_async_plugin_load(self):
        self.status_label.config(text="⏱️ 正在背景載入外掛模組...")
        threading.Thread(target=self._async_plugin_load_worker, daemon=True).start()

    def _async_plugin_load_worker(self):
        t0 = time.perf_counter()
        self._load_plugins()
        plugin_elapsed = time.perf_counter() - t0
            
        combined_default_config = default_config.copy()
        for plugin_id, plugin in self.plugin_manager.items():
            if hasattr(plugin, "get_default_config"):
                try:
                    plugin_defaults = plugin.get_default_config() or {}
                    for k, v in plugin_defaults.items():
                        if k not in combined_default_config:
                            combined_default_config[k] = v
                except Exception as e:
                    print(f"[WARN] 讀取外掛 {plugin_id} 預設值失敗: {e}")
                    
        self.after(0, self._on_plugins_loaded, combined_default_config, plugin_elapsed)

    def _on_plugins_loaded(self, combined_default_config, plugin_elapsed: float = 0.0):
        self.config = load_config(CONFIG_FILE, combined_default_config)
        log_info(f"[啟動] Plugin load completed in {plugin_elapsed:.2f}s ({len(self.plugin_manager)} plugins)")
        
        # 套用外掛樹狀標籤樣式
        for plugin in self.plugin_manager.values():
            if hasattr(plugin, 'get_styles'):
                for tag_name, style_cfg in plugin.get_styles().items():
                    self.tree.tag_configure(tag_name, **style_cfg)
                    
            if hasattr(plugin, 'on_app_ready'):
                try:
                    plugin.on_app_ready(self)
                except Exception as e:
                    utils.log_error(f"執行外掛 {plugin.get_id()} 的 on_app_ready 失敗: {e}")
                    
        self.settings_button.config(state=tk.NORMAL)
        self.start_button.config(state=tk.NORMAL)
        self.status_label.config(text="準備就緒")
        log_info(f"[啟動] GUI ready in {time.perf_counter() - self._startup_t0:.2f}s")
        self.after(500, self._start_async_settings_preload)

    def _start_async_settings_preload(self):
        threading.Thread(target=self._async_settings_preload_worker, daemon=True).start()

    def _async_settings_preload_worker(self):
        t0 = time.perf_counter()
        try:
            # 準備就緒後再預載，避免拖慢主視窗可操作時間。
            import gui.settings_window
            log_info(f"[啟動] Settings window preloaded in {time.perf_counter() - t0:.2f}s")
        except Exception as e:
            log_warning(f"[啟動] Settings window preload failed: {e}")
        
    def _setup_main_window(self):
        self.title(f"{APP_NAME_TC} v{APP_VERSION}")
        self.geometry("1600x900")
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f'{w}x{h}+{self.winfo_screenwidth()//2 - w//2}+{max(20, self.winfo_screenheight()//2 - h//2)}')
        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        sys.excepthook = self.custom_excepthook
        
    def _init_widgets(self):
        if self._widgets_initialized: return
        
        # --- v-MOD: 初始化 Undo Manager ---
        data_dir = self.config.get('data_dir', getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__))))
        self.undo_manager = UndoManager(data_dir)
        
        self.bold_font = self._create_bold_font(); self._create_widgets(); self._bind_keys(); self._widgets_initialized = True
        
    def _create_checkbox_icons(self):
        from PIL import Image, ImageDraw, ImageTk
        size = 16
        img_u = Image.new('RGBA', (size, size), (0,0,0,0)); draw_u = ImageDraw.Draw(img_u)
        draw_u.rectangle([1,1,size-2,size-2], fill=(255,255,255,255), outline=(150,150,150,255), width=1)
        self.icon_unchecked = ImageTk.PhotoImage(img_u)
        
        img_c = Image.new('RGBA', (size, size), (0,0,0,0)); draw_c = ImageDraw.Draw(img_c)
        draw_c.rectangle([1,1,size-2,size-2], fill=(0, 150, 136, 255), outline=(0, 150, 136, 255), width=1)
        draw_c.line([4, 8, 7, 11, 12, 4], fill=(255,255,255,255), width=2)
        self.icon_checked = ImageTk.PhotoImage(img_c)

        img_l = Image.new('RGBA', (size, size), (0,0,0,0)); draw_l = ImageDraw.Draw(img_l)
        draw_l.rectangle([1,1,size-2,size-2], fill=(240, 240, 240, 255), outline=(200,200,200,255), width=1)
        draw_l.rectangle([6, 7, 10, 10], fill=(120, 120, 120, 255))
        draw_l.arc([6, 4, 10, 8], 180, 0, fill=(120, 120, 120, 255), width=1)
        self.icon_locked = ImageTk.PhotoImage(img_l)
        
        img_m = Image.new('RGBA', (size, size), (0,0,0,0)); draw_m = ImageDraw.Draw(img_m)
        draw_m.rectangle([1,1,size-2,size-2], fill=(0, 150, 136, 255), outline=(0, 150, 136, 255), width=1)
        draw_m.rectangle([4, 7, 12, 9], fill=(255,255,255,255))
        self.icon_mixed = ImageTk.PhotoImage(img_m)
        
    def custom_excepthook(self, exc_type, exc_value, exc_traceback):
        log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", True)
        if self.winfo_exists(): messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt'。")
        self.destroy()
        
    def _create_bold_font(self) -> tuple:
        try:
            f = ttk.Style().lookup("TLabel", "font")
            return (self.tk.call('font', 'actual', f, '-family'), abs(int(self.tk.call('font', 'actual', f, '-size'))), 'bold')
        except: return ("TkDefaultFont", 9, 'bold')
        
    def _create_widgets(self):
        style = ttk.Style(self); style.configure("Accent.TButton", font=self.bold_font, foreground='blue'); style.configure("Danger.TButton", font=self.bold_font, foreground='red')
        top_frame=ttk.Frame(self,padding="5"); top_frame.pack(side=tk.TOP,fill=tk.X)
        self.settings_button=ttk.Button(top_frame,text="設定",command=self.open_settings, state=tk.DISABLED); self.settings_button.pack(side=tk.LEFT,padx=5)
        self.start_button=ttk.Button(top_frame,text="開始執行",command=self.start_scan,style="Accent.TButton", state=tk.DISABLED); self.start_button.pack(side=tk.LEFT,padx=5)
        self.pause_button = ttk.Button(top_frame, text="暫停", command=self.toggle_pause, width=8, state=tk.DISABLED); self.pause_button.pack(side=tk.LEFT, padx=5)
        self.cancel_button=ttk.Button(top_frame,text="終止",command=self.cancel_scan, style="Danger.TButton", state=tk.DISABLED); self.cancel_button.pack(side=tk.LEFT, padx=5)
        
        # --- v-MOD: 新增 Undo 按鈕 ---
        undo_frame = ttk.Frame(top_frame)
        undo_frame.pack(side=tk.RIGHT, padx=5)
        self.apply_button = ttk.Button(undo_frame, text="套用刪除", command=self._commit_deletions_now)
        self.apply_button.pack(side=tk.LEFT, padx=5)
        self.undo_button = ttk.Button(undo_frame, text="復原刪除", command=self._undo_last_action)
        self.undo_button.pack(side=tk.LEFT, padx=5)
        self._update_undo_button_state()
        
        main_pane=ttk.Panedwindow(self,orient=tk.HORIZONTAL); main_pane.pack(fill=tk.BOTH,expand=True,padx=10,pady=5)
        left_frame=ttk.Frame(main_pane); main_pane.add(left_frame,weight=3); self._create_treeview(left_frame)
        right_frame=ttk.Frame(main_pane); main_pane.add(right_frame,weight=2); self._create_preview_panels(right_frame)
        bottom_button_container=ttk.Frame(self); bottom_button_container.pack(fill=tk.X,expand=False,padx=10,pady=(0,5)); self._create_bottom_buttons(bottom_button_container)
        
        status_frame=ttk.Frame(self,relief=tk.SUNKEN,padding=2); status_frame.pack(side=tk.BOTTOM,fill=tk.X)
        status_frame.columnconfigure(0, weight=1)
        self.status_label=ttk.Label(status_frame,text="準備就緒", width=1, anchor="w"); self.status_label.grid(row=0, column=0, sticky="ew", padx=5)
        self.progress_bar=ttk.Progressbar(status_frame,orient='horizontal',mode='determinate', length=250); self.progress_bar.grid(row=0, column=1, sticky="e", padx=5)
        
    def _create_treeview(self, parent_frame: ttk.Frame):
        columns=("filename","path","count","size","ctime","similarity"); self.tree=ttk.Treeview(parent_frame,columns=columns,show="tree headings",selectmode="extended")
        self.tree.heading("#0", text="勾選", anchor='center'); self.tree.column("#0", width=60, stretch=False, anchor='w')
        headings={"filename":"羣組/圖片","path":"路徑","count":"數量","size":"大小","ctime":"建立日期","similarity":"相似度/類型"}
        for col, text in headings.items():
            opts = {'text': text}
            if col in ['count', 'size', 'ctime', 'similarity', 'filename']: opts['command'] = lambda c=col: self._on_column_header_click(c)
            self.tree.heading(col, **opts)
        widths={"filename":250,"path":300,"count":50,"size":100,"ctime":110,"similarity":120}
        for col,width in widths.items(): 
            self.tree.column(col,width=width,minwidth=width,stretch=False)
            
        def _initial_resize(event):
            if not getattr(self.tree, '_initial_stretched', False):
                other_cols = ["#0", "path", "count", "size", "ctime", "similarity"]
                used_w = sum(self.tree.column(c, "width") for c in other_cols)
                remain = max(250, event.width - used_w - 5)
                self.tree.column("filename", width=remain)
                self.tree._initial_stretched = True
        self.tree.bind("<Configure>", _initial_resize)
        
        self.tree.tag_configure('child_item', foreground='#555555')
        self.tree.tag_configure('parent_item', font=self.bold_font)
        self.tree.tag_configure('parent_partial_selection', foreground='#00008B')
        self.tree.tag_configure('qr_item', background='#E0FFFF')
        self.tree.tag_configure('ad_like_group', background='#E6F4FF', foreground='#0B5394')
        self.tree.tag_configure('protected_item', background='#FFFACD')
        self.tree.tag_configure('uncensored_item', background='#C8E6C9', foreground='#2E7D32')
        self.tree.tag_configure('empty_folder_group', background='#F5F5F5', foreground='#666666') # 新增：空資料夾樣式

        # 這裡原本有的外掛樣式套用已移動到 _on_plugins_loaded()
        
        vscroll=ttk.Scrollbar(parent_frame,orient="vertical",command=self.tree.yview)
        hscroll=ttk.Scrollbar(parent_frame,orient="horizontal",command=self.tree.xview)
        self.tree.configure(yscrollcommand=vscroll.set, xscrollcommand=hscroll.set)
        
        self.tree.grid(row=0, column=0, sticky='nsew')
        vscroll.grid(row=0, column=1, sticky='ns')
        hscroll.grid(row=1, column=0, sticky='ew')
        parent_frame.grid_rowconfigure(0, weight=1)
        parent_frame.grid_columnconfigure(0, weight=1)
        
        # Tooltip setup is deferred until the first real UI paint is complete.

    def _setup_tree_tooltip(self):
        class TreeviewTooltip:
            def __init__(self, tree):
                self.tree = tree
                self.tooltip_window = None
                self.id = None
                self.text = ""
                self.last_item = ""
                self.last_col = ""
                self.tree.bind("<Motion>", self.on_motion)
                self.tree.bind("<Leave>", self.leave)

            def on_motion(self, event):
                item = self.tree.identify_row(event.y)
                col = self.tree.identify_column(event.x)
                if item and col:
                    if item == self.last_item and col == self.last_col:
                        return
                    self.last_item = item
                    self.last_col = col
                    self.unschedule()
                    self.hidetip()
                    
                    text = ""
                    if col == '#0':
                        text = self.tree.item(item, "text")
                    else:
                        try:
                            # 取得對應欄位文字，col 像是 '#1', '#2'
                            col_idx = int(col.replace('#', '')) - 1
                            values = self.tree.item(item, "values")
                            text = str(values[col_idx]) if col_idx < len(values) else ""
                        except Exception:
                            pass
                    
                    # 取出真實路徑作為標題的輔助顯示 (假如目前是在看長檔名/路徑)
                    if text and text.strip():
                        self.text = text
                        self.id = self.tree.after(500, self.showtip, event.x_root, event.y_root)
                else:
                    self.leave()

            def leave(self, event=None):
                self.unschedule()
                self.hidetip()
                self.last_item = ""
                self.last_col = ""

            def unschedule(self):
                if self.id:
                    self.tree.after_cancel(self.id)
                    self.id = None

            def showtip(self, x, y):
                if self.tooltip_window: return
                self.tooltip_window = tw = tk.Toplevel(self.tree)
                tw.wm_overrideredirect(True)
                tw.wm_geometry(f"+{x+15}+{y+10}")
                label = tk.Label(tw, text=self.text, justify='left', background="#ffffe0", relief='solid', borderwidth=1, font=("tahoma", "9", "normal"), wraplength=800)
                label.pack(ipadx=2, ipady=2)

            def hidetip(self):
                tw = self.tooltip_window
                self.tooltip_window = None
                if tw: tw.destroy()

        self.tree_tooltip = TreeviewTooltip(self.tree)
        
    def _create_preview_panels(self, parent_frame: ttk.Frame):
        right_pane=ttk.Panedwindow(parent_frame,orient=tk.VERTICAL); right_pane.pack(fill=tk.BOTH,expand=True)
        try: label_font = font.nametofont(self.winfo_children()[0].cget("font")); line_height = label_font.metrics("linespace")
        except tk.TclError: line_height = 16
        path_frame_height = line_height * 2 + 6
        self.target_image_frame=ttk.LabelFrame(right_pane,text="選中圖片預覽",padding="5"); right_pane.add(self.target_image_frame,weight=1)
        self.target_image_label=ttk.Label(self.target_image_frame,cursor="hand2"); self.target_image_label.pack(fill=tk.BOTH,expand=True)
        target_path_container = tk.Frame(self.target_image_frame, height=path_frame_height); target_path_container.pack(fill=tk.X, expand=False, pady=(5,0)); target_path_container.pack_propagate(False)
        self.target_path_label=ttk.Label(target_path_container,text="",wraplength=500, anchor="w", justify=tk.LEFT); self.target_path_label.pack(fill=tk.BOTH, expand=True)
        self.target_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(True))
        self.compare_image_frame=ttk.LabelFrame(right_pane,text="基準圖片預覽",padding="5"); right_pane.add(self.compare_image_frame,weight=1)
        
        # --- v-MOD: 顯示差異遮罩核取方塊 ---
        self.show_diff_var = tk.BooleanVar(value=False)
        self.show_diff_check = ttk.Checkbutton(self.compare_image_frame, text="🔍 顯示差異遮罩", variable=self.show_diff_var, command=self._update_all_previews)
        self.show_diff_check.pack(side=tk.TOP, anchor='e')
        
        self.compare_image_label=ttk.Label(self.compare_image_frame,cursor="hand2"); self.compare_image_label.pack(fill=tk.BOTH,expand=True)
        compare_path_container = tk.Frame(self.compare_image_frame, height=path_frame_height); compare_path_container.pack(fill=tk.X, expand=False, pady=(5,0)); compare_path_container.pack_propagate(False)
        self.compare_path_label=ttk.Label(compare_path_container,text="",wraplength=500, anchor="w", justify=tk.LEFT); self.compare_path_label.pack(fill=tk.BOTH, expand=True)
        self.compare_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(False))
        self.target_image_label.bind("<Configure>", self._on_preview_resize); self.compare_image_label.bind("<Configure>", self._on_preview_resize)
        # Context menu setup is deferred until the first real UI paint is complete.
        
    def _create_bottom_buttons(self, parent_frame: ttk.Frame):
        button_frame = ttk.Frame(parent_frame); button_frame.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="選取建議", command=self._select_suggested_smart).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="刪除選中(回收桶)", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=2)
        self.move_to_ad_library_button = ttk.Button(button_frame, text="複製進廣告庫", command=self._copy_selected_to_ad_library)
        self.move_to_ad_library_button.pack(side=tk.LEFT, padx=2); self.move_to_ad_library_button.pack_forget()
        actions_frame = ttk.Frame(parent_frame); actions_frame.pack(side=tk.RIGHT, padx=5, pady=5)
        ttk.Button(actions_frame, text="開啟選中資料夾", command=self._open_selected_folder_single).pack(side=tk.LEFT, padx=2)
        
    def _bind_keys(self):
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select); self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<Double-1>", self._on_treeview_double_click); self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection_with_space); self.tree.bind("<Return>", self._handle_return_key)
        self.tree.bind("<Delete>", lambda e: self._delete_selected_from_disk()); self.tree.bind("<BackSpace>", lambda e: self._delete_selected_from_disk())

    def open_settings(self):
        self.settings_button.config(state=tk.DISABLED)
        try:
            from .settings_window import SettingsGUI
            settings_window = SettingsGUI(self)
            self.wait_window(settings_window)
        finally:
            self.settings_button.config(state=tk.NORMAL)
            try: self.lift(); self.focus_force()
            except tk.TclError: pass

    def start_scan(self):
        if self.scan_thread and self.scan_thread.is_alive():
            messagebox.showwarning("正在執行", "掃描任務正在執行中。")
            return
        if not os.path.isdir(self.config['root_scan_folder']):
            messagebox.showerror("路徑錯誤", "請先在'設定'中指定一個有效的根掃描資料夾。")
            return
        mode = self.config.get('comparison_mode')
        if mode == 'qr_detection' and not utils.QR_SCAN_ENABLED:
            messagebox.showwarning("QR 模式不可用", "此環境缺少 OpenCV / numpy，無法進行 QR 檢測。"); return
        
        if not self.is_paused: 
            utils.append_runtime_log_session_header()
            self._reset_scan_state()
            self.tree.delete(*self.tree.get_children())
            # [SDK dc: 預飛檢查] 若使用 SDK + 建立日期模式，先確認索引
            if (self.config.get('enable_everything_mft_scan') and
                    self.config.get('folder_time_mode') == 'ctime' and
                    self.config.get('everything_dc_choice') != 'always_dm'):
                self._preflight_everything_dc_check()
            elif self.config.get('everything_dc_choice') == 'always_dm':
                self.config['_everything_force_dm'] = True
            
        self.processor_instance = self._get_processor_instance(mode)
        if not self.processor_instance:
            messagebox.showerror("錯誤", f"無法初始化模式: {mode}"); self._reset_control_buttons("初始化失敗"); return
            
        self.start_button.config(state=tk.DISABLED); self.settings_button.config(state=tk.DISABLED)
        self.pause_button.config(text="暫停", state=tk.NORMAL); self.cancel_button.config(state=tk.NORMAL)
        
        if not self.is_paused:
            self.scan_start_time = time.perf_counter()
            
        self.is_paused = False
        
        # 將前置處理移入背景執行緒
        self.scan_thread = threading.Thread(target=self._run_scan_pipeline_in_thread, daemon=True)
        self.scan_thread.start()

    def _run_scan_pipeline_in_thread(self):
        try:
            self._run_preprocessors_before_scan()
            if self.cancel_event.is_set():
                self.scan_queue.put({'type': 'finish', 'text': "任務已取消"})
                return
            self._run_scan_in_thread()
        except Exception as e:
            log_error(f"掃描流程執行失敗: {e}", include_traceback=True)
            self.scan_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})

    def _get_enabled_preprocessor_plugins(self):
        enabled_plugins = []
        for plugin_id, plugin in self.plugin_manager.items():
            if plugin.get_plugin_type() != 'preprocessor':
                continue
            if not self.config.get(f'enable_{plugin_id}', False):
                continue
            enabled_plugins.append((plugin_id, plugin))
        return enabled_plugins

    def _get_eh_hint_cache_signature(self, root: str):
        try:
            entries = []
            with os.scandir(root) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    stat = entry.stat(follow_symlinks=False)
                    entries.append((entry.name, stat.st_mtime_ns))
            entries.sort(key=lambda x: x[0].casefold())
            digest = hashlib.sha1()
            for name, mtime_ns in entries:
                digest.update(name.casefold().encode("utf-8", errors="surrogatepass"))
                digest.update(b"\0")
                digest.update(str(mtime_ns).encode("ascii"))
                digest.update(b"\0")
            return {
                "count": len(entries),
                "digest": digest.hexdigest(),
            }
        except Exception as e:
            log_warning(f"[EH hint cache] Failed to build direct-child signature; bypass cache. {e}")
            return None

    def _get_eh_hint_cache_path(self) -> str:
        return os.path.join(CACHE_DIR, "eh_folder_hints_cache.json")

    def _load_eh_folder_hints_cache(self, root: str, signature):
        if not signature:
            return None
        cache_path = self._get_eh_hint_cache_path()
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if payload.get("root") != os.path.normcase(os.path.normpath(root)):
                return None
            if payload.get("signature") != signature:
                return None
            hints = payload.get("non_empty_folders")
            if not isinstance(hints, list):
                return None
            log_info(f"[EH hint cache] Hit: reused {len(hints)} non-empty direct folders.")
            return {os.path.normpath(p) for p in hints}
        except FileNotFoundError:
            return None
        except Exception as e:
            log_warning(f"[EH hint cache] Failed to read cache; rebuilding. {e}")
            return None

    def _save_eh_folder_hints_cache(self, root: str, signature, non_empty: set):
        if not signature:
            return
        cache_path = self._get_eh_hint_cache_path()
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            payload = {
                "root": os.path.normcase(os.path.normpath(root)),
                "signature": signature,
                "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
                "non_empty_folders": sorted(os.path.normpath(p) for p in non_empty),
            }
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_warning(f"[EH hint cache] Failed to save cache. {e}")

    def _build_eh_folder_hints(self) -> set:
        """用 Everything SDK 查出 root_scan_folder 下所有「有檔案的直接子資料夾」。
        若 SDK 不可用或查詢失敗，回傳 None（外掛將 fallback 到 os.scandir）。
        """
        root = self.config.get('root_scan_folder', '')
        if not root or not self.config.get('enable_everything_mft_scan', False):
            return None
        signature = self._get_eh_hint_cache_signature(root)
        cached_hints = self._load_eh_folder_hints_cache(root, signature)
        if cached_hints is not None:
            return cached_hints
        try:
            from processors.everything_ipc import EverythingIPCManager
            ev = EverythingIPCManager()
            if not ev.is_everything_running():
                log_info("[EH 外掛] Everything 服務未運行，將 fallback 到 os.scandir。")
                return None
            # 查詢根目錄下的所有媒體/壓縮檔（不帶時間限制，只要知道哪個資料夾有內容即可）
            media_exts = [
                '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp',
                '.zip', '.cbz', '.rar', '.cbr', '.7z', '.cb7',
            ]
            all_files = ev.search(
                root_path=root,
                extensions=media_exts,
                excluded_paths=[],
                excluded_names=[],
            )
            if not all_files and all_files is not None:
                # 查詢成功但 0 筆結果，有可能根目錄本身沒有媒體檔（EH 資料夾全空）
                log_info("[EH 外掛] SDK hint：查詢結果為 0，視為全部空資料夾情境。")
                return set()  # 回傳空 set（非 None），外掛會正確走 SDK 快速路徑
            non_empty = set()
            root_norm = os.path.normcase(os.path.normpath(root))
            for file_path in all_files:
                parent = os.path.dirname(file_path)
                parent_norm = os.path.normcase(os.path.normpath(parent))
                # 只計直接子資料夾（depth == 1）
                if os.path.normcase(os.path.dirname(parent_norm)) == root_norm:
                    non_empty.add(os.path.normpath(parent))
            log_info(f"[EH 外掛] SDK hint 建立完成：{len(non_empty)} 個有內容的直接子資料夾。")
            self._save_eh_folder_hints_cache(root, signature, non_empty)
            return non_empty
        except Exception as e:
            log_warning(f"[EH 外掛] 建立 SDK folder hints 失敗（將 fallback 到 os.scandir）: {e}")
            return None


    def _run_preprocessors_before_scan(self):
        preprocessor_plugins = self._get_enabled_preprocessor_plugins()
        if not preprocessor_plugins:
            return

        # 嘗試用 Everything SDK 預先建立「有內容的資料夾」集合，供 EH 外掛加速用
        folder_hints = self._build_eh_folder_hints()

        control_events = {'cancel': self.cancel_event, 'pause': self.pause_event}
        total = len(preprocessor_plugins)
        for idx, (plugin_id, plugin) in enumerate(preprocessor_plugins, start=1):
            if self.cancel_event.is_set():
                return
            plugin_name = plugin.get_name()
            self.scan_queue.put({
                'type': 'text',
                'text': f"[前置處理 {idx}/{total}] 開始執行: {plugin_name}..."
            })
            try:
                plugin_config = self.config.copy()
                # 若 SDK hint 可用，注入給外掛；不可用時不設定此 key，外掛自行 fallback
                if folder_hints is not None:
                    plugin_config['eh_non_empty_folder_hints'] = folder_hints
                res = plugin.run(
                    plugin_config,
                    self.scan_queue,
                    control_events,
                    app_update_callback=lambda: None
                )
                if res:
                    self.preprocessor_results.append(res)
            except Exception as e:
                log_error(f"前置外掛 '{plugin_id}' 執行失敗: {e}", include_traceback=True)
                self.scan_queue.put({
                    'type': 'text',
                    'text': f"[前置處理] {plugin_name} 執行失敗: {e}"
                })
                raise


    def _get_processor_instance(self, mode: str) -> Optional[Union[BaseProcessor, BasePlugin]]:
        try:
            if mode in self.plugin_manager:
                log_info(f"選擇外掛模式: {mode}, 準備啟動外掛...")
                return self.plugin_manager[mode]
            else:
                log_info(f"選擇內建模式: {mode}, 準備啟動內建處理器...")
                if mode == 'ad_comparison' or mode == 'mutual_comparison':
                    from processors.comparison_processor import ComparisonProcessor
                    return ComparisonProcessor(self.config, self.scan_queue, {'cancel': self.cancel_event, 'pause': self.pause_event})
                elif mode == 'qr_detection':
                    from processors.qr_processor import QrProcessor
                    return QrProcessor(self.config, self.scan_queue, {'cancel': self.cancel_event, 'pause': self.pause_event})
        except Exception as e:
            log_error(f"初始化處理器 '{mode}' 失敗: {e}", include_traceback=True)
        return None

    def _run_scan_in_thread(self):
        try:
            result = None
            if hasattr(self.processor_instance, 'run'):
                if isinstance(self.processor_instance, BasePlugin) or type(self.processor_instance).__name__ == 'AppPlugin':
                    result = self.processor_instance.run(config=self.config.copy(), progress_queue=self.scan_queue, control_events={'cancel': self.cancel_event, 'pause': self.pause_event}, app_update_callback=self.update)
                else:
                    result = self.processor_instance.run()
            
            if result is None:
                if self.cancel_event.is_set(): self.scan_queue.put({'type': 'finish', 'text': "任務已取消"})
                else: self.scan_queue.put({'type': 'status_update', 'text': "任務已暫停"})
                return

            found, data, errors = result
            
            # --- v-MOD: 合併前置處理器的偵測結果 (例如空資料夾) ---
            for summary in self.preprocessor_results:
                if hasattr(summary, 'detected_empty_list') and summary.detected_empty_list:
                    for path in summary.detected_empty_list:
                        # 格式: (group_key, item_path, value_str, tag)
                        found.append(("EMPTY_FOLDERS_ROOT", path, "空資料夾 (EH偵測)", "empty_folder_group"))
                    
                    if "EMPTY_FOLDERS_ROOT" not in data:
                        data["EMPTY_FOLDERS_ROOT"] = {"display_name": "📁 空資料夾 (建議清理)"}
            # --- END v-MOD ---

            self.scan_queue.put({'type': 'result', 'data': found, 'meta': data, 'errors': errors})
            base_text = f"✅ 掃描完成！找到 {len(found)} 個目標。"
            if errors:
                base_text += f" (有 {len(errors)} 個項目處理失敗)"
                log_error(f"⚠️ 以下 {len(errors)} 個項目處理失敗：")
                for err_path, err_msg in (errors.items() if isinstance(errors, dict) else [(str(e), '') for e in errors]):
                    log_error(f"  ❌ {err_path}  →  {err_msg}")
            cache_stats = getattr(self.processor_instance, 'cache_stats', None)
            eh_summary_data = self._extract_eh_summary_data()
            self.scan_queue.put({'type': 'finish', 'text': base_text, 'cache_stats': cache_stats, 'error_count': len(errors or {}), 'eh_summary': eh_summary_data})
        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", True)
            self.scan_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})

    def _reset_scan_state(self):
        self.config.pop('_everything_force_dm', None)  # 清理暫態旗標
        self.preprocessor_results = [] # 重設前置處理結果
        self.final_status_text = ""; self.cancel_event.clear(); self.pause_event.clear(); self.is_paused = False; self.processor_instance = None; self.protected_paths.clear(); self.child_to_parent.clear(); self.parent_to_children.clear(); self.item_to_path.clear(); self.banned_groups.clear()

    def cancel_scan(self):
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askyesno("確認終止", "確定要終止目前的掃描任務嗎？"):
                log_info("使用者請求取消任務。"); self.cancel_event.set()
                if self.is_paused: self.pause_event.set()

    def toggle_pause(self):
        if self.is_paused:
            log_info("使用者請求恢復任務。"); self.pause_event.clear(); self.pause_button.config(text="暫停"); self.status_label.config(text="正在恢復任務..."); self.start_scan()
        else:
            log_info("使用者請求暫停任務。"); self.is_paused = True; self.pause_event.set(); self.pause_button.config(text="恢復"); self.status_label.config(text="正在請求暫停...")

    def _reset_control_buttons(self, final_status_text: str = "任務完成"):
        self.status_label.config(text=final_status_text); self.progress_bar['value'] = 0; self.start_button.config(state=tk.NORMAL); self.settings_button.config(state=tk.NORMAL); self.pause_button.config(state=tk.DISABLED, text="暫停"); self.cancel_button.config(state=tk.DISABLED)

    def _build_qr_finish_summary(self) -> str:
        if self.config.get('comparison_mode') != 'qr_detection' or not self.all_found_items:
            return ""
        groups = defaultdict(list)
        for gk, ip, vs, *rest in self.all_found_items:
            tag = rest[0] if rest else ""
            groups[gk].append((ip, vs, tag))
        total_groups = len(groups)
        ad_like_groups = sum(
            1 for items in groups.values()
            if any(str(tag) == 'ad_like_group' for _, _, tag in items)
        )
        new_qr_groups = max(0, total_groups - ad_like_groups)
        return f" [QR 摘要：總 QR {total_groups} 張，似廣告 {ad_like_groups} 張，新 QR {new_qr_groups} 張]"

    def _build_eh_status_bar_summary(self, eh_summary: dict) -> str:
        """
        [EH-FEAT-05] 產生非阻塞狀態列摘要短文字。
        若數值皆為 0 則不顯示，避免噪音。
        """
        if not eh_summary:
            return ""
        
        # 提取數值
        added = eh_summary.get('added', 0)
        deleted = eh_summary.get('deleted', 0)
        restored = eh_summary.get('restored', 0)
        empty = eh_summary.get('empty', 0)
        
        # 判定是否全為 0
        if not any([added, deleted, restored, empty]):
            return ""
            
        return f" [EH: +{added}/-{deleted}/R{restored}/E{empty}]"

    def _extract_eh_summary_data(self) -> Optional[dict]:
        """
        從 preprocessor_results 中提取 EH 執行摘要數據。
        """
        for res in self.preprocessor_results:
            if hasattr(res, 'added'):
                return {
                    'added': getattr(res, 'added', 0),
                    'deleted': getattr(res, 'soft_deleted', 0),
                    'restored': getattr(res, 'restored', 0),
                    'empty': len(getattr(res, 'detected_empty_list', []))
                }
        return None

    def _check_queues(self):
        if self.is_closing or not self.winfo_exists(): return
        try:
            while True:
                msg = self.scan_queue.get_nowait()
                msg_type = msg.get('type')
                if msg_type == 'progress' and not self.is_paused: self.progress_bar['value'] = msg.get('value', 0); self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'text' and not self.is_paused: self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'status_update': self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'result':
                    self.all_found_items, self.all_file_data, failed_tasks = msg.get('data', []), msg.get('meta', {}), msg.get('errors', [])
                    self._process_scan_results(failed_tasks)
                elif msg_type == 'finish':
                    total_dur = ""
                    if getattr(self, 'scan_start_time', None):
                        dur = time.perf_counter() - self.scan_start_time
                        total_dur = f" (總耗時 {dur:.2f} 秒)"
                    qr_summary = self._build_qr_finish_summary()
                    eh_summary = self._build_eh_status_bar_summary(msg.get('eh_summary'))
                    
                    cache_summary = ""
                    cache_stats = msg.get('cache_stats')
                    if cache_stats:
                        cache_summary = f" [快取: 命中 {cache_stats.get('hit', 0)} 筆, 重算 {cache_stats.get('recalc', 0)} 筆, 淘汰 {cache_stats.get('purge', 0)} 筆, 重掃 {cache_stats.get('rescan_folders', 0)} 目錄]"
                    
                    if getattr(self, 'scan_start_time', None):
                        log_info(f"任務總結: {msg.get('text', '任務完成')}{qr_summary}{eh_summary}{cache_summary}{total_dur}")
                    
                    self.final_status_text = f"{msg.get('text', '任務完成')}{qr_summary}{eh_summary}{cache_summary}{total_dur}"
                    self._reset_control_buttons(self.final_status_text)
        except Empty: pass
        try:
            while True:
                msg = self.preview_queue.get_nowait()
                msg_type = msg.get('type')
                if msg_type == 'image_loaded':
                    if msg['is_target']: self.pil_img_target = msg['image']; self._last_target_path = msg.get('path'); self._last_target_src_size = msg.get('src_size')
                    else: self.pil_img_compare = msg['image']; self._last_compare_path = msg.get('path'); self._last_compare_src_size = msg.get('src_size')
                    self._update_all_previews()
                elif msg_type == 'diff_loaded':
                    # 防止舊的進程結果蓋掉新圖
                    if msg.get('compare_path') == self._last_requested_compare_path:
                        self._resize_and_display(self.compare_image_label, msg['image'], False)
        except Empty: pass
        finally:
            if not self.is_closing: self.after(100, self._check_queues)

    def _build_structured_recap(self, mode, status_text, duration_sec, cache_stats=None, results=None, error_count=None):
        """
        [L3-LOG-D] 根據任務狀態產生結構化的摘要區塊 (格式產生器)。
        """
        status = "completed"
        if "取消" in status_text: status = "cancelled"
        elif "錯誤" in status_text: status = "error"
        elif "暫停" in status_text: status = "paused"
        
        lines = ["# RECAP BEGIN", f"mode: {mode}", f"status: {status}", f"duration_sec: {duration_sec:.2f}"]
        if results is not None: lines.append(f"results: groups={results[0]}, items={results[1]}")
        if cache_stats:
            cs = cache_stats
            lines.append(f"cache: hit={cs.get('hit', 0)}, recalc={cs.get('recalc', 0)}, purge={cs.get('purge', 0)}, rescan_folders={cs.get('rescan_folders', 0)}")
        lines.append("warnings: unknown")
        if error_count is not None:
            lines.append(f"errors: {error_count}")
        elif status == "error":
            lines.append("errors: unknown (see log for details)")
        else:
            lines.append("errors: unknown")
        lines.append("# RECAP END")
        return "\n".join(lines)

    def _append_runtime_recap(self, text):
        """
        [L3-LOG-D] 將摘要區塊寫入 runtime log (寫入器)。
        註：app.py 啟動時已安裝 tee，因此 print() 會同步寫入 LOG*.txt。
        為了保持摘要區塊標記乾淨，不使用 log_info()。
        """
        # 先輸出一個換行確保與上方日誌分隔
        print("\n" + text, flush=True)

    def _process_scan_results(self, failed_tasks: list):
        self.protected_paths.clear()
        ad_folder = self.config.get('ad_folder_path')
        if ad_folder and os.path.isdir(ad_folder):
            norm_ad_folder = os.path.normpath(ad_folder).lower()
            all_paths_in_results = {p for item in self.all_found_items for p in item[:2] if p}
            for path in all_paths_in_results:
                real_path = path
                if _is_virtual_path(path):
                    archive_path, _ = _parse_virtual_path(path)
                    if archive_path: real_path = archive_path
                try:
                    if os.path.normpath(real_path).lower().startswith(norm_ad_folder): self.protected_paths.add(path)
                except (TypeError, AttributeError): continue
        mode = self.config.get('comparison_mode')
        show_copy_button = (mode == 'qr_detection') or (mode == 'mutual_comparison' and ad_folder and os.path.isdir(ad_folder))
        if show_copy_button: self.move_to_ad_library_button.pack(side=tk.LEFT, padx=2)
        else: self.move_to_ad_library_button.pack_forget()
        groups = defaultdict(list)
        for gk, ip, vs, *rest in self.all_found_items: 
            tag = rest[0] if rest else ""
            groups[gk].append((ip, vs, tag))
        self.sorted_groups = list(groups.items())
        self._sort_and_redisplay_results()
        if self.tree.get_children(): fc = self.tree.get_children()[0]; self.tree.selection_set(fc); self.tree.focus(fc)

    def _on_column_header_click(self, column_name: str):
        if self.sort_by_column == column_name: self.sort_direction_is_ascending = not self.sort_direction_is_ascending
        else: self.sort_by_column = column_name; self.sort_direction_is_ascending = False
        self.status_label.config(text=f"🔄 正在依「{self.tree.heading(column_name, 'text')}」欄位重新排序...")
        self.after(50, self._sort_and_redisplay_results)
        
    def _sort_and_redisplay_results(self):
        if self.sort_by_column == 'filename': sort_key_func = lambda item: os.path.basename(item[0])
        elif self.sort_by_column == 'count': sort_key_func = lambda item: len(item[1]) + 1
        elif self.sort_by_column == 'size': sort_key_func = lambda item: self.all_file_data.get(item[0], {}).get('size', 0) or 0
        elif self.sort_by_column == 'ctime': sort_key_func = lambda item: self.all_file_data.get(item[0], {}).get('ctime', 0) or 0
        elif self.sort_by_column == 'similarity':
            def get_max_sim(item):
                sims = [float(s.replace('%','').split(' ')[0]) for _,s,_ in item[1] if '%' in str(s)]
                if sims: return max(sims)
                weight = 0
                for _, s, _ in item[1]:
                    s_str = str(s)
                    if "跨名" in s_str: weight = max(weight, 3)
                    elif "重複" in s_str: weight = max(weight, 2)
                    elif "無修正" in s_str: weight = max(weight, 1)
                return weight
            sort_key_func = get_max_sim
        else: sort_key_func = lambda item: len(item[1]) + 1
        self.sorted_groups.sort(key=sort_key_func, reverse=not self.sort_direction_is_ascending)
        if self.config.get('comparison_mode') != 'qr_detection':
            self.sorted_groups.sort(key=lambda item: item[1] and any("(似廣告)" in str(s[1]) for s in item[1]), reverse=True)
        self.tree.delete(*self.tree.get_children())
        self.child_to_parent.clear(); self.parent_to_children.clear(); self.item_to_path.clear()
        self._populate_treeview_logic(self.sorted_groups)
        self.status_label.config(text=self.final_status_text or "排序完成。")
        
    def _populate_treeview_logic(self, groups_to_load: list):
        uid = 0
        mode = self.config.get('comparison_mode')
        uncensored_keywords = ["無修正", "decensored", "uncensored", "步兵", "流出"]
        def check_uncensored(name, value_str):
            if "(無修正)" in str(value_str): return True
            name_lower = name.lower()
            return any(k in name_lower for k in uncensored_keywords)

        if mode == 'qr_detection':
            for gk, items in groups_to_load:
                pid = f"group_{uid}"; uid += 1
                is_ad_like = items and any("ad_like_group" == str(s[2]) for s in items)
                # 判斷是否為「多副本分組」（組長 ≠ 子節點）
                # QR 的似廣告結果仍需保留廣告基準圖作為子節點，與廣告比對體驗一致
                has_children = items and any(path != gk for path, _, _ in items)
                
                is_prot = gk in self.protected_paths
                ptags = ['parent_item', 'qr_item']
                if is_ad_like: ptags.append('ad_like_group')
                if is_prot: ptags.append('protected_item')
                if items and items[0][2]: ptags.append(items[0][2])
                
                gk_data = self.all_file_data.get(gk, {})
                gk_size = f"{gk_data.get('size', 0):,}" if 'size' in gk_data and gk_data.get('size') is not None else "N/A"
                gk_ctime = datetime.datetime.fromtimestamp(gk_data['ctime']).strftime('%Y/%m/%d %H:%M') if gk_data.get('ctime') else "N/A"
                display_path, base_name = gk, os.path.basename(gk)
                if _is_virtual_path(gk):
                    archive_path, inner_path = _parse_virtual_path(gk)
                    if archive_path: display_path = f"{os.path.basename(archive_path)}{VPATH_SEPARATOR}{inner_path}"; base_name = inner_path
                
                if has_children:
                    img_icon = self.icon_locked if is_prot else self.icon_unchecked
                    self.tree.insert("", "end", iid=pid, open=True, image=img_icon, values=(base_name, "", len(items), "", "", items[0][1]), tags=tuple(ptags))
                    self.item_to_path[pid] = gk
                    
                    all_members = [(gk, items[0][1], items[0][2])] + [(p, v, t) for p, v, t in items if p != gk]
                    for path, val_str, tag in all_members:
                        cid = f"item_{uid}"; uid += 1
                        ctags = ['child_item', 'qr_item']
                        if tag: ctags.append(tag)
                        if is_ad_like: ctags.append('ad_like_group')
                        if path in self.protected_paths: ctags.append('protected_item')
                        c_data = self.all_file_data.get(path, {})
                        c_size = f"{c_data.get('size', 0):,}" if 'size' in c_data and c_data.get('size') is not None else "N/A"
                        c_ctime = datetime.datetime.fromtimestamp(c_data['ctime']).strftime('%Y/%m/%d %H:%M') if c_data.get('ctime') else "N/A"
                        is_sel = path in self.selected_files
                        img_icon = self.icon_locked if path in self.protected_paths else (self.icon_checked if is_sel else self.icon_unchecked)
                        c_display_path, c_base_name = path, os.path.basename(path)
                        if _is_virtual_path(path):
                            a_path, i_path = _parse_virtual_path(path)
                            if a_path: c_display_path = f"{os.path.basename(a_path)}{VPATH_SEPARATOR}{i_path}"; c_base_name = i_path
                        self.tree.insert(pid, "end", iid=cid, image=img_icon, values=(f"  └─ {c_base_name}", c_display_path, "", c_size, c_ctime, val_str), tags=tuple(ctags))
                        self.child_to_parent[cid] = pid; self.parent_to_children[pid].append(cid); self.item_to_path[cid] = path
                    self._update_group_checkbox(pid)
                else:
                    # 單張模式：維持原本扁平顯示（一個父節點，可直接選取）
                    is_sel = gk in self.selected_files
                    img_icon = self.icon_locked if is_prot else (self.icon_checked if is_sel else self.icon_unchecked)
                    self.tree.insert("", "end", iid=pid, open=True, image=img_icon, values=(base_name, display_path, 1, gk_size, gk_ctime, items[0][1] if items else ""), tags=tuple(ptags))
                    self.item_to_path[pid] = gk
        else:
            for gk, items in groups_to_load:
                pid = f"group_{uid}"; uid += 1
                is_ad_like = items and any("(似廣告)" in str(s[1]) for s in items)
                is_prot = gk in self.protected_paths
                
                ptags = ['parent_item']
                if is_ad_like: ptags.append('ad_like_group')
                if is_prot: ptags.append('protected_item')
                if check_uncensored(os.path.basename(gk), ""): ptags.append('uncensored_item')
                
                # display_name：若插件提供骨幹名稱則用骨幹，否則用資料夾名
                _gk_display = self.all_file_data.get(gk, {}).get('display_name') or os.path.basename(gk)
                # 若是插件提供的虛擬系列分組（有 display_name），
                # group_key 只是路由鍵，不是「基準圖」，跳過自動加入「基準 (自身)」行；
                # 其他模式（廣告比對、互相比對）不設定 display_name，照常加入。
                if self.all_file_data.get(gk, {}).get('display_name'):
                    disp_list = sorted(items, key=lambda x: x[0])
                else:
                    disp_list = [(gk, "基準 (自身)", "")] + sorted(items, key=lambda x: x[0])
                self.tree.insert("", "end", iid=pid, open=True, values=(_gk_display, "", len(disp_list), "", "", ""), tags=tuple(ptags))
                self.item_to_path[pid] = gk
                
                for path, val_str, tag in disp_list:
                    cid = f"item_{uid}"; uid += 1
                    ctags = ['child_item']
                    if is_ad_like: ctags.append('ad_like_group')
                    if path in self.protected_paths: ctags.append('protected_item')
                    if check_uncensored(os.path.basename(path), val_str): ctags.append('uncensored_item')
                    if tag: ctags.append(tag)  # 外掛自訂標籤最後套用，優先權最高
                        
                    c_data = self.all_file_data.get(path, {})
                    
                    c_count_str = ""
                    if 'page_count' in c_data:
                        c_count_str = f"{c_data['page_count']}P"
                    
                    c_size = f"{c_data.get('size', 0):,}" if 'size' in c_data and c_data.get('size') is not None else "N/A"
                    c_ctime = datetime.datetime.fromtimestamp(c_data['ctime']).strftime('%Y/%m/%d %H:%M') if c_data.get('ctime') else "N/A"
                    is_sel = path in self.selected_files
                    img_icon = self.icon_locked if path in self.protected_paths else (self.icon_checked if is_sel else self.icon_unchecked)
                    display_path, base_name = path, os.path.basename(path)
                    if _is_virtual_path(path):
                        archive_path, inner_path = _parse_virtual_path(path)
                        if archive_path: display_path = f"{os.path.basename(archive_path)}{VPATH_SEPARATOR}{inner_path}"; base_name = inner_path
                    
                    self.tree.insert(pid, "end", iid=cid, image=img_icon, values=(f"  └─ {base_name}", display_path, c_count_str, c_size, c_ctime, val_str), tags=tuple(ctags))
                    
                    self.child_to_parent[cid] = pid; self.parent_to_children[pid].append(cid); self.item_to_path[cid] = path
                self._update_group_checkbox(pid)

    def _select_suggested_smart(self):
        mode = self.config.get('comparison_mode')
        strategy = None
        if mode in self.plugin_manager:
            plugin = self.plugin_manager[mode]
            if hasattr(plugin, 'get_selection_strategy'): strategy = plugin.get_selection_strategy(self.config)
        if not strategy: 
            import core.selection_strategies as selection_strategies
            strategy = selection_strategies.get_strategy(mode, self.config)
        paths_to_select = set()
        if strategy:
            input_data = []
            for item in self.all_found_items: input_data.append(item[:3])
            try:
                paths_to_select = strategy.calculate(input_data)
                if paths_to_select: log_info(f"[選取建議] 策略 '{type(strategy).__name__}' 選取了 {len(paths_to_select)} 個項目。")
            except Exception as e:
                log_error(f"[選取建議] 策略執行失敗: {e}"); paths_to_select = self._fallback_selection()
        else: paths_to_select = self._fallback_selection()

        if not paths_to_select: messagebox.showinfo("提示", "根據當前策略，沒有建議選取的項目。", parent=self); return
        safe_paths = {p for p in paths_to_select if p not in self.protected_paths}
        self.selected_files.update(safe_paths)
        self._refresh_all_checkboxes()

    def _fallback_selection(self):
        paths = set()
        for item in self.all_found_items:
            if "100.0%" in str(item[2]): paths.add(item[1])
        return paths
    
    def _select_suggested_for_deletion(self): self._select_suggested_smart()
    def _select_all(self): self.selected_files.update(self._get_all_selectable_paths()); self._refresh_all_checkboxes()
    def _deselect_all(self): self.selected_files.clear(); self._refresh_all_checkboxes()
    def _invert_selection(self): self.selected_files.symmetric_difference_update(self._get_all_selectable_paths()); self._refresh_all_checkboxes()

    # --- 以下為補回的遺失互動邏輯 ---

    def _on_treeview_click(self, event: tk.Event):
        item_id = self.tree.identify_row(event.y)
        if not item_id or not self.tree.exists(item_id): return
        col = self.tree.identify_column(event.x)
        # 支持點擊 #0 (圖示區) 或是 #1 (檔案名區) 皆可切換勾選
        if col in ("#0", "#1"):
            tags = self.tree.item(item_id, "tags")
            if 'parent_item' in tags:
                if self.tree.get_children(item_id): self._toggle_group_selection(item_id)
                else: self._toggle_selection_by_item_id(item_id)
            elif 'child_item' in tags: self._toggle_selection_by_item_id(item_id)
                
    def _on_treeview_double_click(self, event: tk.Event):
        if self.tree.identify_region(event.x, event.y) == "cell":
            item_id = self.tree.identify_row(event.y)
            if not item_id: return
            if 'parent_item' in self.tree.item(item_id, "tags"):
                is_container_node = self.config.get('comparison_mode') in ['mutual_comparison', 'ad_comparison']
                if is_container_node: self.tree.item(item_id, open=not self.tree.item(item_id, "open"))
            if self.tree.identify_column(event.x) == "#3":
                path_value = self.item_to_path.get(item_id)
                if path_value:
                    # 雙擊第 3 欄開啟資料夾：
                    # - 真實資料夾 → /select 高亮選取（方便在大量資料夾中定位）
                    # - 虛擬路徑（壓縮檔內圖片）→ /select 選取 zip 本身
                    if os.path.isdir(path_value):
                        _reveal_in_explorer(path_value)
                    else:
                        real_path = path_value
                        if _is_virtual_path(real_path):
                            archive_path, _ = _parse_virtual_path(real_path)
                            if archive_path: real_path = archive_path
                        if os.path.exists(real_path):
                            _reveal_in_explorer(real_path)
                        else:
                            log_info(f"無法開啟路徑，因為它不是一個有效的路徑: {real_path}")

                    
    def _handle_return_key(self, event: tk.Event) -> str:
        sel = self.tree.selection()
        if sel and 'parent_item' in self.tree.item(sel[0], "tags"):
             if self.config.get('comparison_mode') != 'qr_detection':
                self.tree.item(sel[0], open=not self.tree.item(sel[0], "open"))
        return "break"
        
    def _on_item_select(self, event: tk.Event):
        if self._after_id: self.after_cancel(self._after_id)
        self._after_id = self.after(self._preview_delay, self._trigger_async_preview)
        
    def _trigger_async_preview(self):
        if self.is_closing or not self.winfo_exists(): return
        self._after_id = None
        sel = self.tree.selection()
        def get_display_path(item_id):
            path = self.item_to_path.get(item_id)
            if not path: return None
            if path in self.all_file_data and 'display_path' in self.all_file_data[path]: return self.all_file_data[path]['display_path']
            return path
        if not sel or not self.tree.exists(sel[0]):
            self.pil_img_target = self.pil_img_compare = None
            self._last_requested_compare_path = None
            self._update_all_previews()
            self.target_path_label.config(text=""); self.compare_path_label.config(text="")
            return
        item_id = sel[0]; preview_path, compare_path = None, None
        tags = self.tree.item(item_id, "tags")
        if 'parent_item' in tags:
            children = self.tree.get_children(item_id)
            if children: preview_path = get_display_path(children[0])
            else: preview_path = get_display_path(item_id)
            compare_path = None
        else:
            preview_path = get_display_path(item_id)
            parent_id = self.child_to_parent.get(item_id)
            if parent_id and self.tree.get_children(parent_id): compare_path = get_display_path(self.tree.get_children(parent_id)[0])
            
        old_compare_path = getattr(self, '_last_requested_compare_path', None)
        self.current_target_path = preview_path
        self.current_compare_path = compare_path
        self._last_requested_compare_path = compare_path
        
        if preview_path: 
            self.executor.submit(self._load_image_worker, preview_path, True)
        else: 
            self.pil_img_target = None; self.target_path_label.config(text=""); self._update_all_previews()
            
        if compare_path: 
            if compare_path != old_compare_path or self.pil_img_compare is None:
                self.executor.submit(self._load_image_worker, compare_path, False)
            else:
                self._update_all_previews()
        else: 
            self.pil_img_compare = None; self.compare_path_label.config(text=""); self._update_all_previews()

    def _load_image_worker(self, path: str, is_target: bool):
        img = None
        try:
            img = _open_image_from_any_path(path)
            if img is None: raise IOError("無法從通用接口開啟圖片")
            try: img.load()
            except Exception: pass
            try: img = ImageOps.exif_transpose(img)
            except Exception: pass
            if img.mode != "RGB": img = img.convert("RGB")
            self.preview_queue.put({'type': 'image_loaded', 'image': img.copy(), 'is_target': is_target})
            display_path = path
            if _is_virtual_path(path):
                archive_path, inner_path = _parse_virtual_path(path)
                if archive_path: display_path = f"{os.path.basename(archive_path)}{VPATH_SEPARATOR}{inner_path}"
            label = self.target_path_label if is_target else self.compare_path_label
            label.after(0, lambda dp=display_path, lbl=label: lbl.config(text=f"路徑: {dp}"))
        except Exception as e:
            label = self.target_path_label if is_target else self.compare_path_label
            basename = os.path.basename(path) if isinstance(path, str) else str(path)
            label.after(0, lambda b=basename, lbl=label: lbl.config(text=f"無法載入: {b}"))
            log_error(f"載入圖片預覽失敗 '{path}': {e}", True)
            try: self.preview_queue.put({'type': 'image_loaded', 'image': None, 'is_target': is_target})
            except Exception: pass
        finally:
            try:
                if img is not None: img.close()
            except Exception: pass

    def _update_all_previews(self):
        self._resize_and_display(self.target_image_label, self.pil_img_target, True)
        
        # --- v-MOD: 處理差異遮罩 ---
        if getattr(self, 'show_diff_var', None) and self.show_diff_var.get() and self.pil_img_target and self.pil_img_compare and ImageChops:
            # 先同步掛載未加料的比對圖
            self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)
            # 背景執行差異運算，避免主執行緒卡死
            self.executor.submit(
                self._async_diff_worker, 
                self.pil_img_target.copy(), 
                self.pil_img_compare.copy(), 
                self._last_requested_compare_path
            )
        else:
            self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)

    def _async_diff_worker(self, target_img: Image.Image, compare_img: Image.Image, compare_path: str):
        try:
            img1 = target_img.convert("RGB")
            img2 = compare_img.convert("RGB")
            if img1.size != img2.size:
                img2 = img2.resize(img1.size, Image.Resampling.LANCZOS)
            
            diff = ImageChops.difference(img1, img2)
            red_layer = Image.new("RGBA", img2.size, (255, 0, 0, 180)) # 半透明紅底
            mask = diff.convert("L").point(lambda p: min(255, p * 4))  # 放大差異
            
            base = compare_img.convert("RGBA")
            if base.size != img2.size:
                base = base.resize(img2.size, Image.Resampling.LANCZOS)
            
            img_with_diff = Image.composite(red_layer, base, mask)
            self.preview_queue.put({'type': 'diff_loaded', 'image': img_with_diff, 'compare_path': compare_path})
        except Exception as e:
            log_error(f"非同步產生差異遮罩失敗: {e}")
            self.preview_queue.put({'type': 'diff_loaded', 'image': compare_img, 'compare_path': compare_path})
        
    def _on_preview_resize(self, event: tk.Event):
        if not self.winfo_exists(): return
        try:
            is_target = event.widget.master == self.target_image_frame
            img = self.pil_img_target if is_target else self.pil_img_compare
            self._resize_and_display(event.widget, img, is_target)
        except tk.TclError: pass
        
    def _resize_and_display(self, label: tk.Label, pil_image: Union[Image.Image, None], is_target: bool):
        if self.is_closing or not self.winfo_exists() or ImageTk is None: return
        if not pil_image:
            label.config(image=""); label.image = None
            if is_target: self.img_tk_target = None
            else: self.img_tk_compare = None
            return
        w, h = label.winfo_width(), label.winfo_height()
        if w <= 1 or h <= 1: return
        img_copy = pil_image.copy(); img_copy.thumbnail((w - 10, h - 10), Image.Resampling.LANCZOS)
        path = self._last_target_path if is_target else self._last_compare_path
        src_size = self._last_target_src_size if is_target else self._last_compare_src_size
        if path and src_size and path in self.all_file_data:
            data = self.all_file_data.get(path, {})
            qr_points = data.get('qr_points')
            if qr_points:
                scale = min(img_copy.width / src_size[0], img_copy.height / src_size[1])
                scaled_polys = [[(int(x * scale), int(y * scale)) for x, y in poly] for poly in qr_points]
                self._draw_qr_polygons_on_image(img_copy, scaled_polys)
        img_tk = ImageTk.PhotoImage(img_copy); label.config(image=img_tk); label.image = img_tk
        if is_target: self.img_tk_target = img_tk
        else: self.img_tk_compare = img_tk
        
    def _draw_qr_polygons_on_image(self, img, polys, color=(0, 255, 0), alpha=90, outline_thickness=None):
        if not polys or ImageDraw is None: return
        if outline_thickness is None: outline_thickness = max(1, int(min(img.size) / 200))
        base = img.convert("RGBA"); overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
        draw_overlay = ImageDraw.Draw(overlay)
        fill_rgba = (color[0], color[1], color[2], max(0, min(255, int(alpha))))
        line_rgba = (color[0], color[1], color[2], 255)
        for poly in polys:
            if len(poly) < 2: continue
            try: draw_overlay.polygon(poly, fill=fill_rgba, outline=line_rgba, width=outline_thickness)
            except TypeError: draw_overlay.polygon(poly, fill=fill_rgba); draw_overlay.line(list(poly) + [poly[0]], fill=line_rgba, width=outline_thickness)
        composed = Image.alpha_composite(base, overlay).convert("RGB"); img.paste(composed)

    def _on_preview_image_click(self, is_target_image: bool):
        current_path = self.current_target_path if is_target_image else self.current_compare_path
        if not current_path: return
        
        try:
            if _is_virtual_path(current_path):
                import tempfile
                from utils import archive_handler
                archive_path, inner_path = _parse_virtual_path(current_path)
                if archive_path and inner_path:
                    bytes_data = archive_handler.get_image_bytes(archive_path, inner_path)
                    if bytes_data:
                        temp_dir = tempfile.gettempdir()
                        import time
                        temp_path = os.path.join(temp_dir, f"ctc_preview_{int(time.time()*1000)}_{os.path.basename(inner_path)}")
                        with open(temp_path, 'wb') as f:
                            f.write(bytes_data)
                        os.startfile(temp_path)
                    else:
                        messagebox.showerror("開啟失敗", "無法從壓縮檔讀取圖片資料。")
            else:
                if os.path.exists(current_path):
                    os.startfile(current_path)
        except Exception as e:
            log_error(f"點擊預覽圖片使用預設程式開啟失敗: {e}", True)
            
    def _toggle_selection_by_item_id(self, item_id: str):
        if 'protected_item' in self.tree.item(item_id, "tags"): return
        path = self.item_to_path.get(item_id)
        if not path: return
        if path in self.selected_files: 
            self.selected_files.discard(path)
            self.tree.item(item_id, image=self.icon_unchecked)
        else: 
            self.selected_files.add(path)
            self.tree.item(item_id, image=self.icon_checked)
        parent_id = self.child_to_parent.get(item_id)
        if parent_id: self._update_group_checkbox(parent_id)
            
    def _toggle_group_selection(self, parent_id: str):
        children = self.parent_to_children.get(parent_id, [])
        if not children: return
        selectable = [self.item_to_path.get(cid) for cid in children if 'protected_item' not in self.tree.item(cid, "tags") and self.item_to_path.get(cid)]
        if not selectable: return
        is_fully_selected = sum(1 for p in selectable if p in self.selected_files) == len(selectable)
        if is_fully_selected: self.selected_files.difference_update(selectable)
        else: self.selected_files.update(selectable)
        self._update_group_checkbox(parent_id)
        
    def _update_group_checkbox(self, parent_id: str):
        if not parent_id or not self.tree.exists(parent_id): return
        children = self.parent_to_children.get(parent_id, [])
        selectable = [cid for cid in children if 'protected_item' not in self.tree.item(cid, "tags")]
        if not selectable: self.tree.item(parent_id, image=""); return
        selected_count = sum(1 for cid in selectable if self.item_to_path.get(cid) in self.selected_files)
        for child_id in children:
            path = self.item_to_path.get(child_id)
            if 'protected_item' in self.tree.item(child_id, "tags"): self.tree.item(child_id, image=self.icon_locked)
            else: self.tree.item(child_id, image=self.icon_checked if path in self.selected_files else self.icon_unchecked)
        tags = list(self.tree.item(parent_id, "tags"))
        if 'parent_partial_selection' in tags: tags.remove('parent_partial_selection')
        if selected_count == 0: self.tree.item(parent_id, image=self.icon_unchecked)
        elif selected_count == len(selectable): self.tree.item(parent_id, image=self.icon_checked)
        else: self.tree.item(parent_id, image=self.icon_mixed); tags.append('parent_partial_selection')
        self.tree.item(parent_id, tags=tuple(tags))
        
    def _toggle_selection_with_space(self, event: tk.Event) -> str:
        sel = self.tree.selection()
        if not sel: return "break"
        item_id = sel[0]; tags = self.tree.item(item_id, "tags")
        if 'parent_item' in tags:
            if self.tree.get_children(item_id): self._toggle_group_selection(item_id)
            else: self._toggle_selection_by_item_id(item_id)
        else: self._toggle_selection_by_item_id(item_id)
        return "break"
        
    def _get_all_selectable_paths(self):
        return {p for iid, p in self.item_to_path.items() if self.tree.exists(iid) and 'protected_item' not in self.tree.item(iid, "tags")}
        
    def _refresh_all_checkboxes(self):
        for item_id in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(item_id, "tags"):
                if self.tree.get_children(item_id):
                    self._update_group_checkbox(item_id)
                else:
                    path = self.item_to_path.get(item_id)
                    if 'protected_item' not in self.tree.item(item_id, "tags"): 
                        self.tree.item(item_id, image=self.icon_checked if path in self.selected_files else self.icon_unchecked)
            
    def _get_unique_ad_path(self, ad_dir: str, suggested_name: str) -> str:
        base, ext = os.path.splitext(suggested_name);
        if not ext: ext = ".png"
        cand = f"ad_{base}{ext}"; i = 1
        while os.path.exists(os.path.join(ad_dir, cand)): cand = f"ad_{base}_{i}{ext}"; i += 1
        return os.path.join(ad_dir, cand)
        
    def _copy_selected_to_ad_library(self):
        ad_dir = self.config.get('ad_folder_path') or ""
        if not ad_dir or not os.path.isdir(ad_dir): messagebox.showerror("錯誤", "請先在設定中指定有效的『廣告圖片資料夾』路徑。"); return
        selected_paths = self.selected_files.copy()
        if not selected_paths: messagebox.showinfo("提示", "請先在結果列表勾選要複製的圖片。"); return
        os.makedirs(ad_dir, exist_ok=True)
        copied = 0
        for src in sorted(selected_paths):
            try:
                if _is_virtual_path(src):
                    archive_path, inner_path = _parse_virtual_path(src)
                    base_name = os.path.basename(inner_path)
                    data = utils.archive_handler.get_image_bytes(archive_path, inner_path)
                    if not data: raise IOError("無法從壓縮檔讀取圖片位元組。")
                    dst = self._get_unique_ad_path(ad_dir, base_name)
                    with open(dst, "wb") as f: f.write(data)
                else: 
                    base_name = os.path.basename(src)
                    dst = self._get_unique_ad_path(ad_dir, base_name)
                    shutil.copy2(src, dst)
                copied += 1
            except Exception as e: log_error(f"複製到廣告庫失敗: {src}: {e}", True)
        if copied:
            self.status_label.config(text=f"📦 已複製 {copied} 張到廣告庫")
            messagebox.showinfo("完成", f"已複製 {copied} 張圖片到廣告庫。\n位置：{ad_dir}")
        else: messagebox.showwarning("未複製", "沒有任何圖片被複製。")
    
    def _delete_selected_from_disk(self):

        if not self.selected_files: return
        to_delete = [p for p in self.selected_files if p not in self.protected_paths]
        if not to_delete: messagebox.showinfo("無需操作", "所有選中的項目均受保護。", parent=self); return
        if not messagebox.askyesno("確認標記刪除", f"確定將 {len(to_delete)} 個圖片標記為刪除嗎？\n（標記的檔案會在程式關閉或按下套用時才會真正被送入回收筒）"): return
        
        # 建立標準化路徑集合，提高比對準確度
        real_delete_list = []
        virtual_paths_failed = []
        already_missing_paths = []
        
        for p in to_delete:
            if not _is_virtual_path(p):
                # 如果檔案早就已經消失，不再呼叫 mark_for_deletion 而是算作 missing
                if not os.path.exists(p):
                    already_missing_paths.append(p)
                else:
                    real_delete_list.append(os.path.abspath(p))
            else:
                virtual_paths_failed.append(p)
                log_error(f"無法直接刪除虛擬路徑: {p}。此功能待實現。")

        # --- v-MOD: 使用 Undo Manager 標記檔案 (只處理存在實體的) ---
        successful_paths, failed_paths = self.undo_manager.mark_for_deletion(real_delete_list)
        
        failed_paths.extend(virtual_paths_failed)
        
        # 用於判斷哪些要從 UI Tree 拔除的清單：真正刪除成功 + 早就已經失蹤的
        success_norm = {os.path.normcase(os.path.abspath(p)) for p in successful_paths}
        missing_norm = {os.path.normcase(os.path.abspath(p)) for p in already_missing_paths}
        ui_remove_set = success_norm.union(missing_norm)
        
        deleted_count = len(successful_paths)
        missing_count = len(already_missing_paths)
        failed_count = len(failed_paths)
        
        self._update_undo_button_state()
        
        msg_del = f"已標記 {deleted_count} 個項目。"
        if missing_count > 0: msg_del += f" (有 {missing_count} 個本已遺失)"
        if failed_count > 0: msg_del += f" (⚠️ {failed_count} 個標記失敗保留在清單中)"
        
        self.status_label.config(text=msg_del)
        
        # --- v-MOD: 恢復為原來安全且穩定的過濾重建邏輯 ---
        new_items = []
        deleted_gui_items = []
        
        # 效能優化：預先快取路徑的 normcase 及 abspath 處理，避免在主執行緒上萬次的高耗能呼叫
        path_cache = {}
        def _get_norm_cached(p):
            if p in path_cache: return path_cache[p]
            if _is_virtual_path(p): path_cache[p] = p
            else: path_cache[p] = os.path.normcase(os.path.abspath(p))
            return path_cache[p]

        for item in self.all_found_items:
            p1 = item[0]
            p2 = item[1]
            p1_check = _get_norm_cached(p1)
            p2_check = _get_norm_cached(p2)
            
            # 如果群組中任一個路徑（基準點或目標點）被觸發隱藏，就將該列紀錄納入被刪除列
            if p1_check in ui_remove_set or p2_check in ui_remove_set:
                deleted_gui_items.append(item)
            else:
                new_items.append(item)
        
        self.all_found_items = new_items
        if not hasattr(self, 'deleted_items_history'): self.deleted_items_history = []
        self.deleted_items_history.append(deleted_gui_items)
        
        # 僅清除(真正成功 + 本身已遺失)的勾選項，讓失敗的項目保持勾選
        self.selected_files = {p for p in self.selected_files if not (not _is_virtual_path(p) and os.path.normcase(os.path.abspath(p)) in ui_remove_set)}
        
        # 強制重新整理介面
        self._process_scan_results([])
        
    def _update_undo_button_state(self):
        count = self.undo_manager.get_undo_count()
        pending = self.undo_manager.get_total_pending_count()
        if count > 0:
            self.undo_button.config(state=tk.NORMAL, text=f"復原 ({count})")
            if hasattr(self, 'apply_button'):
                self.apply_button.config(state=tk.NORMAL, text=f"套用刪除 ({pending})")
        else:
            self.undo_button.config(state=tk.DISABLED, text="復原刪除")
            if hasattr(self, 'apply_button'):
                self.apply_button.config(state=tk.DISABLED, text="套用刪除")

    def _undo_last_action(self):
        c = self.undo_manager.get_undo_count()
        if c == 0: return
        
        undo_result = self.undo_manager.undo_last_mark()
        # 處理舊版與新版 tuple 差異防呆
        if len(undo_result) == 3: success, fail, restored_paths = undo_result
        else: success, fail = undo_result; restored_paths = []
        
        self._update_undo_button_state()
                
        # --- v-MOD: 恢復 UI 顯示 (使用原本的結構庫回填) ---
        if hasattr(self, 'deleted_items_history') and self.deleted_items_history:
            restored_gui_items = self.deleted_items_history.pop()
            self.all_found_items.extend(restored_gui_items)
            self._process_scan_results([])
            
        # 重新勾選復原的項目
        for p in restored_paths:
            self.selected_files.add(p)
        self._refresh_all_checkboxes()
            
        msg_undo = f"成功復原 {success} 個項目的刪除標記。"
        self.status_label.config(text=msg_undo)
    def _open_selected_folder_single(self):
        sel = self.tree.selection()
        if not sel: return
        item_id = sel[0]
        path = self.item_to_path.get(item_id)
        if not path: return

        tags = self.tree.item(item_id, "tags")
        if 'parent_item' in tags:
            # 父節點（系列/群組）→ 開啟共通的父目錄（不選取），方便瀏覽整個系列所在位置
            if _is_virtual_path(path):
                archive_path, _ = _parse_virtual_path(path)
                folder_to_open = os.path.dirname(archive_path) if archive_path else None
            else:
                folder_to_open = os.path.dirname(path) if not os.path.isdir(path) else os.path.dirname(path)
            if folder_to_open and os.path.isdir(folder_to_open):
                _open_folder(folder_to_open)
        else:
            # 子節點（單一卷/檔案）→ 高亮選取，解決在三萬筆資料夾中無法定位的問題
            if _is_virtual_path(path):
                archive_path, _ = _parse_virtual_path(path)
                if archive_path: _reveal_in_explorer(archive_path)
            else:
                _reveal_in_explorer(path)
                
    def _open_image_in_default_viewer(self):
        sel = self.tree.selection()
        if not sel: return
        item_id = sel[0]
        
        # 取得代表該項目的「首張圖片」路徑 (如果是資料夾，則為資料夾內提取的一張圖)
        path = self.item_to_path.get(item_id)
        if not path: return
        if path in self.all_file_data and 'display_path' in self.all_file_data[path]:
            path = self.all_file_data[path]['display_path']
        
        tags = self.tree.item(item_id, "tags")
        if 'child_item' in tags:
            try:
                if _is_virtual_path(path):
                    import tempfile
                    from utils import archive_handler
                    archive_path, inner_path = _parse_virtual_path(path)
                    if archive_path and inner_path:
                        bytes_data = archive_handler.get_image_bytes(archive_path, inner_path)
                        if bytes_data:
                            temp_dir = tempfile.gettempdir()
                            # 加入時間戳避免碰撞
                            import time
                            temp_path = os.path.join(temp_dir, f"ctc_preview_{int(time.time()*1000)}_{os.path.basename(inner_path)}")
                            with open(temp_path, 'wb') as f:
                                f.write(bytes_data)
                            os.startfile(temp_path)
                        else:
                            messagebox.showerror("開啟失敗", "無法從壓縮檔讀取圖片資料。")
                else:
                    if os.path.exists(path):
                        os.startfile(path)
            except Exception as e:
                log_error(f"以預設程式開啟失敗: {e}", True)                
    def _collapse_all_groups(self):
        for item_id in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(item_id, "tags"): self.tree.item(item_id, open=False)
            
    def _expand_all_groups(self):
        for item_id in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(item_id, "tags"): self.tree.item(item_id, open=True)
            
    def _create_context_menu(self):
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="開啟資源所在資料夾", command=self._open_selected_folder_single)
        self.context_menu.add_command(label="用系統預設程式開啟 (瀏覽圖片)", command=self._open_image_in_default_viewer)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="切換勾選狀態 (選取/取消)", command=self._context_toggle_check)
        self.context_menu.add_command(label="刪除已勾選圖片 (移至資源回收桶)", command=self._delete_selected_from_disk)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="全部展開", command=self._expand_all_groups)
        self.context_menu.add_command(label="全部收合", command=self._collapse_all_groups)
        
    def _context_toggle_check(self):
        for item_id in self.tree.selection():
            tags = self.tree.item(item_id, "tags")
            if 'parent_item' in tags:
                if self.config.get('comparison_mode') == 'qr_detection': self._toggle_selection_by_item_id(item_id)
                else: self._toggle_group_selection(item_id)
            else: self._toggle_selection_by_item_id(item_id)
            
    def _show_context_menu(self, event: tk.Event):
        item_id = self.tree.identify_row(event.y)
        if item_id:
            if not hasattr(self, "context_menu"):
                self._create_context_menu()
            if item_id not in self.tree.selection():
                self.tree.selection_set(item_id)
                self.tree.focus(item_id)
                self._on_item_select(None)
            self.context_menu.tk_popup(event.x_root, event.y_root)
        
    def _commit_deletions_impl(self, silent=False):
        paths_to_sync = self.undo_manager.get_all_pending_paths()
        if not paths_to_sync:
            if not silent:
                messagebox.showinfo("提示", "目前沒有等待中的刪除項目。")
            return
            
        if not silent and not messagebox.askyesno("立即套用刪除", f"確定要立即將 {len(paths_to_sync)} 個標記檔案永久移至資源回收筒嗎？"):
            return
            
        if not silent:
            self.status_label.config(text="⏳ 正在執行物理刪除與資料庫同步(若需)...")
            self.start_button.config(state=tk.DISABLED)
            try: self.apply_button.config(state=tk.DISABLED)
            except AttributeError: pass
            self.update_idletasks()
        
        def task():
            deleted_count = self.undo_manager.commit_deletions()
            
            # 資料庫同步鉤子 - 若為廣告比對或 QR 掃描，不應刪除資料庫的資料夾紀錄
            mode = self.config.get('comparison_mode', '')
            skip_sync = mode in ['ad_comparison', 'qr_detection']
            
            if not skip_sync and 'eh_database_tools' in self.plugin_manager:
                plugin = self.plugin_manager['eh_database_tools']
                if hasattr(plugin, 'sync_deleted_files'):
                    plugin.sync_deleted_files(self.config, paths_to_sync)
                    
            if not silent:
                self.after(0, self._on_commit_done, deleted_count, skip_sync)
                
        if silent:
            task()
        else:
            import threading
            threading.Thread(target=task, daemon=True).start()

    def _on_commit_done(self, deleted_count, skip_sync):
        self._update_undo_button_state()
        self.start_button.config(state=tk.NORMAL)
        try: self.apply_button.config(state=tk.NORMAL)
        except AttributeError: pass
        
        msg = f"✅ 已成功從硬碟移除 {deleted_count} 個項目"
        msg_full = msg + ("並同步資料庫。" if not skip_sync else "。")
        
        self.status_label.config(text=msg_full)
        messagebox.showinfo("套用成功", msg_full)

    def _commit_deletions_now(self):
        self._commit_deletions_impl(silent=False)

    def _on_closing(self):
        self.is_closing = True
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askokcancel("關閉程式", "掃描仍在進行中，確定要強制關閉程式嗎？"):
                self.cancel_event.set()
                self.executor.shutdown(wait=False, cancel_futures=True)
                if hasattr(self, 'undo_manager'):
                    self._commit_deletions_impl(silent=True)
                self.destroy()
        else:
            self.executor.shutdown(wait=False, cancel_futures=True)
            if hasattr(self, 'undo_manager'):
                self._commit_deletions_impl(silent=True)
            self.destroy()

    # ------------------------------------------------------------------ #
    #  Everything dc: 索引 預飛檢查                                      #
    # ------------------------------------------------------------------ #

    def _preflight_everything_dc_check(self):
        """在掃描執行前，偵測 Everything dc: 索引是否就緒。必須在主執行緒呼叫。"""
        try:
            from processors.everything_ipc import EverythingIPCManager
            ev = EverythingIPCManager()
            if ev.check_dc_indexed():
                log_info("[SDK 預飛] dc: 索引已開啟，建立日期過濾可正常使用。")
                return  # 已索引，不需任何處理

            log_info("[SDK 預飛] 偵測到 dc: 索引未開啟，顯示詢問對話。")
            choice = self._show_dc_index_dialog()

            if choice == 'enable':
                self.status_label.config(text="⏳ 正在修改 Everything 設定並重新啟動...")
                self.update_idletasks()
                success = ev.enable_dc_index_and_restart()
                if success:
                    messagebox.showinfo(
                        "設定已套用",
                        "✅ Everything 正在背景重建「建立日期」索引（約需數分鐘）。\n\n"
                        "本次掃描將改用「修改日期」過濾。\n"
                        "Everything 建檔完成後，下次掃描將能正確使用「建立日期」。",
                        parent=self
                    )
                else:
                    messagebox.showwarning(
                        "設定失敗",
                        "自動修改設定失敗。\n請手動到 Everything → 工具 → 選項 → 索引 → 勾選「索引建立日期」。",
                        parent=self
                    )
                self.config['_everything_force_dm'] = True

            elif choice == 'always_dm':
                self.config['everything_dc_choice'] = 'always_dm'
                self.config['_everything_force_dm'] = True
                from utils import save_config
                from config import CONFIG_FILE
                save_config(self.config, CONFIG_FILE)
                log_info("[SDK 預飛] 使用者選擇「一律改用修改日期」，已儲存設定。")

            elif choice == 'this_time':
                self.config['_everything_force_dm'] = True
                log_info("[SDK 預飛] 本次改用修改日期過濾。")

            # 否則 choice == 'ignore'：保持 dc: ，將會慢，但尊重使用者選擇
        except Exception as e:
            log_error(f"[SDK 預飛] 預飛檢查發生未預期錯誤: {e}")

    def _show_dc_index_dialog(self) -> str:
        """顯示 dc: 索引選擇對話。回傳 'enable' / 'this_time' / 'always_dm' / 'ignore'。"""
        dialog = tk.Toplevel(self)
        dialog.title("⚠️ Everything 索引設定")
        dialog.resizable(False, False)
        dialog.grab_set()
        dialog.transient(self)

        msg = (
            "偵測到 Everything「建立日期」索引未開啟\n\n"
            "您目前使用「建立日期」作為時間過濾，\n"
            "但 Everything 尚未索引此欄位。\n\n"
            "這會導致每次掃描需要 10~16 分鐘（如上次所見）。\n\n"
            "請選擇解決方案："
        )
        ttk.Label(dialog, text=msg, justify=tk.LEFT, wraplength=420, padding=15).pack()

        result = tk.StringVar(value='ignore')
        btn_frame = ttk.Frame(dialog, padding=(15, 5, 15, 15))
        btn_frame.pack(fill=tk.X)

        def make_choice(val):
            result.set(val)
            dialog.destroy()

        ttk.Button(btn_frame, text="🔧 自動開啟索引並重啟 Everything",
                   command=lambda: make_choice('enable')).pack(fill=tk.X, pady=3)
        ttk.Button(btn_frame, text="⚡ 本次改用「修改日期」（快速，語義稍有不同）",
                   command=lambda: make_choice('this_time')).pack(fill=tk.X, pady=3)
        ttk.Button(btn_frame, text="📦 一律改用「修改日期」（不再提示）",
                   command=lambda: make_choice('always_dm')).pack(fill=tk.X, pady=3)
        ttk.Button(btn_frame, text="⏳ 忽略，繼續使用建立日期（會很慢）",
                   command=lambda: make_choice('ignore')).pack(fill=tk.X, pady=3)

        dialog.update_idletasks()
        x = self.winfo_x() + (self.winfo_width()  - dialog.winfo_width())  // 2
        y = self.winfo_y() + (self.winfo_height() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")
        dialog.wait_window()
        return result.get()
