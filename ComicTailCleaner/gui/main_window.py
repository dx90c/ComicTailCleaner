# ======================================================================
# 檔案名稱：gui/main_window.py
# 模組目的：包含主視窗邏輯 (已模組化拆分)
# 版本：1.8.7 (完整修復版：補齊所有遺漏函式)
# ======================================================================

# --- 1. 標準函式庫 ---
import os
import sys
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

# --- 2. 第三方庫 ---
try:
    from PIL import Image, ImageTk, ImageOps, ImageDraw
except ImportError:
    Image = ImageTk = ImageOps = ImageDraw = None
try:
    import send2trash
except ImportError:
    send2trash = None
    
# --- 3. 本地模組 ---
from config import CONFIG_FILE, default_config, APP_NAME_TC, APP_VERSION
import utils
from utils import (log_info, log_error, log_performance, save_config, load_config, 
                   _is_virtual_path, _parse_virtual_path, _open_folder, _get_file_stat, 
                   _open_image_from_any_path, 
                   ARCHIVE_SUPPORT_ENABLED, QR_SCAN_ENABLED, VPATH_SEPARATOR)

try:
    from plugins.base_plugin import BasePlugin
except ImportError:
    BasePlugin = None 

from core_engine import ImageComparisonEngine
from processors.comparison_processor import ComparisonProcessor
from processors.qr_processor import QrProcessor
from processors.base_processor import BaseProcessor 
from processors.scanner import ScannedImageCacheManager, FolderStateCacheManager

try:
    from multiprocessing import cpu_count
except ImportError:
    def cpu_count(): return 4

# --- v-MOD: 導入同層級模組 ---
from .settings_window import SettingsGUI
from .tooltip import Tooltip
# --- v-MOD END ---

class MainWindow(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if Image is None or ImageTk is None: messagebox.showerror("缺少核心依賴", "Pillow 函式庫未安裝或無法載入，程式無法運行。"); self.destroy(); return
        
        # 1. 載入外掛 (在讀取設定之前)
        self.plugin_manager = {}
        self._load_plugins()

        # 2. 組合一個包含核心與所有外掛預設值的 `combined_default_config`
        combined_default_config = default_config.copy()
        for plugin_id, plugin in self.plugin_manager.items():
            if hasattr(plugin, "get_default_config"):
                try:
                    plugin_defaults = plugin.get_default_config() or {}
                    # 只合併 config 中不存在的鍵，避免覆蓋使用者已有的設定
                    for k, v in plugin_defaults.items():
                        if k not in combined_default_config:
                            combined_default_config[k] = v
                except Exception as e:
                    # 這裡 utils 可能還沒初始化 log，先用 print 或忽略
                    print(f"[WARN] 讀取外掛 {plugin_id} 預設值失敗: {e}")
        
        # 3. 使用合併後的預設值來載入使用者設定檔 `config.json`
        self.config = load_config(CONFIG_FILE, combined_default_config)

        self.pil_img_target = None; self.pil_img_compare = None; self.img_tk_target = None; self.img_tk_compare = None; self._after_id = None
        self.all_found_items, self.all_file_data = [], {}; self.sorted_groups = []
        self.selected_files, self.banned_groups = set(), set(); self.protected_paths = set()
        self.child_to_parent, self.parent_to_children, self.item_to_path = {}, defaultdict(list), {}
        self.scan_thread = None; self.cancel_event, self.pause_event = threading.Event(), threading.Event()
        self.scan_queue, self.preview_queue = Queue(), Queue(); self.executor = ThreadPoolExecutor(max_workers=2)
        self.sort_by_column = 'count'; self.sort_direction_is_ascending = False; self._preview_delay = 150
        self.scan_start_time, self.final_status_text = None, ""; self._widgets_initialized = False; self.is_paused = False; self.is_closing = False
        self.processor_instance: Optional[BaseProcessor] = None
        self.current_target_path: Optional[str] = None
        self.current_compare_path: Optional[str] = None
        self._last_target_path: Optional[str] = None
        self._last_compare_path: Optional[str] = None
        self._last_target_src_size: Optional[Tuple[int, int]] = None
        self._last_compare_src_size: Optional[Tuple[int, int]] = None
        self._qr_style = dict(color=(0, 255, 0), alpha=90, outline_thickness=None)
        
        self._setup_main_window()

    def _load_plugins(self):
        if BasePlugin is None: return
        self.plugin_manager = {}
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            # 注意：因為 main_window.py 在 gui/ 下，所以要往上一層找
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        plugins_dir = os.path.join(base_path, "plugins")
        if not os.path.isdir(plugins_dir):
            return
        
        for plugin_name in os.listdir(plugins_dir):
            plugin_path = os.path.join(plugins_dir, plugin_name)
            if os.path.isdir(plugin_path) and os.path.isfile(os.path.join(plugin_path, "processor.py")):
                try:
                    spec = importlib.util.spec_from_file_location(f"plugins.{plugin_name}.processor", os.path.join(plugin_path, "processor.py"))
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    for attr in dir(module):
                        cls = getattr(module, attr)
                        if isinstance(cls, type) and issubclass(cls, BasePlugin) and cls is not BasePlugin:
                            instance = cls()
                            self.plugin_manager[instance.get_id()] = instance
                except Exception as e:
                    print(f"加載外掛 '{plugin_name}' 失敗: {e}")

    def deiconify(self):
        super().deiconify()
        if not self._widgets_initialized: self._init_widgets(); self._check_queues()
        
    def _setup_main_window(self):
        self.title(f"{APP_NAME_TC} v{APP_VERSION}")
        self.geometry("1600x900")
        self.update_idletasks()
        screen_width = self.winfo_screenwidth(); screen_height = self.winfo_screenheight()
        width = self.winfo_width(); height = self.winfo_height()
        x = (screen_width // 2) - (width // 2); y = (screen_height // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{max(20, y - 50)}')
        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        sys.excepthook = self.custom_excepthook
        
    def _init_widgets(self):
        if self._widgets_initialized: return
        self.bold_font = self._create_bold_font(); self._create_widgets(); self._bind_keys(); self._widgets_initialized = True
        
    def custom_excepthook(self, exc_type, exc_value, exc_traceback):
        log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", True)
        if self.winfo_exists(): messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt'。")
        self.destroy()
        
    def _create_bold_font(self) -> tuple:
        try:
            default_font = ttk.Style().lookup("TLabel", "font")
            font_family = self.tk.call('font', 'actual', default_font, '-family')
            font_size = self.tk.call('font', 'actual', default_font, '-size')
            return (font_family, abs(int(font_size)), 'bold')
        except: return ("TkDefaultFont", 9, 'bold')
        
    def _create_widgets(self):
        style = ttk.Style(self); style.configure("Accent.TButton", font=self.bold_font, foreground='blue'); style.configure("Danger.TButton", font=self.bold_font, foreground='red')
        top_frame=ttk.Frame(self,padding="5"); top_frame.pack(side=tk.TOP,fill=tk.X)
        self.settings_button=ttk.Button(top_frame,text="設定",command=self.open_settings); self.settings_button.pack(side=tk.LEFT,padx=5)
        self.start_button=ttk.Button(top_frame,text="開始執行",command=self.start_scan,style="Accent.TButton"); self.start_button.pack(side=tk.LEFT,padx=5)
        self.pause_button = ttk.Button(top_frame, text="暫停", command=self.toggle_pause, width=8, state=tk.DISABLED); self.pause_button.pack(side=tk.LEFT, padx=5)
        self.cancel_button=ttk.Button(top_frame,text="終止",command=self.cancel_scan, style="Danger.TButton", state=tk.DISABLED); self.cancel_button.pack(side=tk.LEFT, padx=5)
        main_pane=ttk.Panedwindow(self,orient=tk.HORIZONTAL); main_pane.pack(fill=tk.BOTH,expand=True,padx=10,pady=5)
        left_frame=ttk.Frame(main_pane); main_pane.add(left_frame,weight=3); self._create_treeview(left_frame)
        right_frame=ttk.Frame(main_pane); main_pane.add(right_frame,weight=2); self._create_preview_panels(right_frame)
        bottom_button_container=ttk.Frame(self); bottom_button_container.pack(fill=tk.X,expand=False,padx=10,pady=(0,5)); self._create_bottom_buttons(bottom_button_container)
        status_frame=ttk.Frame(self,relief=tk.SUNKEN,padding=2); status_frame.pack(side=tk.BOTTOM,fill=tk.X)
        self.status_label=ttk.Label(status_frame,text="準備就緒"); self.status_label.pack(side=tk.LEFT,padx=5, fill=tk.X, expand=True)
        self.progress_bar=ttk.Progressbar(status_frame,orient='horizontal',mode='determinate'); self.progress_bar.pack(side=tk.RIGHT,fill=tk.X,expand=True,padx=5)
        
    def _create_treeview(self, parent_frame: ttk.Frame):
        columns=("status","filename","path","count","size","ctime","similarity"); self.tree=ttk.Treeview(parent_frame,columns=columns,show="tree headings",selectmode="extended")
        self.tree.heading("#0", text="", anchor='center'); self.tree.column("#0", width=25, stretch=False, anchor='center')
        headings={"status":"狀態","filename":"羣組/圖片","path":"路徑","count":"數量","size":"大小","ctime":"建立日期","similarity":"相似度/類型"}
        for col, text in headings.items():
            heading_options = {'text': text}
            if col in ['count', 'size', 'ctime', 'similarity', 'filename']: heading_options['command'] = lambda c=col: self._on_column_header_click(c)
            self.tree.heading(col, **heading_options)
        widths={"status":40,"filename":300,"path":300,"count":50,"size":100,"ctime":110,"similarity":120}
        for col,width in widths.items(): self.tree.column(col,width=width,minwidth=width,stretch=(col in["filename","path"]))
        self.tree.tag_configure('child_item', foreground='#555555'); self.tree.tag_configure('parent_item', font=self.bold_font); self.tree.tag_configure('parent_partial_selection', foreground='#00008B'); self.tree.tag_configure('qr_item', background='#E0FFFF'); self.tree.tag_configure('ad_like_group', background='#E6F4FF', foreground='#0B5394'); self.tree.tag_configure('protected_item', background='#FFFACD')
        vscroll=ttk.Scrollbar(parent_frame,orient="vertical",command=self.tree.yview); self.tree.configure(yscrollcommand=vscroll.set)
        self.tree.pack(side=tk.LEFT,fill=tk.BOTH,expand=True); vscroll.pack(side=tk.RIGHT,fill=tk.Y)
        
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
        self.compare_image_label=ttk.Label(self.compare_image_frame,cursor="hand2"); self.compare_image_label.pack(fill=tk.BOTH,expand=True)
        compare_path_container = tk.Frame(self.compare_image_frame, height=path_frame_height); compare_path_container.pack(fill=tk.X, expand=False, pady=(5,0)); compare_path_container.pack_propagate(False)
        self.compare_path_label=ttk.Label(compare_path_container,text="",wraplength=500, anchor="w", justify=tk.LEFT); self.compare_path_label.pack(fill=tk.BOTH, expand=True)
        self.compare_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(False))
        self.target_image_label.bind("<Configure>", self._on_preview_resize); self.compare_image_label.bind("<Configure>", self._on_preview_resize)
        self._create_context_menu()
        
    def _create_bottom_buttons(self, parent_frame: ttk.Frame):
        button_frame = ttk.Frame(parent_frame)
        button_frame.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="選取建議", command=self._select_suggested_for_deletion).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="刪除選中(回收桶)", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=2)
        self.move_to_ad_library_button = ttk.Button(button_frame, text="複製進廣告庫", command=self._copy_selected_to_ad_library)
        self.move_to_ad_library_button.pack(side=tk.LEFT, padx=2); self.move_to_ad_library_button.pack_forget()
        actions_frame = ttk.Frame(parent_frame)
        actions_frame.pack(side=tk.RIGHT, padx=5, pady=5)
        ttk.Button(actions_frame, text="開啟選中資料夾", command=self._open_selected_folder_single).pack(side=tk.LEFT, padx=2)
        
    def _bind_keys(self):
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select); self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<Double-1>", self._on_treeview_double_click); self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection_with_space); self.tree.bind("<Return>", self._handle_return_key)
        self.tree.bind("<Delete>", lambda e: self._delete_selected_from_disk()); self.tree.bind("<BackSpace>", lambda e: self._delete_selected_from_disk())

    def open_settings(self):
        self.settings_button.config(state=tk.DISABLED)
        try:
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
            messagebox.showwarning("QR 模式不可用", "此環境缺少 OpenCV / numpy，無法進行 QR 檢測。")
            return
        preprocessor_plugins = {pid: p for pid, p in self.plugin_manager.items() if p.get_plugin_type() == 'preprocessor'}
        for plugin_id, plugin in preprocessor_plugins.items():
            if self.config.get(f'enable_{plugin_id}', False):
                self.status_label.config(text=f"⚙️ 正在執行前置處理: {plugin.get_name()}...")
                self.update_idletasks()
                try:
                    control_events = {'cancel': self.cancel_event, 'pause': self.pause_event}
                    plugin.run(self.config, self.scan_queue, control_events, app_update_callback=self.update)
                except Exception as e:
                    log_error(f"執行前置處理器 '{plugin.get_name()}' 時發生嚴重錯誤: {e}", True)
                    messagebox.showerror("前置處理失敗", f"執行 '{plugin.get_name()}' 時發生錯誤:\n{e}")
                    self._reset_control_buttons("前置處理失敗")
                    return
                if self.cancel_event.is_set():
                    self._reset_control_buttons("任務已取消")
                    return
        if not self.is_paused: self._reset_scan_state()
        self.processor_instance = self._get_processor_instance(mode)
        if not self.processor_instance:
            messagebox.showerror("錯誤", f"無法初始化模式: {mode}")
            self._reset_control_buttons("初始化失敗")
            return
        self.start_button.config(state=tk.DISABLED); self.settings_button.config(state=tk.DISABLED)
        self.pause_button.config(text="暫停", state=tk.NORMAL); self.cancel_button.config(state=tk.NORMAL)
        if not self.is_paused: self.tree.delete(*self.tree.get_children())
        self.is_paused = False
        self.scan_thread = threading.Thread(target=self._run_scan_in_thread, daemon=True)
        self.scan_thread.start()
        
    def _get_processor_instance(self, mode: str) -> Optional[Union[BaseProcessor, BasePlugin]]:
        try:
            if mode in self.plugin_manager:
                log_info(f"選擇外掛模式: {mode}, 準備啟動外掛...")
                return self.plugin_manager[mode]
            else:
                log_info(f"選擇內建模式: {mode}, 準備啟動內建處理器...")
                if mode == 'ad_comparison' or mode == 'mutual_comparison':
                    return ComparisonProcessor(self.config, self.scan_queue, {'cancel': self.cancel_event, 'pause': self.pause_event})
                elif mode == 'qr_detection':
                    return QrProcessor(self.config, self.scan_queue, {'cancel': self.cancel_event, 'pause': self.pause_event})
        except Exception as e:
            log_error(f"初始化處理器 '{mode}' 失敗: {e}", include_traceback=True)
        return None

    def _run_scan_in_thread(self):
        try:
            result = None
            if isinstance(self.processor_instance, BasePlugin):
                result = self.processor_instance.run(config=self.config.copy(), progress_queue=self.scan_queue, control_events={'cancel': self.cancel_event, 'pause': self.pause_event}, app_update_callback=self.update)
            elif isinstance(self.processor_instance, BaseProcessor):
                result = self.processor_instance.run()
            
            if result is None:
                if self.cancel_event.is_set(): self.scan_queue.put({'type': 'finish', 'text': "任務已取消"})
                else: self.scan_queue.put({'type': 'status_update', 'text': "任務已暫停"})
                return

            found, data, errors = result
            self.scan_queue.put({'type': 'result', 'data': found, 'meta': data, 'errors': errors})
            base_text = f"✅ 掃描完成！找到 {len(found)} 個目標。"
            if errors: base_text += f" (有 {len(errors)} 個項目處理失敗)"
            self.scan_queue.put({'type': 'finish', 'text': base_text})
            
        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", True)
            self.scan_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})
            if self.winfo_exists(): messagebox.showerror("執行錯誤", f"程式執行時發生錯誤: {e}")

    def _reset_scan_state(self):
        self.final_status_text = ""
        self.cancel_event.clear(); self.pause_event.clear(); self.is_paused = False
        self.processor_instance = None
        self.protected_paths.clear(); self.child_to_parent.clear(); self.parent_to_children.clear()
        self.item_to_path.clear(); self.banned_groups.clear()

    def cancel_scan(self):
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askyesno("確認終止", "確定要終止目前的掃描任務嗎？"):
                log_info("使用者請求取消任務。")
                self.cancel_event.set()
                if self.is_paused: self.pause_event.set()

    def toggle_pause(self):
        if self.is_paused:
            log_info("使用者請求恢復任務。")
            self.pause_event.clear(); self.pause_button.config(text="暫停")
            self.status_label.config(text="正在恢復任務...")
            self.start_scan()
        else:
            log_info("使用者請求暫停任務。")
            self.is_paused = True; self.pause_event.set()
            self.pause_button.config(text="恢復"); self.status_label.config(text="正在請求暫停...")

    def _reset_control_buttons(self, final_status_text: str = "任務完成"):
        self.status_label.config(text=final_status_text); self.progress_bar['value'] = 0
        self.start_button.config(state=tk.NORMAL); self.settings_button.config(state=tk.NORMAL)
        self.pause_button.config(state=tk.DISABLED, text="暫停"); self.cancel_button.config(state=tk.DISABLED)

    def _check_queues(self):
        if self.is_closing or not self.winfo_exists(): return
        try:
            while True:
                msg = self.scan_queue.get_nowait()
                msg_type = msg.get('type')
                if msg_type == 'progress' and not self.is_paused:
                    self.progress_bar['value'] = msg.get('value', 0)
                    self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'text' and not self.is_paused:
                    self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'status_update': self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'result':
                    self.all_found_items, self.all_file_data, failed_tasks = msg.get('data', []), msg.get('meta', {}), msg.get('errors', [])
                    self._process_scan_results(failed_tasks)
                elif msg_type == 'finish':
                    self.final_status_text = msg.get('text', '任務完成')
                    self._reset_control_buttons(self.final_status_text)
                    if not self.all_found_items and "取消" not in self.final_status_text and "暫停" not in self.final_status_text:
                        messagebox.showinfo("掃描結果", "未找到符合條件的相似或廣告圖片。")
        except Empty: pass
        try:
            while True:
                msg = self.preview_queue.get_nowait()
                if msg['type'] == 'image_loaded':
                    if msg['is_target']:
                        self.pil_img_target = msg['image']; self._last_target_path = msg.get('path'); self._last_target_src_size = msg.get('src_size')
                    else:
                        self.pil_img_compare = msg['image']; self._last_compare_path = msg.get('path'); self._last_compare_src_size = msg.get('src_size')
                    self._update_all_previews()
        except Empty: pass
        finally:
            if not self.is_closing: self.after(100, self._check_queues)

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
        for gk, ip, vs in self.all_found_items: groups[gk].append((ip, vs))
        self.sorted_groups = list(groups.items())
        
        self._sort_and_redisplay_results()
        
        if self.tree.get_children():
            fc = self.tree.get_children()[0]
            self.tree.selection_set(fc); self.tree.focus(fc)

    def _on_column_header_click(self, column_name: str):
        if self.sort_by_column == column_name: self.sort_direction_is_ascending = not self.sort_direction_is_ascending
        else: self.sort_by_column = column_name; self.sort_direction_is_ascending = False
        ht = self.tree.heading(column_name, 'text')
        self.status_label.config(text=f"🔄 正在依「{ht}」欄位重新排序...")
        self.after(50, self._sort_and_redisplay_results)
        
    def _sort_and_redisplay_results(self):
        if self.sort_by_column == 'filename': sort_key_func = lambda item: os.path.basename(item[0])
        elif self.sort_by_column == 'count': sort_key_func = lambda item: len(item[1]) + 1
        elif self.sort_by_column == 'size': sort_key_func = lambda item: self.all_file_data.get(item[0], {}).get('size', 0) or 0
        elif self.sort_by_column == 'ctime': sort_key_func = lambda item: self.all_file_data.get(item[0], {}).get('ctime', 0) or 0
        elif self.sort_by_column == 'similarity':
            def get_max_sim(item):
                sims = [float(s.replace('%','').split(' ')[0]) for _,s in item[1] if '%' in str(s)]
                return max(sims) if sims else 0.0
            sort_key_func = get_max_sim
        else: sort_key_func = lambda item: len(item[1]) + 1
            
        self.sorted_groups.sort(key=sort_key_func, reverse=not self.sort_direction_is_ascending)
        if self.config.get('comparison_mode') != 'qr_detection':
            self.sorted_groups.sort(key=lambda item: item[1] and any("(似廣告)" in str(s) for _, s in item[1]), reverse=True)
        
        self.tree.delete(*self.tree.get_children())
        self.child_to_parent.clear(); self.parent_to_children.clear(); self.item_to_path.clear()
        self._populate_treeview_logic(self.sorted_groups)
        self.status_label.config(text=self.final_status_text or "排序完成。")
        
    def _populate_treeview_logic(self, groups_to_load: list):
        uid = 0
        mode = self.config.get('comparison_mode')
        
        if mode == 'qr_detection':
            for gk, items in groups_to_load:
                pid = f"group_{uid}"; uid += 1
                is_prot = gk in self.protected_paths
                ptags = ['parent_item', 'qr_item'] + (['protected_item'] if is_prot else [])
                gk_data = self.all_file_data.get(gk, {})
                gk_size = f"{gk_data.get('size', 0):,}" if 'size' in gk_data and gk_data.get('size') is not None else "N/A"
                gk_ctime = datetime.datetime.fromtimestamp(gk_data['ctime']).strftime('%Y/%m/%d %H:%M') if gk_data.get('ctime') else "N/A"
                is_sel = gk in self.selected_files
                stat_char = "🔒" if is_prot else ("☑" if is_sel else "☐")
                display_path, base_name = gk, os.path.basename(gk)
                if _is_virtual_path(gk):
                    archive_path, inner_path = _parse_virtual_path(gk)
                    if archive_path: display_path = f"{os.path.basename(archive_path)}{VPATH_SEPARATOR}{inner_path}"; base_name = inner_path
                self.tree.insert("", "end", iid=pid, open=True, values=(stat_char, base_name, display_path, 1, gk_size, gk_ctime, items[0][1]), tags=tuple(ptags))
                self.item_to_path[pid] = gk
        else:
            for gk, items in groups_to_load:
                pid = f"group_{uid}"; uid += 1
                is_ad_like = items and any("(似廣告)" in str(s) for _, s in items)
                is_prot = gk in self.protected_paths
                ptags = ['parent_item'] + (['ad_like_group'] if is_ad_like else []) + (['protected_item'] if is_prot else [])
                disp_list = [(gk, "基準 (自身)")] + sorted(items, key=lambda x:x[0])
                self.tree.insert("", "end", iid=pid, open=True, values=("", os.path.basename(gk), "", len(disp_list), "", "", ""), tags=tuple(ptags))
                self.item_to_path[pid] = gk
                for path, val_str in disp_list:
                    cid = f"item_{uid}"; uid += 1
                    ctags = ['child_item'] + (['ad_like_group'] if is_ad_like else []) + (['protected_item'] if path in self.protected_paths else [])
                    c_data = self.all_file_data.get(path, {}); c_size = f"{c_data.get('size', 0):,}" if 'size' in c_data and c_data.get('size') is not None else "N/A"
                    c_ctime = datetime.datetime.fromtimestamp(c_data['ctime']).strftime('%Y/%m/%d %H:%M') if c_data.get('ctime') else "N/A"
                    is_sel = path in self.selected_files
                    stat_char = "🔒" if path in self.protected_paths else ("☑" if is_sel else "☐")
                    display_path, base_name = path, os.path.basename(path)
                    if _is_virtual_path(path):
                        archive_path, inner_path = _parse_virtual_path(path)
                        if archive_path: display_path = f"{os.path.basename(archive_path)}{VPATH_SEPARATOR}{inner_path}"; base_name = inner_path
                    self.tree.insert(pid, "end", iid=cid, values=(stat_char, f"  └─ {base_name}", display_path, "", c_size, c_ctime, val_str), tags=tuple(ctags))
                    self.child_to_parent[cid] = pid; self.parent_to_children[pid].append(cid); self.item_to_path[cid] = path
                self._update_group_checkbox(pid)

    def _on_treeview_click(self, event: tk.Event):
        item_id = self.tree.identify_row(event.y)
        if not item_id or not self.tree.exists(item_id): return
        if self.tree.identify_column(event.x) in ("#1", "#2"):
            tags = self.tree.item(item_id, "tags")
            if 'parent_item' in tags:
                if self.config.get('comparison_mode') == 'qr_detection': self._toggle_selection_by_item_id(item_id)
                else: self._toggle_group_selection(item_id)
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
                    folder_to_open = None
                    if os.path.isdir(path_value): folder_to_open = path_value
                    else:
                        real_path = path_value
                        if _is_virtual_path(real_path):
                            archive_path, _ = _parse_virtual_path(real_path)
                            if archive_path: real_path = archive_path
                        if os.path.exists(real_path): folder_to_open = os.path.dirname(real_path)
                    if folder_to_open and os.path.isdir(folder_to_open): _open_folder(folder_to_open)
                    else: log_info(f"無法開啓路徑，因為它不是一個有效的資料夾: {folder_to_open}")
                    
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
        self.current_target_path = preview_path; self.current_compare_path = compare_path
        if preview_path: self.executor.submit(self._load_image_worker, preview_path, True)
        else: self.pil_img_target = None; self.target_path_label.config(text=""); self._update_all_previews()
        if compare_path: self.executor.submit(self._load_image_worker, compare_path, False)
        else: self.pil_img_compare = None; self.compare_path_label.config(text=""); self._update_all_previews()

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
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)
        
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
        folder_to_open = None
        if _is_virtual_path(current_path):
            archive_path, _ = _parse_virtual_path(current_path)
            if archive_path: folder_to_open = os.path.dirname(archive_path)
        else: folder_to_open = os.path.dirname(current_path)
        if folder_to_open and os.path.isdir(folder_to_open): _open_folder(folder_to_open)
            
    def _toggle_selection_by_item_id(self, item_id: str):
        if 'protected_item' in self.tree.item(item_id, "tags"): return
        path = self.item_to_path.get(item_id)
        if not path: return
        if path in self.selected_files: self.selected_files.discard(path); self.tree.set(item_id, "status", "☐")
        else: self.selected_files.add(path); self.tree.set(item_id, "status", "☑")
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
        if not selectable: self.tree.set(parent_id, "status", ""); return
        selected_count = sum(1 for cid in selectable if self.item_to_path.get(cid) in self.selected_files)
        for child_id in children:
            path = self.item_to_path.get(child_id)
            if 'protected_item' in self.tree.item(child_id, "tags"): self.tree.set(child_id, "status", "🔒")
            else: self.tree.set(child_id, "status", "☑" if path in self.selected_files else "☐")
        tags = list(self.tree.item(parent_id, "tags"))
        if 'parent_partial_selection' in tags: tags.remove('parent_partial_selection')
        if selected_count == 0: self.tree.set(parent_id, "status", "☐")
        elif selected_count == len(selectable): self.tree.set(parent_id, "status", "☑")
        else: self.tree.set(parent_id, "status", "◪"); tags.append('parent_partial_selection')
        self.tree.item(parent_id, tags=tuple(tags))
        
    def _toggle_selection_with_space(self, event: tk.Event) -> str:
        sel = self.tree.selection()
        if not sel: return "break"
        item_id = sel[0]; tags = self.tree.item(item_id, "tags")
        if 'parent_item' in tags:
            if self.config.get('comparison_mode') == 'qr_detection': self._toggle_selection_by_item_id(item_id)
            else: self._toggle_group_selection(item_id)
        else: self._toggle_selection_by_item_id(item_id)
        return "break"
        
    def _get_all_selectable_paths(self):
        return {p for iid, p in self.item_to_path.items() if self.tree.exists(iid) and 'protected_item' not in self.tree.item(iid, "tags")}
        
    def _refresh_all_checkboxes(self):
        for item_id in self.tree.get_children(""):
            if self.config.get('comparison_mode') == 'qr_detection':
                 path = self.item_to_path.get(item_id)
                 if 'protected_item' not in self.tree.item(item_id, "tags"): self.tree.set(item_id, "status", "☑" if path in self.selected_files else "☐")
            elif 'parent_item' in self.tree.item(item_id, "tags"): self._update_group_checkbox(item_id)
            
    def _select_all(self): self.selected_files.update(self._get_all_selectable_paths()); self._refresh_all_checkboxes()
    def _deselect_all(self): self.selected_files.clear(); self._refresh_all_checkboxes()
    def _invert_selection(self): self.selected_files.symmetric_difference_update(self._get_all_selectable_paths()); self._refresh_all_checkboxes()
    
    def _select_suggested_for_deletion(self):
        paths_to_select = set()
        for parent_id in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(parent_id, "tags"):
                for child_id in self.tree.get_children(parent_id):
                    if 'protected_item' in self.tree.item(child_id, "tags"): continue
                    values = self.tree.item(child_id, "values")
                    if str(values[6]).strip() == "100.0%":
                        path = self.item_to_path.get(child_id)
                        if path: paths_to_select.add(path)
        if not paths_to_select: messagebox.showinfo("提示", "沒有找到相似度為 100.0% 的可選項目。", parent=self); return
        self.selected_files.update(paths_to_select); self._refresh_all_checkboxes()
       
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
        if not self.selected_files or send2trash is None: return
        to_delete = [p for p in self.selected_files if p not in self.protected_paths]
        if not to_delete: messagebox.showinfo("無需操作", "所有選中的項目均受保護。", parent=self); return
        if not messagebox.askyesno("確認刪除", f"確定要將 {len(to_delete)} 個圖片移至回收桶嗎？"): return
        deleted_count, failed_count = 0, 0
        for path in to_delete:
            if _is_virtual_path(path): log_error(f"無法直接刪除虛擬路徑: {path}。此功能待實現。"); failed_count += 1; continue
            try: send2trash.send2trash(os.path.abspath(path)); deleted_count += 1
            except Exception as e: log_error(f"移至回收桶失敗 {path}: {e}", True); failed_count += 1
        messagebox.showinfo("刪除完成", f"成功刪除 {deleted_count} 個檔案。\n{failed_count} 個檔案刪除失敗。")
        self.all_found_items = [(p1, p2, v) for p1, p2, v in self.all_found_items if p1 not in to_delete and p2 not in to_delete]
        self.selected_files.clear(); self._process_scan_results([])
        
    def _open_selected_folder_single(self):
        sel = self.tree.selection()
        if sel:
            path = self.item_to_path.get(sel[0])
            if path:
                folder_to_open = None
                if _is_virtual_path(path):
                    archive_path, _ = _parse_virtual_path(path)
                    if archive_path: folder_to_open = os.path.dirname(archive_path)
                else: folder_to_open = os.path.dirname(path)
                if folder_to_open and os.path.isdir(folder_to_open): _open_folder(folder_to_open)
                
    def _collapse_all_groups(self):
        for item_id in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(item_id, "tags"): self.tree.item(item_id, open=False)
            
    def _expand_all_groups(self):
        for item_id in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(item_id, "tags"): self.tree.item(item_id, open=True)
            
    def _create_context_menu(self):
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="全部展開", command=self._expand_all_groups)
        self.context_menu.add_command(label="全部收合", command=self._collapse_all_groups)
        
    def _show_context_menu(self, event: tk.Event):
        if self.tree.identify_row(event.y): self.context_menu.tk_popup(event.x_root, event.y_root)
        
    def _on_closing(self):
        self.is_closing = True
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askokcancel("關閉程式", "掃描仍在進行中，確定要強制關閉程式嗎？"):
                self.cancel_event.set()
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.destroy()
        else:
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.destroy()