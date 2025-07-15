# ======================================================================
# 檔案名稱：ComicTailCleaner_v12.0.7.py
# 版本號：12.0.7
# 專案名稱：ComicTailCleaner (漫畫尾頁廣告清理)
#
# === 程式說明 ===
# 一個專為清理漫畫檔案尾頁廣告或重複頁面的工具。
# 它能高效地掃描大量漫畫檔案，並通過感知哈希算法找出內容上
# 相似或完全重複的圖片，提升漫畫閱讀體驗。
#
# === 12.0.7 版本更新內容 ===
# - 【錯誤修正】提供了包含所有必要函數定義的完整程式碼，修正了因先前版本
#   省略部分程式碼而導致的 NameError 啟動錯誤。
#
# === 12.0.6 版本更新內容 ===
# - 【核心邏輯重構】: 徹底重寫結果分組邏輯，根除重複顯示問題。
# - 【UI 優化】: 使用背景高亮替代文字標籤來標示基準圖片。
# ======================================================================


# === 1. 標準庫導入 (Python Built-in Libraries) ===
import os
import sys
import json
import shutil
import datetime
import traceback
import subprocess
from collections import deque
from multiprocessing import set_start_method, Pool, cpu_count, Manager
import hashlib
import platform
import threading

# === 2. 第三方庫導入 (Third-party Libraries) ===
from PIL import Image, ImageTk, ImageOps, UnidentifiedImageError

try:
    import imagehash
except ImportError:
    pass

try:
    import cv2
    import pyzbar
    import numpy as np
except ImportError:
    pass

try:
    import send2trash
except ImportError:
    pass

# === 3. Tkinter GUI 庫導入 ===
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from tkinter import messagebox

# === 4. 全局常量和設定 ===
APP_VERSION = "12.0.7"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"
CONFIG_FILE = "config.json"
QR_SCAN_ENABLED = False
HASH_DB_DIR = "image_hash_db"


# === 5. 工具函數 (Helper Functions) ===
def log_error(message, include_traceback=False):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] ERROR: {message}\n"
    if include_traceback:
        log_content += traceback.format_exc() + "\n"
    print(log_content, end='', flush=True)
    try:
        with open("error_log.txt", "a", encoding="utf-8", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"Failed to write to error log: {e}\nOriginal error: {message}", flush=True)

def check_and_install_packages():
    print("正在檢查必要的 Python 套件...", flush=True)
    required_packages = {'PIL': 'Pillow', 'imagehash': 'imagehash', 'send2trash': 'send2trash'}
    missing_packages = []
    for module, package in required_packages.items():
        if module not in sys.modules:
            missing_packages.append(package)
    
    if missing_packages:
        package_str = " ".join(missing_packages)
        messagebox.showerror("缺少核心依賴", f"請安裝必要的 Python 套件：{', '.join(missing_packages)}。\n"
                                             f"可以使用 'pip install {package_str}' 命令安裝。")
        sys.exit(1)
    else:
        print("Pillow, imagehash 和 send2trash 套件檢查通過。", flush=True)

    try:
        if 'tkinter' not in sys.modules:
            raise ImportError("Tkinter 未成功導入")
        print(f"Tkinter Version: {tk.TkVersion}, Tcl Version: {tk.TclVersion}", flush=True)
    except ImportError as e:
        messagebox.showerror("Tkinter 錯誤", f"無法找到 Tkinter ({e})。您的 Python 安裝可能不完整或損壞。")
        sys.exit(1)
        
    global QR_SCAN_ENABLED
    QR_SCAN_ENABLED = False
    try:
        if 'cv2' not in sys.modules or 'pyzbar' not in sys.modules or 'numpy' not in sys.modules:
            raise ImportError("opencv-python 或 pyzbar 或 numpy 未成功導入")
        QR_SCAN_ENABLED = True
        print("OpenCV 和 pyzbar (QR Code 掃描) 套件檢查通過。QR Code 掃描功能已啟用。", flush=True)
    except ImportError as e:
        print(f"警告: 缺少 'opencv-python' 或 'pyzbar' 或 'numpy' ({e})。QR Code 掃描功能將被禁用。", flush=True)
    except Exception as e:
        log_error(f"導入 QR Code 相關套件時發生未知錯誤: {e}。QR Code 掃描功能將被禁用。", include_traceback=True)


# === 6. 配置管理相關函數 ===
default_config = {
    'root_scan_folder': '',
    'ad_folder_path': '',
    'extract_count': 5,
    'enable_extract_count_limit': True,
    'excluded_folders': [],
    'comparison_mode': 'mutual_comparison',
    'similarity_threshold': 95,
    'enable_time_filter': False,
    'start_date_filter': '',
    'end_date_filter': '',
}
def load_config(config_path):
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                merged_config = default_config.copy()
                merged_config.update(config)
                print(f"設定檔 '{config_path}' 已成功載入。", flush=True)
                return merged_config
        else:
            print(f"設定檔 '{config_path}' 不存在，將使用預設設定。", flush=True)
    except json.JSONDecodeError:
        log_error(f"設定檔 '{config_path}' 格式不正確，將使用預設設定。", include_traceback=True)
    except Exception as e:
        log_error(f"載入設定檔 '{config_path}' 時發生錯誤: {e}，將使用預設設定。", include_traceback=True)
    return default_config.copy()

def save_config(config, config_path):
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        print(f"設定已成功保存到 '{config_path}'。", flush=True)
    except Exception as e:
        log_error(f"保存設定檔 '{config_path}' 時發生錯誤: {e}", include_traceback=True)


# === 7. 資料夾時間快取管理類 ===
class FolderCreationCacheManager:
    def __init__(self, cache_file_path="folder_creation_cache.json"):
        self.cache_file_path = cache_file_path
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    loaded_cache = json.load(f)
                    converted_cache = {}
                    for folder_path, timestamp_str in loaded_cache.items():
                        try:
                            converted_cache[folder_path] = float(timestamp_str)
                        except (ValueError, TypeError):
                            log_error(f"快取檔案 '{self.cache_file_path}' 中 '{folder_path}' 的建立時間格式不正確，將忽略此項。", include_traceback=True)
                            continue
                    print(f"資料夾建立時間快取 '{self.cache_file_path}' 已成功載入。", flush=True)
                    return converted_cache
            except json.JSONDecodeError:
                log_error(f"資料夾建立時間快取檔案 '{self.cache_file_path}' 格式不正確，將重建快取。", include_traceback=True)
            except Exception as e:
                log_error(f"載入資料夾建立時間快取時發生錯誤: {e}，將重建快取。", include_traceback=True)
        print(f"資料夾建立時間快取檔案 '{self.cache_file_path}' 不存在或載入失敗，將從空快取開始。", flush=True)
        return {}

    def save_cache(self):
        try:
            serializable_cache = {path: str(timestamp) for path, timestamp in self.cache.items()}
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_cache, f, indent=4, ensure_ascii=False)
            print(f"資料夾建立時間快取已成功保存到 '{self.cache_file_path}'。", flush=True)
        except Exception as e:
            log_error(f"保存資料夾建立時間快取時發生錯誤: {e}", include_traceback=True)

    def get_creation_time(self, folder_path):
        if folder_path in self.cache:
            return self.cache[folder_path]
        try:
            ctime = os.path.getctime(folder_path)
            self.cache[folder_path] = ctime
            return ctime
        except FileNotFoundError:
            log_error(f"資料夾不存在，無法獲取建立時間: {folder_path}", include_traceback=False)
            return None
        except Exception as e:
            log_error(f"獲取資料夾建立時間失敗: {folder_path}, 錯誤: {e}", include_traceback=True)
            return None

    def invalidate_cache(self):
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try:
                os.remove(self.cache_file_path)
                print(f"資料夾建立時間快取檔案 '{self.cache_file_path}' 已刪除。", flush=True)
            except Exception as e:
                log_error(f"刪除快取檔案 '{self.cache_file_path}' 時發生錯誤: {e}", include_traceback=True)
        print("資料夾建立時間快取已失效。", flush=True)


# === 8. 核心工具函數 ===
def get_all_subfolders(root_folder, excluded_folders=None, enable_time_filter=False, start_date=None, end_date=None, creation_cache_manager=None):
    if excluded_folders is None:
        excluded_folders = []
    all_subfolders_to_return = []
    if not os.path.isdir(root_folder):
        log_error(f"根掃描資料夾不存在: {root_folder}", include_traceback=False)
        return []

    excluded_norm_paths = {os.path.normpath(f) for f in excluded_folders}
    folders_to_process_queue = deque([root_folder])
    processed_folders_for_traversal = set()

    while folders_to_process_queue:
        current_folder = folders_to_process_queue.popleft()
        norm_current_folder = os.path.normpath(current_folder)

        if norm_current_folder in processed_folders_for_traversal:
            continue
        processed_folders_for_traversal.add(norm_current_folder)

        if any(norm_current_folder.startswith(excluded_path) for excluded_path in excluded_norm_paths):
            continue

        if current_folder != root_folder:
            if enable_time_filter and creation_cache_manager:
                folder_ctime_timestamp = creation_cache_manager.get_creation_time(current_folder)
                if folder_ctime_timestamp is not None:
                    folder_ctime = datetime.datetime.fromtimestamp(folder_ctime_timestamp)
                    if (start_date and folder_ctime < start_date) or \
                       (end_date and folder_ctime > end_date):
                        continue
                else:
                    log_error(f"無法獲取資料夾建立時間，跳過時間篩選: {current_folder}", include_traceback=False)
                    continue
        
        all_subfolders_to_return.append(current_folder)

        try:
            for entry in os.listdir(current_folder):
                entry_path = os.path.join(current_folder, entry)
                if os.path.isdir(entry_path):
                    folders_to_process_queue.append(entry_path)
        except PermissionError:
            log_error(f"無權限訪問資料夾: {current_folder}", include_traceback=False)
        except Exception as e:
            log_error(f"遍歷資料夾 '{current_folder}' 時發生錯誤: {e}", include_traceback=True)
            
    return all_subfolders_to_return

def extract_last_n_files_from_folders(folder_paths, count, enable_limit):
    extracted_files = {}
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    for folder_path in folder_paths:
        image_files = []
        try:
            with os.scandir(folder_path) as it:
                for entry in it:
                    if entry.is_file() and entry.name.lower().endswith(image_extensions):
                        image_files.append(entry.path)

            image_files.sort()
            if enable_limit:
                extracted_files[folder_path] = image_files[-count:]
            else:
                extracted_files[folder_path] = image_files
        except PermissionError:
            log_error(f"無權限訪問資料夾中的檔案: {folder_path}", include_traceback=False)
        except Exception as e:
            log_error(f"處理資料夾 '{folder_path}' 時發生錯誤: {e}", include_traceback=True)
    return extracted_files

def calculate_image_hash(image_path, hash_size=8):
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            phash = imagehash.phash(img, hash_size=hash_size)
            return phash
    except FileNotFoundError:
        log_error(f"哈希計算失敗: 文件未找到 - {image_path}", include_traceback=False)
        return None
    except UnidentifiedImageError:
        log_error(f"哈希計算失敗: 無法識別圖片格式或文件已損壞 - {image_path}", include_traceback=False)
        return None
    except OSError as e:
        log_error(f"打開圖片檔案時發生操作系統錯誤 '{image_path}': {e}", include_traceback=False)
        return None
    except Exception as e:
        log_error(f"計算圖片哈希時發生錯誤: {image_path}, 錯誤: {e}", include_traceback=True)
        return None

# === 9. 多進程工作函數 ===
def _pool_worker_initializer(lock):
    global _db_lock
    _db_lock = lock

def _pool_worker_process_image(image_path):
    phash = calculate_image_hash(image_path)
    if phash is None:
        return None

    hash_str = str(phash)
    cache_file_path = os.path.join(HASH_DB_DIR, hash_str)
    
    with _db_lock:
        if not os.path.exists(cache_file_path):
            try:
                with open(cache_file_path, 'w', encoding='utf-8') as f:
                    json.dump([image_path], f, ensure_ascii=False)
                return None
            except Exception as e:
                log_error(f"無法寫入快取檔案 {cache_file_path}: {e}")
                return None
        else:
            try:
                with open(cache_file_path, 'r+', encoding='utf-8') as f:
                    try:
                        path_list = json.load(f)
                        original_path = path_list[0]
                        
                        if image_path not in path_list:
                            path_list.append(image_path)
                            f.seek(0)
                            f.truncate()
                            json.dump(path_list, f, ensure_ascii=False)

                        return (original_path, image_path)
                    except json.JSONDecodeError:
                        log_error(f"快取檔案 {cache_file_path} 格式錯誤，將被覆蓋。")
                        f.seek(0)
                        f.truncate()
                        json.dump([image_path], f, ensure_ascii=False)
                        return None
            except Exception as e:
                log_error(f"無法讀寫快取檔案 {cache_file_path}: {e}")
                return None
    return None


# === 10. 核心比對引擎 ===
class ImageComparisonEngine:
    def __init__(self, root_scan_folder, ad_folder_path, extract_count, excluded_folders,
                 enable_time_filter, start_date_filter, end_date_filter,
                 similarity_threshold, comparison_mode, system_qr_scan_capability,
                 enable_extract_count_limit):
        self.root_scan_folder = root_scan_folder
        self.ad_folder_path = ad_folder_path
        self.extract_count = extract_count
        self.enable_extract_count_limit = enable_extract_count_limit
        self.excluded_folders = [os.path.normpath(f) for f in excluded_folders]
        self.enable_time_filter = enable_time_filter
        self.start_date_filter = start_date_filter
        self.end_date_filter = end_date_filter
        self.similarity_threshold = similarity_threshold
        self.comparison_mode = comparison_mode
        self.system_qr_scan_capability = system_qr_scan_capability
        self.processed_files_display_interval = 1000
        print(f"ImageComparisonEngine (v12) initialized.", flush=True)

    def find_duplicates(self):
        os.makedirs(HASH_DB_DIR, exist_ok=True)
        
        if self.comparison_mode == "qr_detection":
            if self.system_qr_scan_capability:
                return self._detect_qr_codes()
            else:
                print("QR Code 掃描功能因缺少依賴而被禁用。")
                return [], {}
        else:
            return self._build_hash_database_and_find_duplicates()

    def _get_files_to_process(self, folder_creation_cache_manager):
        print("正在收集需要處理的檔案...")
        
        target_folders = get_all_subfolders(
            self.root_scan_folder,
            self.excluded_folders,
            self.enable_time_filter,
            self.start_date_filter,
            self.end_date_filter,
            folder_creation_cache_manager
        )
        print(f"過濾後找到 {len(target_folders)} 個目標資料夾進行處理。")
        
        if self.comparison_mode == 'ad_comparison' and self.ad_folder_path:
            target_folders.append(self.ad_folder_path)
            print(f"廣告比對模式：已將廣告資料夾 '{self.ad_folder_path}' 加入掃描目標。")
        
        extracted_files_dict = extract_last_n_files_from_folders(
            target_folders, self.extract_count, self.enable_extract_count_limit
        )
        
        all_files = [
            file_path for file_list in extracted_files_dict.values() for file_path in file_list
        ]
        
        print(f"總共收集了 {len(all_files)} 個檔案進行去重處理。")
        return all_files
        
    def _build_hash_database_and_find_duplicates(self):
        folder_creation_cache_manager = FolderCreationCacheManager()
        files_to_process = self._get_files_to_process(folder_creation_cache_manager)
        folder_creation_cache_manager.save_cache()

        if not files_to_process:
            print("沒有找到任何需要處理的圖片檔案。")
            return [], {}

        num_processes = cpu_count()
        manager = Manager()
        db_lock = manager.Lock()
        
        print(f"啟動多進程（{num_processes}個）建立哈希資料庫並尋找重複項...")
        
        found_duplicates = []
        processed_pairs = set()

        with Pool(processes=num_processes, initializer=_pool_worker_initializer, initargs=(db_lock,)) as pool:
            results_iterator = pool.imap_unordered(_pool_worker_process_image, files_to_process)
            
            processed_count = 0
            total_to_process = len(files_to_process)
            for result_pair in results_iterator:
                processed_count += 1
                if result_pair:
                    original_path, current_path = result_pair
                    
                    pair_key = tuple(sorted((original_path, current_path)))
                    if pair_key not in processed_pairs:
                        if self.comparison_mode == 'ad_comparison':
                            norm_ad_path = os.path.normpath(self.ad_folder_path)
                            is_orig_ad = os.path.normpath(os.path.dirname(original_path)).startswith(norm_ad_path)
                            is_curr_ad = os.path.normpath(os.path.dirname(current_path)).startswith(norm_ad_path)
                            
                            if is_orig_ad != is_curr_ad:
                                ad_path = original_path if is_orig_ad else current_path
                                target_path = current_path if is_orig_ad else original_path
                                found_duplicates.append((target_path, ad_path, 100.0))
                                processed_pairs.add(pair_key)
                        else:
                            found_duplicates.append((current_path, original_path, 100.0))
                            processed_pairs.add(pair_key)

                if processed_count % self.processed_files_display_interval == 0 or processed_count == total_to_process:
                    print(f"  已處理 {processed_count}/{total_to_process} 個檔案...")

        print(f"哈希資料庫建立完成，共找到 {len(found_duplicates)} 對獨立的重複項。")
        return found_duplicates, {}

    def _detect_qr_codes(self):
        folder_creation_cache_manager = FolderCreationCacheManager()
        files_to_process = self._get_files_to_process(folder_creation_cache_manager)
        folder_creation_cache_manager.save_cache()
        
        print("開始檢測圖片中的 QR Code...")
        found_qr_images = []
        progress_interval = max(1, len(files_to_process) // 20)
        qr_detector = cv2.QRCodeDetector()
        
        for i, image_path in enumerate(files_to_process):
            try:
                with Image.open(image_path) as pil_img:
                    pil_img = ImageOps.exif_transpose(pil_img)
                    pil_img = pil_img.convert('RGB')
                    img_cv = np.array(pil_img)
                    gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)
                retval, _, _, _ = qr_detector.detectAndDecodeMulti(gray)
                if retval:
                    found_qr_images.append((image_path, "N/A", 100.0))
            except Exception as e:
                log_error(f"檢測 QR Code 時發生錯誤於圖片 {image_path}: {e}", include_traceback=True)
            
            if (i + 1) % progress_interval == 0 or (i + 1) == len(files_to_process):
                print(f"  已檢測 {i + 1}/{len(files_to_process)} 個圖片...")
        print(f"QR Code 檢測完成。找到 {len(found_qr_images)} 個包含 QR Code 的圖片。", flush=True)
        return found_qr_images, {}


# === 11. GUI 類別 ===
# 【修正】將所有 GUI 類別的定義移至 Application 類之前

class Tooltip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.id = None
        self.x = self.y = 0

    def enter(self, event=None):
        self.schedule(event)

    def leave(self, event=None):
        self.unschedule()
        self.hidetip()

    def schedule(self, event=None):
        self.unschedule()
        if event:
            self.x = event.x_root + 15
            self.y = event.y_root + 10
        self.id = self.widget.after(500, self.showtip)

    def unschedule(self):
        id = self.id
        self.id = None
        if id:
            self.widget.after_cancel(id)

    def showtip(self):
        if self.tooltip_window:
            return
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{self.x}+{self.y}")
        label = tk.Label(tw, text=self.text, justify='left',
                       background="#ffffe0", relief='solid', borderwidth=1,
                       font=("tahoma", "8", "normal"))
        label.pack(ipadx=1)

    def hidetip(self):
        tw = self.tooltip_window
        self.tooltip_window = None
        if tw:
            tw.destroy()

class SettingsGUI:
    def __init__(self, master, on_start_callback):
        self.master = master
        self.on_start_callback = on_start_callback
        self.config_file_path = CONFIG_FILE
        self.qr_scan_feature_enabled_global = QR_SCAN_ENABLED
        self.result_config = None
        self.rebuild_folder_cache_result = False
        self.config = load_config(self.config_file_path)
        
        self.settings_window = tk.Toplevel(self.master)
        self.settings_window.title(f"{APP_NAME_TC} v{APP_VERSION} - 設定")
        self.settings_window.geometry("700x650")
        self.settings_window.resizable(False, False)
        self.settings_window.transient(master)
        self.settings_window.grab_set()
        self.settings_window.focus_force()
        self.settings_window.update_idletasks()
        self.settings_window.lift()
        self.settings_window.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        self.main_frame = ttk.Frame(self.settings_window, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        self.main_frame.grid_columnconfigure(1, weight=1)
        
        self._create_widgets(self.main_frame)
        self._load_settings_into_gui()
        self._setup_bindings()
        
        print("SettingsGUI: Widgets created and config applied.", flush=True)

    def _create_widgets(self, frame):
        row_idx = 0
        path_frame = ttk.LabelFrame(frame, text="路徑設定", padding="10")
        path_frame.grid(row=row_idx, column=0, columnspan=3, sticky="ew", pady=5, padx=5)
        path_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(path_frame, text="根掃描資料夾:").grid(row=0, column=0, sticky="w", pady=2)
        self.root_scan_folder_entry = ttk.Entry(path_frame, width=40)
        self.root_scan_folder_entry.grid(row=0, column=1, sticky="ew", padx=5)
        ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.root_scan_folder_entry)).grid(row=0, column=2, padx=5)
        ttk.Label(path_frame, text="廣告圖片資料夾:").grid(row=1, column=0, sticky="w", pady=2)
        self.ad_folder_entry = ttk.Entry(path_frame, width=40)
        self.ad_folder_entry.grid(row=1, column=1, sticky="ew", padx=5)
        ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.ad_folder_entry)).grid(row=1, column=2, padx=5)
        
        row_idx += 1
        basic_settings_frame = ttk.LabelFrame(frame, text="基本設定", padding="10")
        basic_settings_frame.grid(row=row_idx, column=0, columnspan=3, sticky="ew", pady=5, padx=5)
        basic_settings_frame.grid_columnconfigure(1, weight=1)
        self.enable_extract_count_limit_var = tk.BooleanVar()
        self.chk_enable_extract_count = ttk.Checkbutton(basic_settings_frame, text="啟用圖片抽取數量限制", variable=self.enable_extract_count_limit_var)
        self.chk_enable_extract_count.grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        
        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2)
        self.extract_count_var = tk.StringVar()
        self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5)
        self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="(從每個資料夾末尾提取N張圖片進行比對)").grid(row=1, column=2, sticky="w", padx=5)

        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=2, column=0, sticky="w", pady=2)
        self.similarity_threshold_var = tk.DoubleVar()
        ttk.Scale(basic_settings_frame, from_=80, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label).grid(row=2, column=1, sticky="w", padx=5)
        self.threshold_label = ttk.Label(basic_settings_frame, text="")
        self.threshold_label.grid(row=2, column=2, sticky="w", padx=5)
        
        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (逗號分隔):").grid(row=3, column=0, sticky="w", pady=2)
        self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=4)
        self.excluded_folders_text.grid(row=3, column=1, columnspan=2, sticky="ew", padx=5)
        scrollbar = ttk.Scrollbar(basic_settings_frame, command=self.excluded_folders_text.yview)
        scrollbar.grid(row=3, column=3, sticky="ns")
        self.excluded_folders_text.config(yscrollcommand=scrollbar.set)
        
        row_idx += 1
        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10")
        mode_frame.grid(row=row_idx, column=0, sticky="ew", pady=5, padx=5)
        mode_frame.grid_columnconfigure(0, weight=1)
        self.comparison_mode_var = tk.StringVar()
        ttk.Radiobutton(mode_frame, text="廣告比對 (廣告圖 vs 掃描圖)", variable=self.comparison_mode_var, value="ad_comparison").pack(anchor="w", pady=2)
        ttk.Radiobutton(mode_frame, text="互相比對 (掃描圖 vs 掃描圖)", variable=self.comparison_mode_var, value="mutual_comparison").pack(anchor="w", pady=2)
        self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測 (僅掃描圖)", variable=self.comparison_mode_var, value="qr_detection")
        self.qr_mode_radiobutton.pack(anchor="w", pady=2)
        if not self.qr_scan_feature_enabled_global:
            self.qr_mode_radiobutton.config(state=tk.DISABLED)
            if self.config.get('comparison_mode') == 'qr_detection':
                self.comparison_mode_var.set('ad_comparison')
            ttk.Label(mode_frame, text="(QR Code 檢測功能禁用，缺少依賴)", foreground="red").pack(anchor="w", padx=5)
        self.comparison_mode_var.trace_add("write", self._toggle_ad_folder_entry_state)
        
        cache_time_frame = ttk.LabelFrame(frame, text="快取與時間篩選", padding="10")
        cache_time_frame.grid(row=row_idx, column=1, columnspan=2, sticky="ew", pady=5, padx=5)
        cache_time_frame.grid_columnconfigure(1, weight=1)
        
        self.enable_time_filter_var = tk.BooleanVar()
        self.enable_time_filter_checkbox = ttk.Checkbutton(cache_time_frame, text="啟用資料夾建立時間篩選", variable=self.enable_time_filter_var)
        self.enable_time_filter_checkbox.grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        ttk.Label(cache_time_frame, text="從:").grid(row=1, column=0, sticky="w", padx=5)
        self.start_date_var = tk.StringVar()
        self.start_date_entry = ttk.Entry(cache_time_frame, textvariable=self.start_date_var, width=15)
        self.start_date_entry.grid(row=1, column=1, sticky="ew", padx=5)
        ttk.Label(cache_time_frame, text="(YYYY-MM-DD)").grid(row=1, column=2, sticky="w")
        ttk.Label(cache_time_frame, text="到:").grid(row=2, column=0, sticky="w", padx=5)
        self.end_date_var = tk.StringVar()
        self.end_date_entry = ttk.Entry(cache_time_frame, textvariable=self.end_date_var, width=15)
        self.end_date_entry.grid(row=2, column=1, sticky="ew", padx=5)
        ttk.Label(cache_time_frame, text="(YYYY-MM-DD)").grid(row=2, column=2, sticky="w")
        ttk.Button(cache_time_frame, text="重建資料夾時間快取", command=self._rebuild_folder_cache).grid(row=3, column=0, columnspan=3, sticky="w", pady=5)
        ttk.Button(cache_time_frame, text="清空圖片哈希資料庫", command=self._clear_hash_db).grid(row=4, column=0, columnspan=3, sticky="w", pady=5)
        
        row_idx += 1
        button_frame = ttk.Frame(frame, padding="10")
        button_frame.grid(row=row_idx, column=0, columnspan=3, sticky="ew", pady=10, padx=5)
        self.settings_window.style = ttk.Style()
        self.settings_window.style.configure("Accent.TButton", font=('Arial', 12, 'bold'), foreground='blue')
        self.save_button = ttk.Button(button_frame, text="保存設定", command=self._save_settings)
        self.save_button.pack(side=tk.LEFT, padx=5)
        self.start_button = ttk.Button(button_frame, text="開始執行", command=self._start_execution, style="Accent.TButton")
        self.start_button.pack(side=tk.LEFT, padx=5)
        self.cancel_button = ttk.Button(button_frame, text="取消/退出", command=self._on_closing)
        self.cancel_button.pack(side=tk.RIGHT, padx=5)

    def _load_settings_into_gui(self):
        self.root_scan_folder_entry.insert(0, self.config.get('root_scan_folder', ''))
        self.ad_folder_entry.insert(0, self.config.get('ad_folder_path', ''))
        self.extract_count_var.set(str(self.config.get('extract_count', 5)))
        excluded_folders_str = "\n".join(self.config.get('excluded_folders', []))
        self.excluded_folders_text.delete("1.0", tk.END)
        self.excluded_folders_text.insert(tk.END, excluded_folders_str)
        self.similarity_threshold_var.set(self.config.get('similarity_threshold', 95.0))
        self._update_threshold_label(self.similarity_threshold_var.get())
        comparison_mode_cfg = self.config.get('comparison_mode', 'mutual_comparison')
        self.comparison_mode_var.set(comparison_mode_cfg)
        self._toggle_ad_folder_entry_state()
        self.enable_extract_count_limit_var.set(self.config.get('enable_extract_count_limit', True))
        self._toggle_extract_count_fields()
        self.enable_time_filter_var.set(self.config.get('enable_time_filter', False))
        self.start_date_var.set(self.config.get('start_date_filter', ''))
        self.end_date_var.set(self.config.get('end_date_filter', ''))
        self._toggle_time_filter_fields()

    def _setup_bindings(self):
        self.enable_time_filter_var.trace_add("write", lambda *args: self._toggle_time_filter_fields())
        self.enable_extract_count_limit_var.trace_add("write", lambda *args: self._toggle_extract_count_fields())

    def _toggle_time_filter_fields(self):
        is_enabled = self.enable_time_filter_var.get()
        state = tk.NORMAL if is_enabled else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)
        if not is_enabled:
            self.start_date_var.set("")
            self.end_date_var.set("")

    def _toggle_extract_count_fields(self):
        is_enabled = self.enable_extract_count_limit_var.get()
        state = tk.NORMAL if is_enabled else tk.DISABLED
        self.extract_count_spinbox.config(state=state)

    def _browse_folder(self, entry_widget):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, folder_selected)

    def _update_threshold_label(self, val):
        self.threshold_label.config(text=f"{round(float(val)):d}%")

    def _validate_date(self, date_str):
        if not date_str: return True
        try:
            datetime.datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def _toggle_ad_folder_entry_state(self, *args):
        selected_mode = self.comparison_mode_var.get()
        state = tk.NORMAL if selected_mode == "ad_comparison" else tk.DISABLED
        self.ad_folder_entry.config(state=state)

    def _save_settings(self):
        try:
            extract_count_val = 0
            if self.enable_extract_count_limit_var.get():
                try:
                    extract_count_val = int(self.extract_count_var.get())
                    if extract_count_val <= 0:
                        messagebox.showerror("輸入錯誤", "提取末尾圖片數量必須是大於0的整數！")
                        return False
                except ValueError:
                    messagebox.showerror("輸入錯誤", "提取末尾圖片數量必須是有效數字！")
                    return False
            config_to_save = {
                'root_scan_folder': self.root_scan_folder_entry.get().strip(),
                'ad_folder_path': self.ad_folder_entry.get().strip(),
                'extract_count': extract_count_val,
                'enable_extract_count_limit': self.enable_extract_count_limit_var.get(),
                'excluded_folders': [f.strip() for f in self.excluded_folders_text.get("1.0", tk.END).splitlines() if f.strip()],
                'similarity_threshold': self.similarity_threshold_var.get(),
                'comparison_mode': self.comparison_mode_var.get(),
                'enable_time_filter': self.enable_time_filter_var.get(),
                'start_date_filter': self.start_date_var.get(),
                'end_date_filter': self.end_date_var.get(),
            }
            if not config_to_save["root_scan_folder"] or not os.path.isdir(config_to_save["root_scan_folder"]):
                messagebox.showerror("錯誤", "漫畫掃描根資料夾無效或不存在！")
                return False
            if config_to_save["comparison_mode"] == "ad_comparison" and (not config_to_save["ad_folder_path"] or not os.path.isdir(config_to_save["ad_folder_path"])):
                messagebox.showerror("錯誤", "在廣告比對模式下，廣告圖片資料夾無效或不存在！")
                return False
            if config_to_save["enable_time_filter"]:
                if not self._validate_date(config_to_save["start_date_filter"]) or not self._validate_date(config_to_save["end_date_filter"]):
                    messagebox.showerror("輸入錯誤", "日期格式無效。請使用YYYY-MM-DD 格式。")
                    return False
                if config_to_save["start_date_filter"] and config_to_save["end_date_filter"]:
                    try:
                        start_dt = datetime.datetime.strptime(config_to_save["start_date_filter"], "%Y-%m-%d")
                        end_dt = datetime.datetime.strptime(config_to_save["end_date_filter"], "%Y-%m-%d")
                        if start_dt > end_dt:
                            messagebox.showerror("日期錯誤", "開始日期不能晚於結束日期。")
                            return False
                    except ValueError:
                        messagebox.showerror("日期錯誤", "日期格式錯誤，請檢查。")
                        return False
            save_config(config_to_save, self.config_file_path)
            self.config = config_to_save
            messagebox.showinfo("設定已保存", "您的設定已成功保存。")
            return True
        except ValueError:
            messagebox.showerror("輸入錯誤", "相似度閾值必須是有效數字！")
            return False
        except Exception as e:
            log_error(f"保存或處理設定時發生錯誤: {e}", include_traceback=True)
            messagebox.showerror("錯誤", f"保存或處理設定時發生錯誤: {e}\n{traceback.format_exc()}")
            return False

    def _rebuild_folder_cache(self):
        if messagebox.askyesno("重建快取", "這將清空並重建資料夾建立時間快取，確定要繼續嗎？"):
            self.rebuild_folder_cache_result = True
            messagebox.showinfo("快取重建提示", "資料夾建立時間快取已標記為需要重建。下次運行程式時將自動處理此操作。")
    
    def _clear_hash_db(self):
        if os.path.exists(HASH_DB_DIR):
            if messagebox.askyesno("清空資料庫", f"這將永久刪除整個 '{HASH_DB_DIR}' 資料夾及其所有內容，下次運行將需要重新掃描所有圖片。\n\n確定要繼續嗎？"):
                try:
                    shutil.rmtree(HASH_DB_DIR)
                    messagebox.showinfo("操作成功", f"圖片哈希資料庫 '{HASH_DB_DIR}' 已被成功刪除。")
                except Exception as e:
                    log_error(f"刪除哈希資料庫失敗: {e}", include_traceback=True)
                    messagebox.showerror("操作失敗", f"删除 '{HASH_DB_DIR}' 時發生錯誤:\n{e}")
        else:
            messagebox.showinfo("提示", f"圖片哈希資料庫 '{HASH_DB_DIR}' 不存在，無需清空。")

    def _start_execution(self):
        if self._save_settings():
            self.result_config = {
                'config': self.config,
                'rebuild_folder_cache': self.rebuild_folder_cache_result
            }
            self.settings_window.destroy()
            self.on_start_callback(self.result_config)

    def _on_closing(self):
        if messagebox.askokcancel("關閉程式", "確定要關閉設定視窗並退出程式嗎？"):
            self.settings_window.destroy()
            self.master.destroy()

class MainWindow:
    # ... 此類在 v12.0.5 中已修復，此處僅對 _populate_listbox 進行重構 ...
    def __init__(self, master, all_file_data, similar_files=None, comparison_mode="N/A", initial_similarity_threshold=95.0):
        self.root = master
        self.all_file_data = all_file_data
        self.all_similar_files = similar_files if similar_files is not None else []
        self.grouped_similar_files = {}
        self.selected_files = set()
        self.comparison_mode = comparison_mode
        self.current_display_threshold = tk.DoubleVar(value=initial_similarity_threshold)
        
        self._after_id = None
        self._preview_delay = 250 

        self.banned_ad_images = set()
        self.pil_img_target = None
        self.pil_img_compare = None
        self.img_tk_target = None
        self.img_tk_compare = None
        
        try:
            self.root.title(f"{APP_NAME_TC} v{APP_VERSION} - 比對結果")
            self.root.geometry("1600x900")
            
            self.bold_font = self._create_bold_font(self.root)
            
            self.root.lift()
            self.root.focus_force()
            self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
            self._create_widgets()
            self._populate_listbox()
            self._bind_keys()
            self.root.update_idletasks()
            print("MainWindow: 介面已成功建立並初始化。", flush=True)
            if self.grouped_similar_files:
                children = self.tree.get_children()
                if children:
                    first_parent_id = children[0]
                    self.tree.selection_set(first_parent_id)
                    self.tree.focus(first_parent_id)
                    self._on_item_select(None)
        except Exception as e:
            log_error(f"MainWindow 初始化失敗: {e}\n{traceback.format_exc()}", include_traceback=True)
            messagebox.showerror("GUI 錯誤", f"無法啟動圖片比對結果介面: {e}\n請查看 error_log.txt 獲取詳細信息。")
            if self.root: self.root.destroy()
            sys.exit(1)

    def _create_bold_font(self, master):
        try:
            font_options = master.tk.call('font', 'actual', 'TkDefaultFont')
            family_index = font_options.index('-family')
            font_family = font_options[family_index + 1]
            size_index = font_options.index('-size')
            font_size = abs(int(font_options[size_index + 1]))
            return (font_family, font_size, 'bold')
        except (ValueError, IndexError, tk.TclError) as e:
            log_error(f"無法解析系統預設字體，將使用後備方案: {e}", include_traceback=False)
            return ("TkDefaultFont", 9, 'bold')

    def _create_widgets(self):
        for widget in self.root.winfo_children():
            widget.destroy()
        main_pane = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        left_frame = ttk.Frame(main_pane)
        main_pane.add(left_frame, weight=3)
        right_frame = ttk.Frame(main_pane)
        main_pane.add(right_frame, weight=2)
        
        columns = ("checkbox", "filename", "path", "count", "size", "ctime", "similarity")
        self.tree = ttk.Treeview(left_frame, columns=columns, show="headings", selectmode="extended")
        
        self.tree.heading("checkbox", text="")
        self.tree.heading("filename", text="群組 - 重複圖片")
        self.tree.heading("path", text="路徑")
        self.tree.heading("count", text="重複")
        self.tree.heading("size", text="大小")
        self.tree.heading("ctime", text="建立日期")
        self.tree.heading("similarity", text="相符")

        self.tree.column("checkbox", width=40, minwidth=40, stretch=tk.NO, anchor=tk.W)
        self.tree.column("filename", width=300, minwidth=250, stretch=tk.YES, anchor=tk.W)
        self.tree.column("path", width=300, minwidth=250, stretch=tk.YES, anchor=tk.W)
        self.tree.column("count", width=50, minwidth=50, stretch=tk.NO, anchor=tk.CENTER)
        self.tree.column("size", width=100, minwidth=90, stretch=tk.NO, anchor=tk.E)
        self.tree.column("ctime", width=150, minwidth=140, stretch=tk.NO, anchor=tk.CENTER)
        self.tree.column("similarity", width=60, minwidth=60, stretch=tk.NO, anchor=tk.CENTER)
        
        self.tree.tag_configure('child_item', foreground='#555555')
        self.tree.tag_configure('source_copy_item', background='lightyellow')
        self.tree.tag_configure('ad_parent_item', font=self.bold_font)

        vscroll = ttk.Scrollbar(left_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vscroll.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select)
        self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection)
        self.tree.bind("<Return>", self._toggle_selection)
        self.tree.bind("<Delete>", self._delete_selected_from_disk)
        self.tree.bind("<BackSpace>", self._delete_selected_from_disk)
        
        self.tree.bind("<Motion>", self._on_mouse_motion)
        self.tooltip = None

        right_pane = ttk.Panedwindow(right_frame, orient=tk.VERTICAL)
        right_pane.pack(fill=tk.BOTH, expand=True)

        self.target_image_frame = ttk.LabelFrame(right_pane, text="選中圖片預覽", padding="10")
        right_pane.add(self.target_image_frame, weight=1)
        self.target_image_label = ttk.Label(self.target_image_frame, cursor="hand2")
        self.target_image_label.pack(fill=tk.BOTH, expand=True)
        self.target_path_label = ttk.Label(self.target_image_frame, text="", wraplength=600)
        self.target_path_label.pack(fill=tk.X)
        self.target_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=True))
        
        self.compare_image_frame = ttk.LabelFrame(right_frame, text="群組代表圖片預覽", padding="10")
        right_pane.add(self.compare_image_frame, weight=1)
        self.compare_image_label = ttk.Label(self.compare_image_frame, cursor="hand2")
        self.compare_image_label.pack(fill=tk.BOTH, expand=True)
        self.compare_path_label = ttk.Label(self.compare_image_frame, text="", wraplength=600)
        self.compare_path_label.pack(fill=tk.X)
        self.compare_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=False))

        self.target_image_label.bind("<Configure>", self._on_preview_resize)
        self.compare_image_label.bind("<Configure>", self._on_preview_resize)

        self._create_context_menu()
        
        bottom_button_container = ttk.Frame(self.root)
        bottom_button_container.pack(fill=tk.X, expand=False, padx=10, pady=10)
        button_frame = ttk.Frame(bottom_button_container)
        button_frame.pack(fill=tk.X, expand=True, padx=5, pady=5)
        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="選取建議刪除項", command=self._select_suggested_for_deletion).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="刪除選中(到回收桶)", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="開啟資源回收桶", command=self._open_recycle_bin).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="開啟選中資料夾", command=self._open_selected_folder_single).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="關閉", command=self._on_closing).pack(side=tk.RIGHT, padx=5, pady=5)
        
        filter_frame = ttk.LabelFrame(bottom_button_container, text="相似度篩選 (新架構下僅為顯示)", padding="10")
        filter_frame.pack(fill=tk.X, expand=True, padx=5, pady=5)
        ttk.Label(filter_frame, text="最小相似度 (%):").pack(side=tk.LEFT, pady=2)
        ttk.Scale(filter_frame, from_=80, to=100, orient="horizontal", variable=self.current_display_threshold, length=200, command=self._update_display_threshold).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.display_threshold_label = ttk.Label(filter_frame, text=f"{self.current_display_threshold.get():.1f}%")
        self.display_threshold_label.pack(side=tk.LEFT, padx=5)
        self.current_display_threshold.trace_add("write", self._update_display_threshold)

    def _on_mouse_motion(self, event):
        item_id = self.tree.identify_row(event.y)
        if hasattr(self, 'tooltip_item_id') and self.tooltip_item_id == item_id:
            return

        if self.tooltip:
            self.tooltip.leave()
            self.tooltip = None
        
        self.tooltip_item_id = item_id
        if item_id:
            tags = self.tree.item(item_id, "tags")
            if 'ad_parent_item' in tags:
                self.tooltip = Tooltip(self.tree, "廣告圖片 (基準，不會被刪除)")
                self.tooltip.enter(event)

    def _create_context_menu(self):
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="臨時隱藏此群組", command=self._ban_ad_image)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="取消所有隱藏", command=self._unban_all_ads)

    def _show_context_menu(self, event):
        item_id = self.tree.identify_row(event.y)
        if not item_id: return
        
        if self.comparison_mode == 'ad_comparison' or self.comparison_mode == 'mutual_comparison':
            try:
                self.context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                self.context_menu.grab_release()

    def _ban_ad_image(self):
        selected_items = self.tree.selection()
        if not selected_items: return
        
        item_id = selected_items[0]
        parent_id = self.tree.parent(item_id) or item_id
        
        tags = self.tree.item(parent_id, "tags")
        if 'parent_item' in tags or 'ad_parent_item' in tags:
            key_to_ban = tags[1]
            if key_to_ban:
                self.banned_ad_images.add(key_to_ban)
                print(f"已臨時隱藏群組: {os.path.basename(key_to_ban)}", flush=True)
                self._populate_listbox()
    
    def _unban_all_ads(self):
        if not self.banned_ad_images:
            messagebox.showinfo("提示", "目前沒有被隱藏的群組。")
            return
        self.banned_ad_images.clear()
        print("已取消所有臨時隱藏。", flush=True)
        self._populate_listbox()

    def _update_display_threshold(self, *args):
        if hasattr(self, '_update_threshold_lock') and self._update_threshold_lock:
            return
        self._update_threshold_lock = True
        try:
            current_val = self.current_display_threshold.get()
            self.display_threshold_label.config(text=f"{round(current_val):d}%")
        finally:
            self._update_threshold_lock = False
    
    def _populate_listbox(self):
        self.tree.delete(*self.tree.get_children())
        
        current_selection = self.selected_files.copy()
        self.selected_files.clear()
        
        # 1. 建立鄰接表和所有節點的集合
        adj = {}
        nodes = set()
        for path1, path2, _ in self.all_similar_files:
            nodes.add(path1)
            nodes.add(path2)
            adj.setdefault(path1, set()).add(path2)
            adj.setdefault(path2, set()).add(path1)

        # 2. 尋找所有連通分量（即，獨立的重複群組）
        visited = set()
        all_components = []
        for node in nodes:
            if node not in visited:
                component = set()
                q = deque([node])
                visited.add(node)
                while q:
                    current = q.popleft()
                    component.add(current)
                    for neighbor in adj.get(current, set()):
                        if neighbor not in visited:
                            visited.add(neighbor)
                            q.append(neighbor)
                if len(component) > 1:
                    all_components.append(sorted(list(component)))
        
        # 3. 遍歷每個群組並顯示
        group_count = 0
        item_count = 0
        unique_id_counter = 0

        sorted_components = sorted(all_components, key=lambda x: x[0])

        for component in sorted_components:
            group_key = component[0]
            if group_key in self.banned_ad_images: continue
            
            children_paths = component[1:]
            
            group_count += 1
            item_count += len(component)

            parent_id = f"group_{abs(hash(group_key))}"
            
            checkbox_val = "☐"
            parent_tags = ('parent_item', group_key)
            is_ad_group = self.comparison_mode == 'ad_comparison'

            if is_ad_group:
                parent_tags = ('ad_parent_item', group_key)

            self.tree.insert("", "end", iid=parent_id, 
                             values=(
                                 checkbox_val if not is_ad_group else "",
                                 os.path.basename(group_key),
                                 os.path.dirname(group_key),
                                 len(children_paths),
                                 "N/A", "N/A", "100%"
                             ), 
                             tags=parent_tags, open=True)

            child_paths_in_group = set(component)
            selected_children_in_group = child_paths_in_group.intersection(current_selection)

            if not is_ad_group and len(selected_children_in_group) == len(child_paths_in_group) and child_paths_in_group:
                 self.tree.set(parent_id, column="checkbox", value="☑")
            elif not is_ad_group:
                 self.tree.set(parent_id, column="checkbox", value="☐")

            for path in component:
                is_child_selected = path in current_selection
                if is_child_selected: self.selected_files.add(path)

                child_tags = ['child_item', path, group_key]
                is_source = (path == group_key)
                if is_source:
                    child_tags.append('source_copy_item')

                item_id = f"item_{unique_id_counter}"
                filename_display = f"  └─ {os.path.basename(path)}"

                self.tree.insert(parent_id, "end", iid=item_id, 
                                 values=(
                                     "☑" if is_child_selected else "☐",
                                     filename_display,
                                     os.path.dirname(path), "",
                                     "N/A", "N/A", "100%"
                                 ),
                                 tags=tuple(child_tags))
                unique_id_counter += 1
        
        banned_count = len(self.banned_ad_images)
        banned_info = f" (另有 {banned_count} 個群組被隱藏)" if banned_count > 0 else ""
        print(f"列表刷新完成。顯示 {group_count} 個群組, {item_count} 個項目。{banned_info}", flush=True)


    def _on_treeview_click(self, event):
        item_id = self.tree.identify_row(event.y)
        if not item_id: return
        
        column_id = self.tree.identify_column(event.x)
        if column_id == "#1":
            tags = self.tree.item(item_id, "tags")
            if 'ad_parent_item' not in tags and 'source_copy_item' not in tags:
                self._toggle_selection_by_item_id(item_id)
        else:
            self.tree.selection_set(item_id)
            self.tree.focus(item_id)

    def _on_item_select(self, event):
        if self._after_id:
            self.root.after_cancel(self._after_id)
        
        self._after_id = self.root.after(self._preview_delay, self._load_and_display_selected_image)

    def _load_and_display_selected_image(self):
        self._after_id = None
        selected_items = self.tree.selection()
        if not selected_items:
            self.pil_img_target = self.pil_img_compare = None
            self.target_image_label.config(image="")
            self.compare_image_label.config(image="")
            self.target_path_label.config(text="")
            self.compare_path_label.config(text="")
            return

        item_id = selected_items[0]
        tags = self.tree.item(item_id, "tags")

        selected_path, compare_path = None, None
        
        if 'parent_item' in tags or 'ad_parent_item' in tags:
            selected_path = tags[1]
            compare_path = selected_path
        elif 'child_item' in tags:
            selected_path = tags[1]
            compare_path = tags[2]

        self.pil_img_target = self._load_pil_image(selected_path, self.target_path_label) if selected_path else None
        
        if compare_path and compare_path != "N/A":
            self.pil_img_compare = self._load_pil_image(compare_path, self.compare_path_label)
        else:
            self.pil_img_compare = None
            self.compare_image_label.config(image="")
            self.compare_path_label.config(text="（無比對目標）")

        self._update_all_previews()

    def _load_pil_image(self, image_path, path_label_widget):
        try:
            with Image.open(image_path) as img:
                img = ImageOps.exif_transpose(img)
                path_label_widget.config(text=f"路徑: {image_path}")
                return img.copy()
        except Exception as e:
            error_text = f"無法載入圖片: {os.path.basename(image_path)}"
            if isinstance(e, FileNotFoundError): error_text = f"圖片文件未找到: {image_path}"
            elif isinstance(e, UnidentifiedImageError): error_text = f"圖片格式無法識別或文件已損壞: {image_path}"
            path_label_widget.config(text=error_text)
            log_error(f"載入圖片預覽失敗 '{image_path}': {e}", include_traceback=False)
            return None

    def _update_all_previews(self):
        self._resize_and_display(self.target_image_label, self.pil_img_target, is_target=True)
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, is_target=False)

    def _on_preview_resize(self, event):
        widget = event.widget
        if widget == self.target_image_label:
            self._resize_and_display(widget, self.pil_img_target, is_target=True)
        elif widget == self.compare_image_label:
            self._resize_and_display(widget, self.pil_img_compare, is_target=False)

    def _resize_and_display(self, label_widget, pil_image, is_target):
        if pil_image is None:
            label_widget.config(image="")
            if is_target: self.img_tk_target = None
            else: self.img_tk_compare = None
            return

        width = label_widget.winfo_width()
        height = label_widget.winfo_height()
        if width <= 1 or height <= 1: return
            
        try:
            img_copy = pil_image.copy()
            img_copy.thumbnail((width - 10, height - 10), Image.Resampling.LANCZOS)
            img_tk = ImageTk.PhotoImage(img_copy)
            
            label_widget.config(image=img_tk)
            if is_target: self.img_tk_target = img_tk
            else: self.img_tk_compare = img_tk
        except Exception as e:
            log_error(f"預覽圖縮放時出錯: {e}", include_traceback=True)

    def _on_preview_image_click(self, event, is_target_image):
        path_label = self.target_path_label if is_target_image else self.compare_path_label
        full_path_text = path_label.cget("text")
        if full_path_text.startswith("路徑: "):
            image_path = full_path_text[len("路徑: "):].strip()
            if image_path and os.path.exists(image_path):
                self._open_folder(os.path.dirname(image_path))
            else:
                messagebox.showwarning("路徑無效", "無法開啟資料夾，圖片路徑無效或未載入。")
        else:
            messagebox.showwarning("路徑無效", "無法開啟資料夾，圖片路徑無效或未載入。")

    def _bind_keys(self):
        self.tree.bind("<Up>", self._navigate_image)
        self.tree.bind("<Down>", self._navigate_image)
    
    def _navigate_image(self, event):
        current_selection = self.tree.selection()
        if not current_selection: return "break"
        
        current_item = current_selection[0]
        item_to_select = None

        if event.keysym == "Down":
            parent = self.tree.parent(current_item)
            if not parent and self.tree.item(current_item, "open"):
                children = self.tree.get_children(current_item)
                if children:
                    item_to_select = children[0]
            elif parent:
                siblings = self.tree.get_children(parent)
                current_index = siblings.index(current_item)
                if current_index < len(siblings) - 1:
                    item_to_select = siblings[current_index + 1]
                else: 
                    item_to_select = self.tree.next(parent)
            if not item_to_select:
                item_to_select = self.tree.next(current_item)

        elif event.keysym == "Up":
            parent = self.tree.parent(current_item)
            if parent:
                siblings = self.tree.get_children(parent)
                if siblings and current_item == siblings[0]:
                    item_to_select = parent
            if not item_to_select:
                item_to_select = self.tree.prev(current_item)

        if item_to_select:
            self.tree.selection_set(item_to_select)
            self.tree.focus(item_to_select)
            self.tree.see(item_to_select)
            
        return "break"

    def _toggle_selection_by_item_id(self, item_id):
        if not self.tree.exists(item_id): return
        tags = self.tree.item(item_id, "tags")

        if 'parent_item' in tags or 'ad_parent_item' in tags:
            child_items = self.tree.get_children(item_id)
            current_parent_check_state = self.tree.set(item_id, "checkbox")
            select_all = (current_parent_check_state == "☐")
            
            self.tree.set(item_id, column="checkbox", value="☑" if select_all else "☐")
            
            for child_id in child_items:
                child_tags = self.tree.item(child_id, "tags")
                # 確保不選中原始圖片
                if 'source_copy_item' not in child_tags:
                    path_to_toggle = child_tags[1]
                    if select_all:
                        self.selected_files.add(path_to_toggle)
                        self.tree.set(child_id, column="checkbox", value="☑")
                    else:
                        self.selected_files.discard(path_to_toggle)
                        self.tree.set(child_id, column="checkbox", value="☐")

        elif 'child_item' in tags and 'source_copy_item' not in tags:
            path_to_toggle = tags[1]
            if path_to_toggle in self.selected_files:
                self.selected_files.remove(path_to_toggle)
                self.tree.set(item_id, column="checkbox", value="☐")
            else:
                self.selected_files.add(path_to_toggle)
                self.tree.set(item_id, column="checkbox", value="☑")
            
            parent_id = self.tree.parent(item_id)
            if parent_id and 'ad_parent_item' not in self.tree.item(parent_id, "tags"):
                all_child_ids = self.tree.get_children(parent_id)
                num_selectable = sum(1 for cid in all_child_ids if 'source_copy_item' not in self.tree.item(cid, "tags"))
                num_selected = sum(1 for cid in all_child_ids if self.tree.set(cid, "checkbox") == "☑")
                
                all_children_selected = num_selectable > 0 and num_selected == num_selectable
                self.tree.set(parent_id, column="checkbox", value="☑" if all_children_selected else "☐")

    def _toggle_selection(self, event=None):
        for item_id in self.tree.selection():
            self._toggle_selection_by_item_id(item_id)

    def _update_all_checkboxes_based_on_selection_set(self):
        for parent_id in self.tree.get_children(""):
            all_child_ids = self.tree.get_children(parent_id)
            if not all_child_ids: continue
            
            num_selected = 0
            num_selectable = 0
            for child_id in all_child_ids:
                tags = self.tree.item(child_id, "tags")
                path = tags[1]
                if 'source_copy_item' not in tags:
                    num_selectable += 1
                    if path in self.selected_files:
                        self.tree.set(child_id, column="checkbox", value="☑")
                        num_selected += 1
                    else:
                        self.tree.set(child_id, column="checkbox", value="☐")
                else: # 原始圖片永遠不應被勾選
                    self.tree.set(child_id, column="checkbox", value="☐")

            parent_tags = self.tree.item(parent_id, "tags")
            if 'ad_parent_item' not in parent_tags:
                all_children_selected = num_selectable > 0 and num_selected == num_selectable
                self.tree.set(parent_id, column="checkbox", value="☑" if all_children_selected else "☐")

    def _select_all(self):
        new_selection = set()
        for parent_id in self.tree.get_children(""):
            for child_id in self.tree.get_children(parent_id):
                child_tags = self.tree.item(child_id, "tags")
                if 'child_item' in child_tags and 'source_copy_item' not in child_tags:
                    new_selection.add(child_tags[1])
        self.selected_files = new_selection
        self._update_all_checkboxes_based_on_selection_set()
        print("已選擇所有可刪除項目。", flush=True)

    def _select_suggested_for_deletion(self):
        self.selected_files.clear()
        for parent_id in self.tree.get_children(""):
            for child_id in self.tree.get_children(parent_id):
                child_tags = self.tree.item(child_id, "tags")
                path = child_tags[1]
                
                if 'source_copy_item' not in child_tags:
                    self.selected_files.add(path)
        
        self._update_all_checkboxes_based_on_selection_set()
        print(f"已自動選取 {len(self.selected_files)} 個建議刪除的項目。", flush=True)

    def _deselect_all(self):
        self.selected_files.clear()
        self._update_all_checkboxes_based_on_selection_set()
        print("已取消選擇所有項目。", flush=True)

    def _invert_selection(self):
        all_item_paths = set()
        for parent_id in self.tree.get_children(""):
            for child_id in self.tree.get_children(parent_id):
                child_tags = self.tree.item(child_id, "tags")
                if 'child_item' in child_tags and 'source_copy_item' not in child_tags:
                    all_item_paths.add(child_tags[1])

        self.selected_files = all_item_paths - self.selected_files
        self._update_all_checkboxes_based_on_selection_set()
        print("已反轉選擇。", flush=True)

    def _delete_selected_from_disk(self):
        if not self.selected_files:
            messagebox.showinfo("提示", "沒有選中的圖片。")
            return
        
        if not messagebox.askyesno("確認刪除", f"確定要將這 {len(self.selected_files)} 個選中的圖片移至資源回收桶嗎？"):
            return
        
        deleted_paths = []
        failed_paths = []
        for path1 in list(self.selected_files):
            try:
                abs_path = os.path.abspath(path1)
                send2trash.send2trash(abs_path)
                deleted_paths.append(path1)
                print(f"已移至回收桶: {abs_path}", flush=True)
            except Exception as e:
                failed_paths.append(path1)
                log_error(f"移至回收桶失敗 {path1}: {e}", include_traceback=True)
        
        if failed_paths:
            messagebox.showerror("刪除失敗", f"無法移動以下 {len(failed_paths)} 個文件至回收桶:\n" + "\n".join(failed_paths[:5]) + ("\n..." if len(failed_paths) > 5 else ""))

        if deleted_paths:
            self.all_similar_files = [
                item for item in self.all_similar_files 
                if item[0] not in deleted_paths and item[1] not in deleted_paths
            ]
            
            self.selected_files.clear()
            self._populate_listbox()
            messagebox.showinfo("刪除完成", f"成功將 {len(deleted_paths)} 個文件移至資源回收桶。")
    
    def _open_recycle_bin(self):
        system = platform.system()
        try:
            if system == "Windows":
                subprocess.run(['explorer.exe', 'shell:RecycleBinFolder'])
                print("正在嘗試開啟 Windows 資源回收桶...", flush=True)
            elif system == "Darwin":
                trash_path = os.path.expanduser("~/.Trash")
                subprocess.run(['open', trash_path], check=True)
                print("正在嘗試開啟 macOS 資源回收桶...", flush=True)
            else:
                trash_uri = "trash:/"
                subprocess.run(['xdg-open', trash_uri], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                print("正在嘗試開啟 Linux 資源回收桶...", flush=True)
        except Exception as e:
            log_error(f"開啟資源回收桶失敗: {e}", include_traceback=True)
            messagebox.showerror("開啟失敗", f"無法自動開啟資源回收桶。\n錯誤: {e}\n請手動開啟。")

    def _open_folder(self, folder_path):
            try:
                stable_path = os.path.abspath(os.path.normpath(folder_path))
                if not os.path.isdir(stable_path):
                    messagebox.showwarning("路徑無效", f"資料夾不存在或無效:\n{stable_path}")
                    return

                print(f"嘗試開啟資料夾 (最終方案): {stable_path}", flush=True)
                if sys.platform == "win32": os.startfile(stable_path)
                elif sys.platform == "darwin": subprocess.Popen(["open", stable_path])
                else: subprocess.Popen(["xdg-open", stable_path])
                print(f"已發送開啟資料夾命令: {stable_path}", flush=True)
            except Exception as e:
                log_error(f"開啟資料夾失敗 {folder_path}: {e}", include_traceback=True)
                messagebox.showerror("開啟失敗", f"無法開啟資料夾: {folder_path}\n錯誤: {e}")

    def _open_selected_folder_single(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showinfo("提示", "請先在列表中選中一個圖片。")
            return
        
        item_id = selected_items[0]
        tags = self.tree.item(item_id, "tags")
        path_to_open = None
        if tags:
            path_to_open = tags[1]

        if path_to_open and os.path.exists(path_to_open):
            self._open_folder(os.path.dirname(path_to_open))
        else:
            messagebox.showwarning("路徑無效", f"選中的圖片文件路徑不存在或無效:\n{path_to_open}")

    def _on_closing(self):
        if messagebox.askokcancel("關閉", "確定要關閉比對結果視窗嗎？"):
            if self._after_id:
                self.root.after_cancel(self._after_id)
            self.root.destroy()
            sys.exit(0)

class Application:
    # ... 此類無變更 ...
    def __init__(self, root):
        self.root = root
        self.settings_data = None
        sys.excepthook = self.custom_excepthook

    def custom_excepthook(self, exc_type, exc_value, exc_traceback):
        log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", include_traceback=True)
        if self.root.winfo_exists():
             messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt' 獲取詳細資訊。", parent=self.root)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        self.root.destroy()

    def start(self):
        settings_gui = SettingsGUI(self.root, self.run_core_logic)
        self.root.mainloop()

    def run_core_logic(self, settings_data):
        self.settings_data = settings_data
        main_app_config = self.settings_data['config']
        
        folder_creation_cache_manager = FolderCreationCacheManager()
        if self.settings_data['rebuild_folder_cache']:
            folder_creation_cache_manager.invalidate_cache()
            folder_creation_cache_manager.save_cache()
            print("資料夾建立時間快取已清空。下次運行時將重新建立。", flush=True)

        start_date_dt, end_date_dt = None, None
        if main_app_config.get('enable_time_filter'):
            try:
                if main_app_config.get('start_date_filter'):
                    start_date_dt = datetime.datetime.strptime(main_app_config['start_date_filter'], "%Y-%m-%d")
                if main_app_config.get('end_date_filter'):
                    end_date_dt = datetime.datetime.strptime(main_app_config['end_date_filter'], "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                log_error("時間篩選日期格式錯誤，將禁用時間篩選。", include_traceback=False)
                messagebox.showwarning("日期格式錯誤", "時間篩選日期格式不正確，將禁用時間篩選。", parent=self.root)
                main_app_config['enable_time_filter'] = False

        try:
            engine = ImageComparisonEngine(
                root_scan_folder=main_app_config['root_scan_folder'],
                ad_folder_path=main_app_config['ad_folder_path'],
                extract_count=main_app_config['extract_count'],
                excluded_folders=main_app_config['excluded_folders'],
                enable_time_filter=main_app_config['enable_time_filter'],
                start_date_filter=start_date_dt,
                end_date_filter=end_date_dt,
                similarity_threshold=main_app_config['similarity_threshold'],
                comparison_mode=main_app_config['comparison_mode'],
                system_qr_scan_capability=QR_SCAN_ENABLED,
                enable_extract_count_limit=main_app_config['enable_extract_count_limit'],
            )

            similar_files, all_file_data = engine.find_duplicates()

            if similar_files:
                self.root.deiconify()
                MainWindow(self.root, all_file_data, similar_files, engine.comparison_mode, initial_similarity_threshold=main_app_config['similarity_threshold'])
            else:
                messagebox.showinfo("掃描結果", "未找到重複或廣告圖片，或沒有檢測到 QR Code。", parent=self.root)
                self.root.destroy()
            
            print("任務完成。", flush=True)

        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", include_traceback=True)
            messagebox.showerror("執行錯誤", f"程式執行時發生錯誤: {e}", parent=self.root)
            self.root.destroy()


def main():
    if sys.platform.startswith('win'):
        try:
            set_start_method('spawn', force=True)
            print("多進程啟動方法已設置為 'spawn'。", flush=True)
        except RuntimeError:
            print("多進程啟動方法已在其他地方設置，或無法設置。", flush=True)
        except Exception as e:
            log_error(f"設置多進程啟動方法時發生錯誤: {e}", include_traceback=True)
    
    print(f"=== {APP_NAME_TC} v{APP_VERSION} - 啟動中 ===", flush=True)
    
    root = tk.Tk()
    root.title(f"{APP_NAME_TC} v{APP_VERSION} - 主控台")
    root.geometry("400x50")
    
    try:
        check_and_install_packages()
    except SystemExit:
        root.destroy()
        return

    app = Application(root)
    app.start()

if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()