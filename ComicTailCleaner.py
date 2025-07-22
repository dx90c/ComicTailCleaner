# ======================================================================
# 檔案名稱：ComicTailCleaner_v12.7.4.py
# 版本號：12.7.4
# 專案名稱：ComicTailCleaner (漫畫尾頁廣告清理)
#
# === 程式說明 ===
# 一個專為清理漫畫檔案尾頁廣告或重複頁面的工具。
# 它能高效地掃描大量漫畫檔案，並通過感知哈希算法找出內容上
# 相似或完全重複的圖片，提升漫畫閱讀體驗。
#
# === 12.7.4 版本更新內容 ===
# - 【核心 Bug 修正】修復了 QR 混合模式 (啟用廣告庫快速匹配) 下因 `lambda` 函數
#   無法序列化而導致的多進程錯誤，恢復了該模式的完整功能。
#
# === 12.7.3 版本更新內容 ===
# - 【穩定性】將「重建快取」功能從掃描流程中剝離，改為獨立的「清理快取」按鈕，
#   從根本上解決了相關的緒死鎖和無響應 BUG。
# - 【性能調優】為「互相比對」的並行比對階段設定 CPU 冗餘 (保留4核心)，
#   在提升速度的同時確保系統響應流暢。
# - 【功能增強】引入可選依賴 `psutil`，實現詳細的性能日誌記錄(CPU/記憶體)。
# ======================================================================

# === 1. 標準庫導入 (Python Built-in Libraries) ===
import os
import sys
import json
import shutil
import datetime
import traceback
import subprocess
from collections import deque, defaultdict
from multiprocessing import set_start_method, Pool, cpu_count
import platform
import threading
import time
from queue import Queue, Empty
import hashlib

# === 2. 第三方庫導入 (Third-party Libraries) ===
from PIL import Image, ImageTk, ImageOps, ImageDraw, UnidentifiedImageError

try:
    import pkg_resources
except ImportError:
    pkg_resources = None

try:
    import imagehash
except ImportError:
    pass

try:
    import cv2
    import numpy as np
except ImportError:
    pass

try:
    import send2trash
except ImportError:
    pass

try:
    import psutil
except ImportError:
    psutil = None

# === 3. Tkinter GUI 庫導入 ===
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from tkinter import messagebox

# === 4. 全局常量和設定 ===
APP_VERSION = "12.7.4"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"
CONFIG_FILE = "config.json"
QR_SCAN_ENABLED = False
PERFORMANCE_LOGGING_ENABLED = False

# Global context for worker processes
_WORKER_CONTEXT = {}

# === 5. 工具函數 (Helper Functions) ===
def log_error(message: str, include_traceback: bool = False):
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

def log_info(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] INFO: {message}\n"
    print(log_content, end='', flush=True)
    try:
        with open("info_log.txt", "a", encoding="utf-8", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"Failed to write to info log: {e}", flush=True)

def log_performance(message: str):
    performance_info = ""
    if PERFORMANCE_LOGGING_ENABLED and psutil:
        process = psutil.Process(os.getpid())
        cpu_percent = process.cpu_percent(interval=0.1)
        memory_mb = process.memory_info().rss / (1024 * 1024)
        performance_info = f" (CPU: {cpu_percent:.1f}%, Mem: {memory_mb:.1f} MB)"
    log_info(f"{message}{performance_info}")

def check_and_install_packages():
    print("正在檢查必要的 Python 套件...", flush=True)
    
    required = {'Pillow': 'Pillow>=9.0.0', 'imagehash': 'imagehash>=4.2.1', 'send2trash': 'send2trash>=1.8.0'}
    optional = {
        'opencv-python': 'opencv-python>=4.5.0',
        'numpy': 'numpy>=1.20.0',
        'psutil': 'psutil>=5.8.0'
    }
    
    missing_core = []
    missing_optional = []

    if pkg_resources:
        for name, req_str in required.items():
            try: pkg_resources.require(req_str)
            except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict): missing_core.append(name)
        for name, req_str in optional.items():
            try: pkg_resources.require(req_str)
            except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict): missing_optional.append(name)
    else:
        try: from PIL import Image; Image.new('RGB', (1, 1))
        except (ImportError, AttributeError): missing_core.append('Pillow')
        try: import imagehash; imagehash.average_hash(Image.new('RGB', (8, 8)))
        except (ImportError, AttributeError): missing_core.append('imagehash')
        try: import send2trash
        except (ImportError, AttributeError): missing_core.append('send2trash')
        try: import cv2; import numpy; cv2.QRCodeDetector(); numpy.array([1])
        except (ImportError, AttributeError, cv2.error): missing_optional.extend(['opencv-python', 'numpy'])
        try: import psutil; psutil.cpu_percent()
        except (ImportError, AttributeError): missing_optional.append('psutil')

    if missing_core:
        req_strings = [required[pkg] for pkg in missing_core]
        package_str = " ".join(req_strings)
        response = messagebox.askyesno(
            "缺少核心依賴",
            f"缺少必要套件：{', '.join(missing_core)}。\n\n是否嘗試自動安裝？\n（將執行命令：pip install {package_str}）",
        )
        if response:
            try:
                print(f"正在執行: {sys.executable} -m pip install {package_str}", flush=True)
                subprocess.check_call([sys.executable, "-m", "pip", "install", *req_strings])
                messagebox.showinfo("安裝成功", "核心套件安裝成功，請重新啟動程式。")
                sys.exit(0)
            except subprocess.CalledProcessError as e:
                messagebox.showerror("安裝失敗", f"自動安裝套件失敗：{e}\n請手動打開命令提示字元並執行 'pip install {package_str}'")
                sys.exit(1)
        else:
            messagebox.showerror("缺少核心依賴", f"請手動安裝必要套件：{', '.join(missing_core)}。\n命令：pip install {package_str}")
            sys.exit(1)
            
    global QR_SCAN_ENABLED, PERFORMANCE_LOGGING_ENABLED
    QR_SCAN_ENABLED = 'opencv-python' not in missing_optional and 'numpy' not in missing_optional
    PERFORMANCE_LOGGING_ENABLED = 'psutil' not in missing_optional

    if missing_optional:
        warning_message = f"缺少可選套件：{', '.join(missing_optional)}。\n\n"
        if not QR_SCAN_ENABLED:
            warning_message += "QR Code 相關功能將被禁用。\n要啟用，請安裝：pip install opencv-python>=4.5.0 numpy>=1.2.0\n\n"
        if not PERFORMANCE_LOGGING_ENABLED:
            warning_message += "性能日誌功能將被禁用。\n要啟用，請安裝：pip install psutil>=5.8.0"
        
        messagebox.showwarning("缺少可選依賴", warning_message)
        print(f"警告: 缺少 {', '.join(missing_optional)}，相關功能已禁用。", flush=True)

    print("所有必要套件檢查通過。", flush=True)

def _pool_worker_process_image(image_path: str) -> tuple[str, dict | None]:
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            phash_val = imagehash.phash(img, hash_size=8)
            stat_info = os.stat(image_path)
            return (image_path, {
                'phash': phash_val, 'size': stat_info.st_size,
                'ctime': stat_info.st_ctime, 'mtime': stat_info.st_mtime
            })
    except Exception: return (image_path, None)

def _detect_qr_on_image(img: Image.Image) -> list | None:
    img_cv = np.array(img.convert('RGB'))
    qr_detector = cv2.QRCodeDetector()
    retval, decoded_info, points, _ = qr_detector.detectAndDecodeMulti(img_cv)
    if retval and decoded_info and any(info for info in decoded_info if info):
        return points.tolist()
    return None

def _pool_worker_detect_qr_code(image_path: str, resize_size: int = 800) -> tuple[str, list | None]:
    try:
        with Image.open(image_path) as pil_img:
            pil_img = ImageOps.exif_transpose(pil_img)
            
            resized_img = pil_img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            points = _detect_qr_on_image(resized_img)
            
            if not points:
                points = _detect_qr_on_image(pil_img)
                
            return (image_path, points)
    except Exception: return (image_path, None)

def _pool_worker_qr_proxy(image_path: str) -> tuple[str, list | None]:
    resize_size = _WORKER_CONTEXT.get('qr_resize_size', 800)
    return _pool_worker_detect_qr_code(image_path, resize_size)

def _pool_worker_process_image_full(image_path: str, resize_size: int = 800) -> tuple[str, dict | None]:
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            phash_val = imagehash.phash(img, hash_size=8)
            
            resized_img = img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            qr_points_val = _detect_qr_on_image(resized_img)
            if not qr_points_val:
                qr_points_val = _detect_qr_on_image(img)
            
        stat_info = os.stat(image_path)
        return (image_path, {
            'phash': phash_val, 'qr_points': qr_points_val,
            'size': stat_info.st_size, 'ctime': stat_info.st_ctime, 'mtime': stat_info.st_mtime
        })
    except Exception: return (image_path, None)

# v12.7.4: New proxy function for QR Hybrid mode
def _pool_worker_process_image_full_hybrid(image_path: str) -> tuple[str, dict | None]:
    resize_size = _WORKER_CONTEXT.get('qr_resize_size', 800)
    return _pool_worker_process_image_full(image_path, resize_size)

def _pool_worker_compare_hashes(work_chunk) -> list:
    i_start, i_end, unique_hashes, hash_groups, max_diff = work_chunk
    found = []
    for i in range(i_start, i_end):
        hash1 = unique_hashes[i]
        for j in range(i + 1, len(unique_hashes)):
            hash2 = unique_hashes[j]
            if hash1 - hash2 <= max_diff:
                sim = (1 - (hash1 - hash2) / 64) * 100
                for path1 in hash_groups[hash1]:
                    for path2 in hash_groups[hash2]: 
                        found.append((min(path1, path2), max(path1, path2), f"{sim:.1f}%"))
    return found

# === 6. 配置管理相關函數 ===
default_config = {
    'root_scan_folder': '', 'ad_folder_path': '', 'extract_count': 5,
    'enable_extract_count_limit': True, 'excluded_folders': [],
    'comparison_mode': 'mutual_comparison', 'similarity_threshold': 98,
    'enable_time_filter': False, 'start_date_filter': '', 'end_date_filter': '',
    'enable_qr_hybrid_mode': True, 'qr_resize_size': 800,
    'worker_processes': 0
}
def load_config(config_path: str) -> dict:
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                merged_config = default_config.copy()
                merged_config.update(config); return merged_config
    except Exception: pass
    return default_config.copy()

def save_config(config: dict, config_path: str):
    try:
        config_to_save = config.copy()
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_to_save, f, indent=4, ensure_ascii=False)
    except Exception as e:
        log_error(f"保存設定檔 '{config_path}' 時發生錯誤: {e}", True)

# === 7. 快取管理類與函數 ===
class ScannedImageCacheManager:
    def __init__(self, root_scan_folder: str):
        normalized_path = os.path.normpath(root_scan_folder).replace('\\', '/')
        hash_object = hashlib.sha256(normalized_path.encode('utf-8'))
        self.cache_file_path = f"scanned_hashes_cache_{hash_object.hexdigest()}.json"
        self.cache = self._load_cache()
    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    converted_cache = {}
                    for path, data in loaded_data.items():
                        if isinstance(data, dict):
                            converted_data = data.copy()
                            for key in ['phash', 'whash']:
                                if key in converted_data and converted_data[key] is not None:
                                    try: converted_data[key] = imagehash.hex_to_hash(converted_data[key])
                                    except (TypeError, ValueError): converted_data[key] = None
                            converted_cache[path] = converted_data
                    log_info(f"掃描圖片快取 '{self.cache_file_path}' 已成功載入。")
                    return converted_cache
            except (json.JSONDecodeError, Exception):
                log_info(f"掃描圖片快取檔案 '{self.cache_file_path}' 格式不正確，將重建。")
        return {}
    def save_cache(self) -> None:
        max_retries = 3; retry_delay = 0.5
        serializable_cache = {path: {k: str(v) if isinstance(v, imagehash.ImageHash) else v for k, v in data.items()} for path, data in self.cache.items() if data}
        for attempt in range(max_retries):
            try:
                os.makedirs(os.path.dirname(self.cache_file_path) or '.', exist_ok=True)
                temp_file_path = self.cache_file_path + f".tmp{os.getpid()}"
                with open(temp_file_path, 'w', encoding='utf-8') as f:
                    json.dump(serializable_cache, f, indent=2)
                os.replace(temp_file_path, self.cache_file_path)
                log_info(f"掃描圖片快取已成功保存到 '{self.cache_file_path}'。")
                return
            except (IOError, OSError) as e:
                log_error(f"保存快取失敗 (嘗試 {attempt + 1}/{max_retries}): {e}", True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    messagebox.showerror("快取保存失敗", f"無法保存快取檔案 '{self.cache_file_path}'，請檢查檔案權限或關閉占用檔案的程式（例如防毒軟體）。\n錯誤: {e}")
                    break
    def get_data(self, file_path: str) -> dict | None:
        if file_path in self.cache:
            cached_data = self.cache[file_path]
            try:
                if abs(os.path.getmtime(file_path) - cached_data.get('mtime', 0)) < 1e-6: return cached_data
            except (FileNotFoundError, Exception): pass
        return None
    def update_data(self, file_path: str, data: dict) -> None:
        if self.cache.get(file_path):
            self.cache[file_path].update(data)
        else:
            self.cache[file_path] = data
    def invalidate_cache(self) -> None:
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try: 
                log_info(f"[快取清理] 準備將圖片快取檔案 '{self.cache_file_path}' 移至回收桶。")
                send2trash.send2trash(self.cache_file_path)
                log_info(f"[快取清理] 圖片快取檔案已成功移至回收桶。")
            except Exception as e: 
                log_error(f"刪除掃描快取檔案時發生錯誤: {e}", True)
                try:
                    os.remove(self.cache_file_path)
                    log_info(f"[快取清理] Fallback: 圖片快取檔案已被永久刪除。")
                except Exception as e2:
                    log_error(f"Fallback 刪除掃描快取檔案失敗: {e2}", True)


class FolderCreationCacheManager:
    def __init__(self, cache_file_path: str = "folder_creation_cache.json"):
        self.cache_file_path = cache_file_path
        self.cache = self._load_cache()
    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f: 
                    cache = json.load(f)
                    log_info(f"資料夾建立時間快取 '{self.cache_file_path}' 已成功載入。")
                    return cache
            except Exception as e: log_error(f"載入資料夾建立時間快取時發生錯誤: {e}", True)
        return {}
    def save_cache(self) -> None:
        max_retries = 3; retry_delay = 0.5
        for attempt in range(max_retries):
            try:
                os.makedirs(os.path.dirname(self.cache_file_path) or '.', exist_ok=True)
                temp_file_path = self.cache_file_path + f".tmp{os.getpid()}"
                with open(temp_file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, indent=2)
                os.replace(temp_file_path, self.cache_file_path)
                log_info(f"資料夾建立時間快取已成功保存到 '{self.cache_file_path}'。")
                return
            except (IOError, OSError) as e:
                log_error(f"保存資料夾快取失敗 (嘗試 {attempt + 1}/{max_retries}): {e}", True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    messagebox.showerror("快取保存失敗", f"無法保存資料夾快取檔案 '{self.cache_file_path}'，請檢查檔案權限。\n錯誤: {e}")
                    break
    def get_creation_time(self, folder_path: str) -> float | None:
        if folder_path in self.cache: return self.cache[folder_path]
        try: 
            ctime = os.path.getctime(folder_path)
            self.cache[folder_path] = ctime
            return ctime
        except Exception as e: 
            log_error(f"獲取資料夾建立時間失敗 {folder_path}: {e}")
            return None
    def invalidate_cache(self) -> None:
        self.cache = {};
        if os.path.exists(self.cache_file_path):
            try: 
                log_info(f"[快取清理] 準備將資料夾快取檔案 '{self.cache_file_path}' 移至回收桶。")
                send2trash.send2trash(self.cache_file_path)
                log_info(f"[快取清理] 資料夾快取檔案已成功移至回收桶。")
            except Exception as e: 
                log_error(f"刪除資料夾建立時間快取檔案時發生錯誤: {e}", True)
                try:
                    os.remove(self.cache_file_path)
                    log_info(f"[快取清理] Fallback: 資料夾快取檔案已被永久刪除。")
                except Exception as e2:
                    log_error(f"Fallback 刪除資料夾建立時間快取檔案失敗: {e2}", True)


# === 8. 核心工具函數 (續) ===
def get_all_subfolders(root_folder: str, excluded_folders: list[str] | None = None, progress_queue: Queue | None = None, control_events: dict | None = None, time_filter_config: dict | None = None) -> list[str]:
    all_subfolders = []
    if not os.path.isdir(root_folder): return []
    if progress_queue: progress_queue.put({'type': 'text', 'text': "開始掃描資料夾..."})
    excluded_norm_paths = {os.path.normpath(f) for f in (excluded_folders or [])}
    queue = deque([root_folder]); processed_count = 0
    creation_cache_manager = time_filter_config.get('manager') if time_filter_config else None
    while queue:
        if control_events and control_events['cancel'].is_set(): return []
        if control_events and control_events['pause'].is_set(): control_events['pause'].wait()
        current = queue.popleft()
        if any(os.path.normpath(current).startswith(ex) for ex in excluded_norm_paths): continue
        if time_filter_config and time_filter_config.get('enabled') and current != root_folder:
            folder_ctime_ts = creation_cache_manager.get_creation_time(current)
            if folder_ctime_ts:
                folder_ctime = datetime.datetime.fromtimestamp(folder_ctime_ts)
                start_date, end_date = time_filter_config['start'], time_filter_config['end']
                if (start_date and folder_ctime < start_date) or (end_date and folder_ctime > end_date):
                    continue
        all_subfolders.append(current); processed_count += 1
        if progress_queue and processed_count % 100 == 0: progress_queue.put({'type': 'text', 'text': f"掃描資料夾中... (已找到 {processed_count} 個)"})
        try:
            for entry in os.scandir(current):
                if entry.is_dir(): queue.append(entry.path)
        except OSError: pass
    if creation_cache_manager: creation_cache_manager.save_cache()
    if progress_queue: progress_queue.put({'type': 'text', 'text': f"資料夾掃描完成，共找到 {len(all_subfolders)} 個。"})
    return all_subfolders

def extract_last_n_files_from_folders(folder_paths: list[str], count: int, enable_limit: bool, progress_queue: Queue | None = None, control_events: dict | None = None) -> dict:
    extracted = {}; exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff'); total_folders = len(folder_paths)
    for i, path in enumerate(folder_paths):
        if control_events and control_events['cancel'].is_set(): return {}
        if control_events and control_events['pause'].is_set(): control_events['pause'].wait()
        try:
            files = sorted([os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(exts) and os.path.isfile(os.path.join(path, f))])
            extracted[path] = files[-count:] if enable_limit else files
        except OSError: pass
        if progress_queue and (i + 1) % 200 == 0 or (i + 1) == total_folders:
            progress_queue.put({'type': 'text', 'text': f"正在提取檔案... ({i + 1}/{total_folders})"})
    if progress_queue:
        total_files = sum(len(files) for files in extracted.values())
        progress_queue.put({'type': 'text', 'text': f"檔案提取完成，共 {total_files} 個檔案待處理。"})
    return extracted

# === 9. 核心比對引擎 ===
class ImageComparisonEngine:
    def __init__(self, config: dict, progress_queue: Queue | None = None, control_events: dict | None = None):
        self.config, self.progress_queue, self.control_events = config, progress_queue, control_events
        self.system_qr_scan_capability = QR_SCAN_ENABLED
        log_performance("[初始化] 掃描引擎")

    def _check_control(self) -> bool:
        if self.control_events:
            if self.control_events['cancel'].is_set(): return True
            if self.control_events['pause'].is_set():
                if not hasattr(self, '_paused_once'):
                    self._update_progress(text="任務已暫停..."); self._paused_once = True
                self.control_events['pause'].wait()
                if self.control_events['cancel'].is_set(): return True
                self._update_progress(text="任務已恢復...")
                if hasattr(self, '_paused_once'): del self._paused_once
        return False

    def _update_progress(self, p_type: str = 'text', value: int | None = None, text: str | None = None) -> None:
        if self.progress_queue: self.progress_queue.put({'type': p_type, 'value': value, 'text': text})

    def find_duplicates(self) -> tuple[list, dict]:
        self._update_progress(text="任務開始...")
        log_performance("[開始] 掃描任務")
        if self._check_control(): return [], {}
        time_filter_config = {'enabled': self.config.get('enable_time_filter', False)}
        if time_filter_config['enabled']:
            manager = FolderCreationCacheManager()
            time_filter_config['manager'] = manager
            try:
                time_filter_config['start'] = datetime.datetime.strptime(self.config['start_date_filter'], "%Y-%m-%d") if self.config.get('start_date_filter') else None
                time_filter_config['end'] = datetime.datetime.strptime(self.config['end_date_filter'], "%Y-%m-%d").replace(hour=23, minute=59, second=59) if self.config.get('end_date_filter') else None
            except ValueError: time_filter_config['enabled'] = False
        all_folders = get_all_subfolders(self.config['root_scan_folder'], self.config['excluded_folders'], self.progress_queue, self.control_events, time_filter_config)
        if self._check_control(): return [], {}
        log_performance("[完成] 資料夾掃描")
        files_dict = extract_last_n_files_from_folders(all_folders, self.config['extract_count'], self.config['enable_extract_count_limit'], self.progress_queue, self.control_events)
        if self._check_control(): return [], {}
        log_performance("[完成] 檔案提取")
        files_to_process = [file for files in files_dict.values() for file in files]
        if not files_to_process:
            self._update_progress(text="在指定路徑下未找到任何圖片檔案。")
            return [], {}
        scan_cache_manager = ScannedImageCacheManager(self.config['root_scan_folder'])
        if self.config['comparison_mode'] == "qr_detection":
            if self.config.get('enable_qr_hybrid_mode'):
                return self._detect_qr_codes_hybrid(files_to_process, scan_cache_manager)
            else:
                return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        else:
            return self._find_similar_images(files_to_process, scan_cache_manager)

    def _process_images_with_cache(self, file_paths: list[str], cache_manager: ScannedImageCacheManager, description: str, worker_function: callable, data_key: str) -> dict | None:
        self._update_progress(text=f"正在檢查 {len(file_paths)} 個{description}的快取...")
        file_data, paths_to_recalc, cache_hits = {}, [], 0
        for path in file_paths:
            if self._check_control(): return None
            cached_data = cache_manager.get_data(path)
            if cached_data and data_key in cached_data:
                file_data[path] = cached_data; cache_hits += 1
            else:
                paths_to_recalc.append(path)
                if cached_data: file_data[path] = cached_data
        hit_rate = (cache_hits / len(file_paths) * 100) if file_paths else 100
        log_info(f"快取檢查 ({description}) - 命中率: {hit_rate:.1f}% ({cache_hits}/{len(file_paths)})")
        self._update_progress(text=f"{description}快取命中率: {hit_rate:.1f}%")
        
        if paths_to_recalc:
            user_proc_setting = self.config.get('worker_processes', 0)
            is_qr_mode = self.config.get('comparison_mode') == 'qr_detection'
            
            if user_proc_setting == 0:
                if is_qr_mode: pool_size = max(1, min(cpu_count() - 2, 12))
                else: pool_size = max(1, min(cpu_count() // 2, 8))
            else:
                pool_size = max(1, min(user_proc_setting, cpu_count()))
            log_info(f"使用 {pool_size} 個進程計算哈希 (使用者設定: {user_proc_setting}, CPU核心數: {cpu_count()}, 模式: {'QR' if is_qr_mode else 'Hash'})")
            
            self._update_progress(text=f"使用 {pool_size} 進程計算 {len(paths_to_recalc)} 個新檔案...", p_type='progress', value=0)
            
            total_to_calc = len(paths_to_recalc)
            if total_to_calc < 1000: update_interval = 50
            elif total_to_calc < 10000: update_interval = 200
            else: update_interval = 500

            try:
                with Pool(processes=pool_size) as pool:
                    try:
                        results_iterator = pool.imap_unordered(worker_function, paths_to_recalc)
                        for i, (path, result) in enumerate(results_iterator):
                            if self._check_control():
                                pool.terminate(); pool.join(); return None
                            data_to_update = result[1] if isinstance(result, tuple) and len(result) == 2 else result
                            current_data = file_data.get(path, {})
                            if worker_function in [_pool_worker_process_image, _pool_worker_process_image_full, _pool_worker_qr_proxy, _pool_worker_process_image_full_hybrid]:
                                if isinstance(data_to_update, dict):
                                    current_data.update(data_to_update)
                                else:
                                    current_data[data_key] = data_to_update
                            else:
                                current_data[data_key] = data_to_update
                            file_data[path] = current_data
                            cache_manager.update_data(path, current_data)
                            
                            if (i + 1) % update_interval == 0 or (i + 1) == total_to_calc:
                                current_progress = int((i + 1) / total_to_calc * 100)
                                self._update_progress(p_type='progress', value=current_progress, text=f"計算{description}中...({i+1}/{total_to_calc})")
                    except Exception as e:
                        log_error(f"進程池執行失敗: {e}", True); return None
                    finally:
                        pool.close(); pool.join()
            except Exception as e:
                log_error(f"初始化進程池失敗: {e}", True); return None
        
        log_performance(f"[完成] {description}計算")
        cache_manager.save_cache()
        return file_data

    def _find_similar_images(self, target_files: list[str], scan_cache_manager: ScannedImageCacheManager) -> tuple[list, dict]:
        target_file_data = self._process_images_with_cache(target_files, scan_cache_manager, "掃描目標雜湊", _pool_worker_process_image, 'phash')
        if target_file_data is None: return [], {}
        ad_file_data = {}
        if self.config['comparison_mode'] == 'ad_comparison':
            ad_folder_path = self.config['ad_folder_path']
            if not os.path.isdir(ad_folder_path):
                self._update_progress(text="錯誤：廣告圖片資料夾無效。"); return [], {}
            ad_paths = [os.path.join(r, f) for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg','.webp'))]
            ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
            ad_file_data = self._process_images_with_cache(ad_paths, ad_cache_manager, "廣告圖片雜湊", _pool_worker_process_image, 'phash')
            if ad_file_data is None: return [], {}
            ad_hashes = {path: data['phash'] for path, data in ad_file_data.items() if data and data.get('phash')}
        all_file_data = {**target_file_data, **ad_file_data}
        self._update_progress(text="開始比對相似圖片...", p_type='progress', value=0)
        max_diff = int((100 - self.config['similarity_threshold']) / 100 * 64)
        found_items = []
        norm_ad_path = os.path.normpath(self.config.get('ad_folder_path', '')) if self.config.get('ad_folder_path') else None
        
        log_performance("[開始] 比對階段")

        if self.config['comparison_mode'] == 'ad_comparison':
            total_comparisons = len(target_file_data)
            for i, (target_path, target_data) in enumerate(target_file_data.items()):
                if self._check_control(): return [], {}
                if norm_ad_path and os.path.normpath(target_path).startswith(norm_ad_path): continue
                target_hash = target_data.get('phash')
                if not target_hash: continue
                for ad_path, ad_hash in ad_hashes.items():
                    if ad_hash and target_hash - ad_hash <= max_diff:
                        sim = (1 - (target_hash - ad_hash) / 64) * 100
                        found_items.append((ad_path, target_path, f"{sim:.1f}%"))
                if (i + 1) % 500 == 0 or (i + 1) == total_comparisons: self._update_progress(p_type='progress', value=int((i+1)/total_comparisons*100), text=f"廣告比對中... ({i+1}/{total_comparisons})")
        
        elif self.config['comparison_mode'] == 'mutual_comparison':
            hash_groups = defaultdict(list)
            for path, data in target_file_data.items():
                if data and data.get('phash'): hash_groups[data['phash']].append(path)
            
            for h, paths in hash_groups.items():
                if len(paths) > 1:
                    base_path = min(paths)
                    for other_path in paths:
                        if base_path == other_path: continue
                        found_items.append((base_path, other_path, "100.0%"))
            
            unique_hashes = list(hash_groups.keys())
            n = len(unique_hashes)
            self._update_progress(text=f"預分組完成，將在 {n} 個唯一哈希間並行比對...")
            
            comparison_procs = max(1, cpu_count() - 4)
            chunk_size = max(1, n // (comparison_procs * 4))
            work_chunks = [(i, min(i + chunk_size, n), unique_hashes, hash_groups, max_diff) for i in range(0, n, chunk_size)]
            
            try:
                with Pool(processes=comparison_procs) as pool:
                    log_info(f"使用 {comparison_procs} 個進程並行比對 {n} 個唯一哈希。")
                    results = pool.imap_unordered(_pool_worker_compare_hashes, work_chunks)
                    for i, res_chunk in enumerate(results):
                        if self._check_control():
                             pool.terminate(); pool.join(); return [], {}
                        found_items.extend(res_chunk)
                        progress = int((i + 1) / len(work_chunks) * 100)
                        if (i+1) % 10 == 0 or (i+1) == len(work_chunks):
                            self._update_progress(p_type='progress', value=progress, text=f"唯一哈希互相比對中... ({progress}%)")
            except Exception as e:
                log_error(f"並行比對進程池執行失敗: {e}", True)
                return [], {}

        log_performance("[完成] 比對階段")
        self._update_progress(p_type='progress', value=100, text=f"比對完成，找到 {len(found_items)} 對相似項。")
        return found_items, all_file_data
    
    def _detect_qr_codes_pure(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> tuple[list, dict]:
        global _WORKER_CONTEXT
        _WORKER_CONTEXT['qr_resize_size'] = self.config.get('qr_resize_size', 800)
        
        all_file_data = self._process_images_with_cache(files_to_process, scan_cache_manager, "QR Code 檢測", _pool_worker_qr_proxy, 'qr_points')
        if all_file_data is None: return [], {}
        
        found_qr_images = []
        for image_path, data in all_file_data.items():
            if data and data.get('qr_points'):
                found_qr_images.append((image_path, image_path, "QR Code 檢出"))
        self._update_progress(p_type='progress', value=100, text=f"QR Code 檢測完成。找到 {len(found_qr_images)} 個目標。")
        return found_qr_images, all_file_data

    def _detect_qr_codes_hybrid(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> tuple[list, dict]:
        self._update_progress(text="混合模式：開始 pHash 快速匹配...")
        ad_folder_path = self.config['ad_folder_path']
        if not os.path.isdir(ad_folder_path):
            self._update_progress(text="混合模式錯誤：廣告資料夾無效。轉為純粹 QR 掃描...")
            return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        
        ad_paths = [os.path.join(r, f) for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg','.webp'))]
        ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
        resize_size = self.config.get('qr_resize_size', 800)
        
        global _WORKER_CONTEXT
        _WORKER_CONTEXT['qr_resize_size'] = resize_size
        
        # v12.7.4: Use top-level proxy function to fix serialization error
        ad_file_data = self._process_images_with_cache(ad_paths, ad_cache_manager, "廣告圖片屬性", _pool_worker_process_image_full_hybrid, 'qr_points')
        if ad_file_data is None: return [], {}
        
        ad_hashes = {path: data['phash'] for path, data in ad_file_data.items() if data and data.get('phash')}
        
        target_file_data = self._process_images_with_cache(files_to_process, scan_cache_manager, "掃描目標雜湊", _pool_worker_process_image, 'phash')
        if target_file_data is None: return [], {}
        
        found_items, remaining_files = [], []
        max_diff = int((100 - self.config['similarity_threshold']) / 100 * 64)
        for path, data in target_file_data.items():
            target_hash = data.get('phash')
            if not target_hash:
                remaining_files.append(path); continue
            match_found_and_skipped = False
            for ad_path, ad_hash in ad_hashes.items():
                if ad_hash and target_hash - ad_hash <= max_diff:
                    ad_has_qr = ad_file_data.get(ad_path) and ad_file_data[ad_path].get('qr_points')
                    if ad_has_qr:
                        found_items.append((ad_path, path, "廣告匹配(快速)"))
                        target_file_data.setdefault(path, {})['qr_points'] = ad_file_data[ad_path]['qr_points']
                        match_found_and_skipped = True
                    break
            if not match_found_and_skipped:
                remaining_files.append(path)
        
        ad_match_count = len([it for it in found_items if it[2]=='廣告匹配(快速)'])
        self._update_progress(text=f"快速匹配完成，找到 {ad_match_count} 個廣告。對 {len(remaining_files)} 個檔案進行 QR 掃描...")
        
        if remaining_files:
            qr_results, qr_file_data = self._detect_qr_codes_pure(remaining_files, scan_cache_manager)
            existing_targets = {item[1] for item in found_items}
            for qr_item in qr_results:
                if qr_item[1] not in existing_targets:
                    found_items.append(qr_item)
            target_file_data.update(qr_file_data)
        
        all_file_data = {**target_file_data, **ad_file_data}
        self._update_progress(p_type='progress', value=100, text=f"混合掃描完成。共找到 {len(found_items)} 個目標。")
        return found_items, all_file_data
#第二部分
# === 10. GUI 類別 ===
class Tooltip:
    def __init__(self, widget: tk.Widget, text: str):
        self.widget, self.text, self.tooltip_window, self.id = widget, text, None, None
    def enter(self, event: tk.Event | None = None) -> None: self.schedule(event)
    def leave(self, event: tk.Event | None = None) -> None: self.unschedule(); self.hidetip()
    def schedule(self, event: tk.Event | None = None) -> None:
        self.unschedule()
        if event:
            self.x, self.y = event.x_root + 15, event.y_root + 10
        self.id = self.widget.after(500, self.showtip)
    def unschedule(self) -> None:
        if self.id: self.widget.after_cancel(self.id); self.id = None
    def showtip(self) -> None:
        if self.tooltip_window: return
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True); tw.wm_geometry(f"+{self.x}+{self.y}")
        tk.Label(tw, text=self.text, justify='left', background="#ffffe0", relief='solid', borderwidth=1, font=("tahoma", "8", "normal")).pack(ipadx=1)
    def hidetip(self) -> None:
        if self.tooltip_window: self.tooltip_window.destroy(); self.tooltip_window = None

class SettingsGUI(tk.Toplevel):
    def __init__(self, master: "MainWindow"):
        super().__init__(master)
        self.master = master
        self.config = master.config.copy()
        self.title(f"{APP_NAME_TC} v{APP_VERSION} - 設定")
        self.geometry("700x720"); self.resizable(False, False); self.transient(master); self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        main_frame = ttk.Frame(self, padding="10"); main_frame.pack(fill=tk.BOTH, expand=True); main_frame.grid_columnconfigure(1, weight=1)
        self._create_widgets(main_frame); self._load_settings_into_gui(); self._setup_bindings()
        self.wait_window(self)
        
    def _create_widgets(self, frame: ttk.Frame) -> None:
        row_idx = 0
        path_frame = ttk.LabelFrame(frame, text="路徑設定", padding="10"); path_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5); path_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(path_frame, text="根掃描資料夾:").grid(row=0, column=0, sticky="w", pady=2); self.root_scan_folder_entry = ttk.Entry(path_frame); self.root_scan_folder_entry.grid(row=0, column=1, sticky="ew", padx=5); ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.root_scan_folder_entry)).grid(row=0, column=2)
        ttk.Label(path_frame, text="廣告圖片資料夾:").grid(row=1, column=0, sticky="w", pady=2); self.ad_folder_entry = ttk.Entry(path_frame); self.ad_folder_entry.grid(row=1, column=1, sticky="ew", padx=5); ttk.Button(path_frame, text="瀏覽...", command=lambda: self._browse_folder(self.ad_folder_entry)).grid(row=1, column=2)
        
        row_idx += 1
        basic_settings_frame = ttk.LabelFrame(frame, text="基本與性能設定", padding="10"); basic_settings_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5); basic_settings_frame.grid_columnconfigure(1, weight=1)
        
        self.enable_extract_count_limit_var = tk.BooleanVar(); ttk.Checkbutton(basic_settings_frame, text="啟用圖片抽取數量限制", variable=self.enable_extract_count_limit_var).grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        
        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2); self.extract_count_var = tk.StringVar(); self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5); self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5); ttk.Label(basic_settings_frame, text="(從每個資料夾末尾提取N張圖片)").grid(row=1, column=2, sticky="w")
        
        ttk.Label(basic_settings_frame, text="工作進程數:").grid(row=2, column=0, sticky="w", pady=2)
        self.worker_processes_var = tk.StringVar()
        max_proc = cpu_count()
        self.worker_processes_spinbox = ttk.Spinbox(basic_settings_frame, from_=0, to=max_proc, textvariable=self.worker_processes_var, width=5)
        self.worker_processes_spinbox.grid(row=2, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text=f"(0=自動, QR模式建議設為 CPU核心數-2)").grid(row=2, column=2, sticky="w")

        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=3, column=0, sticky="w", pady=2); self.similarity_threshold_var = tk.DoubleVar(); ttk.Scale(basic_settings_frame, from_=80, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label).grid(row=3, column=1, sticky="w", padx=5); self.threshold_label = ttk.Label(basic_settings_frame, text=""); self.threshold_label.grid(row=3, column=2, sticky="w")
        
        ttk.Label(basic_settings_frame, text="QR 檢測縮放尺寸:").grid(row=4, column=0, sticky="w", pady=2)
        self.qr_resize_var = tk.StringVar(); ttk.Spinbox(basic_settings_frame, from_=400, to=1600, increment=200, textvariable=self.qr_resize_var, width=5).grid(row=4, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="px (較大尺寸提高準確性但降速)").grid(row=4, column=2, sticky="w")

        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (換行分隔):").grid(row=5, column=0, sticky="w", pady=2); self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=3); self.excluded_folders_text.grid(row=5, column=1, columnspan=2, sticky="ew", padx=5)

        row_idx += 1
        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10"); mode_frame.grid(row=row_idx, column=0, sticky="nsew", pady=5, padx=5)
        self.comparison_mode_var = tk.StringVar(); 
        ttk.Radiobutton(mode_frame, text="廣告比對", variable=self.comparison_mode_var, value="ad_comparison").pack(anchor="w")
        ttk.Radiobutton(mode_frame, text="互相比對", variable=self.comparison_mode_var, value="mutual_comparison").pack(anchor="w")
        self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測", variable=self.comparison_mode_var, value="qr_detection")
        self.qr_mode_radiobutton.pack(anchor="w")
        self.enable_qr_hybrid_var = tk.BooleanVar()
        self.qr_hybrid_cb = ttk.Checkbutton(mode_frame, text="啟用廣告庫快速匹配", variable=self.enable_qr_hybrid_var)
        self.qr_hybrid_cb.pack(anchor="w", padx=20)
        if not QR_SCAN_ENABLED: 
            self.qr_mode_radiobutton.config(state=tk.DISABLED)
            self.qr_hybrid_cb.config(state=tk.DISABLED)
            ttk.Label(mode_frame, text="(缺少依賴)", foreground="red").pack(anchor="w")
            
        cache_time_frame = ttk.LabelFrame(frame, text="快取管理", padding="10"); cache_time_frame.grid(row=row_idx, column=1, sticky="nsew", pady=5, padx=5)
        ttk.Button(cache_time_frame, text="清理圖片快取 (移至回收桶)", command=self._clear_image_cache).pack(anchor="w", pady=2)
        ttk.Button(cache_time_frame, text="清理資料夾快取 (移至回收桶)", command=self._clear_folder_cache).pack(anchor="w", pady=2)
        
        ttk.Separator(cache_time_frame, orient='horizontal').pack(fill='x', pady=5)
        
        self.enable_time_filter_var = tk.BooleanVar(); self.time_filter_cb = ttk.Checkbutton(cache_time_frame, text="啟用資料夾建立時間篩選", variable=self.enable_time_filter_var); self.time_filter_cb.pack(anchor="w")
        time_inputs_frame = ttk.Frame(cache_time_frame); time_inputs_frame.pack(anchor='w', padx=20)
        ttk.Label(time_inputs_frame, text="從:").grid(row=0, column=0, sticky="w"); self.start_date_var = tk.StringVar(); self.start_date_entry = ttk.Entry(time_inputs_frame, textvariable=self.start_date_var, width=15); self.start_date_entry.grid(row=0, column=1, sticky="ew")
        ttk.Label(time_inputs_frame, text="到:").grid(row=1, column=0, sticky="w"); self.end_date_var = tk.StringVar(); self.end_date_entry = ttk.Entry(time_inputs_frame, textvariable=self.end_date_var, width=15); self.end_date_entry.grid(row=1, column=1, sticky="ew")
        
        row_idx += 1
        button_frame = ttk.Frame(frame, padding="10"); button_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=10)
        ttk.Button(button_frame, text="保存並關閉", command=self._save_and_close).pack(side=tk.RIGHT, padx=5); ttk.Button(button_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT)

    def _clear_image_cache(self):
        root_scan_folder = self.root_scan_folder_entry.get().strip()
        ad_folder_path = self.ad_folder_entry.get().strip()
        if not root_scan_folder:
            messagebox.showwarning("無法清理", "請先在「路徑設定」中指定根掃描資料夾。", parent=self)
            return

        if messagebox.askyesno("確認清理", "確定要將所有圖片哈希快取移至回收桶嗎？\n下次掃描將會重新計算所有圖片的哈希值。", parent=self):
            try:
                cache_manager = ScannedImageCacheManager(root_scan_folder)
                cache_manager.invalidate_cache()
                if ad_folder_path and os.path.isdir(ad_folder_path):
                    ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
                    ad_cache_manager.invalidate_cache()
                messagebox.showinfo("清理成功", "所有圖片快取檔案已移至回收桶。", parent=self)
            except Exception as e:
                log_error(f"清理圖片快取時發生錯誤: {e}", True)
                messagebox.showerror("清理失敗", f"清理圖片快取時發生錯誤：\n{e}", parent=self)

    def _clear_folder_cache(self):
        if messagebox.askyesno("確認清理", "確定要將資料夾時間快取移至回收桶嗎？\n下次使用時間篩選時將會重新掃描資料夾。", parent=self):
            try:
                cache_manager = FolderCreationCacheManager()
                cache_manager.invalidate_cache()
                messagebox.showinfo("清理成功", "資料夾快取檔案已移至回收桶。", parent=self)
            except Exception as e:
                log_error(f"清理資料夾快取時發生錯誤: {e}", True)
                messagebox.showerror("清理失敗", f"清理資料夾快取時發生錯誤：\n{e}", parent=self)

    def _load_settings_into_gui(self) -> None:
        self.root_scan_folder_entry.insert(0, self.config.get('root_scan_folder', ''))
        self.ad_folder_entry.insert(0, self.config.get('ad_folder_path', ''))
        self.extract_count_var.set(str(self.config.get('extract_count', 5)))
        self.worker_processes_var.set(str(self.config.get('worker_processes', 0)))
        self.excluded_folders_text.insert(tk.END, "\n".join(self.config.get('excluded_folders', [])))
        self.similarity_threshold_var.set(self.config.get('similarity_threshold', 98.0))
        self._update_threshold_label(self.config.get('similarity_threshold', 98.0))
        self.comparison_mode_var.set(self.config.get('comparison_mode', 'mutual_comparison'))
        self.enable_extract_count_limit_var.set(self.config.get('enable_extract_count_limit', True))
        today = datetime.date.today()
        start_date = self.config.get('start_date_filter', '') or today.replace(month=1, day=1).strftime("%Y-%m-%d")
        end_date = self.config.get('end_date_filter', '') or today.strftime("%Y-%m-%d")
        self.enable_time_filter_var.set(self.config.get('enable_time_filter', False))
        self.start_date_var.set(start_date)
        self.end_date_var.set(end_date)
        self.enable_qr_hybrid_var.set(self.config.get('enable_qr_hybrid_mode', True))
        self.qr_resize_var.set(str(self.config.get('qr_resize_size', 800)))
        
        self._toggle_ad_folder_entry_state()
        self._toggle_time_filter_fields()
        self._toggle_hybrid_qr_option_state()

    def _setup_bindings(self) -> None: 
        self.comparison_mode_var.trace_add("write", self._on_mode_change)
        self.enable_time_filter_var.trace_add("write", self._toggle_time_filter_fields)
        self.enable_qr_hybrid_var.trace_add("write", self._toggle_ad_folder_entry_state)
    
    def _on_mode_change(self, *args):
        self._toggle_ad_folder_entry_state()
        self._toggle_hybrid_qr_option_state()
        
    def _browse_folder(self, entry: ttk.Entry) -> None:
        folder = filedialog.askdirectory(parent=self)
        if folder:
            entry.delete(0, tk.END)
            entry.insert(0, folder)
            
    def _update_threshold_label(self, val: float) -> None:
        self.threshold_label.config(text=f"{float(val):.0f}%")
        
    def _toggle_ad_folder_entry_state(self, *args) -> None:
        mode = self.comparison_mode_var.get()
        is_hybrid_qr = mode == "qr_detection" and self.enable_qr_hybrid_var.get()
        state = tk.NORMAL if mode == "ad_comparison" or is_hybrid_qr else tk.DISABLED
        self.ad_folder_entry.config(state=state)
        
    def _toggle_hybrid_qr_option_state(self, *args):
        state = tk.NORMAL if self.comparison_mode_var.get() == "qr_detection" and QR_SCAN_ENABLED else tk.DISABLED
        self.qr_hybrid_cb.config(state=state)
        self._toggle_ad_folder_entry_state()
        
    def _toggle_time_filter_fields(self, *args) -> None:
        state = tk.NORMAL if self.enable_time_filter_var.get() else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)
        
    def _save_and_close(self) -> None:
        if self._save_settings():
            self.destroy()
            
    def _save_settings(self) -> bool:
        try:
            config = {
                'root_scan_folder': self.root_scan_folder_entry.get().strip(),
                'ad_folder_path': self.ad_folder_entry.get().strip(),
                'extract_count': int(self.extract_count_var.get()),
                'worker_processes': int(self.worker_processes_var.get()),
                'enable_extract_count_limit': self.enable_extract_count_limit_var.get(),
                'excluded_folders': [f.strip() for f in self.excluded_folders_text.get("1.0", tk.END).splitlines() if f.strip()],
                'similarity_threshold': self.similarity_threshold_var.get(),
                'comparison_mode': self.comparison_mode_var.get(),
                'enable_time_filter': self.enable_time_filter_var.get(),
                'start_date_filter': self.start_date_var.get(),
                'end_date_filter': self.end_date_var.get(),
                'enable_qr_hybrid_mode': self.enable_qr_hybrid_var.get()
            }
            
            qr_resize_input = self.qr_resize_var.get()
            try:
                qr_resize_size = int(qr_resize_input)
                if not (400 <= qr_resize_size <= 1600):
                    messagebox.showerror("錯誤", f"QR 縮放尺寸 '{qr_resize_input}' 無效，必須在 400 到 1600 之間。", parent=self)
                    return False
                config['qr_resize_size'] = qr_resize_size
            except ValueError:
                messagebox.showerror("錯誤", f"QR 縮放尺寸 '{qr_resize_input}' 必須是有效的數字。", parent=self)
                return False
            
            if not os.path.isdir(config['root_scan_folder']):
                messagebox.showerror("錯誤", "根掃描資料夾無效！", parent=self)
                return False
                
            is_ad_mode_active = config['comparison_mode'] == 'ad_comparison' or (config['comparison_mode'] == 'qr_detection' and config['enable_qr_hybrid_mode'])
            if is_ad_mode_active and not os.path.isdir(config['ad_folder_path']):
                messagebox.showerror("錯誤", "此模式需要有效的廣告圖片資料夾！", parent=self)
                return False
                
            if config['enable_time_filter']:
                try: 
                    start = datetime.datetime.strptime(config['start_date_filter'], "%Y-%m-%d")
                    end = datetime.datetime.strptime(config['end_date_filter'], "%Y-%m-%d")
                    if start > end:
                        messagebox.showerror("錯誤", "開始日期不能晚於結束日期。", parent=self)
                        return False
                except ValueError:
                    messagebox.showerror("錯誤", "日期格式不正確，請使用 YYYY-MM-DD。", parent=self)
                    return False
                    
            self.master.config.update(config)
            save_config(self.master.config, CONFIG_FILE)
            return True
        except ValueError:
            messagebox.showerror("錯誤", "提取數量或工作進程數必須是有效的數字。", parent=self)
            return False
        except Exception as e:
            messagebox.showerror("錯誤", f"保存設定時出錯: {e}", parent=self)
            return False
#第二部分
from concurrent.futures import ThreadPoolExecutor

class MainWindow(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = load_config(CONFIG_FILE)
        
        self.all_found_items, self.all_file_data = [], {}
        self.selected_files, self.banned_groups = set(), set()
        self.pil_img_target, self.pil_img_compare = None, None
        self.img_tk_target, self.img_tk_compare = None, None
        self.scan_thread, self._after_id = None, None
        self.cancel_event, self.pause_event = threading.Event(), threading.Event()
        self.selectable_child_ids = []
        
        self.scan_queue, self.preview_queue = Queue(), Queue()
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.sorted_groups = []
        self.current_page = 0
        self.page_size = 100
        self.is_loading_page = False
        self._preview_delay = 150

        self.scan_start_time = None
        self.final_status_text = ""

        self._setup_main_window()
        self._create_widgets()
        self._bind_keys()
        self._check_queues()

    def _setup_main_window(self) -> None:
        self.title(f"{APP_NAME_TC} v{APP_VERSION}")
        self.geometry("1600x900")
        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        sys.excepthook = self.custom_excepthook
        self.bold_font = self._create_bold_font()

    def custom_excepthook(self, exc_type, exc_value, exc_traceback) -> None:
        log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", True)
        if self.winfo_exists():
            messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt'。")
        self.destroy()

    def _create_bold_font(self) -> tuple:
        try:
            default_font = ttk.Style().lookup("TLabel", "font")
            family = self.tk.call('font', 'actual', default_font, '-family')
            size = self.tk.call('font', 'actual', default_font, '-size')
            return (family, abs(int(size)), 'bold')
        except:
            return ("TkDefaultFont", 9, 'bold')

    def _create_widgets(self) -> None:
        style = ttk.Style(self); style.configure("Accent.TButton", font=self.bold_font, foreground='blue'); style.configure("Danger.TButton", foreground='red')
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

    def _create_treeview(self, parent_frame: ttk.Frame) -> None:
        columns=("checkbox","filename","path","count","size","ctime","similarity"); self.tree=ttk.Treeview(parent_frame,columns=columns,show="headings",selectmode="extended")
        headings={"checkbox":"","filename":"群組/圖片","path":"路徑","count":"數量","size":"大小","ctime":"建立日期","similarity":"相似度/類型"}; widths={"checkbox":40,"filename":300,"path":300,"count":50,"size":100,"ctime":150,"similarity":80}
        for col,text in headings.items():self.tree.heading(col,text=text)
        for col,width in widths.items():self.tree.column(col,width=width,minwidth=width,stretch=(col in["filename","path"]))
        self.tree.tag_configure('child_item',foreground='#555555');self.tree.tag_configure('source_copy_item',background='lightyellow');self.tree.tag_configure('ad_parent_item',font=self.bold_font,background='#FFFACD');self.tree.tag_configure('parent_item',font=self.bold_font); self.tree.tag_configure('qr_item', background='#E0FFFF'); self.tree.tag_configure('ad_match_item', background='#FFE4E1')
        vscroll=ttk.Scrollbar(parent_frame,orient="vertical",command=self.tree.yview);self.tree.configure(yscrollcommand=vscroll.set)
        vscroll.bind("<B1-Motion>", self._on_scroll)
        self.tree.pack(side=tk.LEFT,fill=tk.BOTH,expand=True);vscroll.pack(side=tk.RIGHT,fill=tk.Y)

    def _create_preview_panels(self, parent_frame: ttk.Frame) -> None:
        right_pane=ttk.Panedwindow(parent_frame,orient=tk.VERTICAL);right_pane.pack(fill=tk.BOTH,expand=True)
        self.target_image_frame=ttk.LabelFrame(right_pane,text="選中圖片預覽",padding="5");right_pane.add(self.target_image_frame,weight=1); self.target_image_label=ttk.Label(self.target_image_frame,cursor="hand2");self.target_image_label.pack(fill=tk.BOTH,expand=True); self.target_path_label=ttk.Label(self.target_image_frame,text="",wraplength=500);self.target_path_label.pack(fill=tk.X); self.target_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,True))
        self.compare_image_frame=ttk.LabelFrame(right_pane,text="群組基準圖片預覽",padding="5");right_pane.add(self.compare_image_frame,weight=1); self.compare_image_label=ttk.Label(self.compare_image_frame,cursor="hand2");self.compare_image_label.pack(fill=tk.BOTH,expand=True); self.compare_path_label=ttk.Label(self.compare_image_frame,text="",wraplength=500);self.compare_path_label.pack(fill=tk.X); self.compare_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,False))
        self.target_image_label.bind("<Configure>",self._on_preview_resize);self.compare_image_label.bind("<Configure>",self._on_preview_resize)
        self._create_context_menu()

    def _create_bottom_buttons(self, parent_frame: ttk.Frame) -> None:
        button_frame=ttk.Frame(parent_frame);button_frame.pack(side=tk.LEFT,padx=5,pady=5)
        buttons={"全選":self._select_all,"選取建議":self._select_suggested_for_deletion,"取消全選":self._deselect_all,"反選":self._invert_selection,"刪除選中(回收桶)":self._delete_selected_from_disk}
        for text,cmd in buttons.items():ttk.Button(button_frame,text=text,command=cmd).pack(side=tk.LEFT,padx=2)
        actions_frame=ttk.Frame(parent_frame);actions_frame.pack(side=tk.RIGHT,padx=5,pady=5)
        ttk.Button(actions_frame,text="開啟選中資料夾",command=self._open_selected_folder_single).pack(side=tk.LEFT,padx=2); ttk.Button(actions_frame,text="開啟回收桶",command=self._open_recycle_bin).pack(side=tk.LEFT,padx=2)

    def _bind_keys(self) -> None:
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select); self.tree.bind("<Button-1>", self._on_treeview_click); self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection); self.tree.bind("<Return>", self._toggle_selection); self.tree.bind("<Delete>", lambda e: self._delete_selected_from_disk()); self.tree.bind("<BackSpace>", lambda e: self._delete_selected_from_disk())
        self.tree.bind("<Motion>", self._on_mouse_motion); self.tooltip = None; self.tree.bind("<Up>", lambda e: self._navigate_image(e, "Up")); self.tree.bind("<Down>", lambda e: self._navigate_image(e, "Down"))

    def open_settings(self) -> None:
        self.settings_button.config(state=tk.DISABLED)
        SettingsGUI(self)
        self.settings_button.config(state=tk.NORMAL)

    def start_scan(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            messagebox.showwarning("正在執行", "掃描任務正在執行中。")
            return
        
        self.scan_start_time = time.time()
        self.final_status_text = "" 

        self.cancel_event.clear(); self.pause_event.clear()
        self.start_button.config(state=tk.DISABLED); self.settings_button.config(state=tk.DISABLED)
        self.pause_button.config(text="暫停", state=tk.NORMAL); self.cancel_button.config(state=tk.NORMAL)
        self.tree.delete(*self.tree.get_children())
        self.all_found_items.clear(); self.all_file_data.clear()
        self.scan_thread = threading.Thread(target=self._run_scan_in_thread, args=(self.config.copy(),), daemon=True)
        self.scan_thread.start()

    def cancel_scan(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askyesno("確認終止", "確定要終止目前的掃描任務嗎？"):
                self.cancel_event.set()
                self.pause_event.clear()

    def toggle_pause(self) -> None:
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button.config(text="暫停")
            self.status_label.config(text="任務已恢復...")
        else:
            self.pause_event.set()
            self.pause_button.config(text="恢復")
            self.status_label.config(text="任務已暫停...")

    def _reset_control_buttons(self, final_status_text: str = "任務完成") -> None:
        self.status_label.config(text=final_status_text)
        self.progress_bar['value'] = 0
        self.start_button.config(state=tk.NORMAL)
        self.settings_button.config(state=tk.NORMAL)
        self.pause_button.config(state=tk.DISABLED, text="暫停")
        self.cancel_button.config(state=tk.DISABLED)

    def _check_queues(self) -> None:
        try:
            while True:
                msg = self.scan_queue.get_nowait()
                if msg['type'] == 'progress':
                    self.progress_bar['value'] = msg.get('value', 0)
                    self.status_label['text'] = msg.get('text', '')
                elif msg['type'] == 'text':
                    self.status_label['text'] = msg.get('text', '')
                elif msg['type'] == 'result':
                    self.all_found_items, self.all_file_data = msg.get('data', []), msg.get('meta', {})
                    self._process_scan_results()
                elif msg['type'] == 'finish':
                    self.final_status_text = msg.get('text', '任務完成')
                    self._reset_control_buttons(self.final_status_text)
                    if self.scan_start_time:
                        duration = time.time() - self.scan_start_time
                        log_performance(f"掃描任務完成，總耗時: {duration:.2f} 秒。")
                    if not self.all_found_items and "取消" not in self.final_status_text:
                        messagebox.showinfo("掃描結果", "未找到符合條件的相似或廣告圖片。")
        except Empty: pass
        try:
            while True:
                msg = self.preview_queue.get_nowait()
                if msg['type'] == 'image_loaded':
                    is_target = msg['is_target']
                    pil_image = msg['image']
                    if is_target: self.pil_img_target = pil_image
                    else: self.pil_img_compare = pil_image
                    self._update_all_previews()
        except Empty: pass
        finally: self.after(100, self._check_queues)

    def _run_scan_in_thread(self, scan_config: dict) -> None:
        control_events = {'cancel': self.cancel_event, 'pause': self.pause_event}
        try:
            engine = ImageComparisonEngine(scan_config, self.scan_queue, control_events)
            found_items, all_file_data = engine.find_duplicates()
            if self.cancel_event.is_set():
                self.scan_queue.put({'type': 'finish', 'text': "任務已取消"})
                return
            self.scan_queue.put({'type': 'result', 'data': found_items, 'meta': all_file_data})
            unique_targets = len(set(p[1] for p in found_items))
            final_text = f"掃描完成。找到 {unique_targets} 個不重複的目標。" if scan_config['comparison_mode'] != 'qr_detection' else f"掃描完成。共找到 {len(found_items)} 個目標。"
            self.scan_queue.put({'type': 'finish', 'text': final_text})
        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", True)
            self.scan_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})
            if self.winfo_exists():
                messagebox.showerror("執行錯誤", f"程式執行時發生錯誤: {e}")

    def _process_scan_results(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.selected_files.clear()
        self.selectable_child_ids.clear()
        self.is_loading_page = False
        self.current_page = 0
        
        groups = defaultdict(list)
        for group_key, item_path, value_str in self.all_found_items:
            groups[group_key].append((item_path, value_str))
        
        self.sorted_groups = sorted(groups.items(), key=lambda item: item[0])
        
        self._load_next_page()
        if self.tree.get_children():
            first_item = self.tree.get_children()[0]
            self.tree.selection_set(first_item)
            self.tree.focus(first_item)

    def _populate_treeview_page(self, page_num: int) -> None:
        start_index = page_num * self.page_size
        end_index = start_index + self.page_size
        groups_to_load = self.sorted_groups[start_index:end_index]
        
        if not groups_to_load:
            self.is_loading_page = False
            return
        uid = start_index * 1000 
        for group_key, items in groups_to_load:
            if group_key in self.banned_groups: continue
            is_ad_mode = self.config['comparison_mode'] == 'ad_comparison'
            is_mutual_mode = self.config['comparison_mode'] == 'mutual_comparison'
            parent_id = f"group_{uid}"; uid += 1
            p_data = self.all_file_data.get(group_key, {})
            p_size = f"{p_data.get('size', 0):,}" if 'size' in p_data else "N/A"
            p_ctime = datetime.datetime.fromtimestamp(p_data.get('ctime')).strftime('%Y/%m/%d %H:%M') if p_data.get('ctime') else "N/A"
            first_item_path, first_value_str = items[0]
            is_qr_scan_result = first_value_str == "QR Code 檢出"
            is_ad_match_result = first_value_str == "廣告匹配(快速)"

            if is_qr_scan_result:
                self.tree.insert("", "end", iid=parent_id, values=("☐", os.path.basename(group_key), os.path.dirname(group_key), "", p_size, p_ctime, first_value_str), tags=('qr_item', group_key))
                self.selectable_child_ids.append(parent_id)
            elif is_ad_mode or is_ad_match_result:
                count = len(items)
                self.tree.insert("", "end", iid=parent_id, values=("", os.path.basename(group_key), os.path.dirname(group_key), count, p_size, p_ctime, "基準廣告"), tags=('ad_parent_item', group_key), open=True)
            elif is_mutual_mode:
                component = [group_key] + [item[0] for item in items]
                count = len(set(component))
                self.tree.insert("", "end", iid=parent_id, values=("☐", os.path.basename(group_key), os.path.dirname(group_key), count, p_size, p_ctime, "基準"), tags=('parent_item', group_key), open=True)

            if not is_qr_scan_result:
                norm_ad_path = os.path.normpath(self.config.get('ad_folder_path', '')) if self.config.get('ad_folder_path') else None
                for path, value_str in sorted(items, key=lambda x: x[0]):
                    tags = ['child_item', path, group_key]
                    is_in_ad_folder = norm_ad_path and os.path.normpath(path).startswith(norm_ad_path)
                    if is_in_ad_folder: tags.append('protected_item')
                    if is_mutual_mode and path == group_key: tags.append('source_copy_item')
                    if is_ad_match_result: tags.append('ad_match_item')
                    item_id = f"item_{uid}"; uid += 1
                    c_data = self.all_file_data.get(path, {})
                    c_size = f"{c_data.get('size', 0):,}" if 'size' in c_data else "N/A"
                    c_ctime = datetime.datetime.fromtimestamp(c_data.get('ctime')).strftime('%Y/%m/%d %H:%M') if c_data.get('ctime') else "N/A"
                    checkbox_val = "" if is_in_ad_folder else "☐"
                    self.tree.insert(parent_id, "end", iid=item_id, values=(checkbox_val, f"  └─ {os.path.basename(path)}", os.path.dirname(path), "", c_size, c_ctime, value_str), tags=tuple(tags))
                    if not is_in_ad_folder and 'source_copy_item' not in tags:
                        self.selectable_child_ids.append(item_id)
        self.is_loading_page = False
        
    def _on_scroll(self, event: tk.Event) -> None:
        yview = self.tree.yview()
        if yview[1] > 0.95 and not self.is_loading_page:
            self._load_next_page()

    def _load_next_page(self) -> None:
        if self.is_loading_page: return

        if self.current_page * self.page_size >= len(self.sorted_groups):
            if self.final_status_text:
                self.status_label.config(text=self.final_status_text)
            return

        self.is_loading_page = True
        self.status_label.config(text=f"正在載入第 {self.current_page + 1} 頁...")
        self._populate_treeview_page(self.current_page)
        self.current_page += 1

    def _on_treeview_click(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if not item_id or not self.tree.exists(item_id):
            return
        tags = self.tree.item(item_id, "tags")
        if self.tree.identify_column(event.x) == "#1":
            if 'ad_parent_item' not in tags and 'source_copy_item' not in tags and 'protected_item' not in tags:
                self._toggle_selection_by_item_id(item_id)
        else:
            self.tree.selection_set(item_id)
            self.tree.focus(item_id)

    def _on_item_select(self, event: tk.Event) -> None:
        if self._after_id:
            self.after_cancel(self._after_id)
        self._after_id = self.after(self._preview_delay, self._trigger_async_preview)
        
    def _trigger_async_preview(self) -> None:
        self._after_id = None
        selected = self.tree.selection()
        
        for label in [self.target_image_label, self.compare_image_label]: label.config(image="")
        self.pil_img_target, self.pil_img_compare = None, None
        self.target_path_label.config(text="載入中...")
        self.compare_path_label.config(text="載入中...")
        
        if not selected or not self.tree.exists(selected[0]):
            self.target_path_label.config(text="")
            self.compare_path_label.config(text="")
            return
            
        item_id = selected[0]
        tags = self.tree.item(item_id, "tags")
        sel_path, cmp_path = None, None

        if 'qr_item' in tags: sel_path = cmp_path = tags[1]
        elif 'parent_item' in tags or 'ad_parent_item' in tags: sel_path = cmp_path = tags[1]
        elif 'child_item' in tags: sel_path, cmp_path = tags[1], tags[2]

        if sel_path: self.executor.submit(self._load_image_worker, sel_path, self.target_path_label, True)
        else: self.target_path_label.config(text="")
        if cmp_path: self.executor.submit(self._load_image_worker, cmp_path, self.compare_path_label, False)
        else: self.compare_path_label.config(text="")

    def _load_image_worker(self, path: str, label_widget: tk.Label, is_target: bool) -> None:
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img).convert('RGB')
                if path in self.all_file_data and self.all_file_data[path].get('qr_points'):
                    img = self._draw_qr_outline(img, self.all_file_data[path]['qr_points'])
                label_widget.config(text=f"路徑: {path}")
                self.preview_queue.put({'type': 'image_loaded', 'image': img.copy(), 'is_target': is_target})
        except Exception as e:
            label_widget.config(text=f"無法載入: {os.path.basename(path)}")
            log_error(f"載入圖片預覽失敗 '{path}': {e}", True)
            self.preview_queue.put({'type': 'image_loaded', 'image': None, 'is_target': is_target})

    def _draw_qr_outline(self, image: Image.Image, qr_points_list: list) -> Image.Image:
        if not qr_points_list or not isinstance(qr_points_list, (list, np.ndarray)): return image
        draw = ImageDraw.Draw(image)
        try:
            for qr_points_single in qr_points_list:
                if not isinstance(qr_points_single, (list, np.ndarray)) or len(qr_points_single) < 3: continue
                points = [(int(p[0]), int(p[1])) for p in qr_points_single]
                draw.polygon(points, outline="blue", width=max(3, int(min(image.size) * 0.01)))
        except (ValueError, TypeError, IndexError) as e:
            log_error(f"Failed to draw QR outline: {e}", True)
        return image

    def _update_all_previews(self) -> None:
        self._resize_and_display(self.target_image_label, self.pil_img_target, True)
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)

    def _on_preview_resize(self, event: tk.Event) -> None:
        is_target = (event.widget == self.target_image_label)
        self._resize_and_display(event.widget, self.pil_img_target if is_target else self.pil_img_compare, is_target)

    def _resize_and_display(self, label: tk.Label, pil_image: Image.Image | None, is_target: bool) -> None:
        if not pil_image: label.config(image="")
        else:
            w, h = label.winfo_width(), label.winfo_height()
            if w > 1 and h > 1:
                img_copy = pil_image.copy(); img_copy.thumbnail((w - 10, h - 10), Image.Resampling.LANCZOS)
                img_tk = ImageTk.PhotoImage(img_copy); label.config(image=img_tk)
                if is_target: self.img_tk_target = img_tk
                else: self.img_tk_compare = img_tk

    def _on_preview_image_click(self, event: tk.Event, is_target_image: bool) -> None:
        text = (self.target_path_label if is_target_image else self.compare_path_label).cget("text")
        if text.startswith("路徑: "):
            path = text[len("路徑: "):].strip()
            if path and os.path.exists(path): self._open_folder(os.path.dirname(path))

    def _navigate_image(self, event: tk.Event, direction: str) -> str:
        selected_id = self.tree.selection()
        if not selected_id: return "break"
        current_id, target_id = selected_id[0], None
        if direction == "Down":
            parent = self.tree.parent(current_id)
            if parent == "" and self.tree.item(current_id, "open"):
                children = self.tree.get_children(current_id)
                target_id = children[0] if children else self.tree.next(current_id)
            elif parent != "":
                siblings = self.tree.get_children(parent)
                try:
                    idx = siblings.index(current_id)
                    if idx < len(siblings) - 1: target_id = siblings[idx + 1]
                    else: target_id = self.tree.next(parent)
                except ValueError: target_id = self.tree.next(current_id)
            else: target_id = self.tree.next(current_id)
        elif direction == "Up":
            parent = self.tree.parent(current_id)
            if parent != "" and current_id == self.tree.get_children(parent)[0]: target_id = parent
            else:
                prev_id = self.tree.prev(current_id)
                if prev_id and self.tree.parent(prev_id) == "" and self.tree.item(prev_id, "open"):
                    children = self.tree.get_children(prev_id)
                    target_id = children[-1] if children else prev_id
                else: target_id = prev_id
        if target_id: self.tree.selection_set(target_id); self.tree.focus(target_id); self.tree.see(target_id)
        return "break"

    def _toggle_selection_by_item_id(self, item_id: str) -> None:
        tags = self.tree.item(item_id, "tags")
        if 'protected_item' in tags: return
        if 'qr_item' in tags:
            self._update_child_selection(item_id, self.tree.set(item_id, "checkbox") == "☐")
        elif 'parent_item' in tags:
            select = self.tree.set(item_id, "checkbox") == "☐"
            self.tree.set(item_id, "checkbox", "☑" if select else "☐")
            for child_id in self.tree.get_children(item_id):
                if 'protected_item' not in self.tree.item(child_id, "tags"): self._update_child_selection(child_id, select)
        elif 'child_item' in tags:
            is_selected = self.tree.item(item_id, "tags")[1] in self.selected_files
            self._update_child_selection(item_id, not is_selected)
            if (parent_id := self.tree.parent(item_id)): self._update_parent_checkbox(parent_id)

    def _update_child_selection(self, child_id: str, select: bool) -> None:
        path = self.tree.item(child_id, "tags")[1]
        if select: self.selected_files.add(path); self.tree.set(child_id, "checkbox", "☑")
        else: self.selected_files.discard(path); self.tree.set(child_id, "checkbox", "☐")

    def _update_parent_checkbox(self, parent_id: str) -> None:
        if 'ad_parent_item' in self.tree.item(parent_id, "tags"): return
        children = [cid for cid in self.tree.get_children(parent_id) if 'protected_item' not in self.tree.item(cid, "tags")]
        if not children: return
        selected_count = sum(1 for cid in children if self.tree.set(cid, "checkbox") == "☑")
        self.tree.set(parent_id, "checkbox", "☑" if children and selected_count == len(children) else "☐")

    def _toggle_selection(self, event: tk.Event | None = None) -> None:
        for item_id in self.tree.selection(): self._toggle_selection_by_item_id(item_id)

    def _update_all_checkboxes(self, select_logic: callable) -> None:
        all_paths = {self.tree.item(cid, "tags")[1] for cid in self.selectable_child_ids}
        self.selected_files = select_logic(all_paths, self.selected_files)
        for cid in self.selectable_child_ids:
            self._update_child_selection(cid, self.tree.item(cid, "tags")[1] in self.selected_files)
        for pid in self.tree.get_children(""):
            if 'parent_item' in self.tree.item(pid, "tags"): self._update_parent_checkbox(pid)

    def _select_all(self) -> None: self._update_all_checkboxes(lambda all_p, sel_p: all_p)
    def _select_suggested_for_deletion(self) -> None:
        paths_to_select = set()
        for group_id in self.tree.get_children(""):
            tags = self.tree.item(group_id, "tags")
            if 'qr_item' in tags: paths_to_select.add(tags[1])
            else:
                for child_id in self.tree.get_children(group_id):
                     child_tags = self.tree.item(child_id, "tags")
                     if 'protected_item' not in child_tags and 'source_copy_item' not in child_tags:
                        paths_to_select.add(child_tags[1])
        self._update_all_checkboxes(lambda all_p, sel_p: paths_to_select)
    def _deselect_all(self) -> None: self.selected_files.clear(); self._update_all_checkboxes(lambda all_p, sel_p: set())
    def _invert_selection(self) -> None: self._update_all_checkboxes(lambda all_p, sel_p: all_p - sel_p)

    def _delete_selected_from_disk(self) -> None:
        if not self.selected_files or not messagebox.askyesno("確認刪除", f"確定要將 {len(self.selected_files)} 個圖片移至回收桶嗎？"): return
        deleted_count, failed_count = 0, 0
        items_to_remove_from_data = list(self.selected_files)
        for path in items_to_remove_from_data:
            if self._send2trash(path):
                deleted_count += 1
                self.selected_files.discard(path)
                if path in self.all_file_data: del self.all_file_data[path]
            else: failed_count += 1
        if failed_count > 0: messagebox.showerror("刪除失敗", f"有 {failed_count} 個檔案刪除失敗。詳情請查看 error_log.txt。")
        if deleted_count > 0:
            self.all_found_items = [(p1, p2, v) for p1, p2, v in self.all_found_items if p1 not in items_to_remove_from_data and p2 not in items_to_remove_from_data]
            self._process_scan_results()
            messagebox.showinfo("刪除完成", f"成功將 {deleted_count} 個文件移至回收桶。")

    def _send2trash(self, path: str) -> bool:
        try: send2trash.send2trash(os.path.abspath(path)); return True
        except Exception as e: log_error(f"移至回收桶失敗 {path}: {e}", True); return False

    def _open_recycle_bin(self) -> None:
        try:
            if sys.platform == "win32": subprocess.run(['explorer.exe', 'shell:RecycleBinFolder'])
            elif sys.platform == "darwin": subprocess.run(['open', os.path.expanduser("~/.Trash")])
            else: subprocess.run(['xdg-open', "trash:/"])
        except: messagebox.showerror("開啟失敗", "無法自動開啟回收桶")

    def _open_folder(self, folder_path: str) -> None:
        try:
            if os.path.isdir(folder_path):
                if sys.platform == "win32": os.startfile(folder_path)
                elif sys.platform == "darwin": subprocess.Popen(["open", folder_path])
                else: subprocess.Popen(["xdg-open", folder_path])
        except: log_error(f"開啟資料夾失敗 {folder_path}", True)

    def _open_selected_folder_single(self) -> None:
        selected = self.tree.selection()
        if selected:
            path_tag = self.tree.item(selected[0], "tags")[1]
            if os.path.isfile(path_tag): self._open_folder(os.path.dirname(path_tag))

    def _create_context_menu(self) -> None:
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="臨時隱藏此群組", command=self._ban_group)
        self.context_menu.add_separator(); self.context_menu.add_command(label="取消所有隱藏", command=self._unban_all_groups)

    def _show_context_menu(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if not item_id: return
        tags = self.tree.item(item_id, "tags")
        if 'qr_item' in tags: self.context_menu.entryconfig("臨時隱藏此群組", state="disabled")
        else: self.context_menu.entryconfig("臨時隱藏此群組", state="normal")
        self.context_menu.tk_popup(event.x_root, event.y_root)

    def _ban_group(self) -> None:
        selected = self.tree.selection()
        if not selected: return
        item_id = selected[0]
        parent_id = self.tree.parent(item_id) or item_id
        group_key = self.tree.item(parent_id, "tags")[1]
        if group_key: self.banned_groups.add(group_key); self._process_scan_results()

    def _unban_all_groups(self) -> None: self.banned_groups.clear(); self._process_scan_results()

    def _on_mouse_motion(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if hasattr(self, 'tooltip_item_id') and self.tooltip_item_id == item_id: return
        if self.tooltip: self.tooltip.leave(); self.tooltip = None; self.tooltip_item_id = None
        if item_id and 'ad_parent_item' in self.tree.item(item_id, "tags"):
            self.tooltip = Tooltip(self.tree, "廣告圖片 (基準，不會被刪除)"); self.tooltip.enter(event)
            self.tooltip_item_id = item_id

    def _on_closing(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askokcancel("關閉程式", "掃描仍在進行中，確定要強制關閉程式嗎？"):
                self.cancel_event.set(); self.pause_event.clear()
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.destroy()
        else:
            if messagebox.askokcancel("關閉程式", "確定要關閉程式嗎？"):
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.destroy()

# 在您的 .py 檔案最底部

def main() -> None:
    if sys.platform.startswith('win'):
        try: 
            set_start_method('spawn', force=True)
        except RuntimeError: 
            pass
    
    # 1. 創建 MainWindow 實例，這就是你唯一的 tk.Tk()
    #    因為 class MainWindow(tk.Tk):
    app = MainWindow()
    
    # 2. 立刻隱藏它，為接下來的檢查做準備
    app.withdraw()
    
    try:
        # 3. 在隱藏的主視窗背景下，執行套件檢查
        #    這時彈出的 messagebox 有一個依附的根，所以不會出錯
        check_and_install_packages()
    except SystemExit:
        # 4. 如果檢查失敗需要退出，那就銷毀這個唯一的視窗
        app.destroy()
        return
    
    # 5. 如果檢查成功，取消隱藏，讓主視窗顯示出來
    app.deiconify()
    
    # 6. 啟動主事件循環
    app.mainloop()

if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()

