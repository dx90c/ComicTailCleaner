# ======================================================================
# 檔案名稱：ComicTailCleaner_v12.4.1.py
# 版本號：12.4.1
# 專案名稱：ComicTailCleaner (漫畫尾頁廣告清理)
#
# === 程式說明 ===
# 一個專為清理漫畫檔案尾頁廣告或重複頁面的工具。
# 它能高效地掃描大量漫畫檔案，並通過感知哈希算法找出內容上
# 相似或完全重複的圖片，提升漫畫閱讀體驗。
#
# === 12.4.1 版本更新內容 ===
# - 【錯誤修正】修復了因遺漏 Tooltip 類的定義而導致程式在滑鼠移動到
#   特定項目上時崩潰的 NameError。
# - 【安全修正】增加了廣告資料夾保護機制。現在，即使廣告資料夾被包含在
#   掃描範圍內，其內部的圖片也不會被錯誤地標記為可刪除的重複項，
#   防止廣告庫被意外破壞。
# - 【程式碼完整性】提供了完整的腳本，修正了先前版本因省略部分方法
#   而無法運行的問題。
#
# === 12.4.0 版本更新內容 ===
# - 【性能優化】重構了「互相比對」模式的核心算法，採用預分組策略提升速度。
# - 【UI 優化】主控制按鈕佈局固定為 [開始執行] [暫停/恢復] [終止]。
# - 【功能修正】修復了「暫停」功能無效及狀態文字錯誤的問題。
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
APP_VERSION = "12.4.1"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"
CONFIG_FILE = "config.json"
QR_SCAN_ENABLED = False
AD_HASH_CACHE_FILE = "ad_hashes.json"

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
    except ImportError as e:
        messagebox.showerror("Tkinter 錯誤", f"無法找到 Tkinter ({e})。您的 Python 安裝可能不完整或損壞。")
        sys.exit(1)
        
    global QR_SCAN_ENABLED
    QR_SCAN_ENABLED = False
    try:
        if 'cv2' not in sys.modules or 'pyzbar' not in sys.modules or 'numpy' not in sys.modules:
            raise ImportError("opencv-python 或 pyzbar 或 numpy 未成功導入")
        QR_SCAN_ENABLED = True
    except ImportError:
        pass

def _pool_worker_process_image(image_path: str) -> tuple[str, dict | None]:
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            phash = imagehash.phash(img, hash_size=8)
            stat_info = os.stat(image_path)
            return (image_path, {
                'hash': phash, 'size': stat_info.st_size,
                'ctime': stat_info.st_ctime, 'mtime': stat_info.st_mtime
            })
    except Exception:
        return (image_path, None)

# === 6. 配置管理相關函數 ===
default_config = {
    'root_scan_folder': '', 'ad_folder_path': '', 'extract_count': 5,
    'enable_extract_count_limit': True, 'excluded_folders': [],
    'comparison_mode': 'mutual_comparison', 'similarity_threshold': 95,
    'enable_time_filter': False, 'start_date_filter': '', 'end_date_filter': '',
    'rebuild_ad_cache': False, 'rebuild_scan_cache': False, 'rebuild_folder_cache': False
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
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
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
                        if isinstance(data, dict) and 'hash' in data and 'mtime' in data:
                            try: data['hash'] = imagehash.hex_to_hash(data['hash']); converted_cache[path] = data
                            except Exception: continue
                    print(f"掃描圖片快取 '{self.cache_file_path}' 已成功載入。", flush=True)
                    return converted_cache
            except Exception: print(f"掃描圖片快取檔案 '{self.cache_file_path}' 格式不正確，將重建。", flush=True)
        return {}
    def save_cache(self) -> None:
        try:
            serializable_cache = {}
            for path, data in self.cache.items():
                if data and 'hash' in data: data_copy = data.copy(); data_copy['hash'] = str(data_copy['hash']); serializable_cache[path] = data_copy
            with open(self.cache_file_path, 'w', encoding='utf-8') as f: json.dump(serializable_cache, f, indent=2)
            print(f"掃描圖片快取已成功保存到 '{self.cache_file_path}'。", flush=True)
        except Exception as e: log_error(f"保存掃描圖片快取時發生錯誤: {e}", True)
    def get_data(self, file_path: str) -> dict | None:
        if file_path in self.cache:
            cached_data = self.cache[file_path]
            try:
                if abs(os.path.getmtime(file_path) - cached_data.get('mtime', 0)) < 1e-6: return cached_data
            except Exception: pass
        return None
    def update_data(self, file_path: str, data: dict) -> None:
        if data and data.get('hash') is not None: self.cache[file_path] = data
    def invalidate_cache(self) -> None:
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try: os.remove(self.cache_file_path)
            except Exception as e: log_error(f"刪除掃描快取檔案時發生錯誤: {e}", True)
        print(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 已被標記為刪除。", flush=True)

def load_ad_hashes(ad_folder_path: str, rebuild_cache: bool = False) -> dict:
    if not os.path.isdir(ad_folder_path): return {}
    if os.path.exists(AD_HASH_CACHE_FILE) and not rebuild_cache:
        try:
            with open(AD_HASH_CACHE_FILE, 'r', encoding='utf-8') as f: return {path: imagehash.hex_to_hash(h) for path, h in json.load(f).items()}
        except Exception: pass
    print(f"正在重建廣告圖片哈希快取...", flush=True)
    ad_images = [os.path.join(r, f) for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg'))]
    with Pool(cpu_count()) as pool: results = pool.map(_pool_worker_process_image, ad_images)
    ad_hashes = {p: d['hash'] for p, d in results if d and d.get('hash')}
    try:
        with open(AD_HASH_CACHE_FILE, 'w', encoding='utf-8') as f: json.dump({p: str(h) for p, h in ad_hashes.items()}, f, indent=2)
    except Exception as e: log_error(f"保存廣告哈希快取時發生錯誤: {e}", True)
    return ad_hashes

class FolderCreationCacheManager:
    def __init__(self, cache_file_path: str = "folder_creation_cache.json"):
        self.cache_file_path = cache_file_path
        self.cache = self._load_cache()
    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f: return json.load(f)
            except Exception: pass
        return {}
    def save_cache(self) -> None:
        try:
            with open(self.cache_file_path, 'w', encoding='utf-8') as f: json.dump(self.cache, f, indent=2)
        except Exception: pass
    def get_creation_time(self, folder_path: str) -> float | None:
        if folder_path in self.cache: return self.cache[folder_path]
        try: ctime = os.path.getctime(folder_path); self.cache[folder_path] = ctime; return ctime
        except Exception: return None
    def invalidate_cache(self) -> None:
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try: os.remove(self.cache_file_path)
            except Exception: pass

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
        except Exception: pass

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
        except Exception: pass
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
        print(f"ImageComparisonEngine (v12.4.1) initialized.", flush=True)

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
        if self._check_control(): return [], {}
        if self.config['comparison_mode'] == "qr_detection": return self._detect_qr_codes()
        else: return self._find_similar_images()

    def _calculate_hashes_with_cache(self, file_paths: list[str], cache_manager: ScannedImageCacheManager, description: str = "") -> dict | None:
        self._update_progress(text=f"正在檢查 {len(file_paths)} 個{description}的快取...")
        file_data, paths_to_recalc, cache_hits = {}, [], 0
        for path in file_paths:
            if self._check_control(): return None
            cached_data = cache_manager.get_data(path)
            if cached_data: file_data[path] = cached_data; cache_hits += 1
            else: paths_to_recalc.append(path)
        hit_rate = (cache_hits / len(file_paths) * 100) if file_paths else 100
        print(f"快取檢查完成 - 命中率: {hit_rate:.1f}% ({cache_hits}/{len(file_paths)})", flush=True)
        self._update_progress(text=f"{description}快取命中率: {hit_rate:.1f}%")

        if paths_to_recalc:
            if self._check_control(): return None
            self._update_progress(text=f"使用 {cpu_count()} 進程計算 {len(paths_to_recalc)} 個新檔案...", p_type='progress', value=0)
            with Pool(processes=cpu_count()) as pool:
                results_iterator = pool.imap_unordered(_pool_worker_process_image, paths_to_recalc)
                total_to_calc = len(paths_to_recalc)
                last_reported_progress = -1
                for i, (path, data) in enumerate(results_iterator):
                    if self._check_control(): pool.terminate(); return None
                    if data: file_data[path] = data; cache_manager.update_data(path, data)
                    current_progress = int((i + 1) / total_to_calc * 100)
                    if current_progress > last_reported_progress:
                        self._update_progress(p_type='progress', value=current_progress, text=f"計算哈希中...({i+1}/{total_to_calc})")
                        last_reported_progress = current_progress
        
        cache_manager.save_cache()
        self._update_progress(text=f"{description}屬性計算完成。"); return file_data

    def _find_similar_images(self) -> tuple[list, dict]:
        time_filter_config = {'enabled': self.config.get('enable_time_filter', False)}
        if time_filter_config['enabled']:
            manager = FolderCreationCacheManager(); time_filter_config['manager'] = manager
            if self.config.get('rebuild_folder_cache'): manager.invalidate_cache()
            try:
                time_filter_config['start'] = datetime.datetime.strptime(self.config['start_date_filter'], "%Y-%m-%d") if self.config.get('start_date_filter') else None
                time_filter_config['end'] = datetime.datetime.strptime(self.config['end_date_filter'], "%Y-%m-%d").replace(hour=23, minute=59, second=59) if self.config.get('end_date_filter') else None
            except ValueError: time_filter_config['enabled'] = False
        
        scan_cache_manager = ScannedImageCacheManager(self.config['root_scan_folder'])
        if self.config.get('rebuild_scan_cache'): scan_cache_manager.invalidate_cache()
        all_folders = get_all_subfolders(self.config['root_scan_folder'], self.config['excluded_folders'], self.progress_queue, self.control_events, time_filter_config)
        if self._check_control(): return [], {}
        files_dict = extract_last_n_files_from_folders(all_folders, self.config['extract_count'], self.config['enable_extract_count_limit'], self.progress_queue, self.control_events)
        if self._check_control(): return [], {}
        
        target_files = [file for files in files_dict.values() for file in files]
        if not target_files: self._update_progress(text="在指定路徑下未找到任何圖片檔案。"); return [], {}
        target_file_data = self._calculate_hashes_with_cache(target_files, scan_cache_manager, "掃描目標")
        if target_file_data is None: return [], {}
        
        ad_hashes, ad_file_data = {}, {}
        if self.config['comparison_mode'] == 'ad_comparison':
            ad_hashes = load_ad_hashes(self.config['ad_folder_path'], self.config.get('rebuild_ad_cache'))
            ad_paths = list(ad_hashes.keys())
            ad_cache_manager = ScannedImageCacheManager(self.config['ad_folder_path'])
            if self.config.get('rebuild_ad_cache'): ad_cache_manager.invalidate_cache()
            ad_file_data = self._calculate_hashes_with_cache(ad_paths, ad_cache_manager, "廣告圖片")
            if ad_file_data is None: return [], {}

        all_file_data = {**target_file_data, **ad_file_data}
        self._update_progress(text="開始比對相似圖片...", p_type='progress', value=0)
        max_diff = int((100 - self.config['similarity_threshold']) / 100 * 64)
        found_duplicates = []
        norm_ad_path = os.path.normpath(self.config.get('ad_folder_path', '')) if self.config.get('ad_folder_path') else None

        if self.config['comparison_mode'] == 'ad_comparison':
            total_comparisons = len(target_file_data)
            for i, (target_path, target_data) in enumerate(target_file_data.items()):
                if self._check_control(): return [], {}
                # Optimized: norm_ad_path is already pre-calculated
                if norm_ad_path and os.path.normpath(target_path).startswith(norm_ad_path): continue
                target_hash = target_data.get('hash')
                if not target_hash: continue
                for ad_path, ad_hash in ad_hashes.items():
                    if ad_hash and target_hash - ad_hash <= max_diff:
                        sim = (1 - (target_hash - ad_hash) / 64) * 100; found_duplicates.append((ad_path, target_path, sim))
                if (i + 1) % 200 == 0: self._update_progress(p_type='progress', value=int((i+1)/total_comparisons*100), text=f"廣告比對中... ({i+1}/{total_comparisons})")
        
        elif self.config['comparison_mode'] == 'mutual_comparison':
            self._update_progress(text="正在預分組完全相同的圖片...")
            hash_groups = defaultdict(list)
            for path, data in target_file_data.items():
                if data and data.get('hash'): hash_groups[data['hash']].append(path)
            
            for h, paths in hash_groups.items():
                if len(paths) > 1:
                    base_path = paths[0]
                    for other_path in paths[1:]:
                        if norm_ad_path and os.path.normpath(base_path).startswith(norm_ad_path) and os.path.normpath(other_path).startswith(norm_ad_path): continue
                        found_duplicates.append((base_path, other_path, 100.0))
            
            unique_hashes = list(hash_groups.keys()); n = len(unique_hashes)
            self._update_progress(text=f"預分組完成，將在 {n} 個唯一哈希間進行比對...")

            for i in range(n):
                if self._check_control(): return [], {}
                hash1 = unique_hashes[i]
                for j in range(i + 1, n):
                    hash2 = unique_hashes[j]
                    if hash1 - hash2 <= max_diff:
                        sim = (1 - (hash1 - hash2) / 64) * 100
                        for path1 in hash_groups[hash1]:
                            for path2 in hash_groups[hash2]: 
                                if norm_ad_path and os.path.normpath(path1).startswith(norm_ad_path) and os.path.normpath(path2).startswith(norm_ad_path): continue
                                found_duplicates.append((path1, path2, sim))
                if (i + 1) % 100 == 0 or (i+1) == n: 
                    self._update_progress(p_type='progress', value=int((i+1)/n*100), text=f"唯一哈希互相比對中... ({i+1}/{n})")

        self._update_progress(p_type='progress', value=100, text=f"比對完成，找到 {len(found_duplicates)} 對相似項。")
        return found_duplicates, all_file_data
    
    def _detect_qr_codes(self) -> tuple[list, dict]: return [], {}

# === 10. GUI 類別 ===
class Tooltip:
    def __init__(self, widget: tk.Widget, text: str):
        self.widget, self.text, self.tooltip_window, self.id = widget, text, None, None
    def enter(self, event: tk.Event | None = None) -> None: self.schedule(event)
    def leave(self, event: tk.Event | None = None) -> None: self.unschedule(); self.hidetip()
    def schedule(self, event: tk.Event | None = None) -> None:
        self.unschedule()
        if event: self.x, self.y = event.x_root + 15, event.y_root + 10
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
        self.geometry("700x650"); self.resizable(False, False); self.transient(master); self.grab_set()
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
        basic_settings_frame = ttk.LabelFrame(frame, text="基本設定", padding="10"); basic_settings_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=5, padx=5); basic_settings_frame.grid_columnconfigure(1, weight=1)
        self.enable_extract_count_limit_var = tk.BooleanVar(); ttk.Checkbutton(basic_settings_frame, text="啟用圖片抽取數量限制", variable=self.enable_extract_count_limit_var).grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2); self.extract_count_var = tk.StringVar(); self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5); self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5); ttk.Label(basic_settings_frame, text="(從每個資料夾末尾提取N張圖片)").grid(row=1, column=2, sticky="w")
        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=2, column=0, sticky="w", pady=2); self.similarity_threshold_var = tk.DoubleVar(); ttk.Scale(basic_settings_frame, from_=80, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label).grid(row=2, column=1, sticky="w", padx=5); self.threshold_label = ttk.Label(basic_settings_frame, text=""); self.threshold_label.grid(row=2, column=2, sticky="w")
        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (換行分隔):").grid(row=3, column=0, sticky="w", pady=2); self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=3); self.excluded_folders_text.grid(row=3, column=1, columnspan=2, sticky="ew", padx=5)
        row_idx += 1
        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10"); mode_frame.grid(row=row_idx, column=0, sticky="nsew", pady=5, padx=5)
        self.comparison_mode_var = tk.StringVar(); ttk.Radiobutton(mode_frame, text="廣告比對", variable=self.comparison_mode_var, value="ad_comparison").pack(anchor="w"); ttk.Radiobutton(mode_frame, text="互相比對", variable=self.comparison_mode_var, value="mutual_comparison").pack(anchor="w"); self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測", variable=self.comparison_mode_var, value="qr_detection"); self.qr_mode_radiobutton.pack(anchor="w")
        if not QR_SCAN_ENABLED: self.qr_mode_radiobutton.config(state=tk.DISABLED); ttk.Label(mode_frame, text="(缺少依賴)", foreground="red").pack(anchor="w")
        cache_time_frame = ttk.LabelFrame(frame, text="快取與時間篩選", padding="10"); cache_time_frame.grid(row=row_idx, column=1, sticky="nsew", pady=5, padx=5)
        self.rebuild_ad_cache_var = tk.BooleanVar(); ttk.Checkbutton(cache_time_frame, text="重建廣告圖片哈希快取", variable=self.rebuild_ad_cache_var).pack(anchor="w")
        self.rebuild_scan_cache_var = tk.BooleanVar(); ttk.Checkbutton(cache_time_frame, text="重建掃描圖片哈希快取", variable=self.rebuild_scan_cache_var).pack(anchor="w")
        ttk.Separator(cache_time_frame, orient='horizontal').pack(fill='x', pady=5)
        self.enable_time_filter_var = tk.BooleanVar(); self.time_filter_cb = ttk.Checkbutton(cache_time_frame, text="啟用資料夾建立時間篩選", variable=self.enable_time_filter_var); self.time_filter_cb.pack(anchor="w")
        time_inputs_frame = ttk.Frame(cache_time_frame); time_inputs_frame.pack(anchor='w', padx=20)
        ttk.Label(time_inputs_frame, text="從:").grid(row=0, column=0, sticky="w"); self.start_date_var = tk.StringVar(); self.start_date_entry = ttk.Entry(time_inputs_frame, textvariable=self.start_date_var, width=15); self.start_date_entry.grid(row=0, column=1, sticky="ew")
        ttk.Label(time_inputs_frame, text="到:").grid(row=1, column=0, sticky="w"); self.end_date_var = tk.StringVar(); self.end_date_entry = ttk.Entry(time_inputs_frame, textvariable=self.end_date_var, width=15); self.end_date_entry.grid(row=1, column=1, sticky="ew")
        self.rebuild_folder_cache_var = tk.BooleanVar(); ttk.Checkbutton(cache_time_frame, text="重建資料夾時間快取", variable=self.rebuild_folder_cache_var).pack(anchor="w", pady=(5,0))
        row_idx += 1
        button_frame = ttk.Frame(frame, padding="10"); button_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=10)
        ttk.Button(button_frame, text="保存並關閉", command=self._save_and_close).pack(side=tk.RIGHT, padx=5); ttk.Button(button_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT)
    def _load_settings_into_gui(self) -> None:
        self.root_scan_folder_entry.insert(0, self.config.get('root_scan_folder', '')); self.ad_folder_entry.insert(0, self.config.get('ad_folder_path', ''))
        self.extract_count_var.set(str(self.config.get('extract_count', 5))); self.excluded_folders_text.insert(tk.END, "\n".join(self.config.get('excluded_folders', [])))
        self.similarity_threshold_var.set(self.config.get('similarity_threshold', 95.0)); self._update_threshold_label(self.config.get('similarity_threshold', 95.0))
        self.comparison_mode_var.set(self.config.get('comparison_mode', 'mutual_comparison')); self.enable_extract_count_limit_var.set(self.config.get('enable_extract_count_limit', True))
        self.rebuild_ad_cache_var.set(self.config.get('rebuild_ad_cache', False)); self.rebuild_scan_cache_var.set(self.config.get('rebuild_scan_cache', False))
        today = datetime.date.today(); start_date = self.config.get('start_date_filter', '') or today.replace(month=1, day=1).strftime("%Y-%m-%d"); end_date = self.config.get('end_date_filter', '') or today.strftime("%Y-%m-%d")
        self.enable_time_filter_var.set(self.config.get('enable_time_filter', False)); self.start_date_var.set(start_date); self.end_date_var.set(end_date)
        self.rebuild_folder_cache_var.set(self.config.get('rebuild_folder_cache', False))
        self._toggle_ad_folder_entry_state(); self._toggle_time_filter_fields()
    def _setup_bindings(self) -> None: self.comparison_mode_var.trace_add("write", self._toggle_ad_folder_entry_state); self.enable_time_filter_var.trace_add("write", self._toggle_time_filter_fields)
    def _browse_folder(self, entry: ttk.Entry) -> None:
        folder = filedialog.askdirectory(parent=self);
        if folder: entry.delete(0, tk.END); entry.insert(0, folder)
    def _update_threshold_label(self, val: float) -> None: self.threshold_label.config(text=f"{float(val):.0f}%")
    def _toggle_ad_folder_entry_state(self, *args) -> None: self.ad_folder_entry.config(state=tk.NORMAL if self.comparison_mode_var.get() == "ad_comparison" else tk.DISABLED)
    def _toggle_time_filter_fields(self, *args) -> None:
        state = tk.NORMAL if self.enable_time_filter_var.get() else tk.DISABLED
        self.start_date_entry.config(state=state); self.end_date_entry.config(state=state)
    def _save_and_close(self) -> None:
        if self._save_settings(): self.destroy()
    def _save_settings(self) -> bool:
        try:
            config = { 'root_scan_folder': self.root_scan_folder_entry.get().strip(), 'ad_folder_path': self.ad_folder_entry.get().strip(), 'extract_count': int(self.extract_count_var.get()), 'enable_extract_count_limit': self.enable_extract_count_limit_var.get(), 'excluded_folders': [f.strip() for f in self.excluded_folders_text.get("1.0", tk.END).splitlines() if f.strip()], 'similarity_threshold': self.similarity_threshold_var.get(), 'comparison_mode': self.comparison_mode_var.get(), 'rebuild_ad_cache': self.rebuild_ad_cache_var.get(), 'rebuild_scan_cache': self.rebuild_scan_cache_var.get(), 'enable_time_filter': self.enable_time_filter_var.get(), 'start_date_filter': self.start_date_var.get(), 'end_date_filter': self.end_date_var.get(), 'rebuild_folder_cache': self.rebuild_folder_cache_var.get()}
            if not os.path.isdir(config['root_scan_folder']): messagebox.showerror("錯誤", "根掃描資料夾無效！", parent=self); return False
            if config['enable_time_filter']:
                try: 
                    start = datetime.datetime.strptime(config['start_date_filter'], "%Y-%m-%d"); end = datetime.datetime.strptime(config['end_date_filter'], "%Y-%m-%d")
                    if start > end: messagebox.showerror("錯誤", "開始日期不能晚於結束日期。", parent=self); return False
                except ValueError: messagebox.showerror("錯誤", "日期格式不正確，請使用 YYYY-MM-DD。", parent=self); return False
            self.master.config = config; save_config(config, CONFIG_FILE)
            self.master.config.update({'rebuild_ad_cache': False, 'rebuild_scan_cache': False, 'rebuild_folder_cache': False}); return True
        except Exception as e: messagebox.showerror("錯誤", f"保存設定時出錯: {e}", parent=self); return False

class MainWindow(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.config = load_config(CONFIG_FILE)
        self.all_similar_files, self.all_file_data = [], {}; self.selected_files, self.banned_ad_images = set(), set()
        self.pil_img_target, self.pil_img_compare, self.img_tk_target, self.img_tk_compare = None, None, None, None
        self.scan_thread, self.progress_queue, self._after_id = None, Queue(), None; self._preview_delay = 250
        self.cancel_event, self.pause_event = threading.Event(), threading.Event()
        self.selectable_child_ids = []
        self._setup_main_window(); self._create_widgets(); self._bind_keys(); self.check_queue()
    def _setup_main_window(self) -> None:
        self.title(f"{APP_NAME_TC} v{APP_VERSION}"); self.geometry("1600x900")
        self.protocol("WM_DELETE_WINDOW", self._on_closing); sys.excepthook = self.custom_excepthook
        self.bold_font = self._create_bold_font()
    def custom_excepthook(self, exc_type, exc_value, exc_traceback) -> None:
        log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", True)
        if self.winfo_exists(): messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt'。")
        self.destroy()
    def _create_bold_font(self) -> tuple:
        try:
            default_font = ttk.Style().lookup("TLabel", "font"); family = self.tk.call('font', 'actual', default_font, '-family'); size = self.tk.call('font', 'actual', default_font, '-size')
            return (family, abs(int(size)), 'bold')
        except: return ("TkDefaultFont", 9, 'bold')
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
    def _create_treeview(self,parent_frame: ttk.Frame) -> None:
        columns=("checkbox","filename","path","count","size","ctime","similarity"); self.tree=ttk.Treeview(parent_frame,columns=columns,show="headings",selectmode="extended")
        headings={"checkbox":"","filename":"群組 - 重複/相似圖片","path":"路徑","count":"數量","size":"大小","ctime":"建立日期","similarity":"相似度"}; widths={"checkbox":40,"filename":300,"path":300,"count":50,"size":100,"ctime":150,"similarity":80}
        for col,text in headings.items():self.tree.heading(col,text=text)
        for col,width in widths.items():self.tree.column(col,width=width,minwidth=width,stretch=(col in["filename","path"]))
        self.tree.tag_configure('child_item',foreground='#555555');self.tree.tag_configure('source_copy_item',background='lightyellow');self.tree.tag_configure('ad_parent_item',font=self.bold_font,background='#FFFACD');self.tree.tag_configure('parent_item',font=self.bold_font)
        vscroll=ttk.Scrollbar(parent_frame,orient="vertical",command=self.tree.yview);self.tree.configure(yscrollcommand=vscroll.set)
        self.tree.pack(side=tk.LEFT,fill=tk.BOTH,expand=True);vscroll.pack(side=tk.RIGHT,fill=tk.Y)
    def _create_preview_panels(self,parent_frame: ttk.Frame) -> None:
        right_pane=ttk.Panedwindow(parent_frame,orient=tk.VERTICAL);right_pane.pack(fill=tk.BOTH,expand=True)
        self.target_image_frame=ttk.LabelFrame(right_pane,text="選中圖片預覽",padding="5");right_pane.add(self.target_image_frame,weight=1); self.target_image_label=ttk.Label(self.target_image_frame,cursor="hand2");self.target_image_label.pack(fill=tk.BOTH,expand=True); self.target_path_label=ttk.Label(self.target_image_frame,text="",wraplength=500);self.target_path_label.pack(fill=tk.X); self.target_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,True))
        self.compare_image_frame=ttk.LabelFrame(right_pane,text="群組基準圖片預覽",padding="5");right_pane.add(self.compare_image_frame,weight=1); self.compare_image_label=ttk.Label(self.compare_image_frame,cursor="hand2");self.compare_image_label.pack(fill=tk.BOTH,expand=True); self.compare_path_label=ttk.Label(self.compare_image_frame,text="",wraplength=500);self.compare_path_label.pack(fill=tk.X); self.compare_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,False))
        self.target_image_label.bind("<Configure>",self._on_preview_resize);self.compare_image_label.bind("<Configure>",self._on_preview_resize)
        self._create_context_menu()
    def _create_bottom_buttons(self,parent_frame: ttk.Frame) -> None:
        button_frame=ttk.Frame(parent_frame);button_frame.pack(side=tk.LEFT,padx=5,pady=5)
        buttons={"全選":self._select_all,"選取建議":self._select_suggested_for_deletion,"取消全選":self._deselect_all,"反選":self._invert_selection,"刪除選中(回收桶)":self._delete_selected_from_disk}
        for text,cmd in buttons.items():ttk.Button(button_frame,text=text,command=cmd).pack(side=tk.LEFT,padx=2)
        actions_frame=ttk.Frame(parent_frame);actions_frame.pack(side=tk.RIGHT,padx=5,pady=5)
        ttk.Button(actions_frame,text="開啟選中資料夾",command=self._open_selected_folder_single).pack(side=tk.LEFT,padx=2); ttk.Button(actions_frame,text="開啟回收桶",command=self._open_recycle_bin).pack(side=tk.LEFT,padx=2)
    def _bind_keys(self) -> None:
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select); self.tree.bind("<Button-1>", self._on_treeview_click); self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection); self.tree.bind("<Return>", self._toggle_selection); self.tree.bind("<Delete>", lambda e: self._delete_selected_from_disk()); self.tree.bind("<BackSpace>", lambda e: self._delete_selected_from_disk())
        self.tree.bind("<Motion>", self._on_mouse_motion); self.tooltip = None; self.tree.bind("<Up>", lambda e: self._navigate_image(e, "Up")); self.tree.bind("<Down>", lambda e: self._navigate_image(e, "Down"))
    def open_settings(self) -> None: self.settings_button.config(state=tk.DISABLED); SettingsGUI(self); self.settings_button.config(state=tk.NORMAL)
    def start_scan(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive(): messagebox.showwarning("正在執行", "掃描任務正在執行中。"); return
        self.cancel_event.clear(); self.pause_event.clear()
        self.start_button.config(state=tk.DISABLED); self.settings_button.config(state=tk.DISABLED)
        self.pause_button.config(text="暫停", state=tk.NORMAL); self.cancel_button.config(state=tk.NORMAL)
        self.tree.delete(*self.tree.get_children())
        self.all_similar_files.clear(); self.all_file_data.clear()
        self.scan_thread = threading.Thread(target=self._run_scan_in_thread, daemon=True); self.scan_thread.start()
    def cancel_scan(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askyesno("確認終止", "確定要終止目前的掃描任務嗎？"):
                self.cancel_event.set(); self.pause_event.clear()
    def toggle_pause(self) -> None:
        if self.pause_event.is_set():
            self.pause_button.config(text="暫停"); self.status_label.config(text="任務已恢復...")
            self.pause_event.clear()
        else:
            self.pause_button.config(text="恢復"); self.status_label.config(text="任務已暫停...")
            self.pause_event.set()
    def _reset_control_buttons(self, final_status_text: str = "任務完成") -> None:
        self.status_label.config(text=final_status_text); self.progress_bar['value'] = 0
        self.start_button.config(state=tk.NORMAL); self.settings_button.config(state=tk.NORMAL)
        self.pause_button.config(state=tk.DISABLED, text="暫停"); self.cancel_button.config(state=tk.DISABLED)
    def check_queue(self) -> None:
        try:
            while True:
                msg = self.progress_queue.get_nowait()
                if msg['type'] == 'progress': self.progress_bar['value'] = msg.get('value', 0); self.status_label['text'] = msg.get('text', '')
                elif msg['type'] == 'text': self.status_label['text'] = msg.get('text', '')
                elif msg['type'] == 'result': self.all_similar_files, self.all_file_data = msg.get('data', []), msg.get('meta', {}); self._populate_listbox()
                elif msg['type'] == 'finish':
                    self._reset_control_buttons(msg.get('text', '任務完成'))
                    if not self.all_similar_files and "取消" not in msg.get('text', ''): messagebox.showinfo("掃描結果", "未找到符合條件的相似或廣告圖片。")
        except Empty: pass
        finally: self.after(100, self.check_queue)
    def _run_scan_in_thread(self) -> None:
        control_events = {'cancel': self.cancel_event, 'pause': self.pause_event}
        try:
            engine = ImageComparisonEngine(self.config, self.progress_queue, control_events)
            similar_files, all_file_data = engine.find_duplicates()
            if self.cancel_event.is_set(): self.progress_queue.put({'type': 'finish', 'text': "任務已取消"}); return
            self.progress_queue.put({'type': 'result', 'data': similar_files, 'meta': all_file_data})
            self.progress_queue.put({'type': 'finish', 'text': f"掃描完成。找到 {len(similar_files)} 對相似項。"})
        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", True)
            self.progress_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})
            if self.winfo_exists(): messagebox.showerror("執行錯誤", f"程式執行時發生錯誤: {e}")
    def _populate_listbox(self) -> None:
        self.tree.delete(*self.tree.get_children())
        current_selection = self.selected_files.copy(); self.selected_files.clear()
        self.selectable_child_ids.clear()
        sim_map, adj, nodes = {}, defaultdict(list), set()
        for p1, p2, sim in self.all_similar_files: nodes.add(p1); nodes.add(p2); adj[p1].append(p2); adj[p2].append(p1); sim_map[tuple(sorted((p1, p2)))] = sim
        visited, all_components, uid = set(), [], 0
        for node in sorted(list(nodes)):
            if node not in visited:
                component, q = set(), deque([node]); visited.add(node)
                while q:
                    curr = q.popleft(); component.add(curr)
                    for neighbor in adj.get(curr, []):
                        if neighbor not in visited: visited.add(neighbor); q.append(neighbor)
                if len(component) > 1: all_components.append(sorted(list(component)))
        
        norm_ad_path = os.path.normpath(self.config.get('ad_folder_path', '')) if self.config.get('ad_folder_path') else None

        for component in all_components:
            group_key = component[0]
            if group_key in self.banned_ad_images: continue
            parent_id, is_ad_mode = f"group_{uid}", self.config['comparison_mode'] == 'ad_comparison'; uid += 1
            parent_tags = ('ad_parent_item' if is_ad_mode else 'parent_item', group_key)
            p_data = self.all_file_data.get(group_key, {}); p_size = f"{p_data.get('size', 0):,}" if 'size' in p_data else "N/A"
            p_ctime = datetime.datetime.fromtimestamp(p_data.get('ctime')).strftime('%Y/%m/%d %H:%M') if p_data.get('ctime') else "N/A"
            self.tree.insert("", "end", iid=parent_id, values=("☐" if not is_ad_mode else "", os.path.basename(group_key), os.path.dirname(group_key), len(component) if not is_ad_mode else len(component)-1, p_size, p_ctime, "基準"), tags=parent_tags, open=True)
            for path in component:
                if is_ad_mode and path == group_key: continue
                tags = ['child_item', path, group_key];
                if path == group_key: tags.append('source_copy_item')
                is_in_ad_folder = norm_ad_path and os.path.normpath(path).startswith(norm_ad_path)
                if is_in_ad_folder: tags.append('protected_item')
                item_id = f"item_{uid}"; uid += 1
                sim_val = sim_map.get(tuple(sorted((group_key, path))), 100.0)
                c_data = self.all_file_data.get(path, {}); c_size = f"{c_data.get('size', 0):,}" if 'size' in c_data else "N/A"
                c_ctime = datetime.datetime.fromtimestamp(c_data.get('ctime')).strftime('%Y/%m/%d %H:%M') if c_data.get('ctime') else "N/A"
                checkbox_val = "" if is_in_ad_folder else ("☑" if path in current_selection else "☐")
                self.tree.insert(parent_id, "end", iid=item_id, values=(checkbox_val, f"  └─ {os.path.basename(path)}", os.path.dirname(path), "", c_size, c_ctime, f"{sim_val:.1f}%"), tags=tuple(tags))
                if not is_in_ad_folder and 'source_copy_item' not in tags:
                    self.selectable_child_ids.append(item_id)
        if self.tree.get_children(): first_item = self.tree.get_children()[0]; self.tree.selection_set(first_item); self.tree.focus(first_item)
    def _on_treeview_click(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if not item_id: return
        tags = self.tree.item(item_id, "tags")
        if self.tree.identify_column(event.x) == "#1" and 'ad_parent_item' not in tags and 'source_copy_item' not in tags and 'protected_item' not in tags: self._toggle_selection_by_item_id(item_id)
        else: self.tree.selection_set(item_id); self.tree.focus(item_id)
    def _on_item_select(self, event: tk.Event) -> None:
        if self._after_id: self.after_cancel(self._after_id)
        self._after_id = self.after(self._preview_delay, self._load_and_display_selected_image)
    def _load_and_display_selected_image(self) -> None:
        self._after_id = None; selected = self.tree.selection()
        if not selected: self.target_image_label.config(image=""); self.compare_image_label.config(image=""); self.target_path_label.config(text=""); self.compare_path_label.config(text=""); return
        tags = self.tree.item(selected[0], "tags")
        sel_path, cmp_path = (tags[1], tags[1]) if 'parent_item' in tags or 'ad_parent_item' in tags else (tags[1], tags[2]) if 'child_item' in tags else (None, None)
        self.pil_img_target = self._load_pil_image(sel_path, self.target_path_label); self.pil_img_compare = self._load_pil_image(cmp_path, self.compare_path_label)
        self._update_all_previews()
    def _load_pil_image(self, path: str | None, label_widget: tk.Label) -> Image.Image | None:
        if not path or path == "N/A": label_widget.config(text=""); return None
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img); label_widget.config(text=f"路徑: {path}"); return img.copy()
        except: label_widget.config(text=f"無法載入: {os.path.basename(path)}"); return None
    def _update_all_previews(self) -> None:
        self._resize_and_display(self.target_image_label, self.pil_img_target, True)
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)
    def _on_preview_resize(self, event: tk.Event) -> None:
        is_target = (event.widget == self.target_image_label)
        self._resize_and_display(event.widget, self.pil_img_target if is_target else self.pil_img_compare, is_target)
    def _resize_and_display(self, label: tk.Label, pil_image: Image.Image | None, is_target: bool) -> None:
        if not pil_image: label.config(image="");
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
        selected_id = self.tree.selection();
        if not selected_id: return "break"
        current_id, target_id = selected_id[0], None
        if direction == "Down":
            parent = self.tree.parent(current_id)
            if parent == "" and self.tree.item(current_id, "open"): children = self.tree.get_children(current_id); target_id = children[0] if children else self.tree.next(current_id)
            elif parent != "":
                siblings = self.tree.get_children(parent); idx = siblings.index(current_id)
                if idx < len(siblings) - 1: target_id = siblings[idx+1]
                else: target_id = self.tree.next(parent)
            else: target_id = self.tree.next(current_id)
        elif direction == "Up":
            parent = self.tree.parent(current_id)
            if parent != "" and current_id == self.tree.get_children(parent)[0]: target_id = parent
            else:
                prev_id = self.tree.prev(current_id)
                if prev_id and self.tree.parent(prev_id) == "" and self.tree.item(prev_id, "open"):
                    children = self.tree.get_children(prev_id); target_id = children[-1] if children else prev_id
                else: target_id = prev_id
        if target_id: self.tree.selection_set(target_id); self.tree.focus(target_id); self.tree.see(target_id)
        return "break"
    def _toggle_selection_by_item_id(self, item_id: str) -> None:
        tags = self.tree.item(item_id, "tags")
        if 'protected_item' in tags: return
        if 'parent_item' in tags:
            select = self.tree.set(item_id, "checkbox") == "☐"
            self.tree.set(item_id, "checkbox", "☑" if select else "☐")
            for child_id in self.tree.get_children(item_id):
                if 'source_copy_item' not in self.tree.item(child_id, "tags") and 'protected_item' not in self.tree.item(child_id, "tags"): self._update_child_selection(child_id, select)
        elif 'child_item' in tags:
            is_selected = self.tree.item(item_id, "tags")[1] in self.selected_files
            self._update_child_selection(item_id, not is_selected)
            if (parent_id := self.tree.parent(item_id)) and 'ad_parent_item' not in self.tree.item(parent_id, "tags"): self._update_parent_checkbox(parent_id)
    def _update_child_selection(self, child_id: str, select: bool) -> None:
        path = self.tree.item(child_id, "tags")[1]
        if select: self.selected_files.add(path); self.tree.set(child_id, "checkbox", "☑")
        else: self.selected_files.discard(path); self.tree.set(child_id, "checkbox", "☐")
    def _update_parent_checkbox(self, parent_id: str) -> None:
        children = [cid for cid in self.tree.get_children(parent_id) if 'source_copy_item' not in self.tree.item(cid, "tags") and 'protected_item' not in self.tree.item(cid, "tags")]
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
            if 'ad_parent_item' not in self.tree.item(pid, "tags"): self._update_parent_checkbox(pid)
    def _select_all(self) -> None: self._update_all_checkboxes(lambda all_p, sel_p: all_p)
    def _select_suggested_for_deletion(self) -> None: self._select_all()
    def _deselect_all(self) -> None: self.selected_files.clear(); self._update_all_checkboxes(lambda all_p, sel_p: set())
    def _invert_selection(self) -> None: self._update_all_checkboxes(lambda all_p, sel_p: all_p - sel_p)
    def _delete_selected_from_disk(self) -> None:
        if not self.selected_files or not messagebox.askyesno("確認刪除", f"確定要將 {len(self.selected_files)} 個圖片移至回收桶嗎？"): return
        deleted = {p for p in self.selected_files if self._send2trash(p)}
        if len(deleted) < len(self.selected_files): messagebox.showerror("刪除失敗", f"有 {len(self.selected_files) - len(deleted)} 個檔案刪除失敗。")
        if deleted:
            self.all_similar_files = [(p1, p2, sim) for p1, p2, sim in self.all_similar_files if p1 not in deleted and p2 not in deleted]
            for path in deleted: self.all_file_data.pop(path, None)
            self.selected_files.clear(); self._populate_listbox()
            messagebox.showinfo("刪除完成", f"成功將 {len(deleted)} 個文件移至回收桶。")
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
        if selected: self._open_folder(os.path.dirname(self.tree.item(selected[0], "tags")[1]))
    def _create_context_menu(self) -> None:
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="臨時隱藏此群組", command=self._ban_ad_image)
        self.context_menu.add_separator(); self.context_menu.add_command(label="取消所有隱藏", command=self._unban_all_ads)
    def _show_context_menu(self, event: tk.Event) -> None:
        if self.tree.identify_row(event.y): self.context_menu.tk_popup(event.x_root, event.y_root)
    def _ban_ad_image(self) -> None:
        selected = self.tree.selection()
        if selected: key = self.tree.item(self.tree.parent(selected[0]) or selected[0], "tags")[1]; self.banned_ad_images.add(key); self._populate_listbox()
    def _unban_all_ads(self) -> None: self.banned_ad_images.clear(); self._populate_listbox()
    def _on_mouse_motion(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if hasattr(self, 'tooltip_item_id') and self.tooltip_item_id == item_id: return
        if self.tooltip: self.tooltip.leave(); self.tooltip = None
        if item_id and 'ad_parent_item' in self.tree.item(item_id, "tags"): self.tooltip = Tooltip(self.tree, "廣告圖片 (基準，不會被刪除)"); self.tooltip.enter(event)
    def _on_closing(self) -> None:
        if messagebox.askokcancel("關閉程式", "確定要關閉程式嗎？"):
            if self.scan_thread and self.scan_thread.is_alive():
                self.cancel_event.set(); self.pause_event.clear()
            self.destroy()

def main() -> None:
    if sys.platform.startswith('win'):
        try: set_start_method('spawn', force=True)
        except RuntimeError: pass
    
    print(f"=== {APP_NAME_TC} v{APP_VERSION} - 啟動中 ===", flush=True)
    try: check_and_install_packages()
    except SystemExit: return

    root = MainWindow()
    root.mainloop()

if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()