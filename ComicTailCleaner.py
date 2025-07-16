# ======================================================================
# 檔案名稱：ComicTailCleaner_v12.2.2.py
# 版本號：12.2.2
# 專案名稱：ComicTailCleaner (漫畫尾頁廣告清理)
#
# === 程式說明 ===
# 一個專為清理漫畫檔案尾頁廣告或重複頁面的工具。
# 它能高效地掃描大量漫畫檔案，並通過感知哈希算法找出內容上
# 相似或完全重複的圖片，提升漫畫閱讀體驗。
#
# === 12.2.2 版本更新內容 ===
# - 【功能修正】恢復了在結果列表中顯示圖片「大小」和「建立日期」的功能，
#   此功能在 v12.2.0 的重構中被意外遺漏。
# - 【體驗優化】重寫了結果列表中的鍵盤導航邏輯。現在使用「上/下」方向鍵
#   可以在父項和子項之間進行符合直覺的、層級感分明的跳轉，大幅提升操作流暢性。
# - 【錯誤修正】修正了在互相比對模式下，基準圖片的相似度顯示不固定的問題。
#
# === 12.2.1 版本更新內容 ===
# - 【錯誤修正】修復了在 v12.2.0 中引入的一個嚴重邏輯錯誤，該錯誤導致即使
#   找到了相似圖片，結果列表也無法正確顯示任何內容的問題。
# - 【邏輯優化】簡化並加固了結果列表(_populate_listbox)中的圖論分組算法。
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
APP_VERSION = "12.2.2"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"
CONFIG_FILE = "config.json"
QR_SCAN_ENABLED = False

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
                            continue
                    return converted_cache
            except (json.JSONDecodeError, Exception):
                pass
        return {}

    def save_cache(self):
        try:
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=4, ensure_ascii=False)
        except Exception as e:
            log_error(f"保存資料夾建立時間快取時發生錯誤: {e}", include_traceback=True)

    def get_creation_time(self, folder_path):
        if folder_path in self.cache:
            return self.cache[folder_path]
        try:
            ctime = os.path.getctime(folder_path)
            self.cache[folder_path] = ctime
            return ctime
        except (FileNotFoundError, Exception):
            return None

    def invalidate_cache(self):
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try:
                os.remove(self.cache_file_path)
            except Exception as e:
                log_error(f"刪除快取檔案 '{self.cache_file_path}' 時發生錯誤: {e}", include_traceback=True)


# === 8. 核心工具函數 ===
def get_all_subfolders(root_folder, excluded_folders=None, enable_time_filter=False, start_date=None, end_date=None, creation_cache_manager=None, progress_queue=None):
    if excluded_folders is None:
        excluded_folders = []
    all_subfolders_to_return = []
    if not os.path.isdir(root_folder):
        return []

    if progress_queue:
        progress_queue.put({'type': 'text', 'value': "正在收集資料夾..."})

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
        
        all_subfolders_to_return.append(current_folder)

        try:
            for entry in os.listdir(current_folder):
                entry_path = os.path.join(current_folder, entry)
                if os.path.isdir(entry_path):
                    folders_to_process_queue.append(entry_path)
        except (PermissionError, Exception):
            pass
            
    return all_subfolders_to_return

def extract_last_n_files_from_folders(folder_paths, count, enable_limit):
    extracted_files = {}
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    for folder_path in folder_paths:
        image_files = []
        try:
            entries = os.listdir(folder_path)
            for entry in entries:
                if entry.lower().endswith(image_extensions):
                    full_path = os.path.join(folder_path, entry)
                    if os.path.isfile(full_path):
                        image_files.append(full_path)

            image_files.sort()
            if enable_limit:
                extracted_files[folder_path] = image_files[-count:]
            else:
                extracted_files[folder_path] = image_files
        except (PermissionError, Exception):
            pass
    return extracted_files

def _pool_worker_process_image(image_path):
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            phash = imagehash.phash(img, hash_size=8)
            stat_info = os.stat(image_path)
            return (image_path, {
                'hash': phash,
                'size': stat_info.st_size,
                'ctime': stat_info.st_ctime
            })
    except (FileNotFoundError, UnidentifiedImageError, OSError, Exception):
        return (image_path, None)

# === 9. 核心比對引擎 ===
class ImageComparisonEngine:
    def __init__(self, config, progress_queue=None):
        self.config = config
        self.progress_queue = progress_queue
        self.system_qr_scan_capability = QR_SCAN_ENABLED
        print(f"ImageComparisonEngine (v12.2.2) initialized.", flush=True)

    def find_duplicates(self):
        start_date_dt, end_date_dt = None, None
        if self.config.get('enable_time_filter'):
            try:
                if self.config.get('start_date_filter'):
                    start_date_dt = datetime.datetime.strptime(self.config['start_date_filter'], "%Y-%m-%d")
                if self.config.get('end_date_filter'):
                    end_date_dt = datetime.datetime.strptime(self.config['end_date_filter'], "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                self._update_progress(text="時間篩選日期格式錯誤，將被忽略。")

        if self.config['comparison_mode'] == "qr_detection":
            if self.system_qr_scan_capability:
                return self._detect_qr_codes(start_date_dt, end_date_dt)
            else:
                self._update_progress(text="QR Code 掃描功能因缺少依賴而被禁用。")
                return [], {}
        else:
            return self._find_similar_images(start_date_dt, end_date_dt)

    def _update_progress(self, p_type='text', value=None, text=None):
        if self.progress_queue:
            msg = {'type': p_type}
            if value is not None:
                msg['value'] = value
            if text is not None:
                msg['text'] = text
            self.progress_queue.put(msg)
            
    def _get_files_to_process(self, folder_creation_cache_manager, start_date_dt, end_date_dt):
        self._update_progress(text="正在收集目標資料夾...")
        
        target_folders = get_all_subfolders(
            self.config['root_scan_folder'],
            self.config['excluded_folders'],
            self.config['enable_time_filter'],
            start_date_dt,
            end_date_dt,
            folder_creation_cache_manager
        )
        self._update_progress(text=f"過濾後找到 {len(target_folders)} 個目標資料夾。")
        
        ad_files = []
        if self.config['comparison_mode'] == 'ad_comparison' and self.config['ad_folder_path']:
            ad_folders_dict = extract_last_n_files_from_folders([self.config['ad_folder_path']], 0, False)
            ad_files = [file for files in ad_folders_dict.values() for file in files]
            self._update_progress(text=f"廣告比對模式：找到 {len(ad_files)} 個廣告圖片。")

        self._update_progress(text="正在從資料夾中提取圖片檔案...")
        target_files_dict = extract_last_n_files_from_folders(
            target_folders, self.config['extract_count'], self.config['enable_extract_count_limit']
        )
        target_files = [file for files in target_files_dict.values() for file in files]
        
        self._update_progress(text=f"總共收集了 {len(target_files)} 個掃描目標檔案。")
        return target_files, ad_files

    def _calculate_hashes_in_parallel(self, files_to_process, description=""):
        if not files_to_process:
            return {}
        
        num_processes = cpu_count()
        self._update_progress(text=f"啟動 {num_processes} 個進程計算 {description} 哈希與元數據...")
        
        file_data = {}
        with Pool(processes=num_processes) as pool:
            results_iterator = pool.imap_unordered(_pool_worker_process_image, files_to_process)
            
            processed_count = 0
            total_to_process = len(files_to_process)
            for path, data in results_iterator:
                processed_count += 1
                if data and data.get('hash') is not None:
                    file_data[path] = data
                
                if processed_count % 100 == 0 or processed_count == total_to_process:
                    progress = int((processed_count / total_to_process) * 100)
                    self._update_progress(p_type='progress', value=progress, text=f"計算 {description} 屬性: {processed_count}/{total_to_process}")

        self._update_progress(text=f"{description} 屬性計算完成，成功獲取 {len(file_data)} 個檔案資訊。")
        return file_data

    def _find_similar_images(self, start_date_dt, end_date_dt):
        folder_cache_manager = FolderCreationCacheManager()
        target_files, ad_files = self._get_files_to_process(folder_cache_manager, start_date_dt, end_date_dt)
        folder_cache_manager.save_cache()

        if not target_files and self.config['comparison_mode'] != 'ad_comparison':
            self._update_progress(text="沒有找到任何需要處理的圖片檔案。")
            return [], {}

        target_file_data = self._calculate_hashes_in_parallel(target_files, "掃描目標")
        ad_file_data = {}
        if self.config['comparison_mode'] == 'ad_comparison' and ad_files:
            ad_file_data = self._calculate_hashes_in_parallel(ad_files, "廣告圖片")
            if not ad_file_data:
                self._update_progress(text="警告：廣告資料夾中未成功計算出任何哈希值。")
        
        all_file_data = {**target_file_data, **ad_file_data}

        self._update_progress(text="開始比對相似圖片...", p_type='progress', value=0)
        
        hash_size = 8
        max_diff = int((100 - self.config['similarity_threshold']) / 100 * (hash_size ** 2))
        found_duplicates = []

        if self.config['comparison_mode'] == 'ad_comparison':
            total_comparisons = len(target_file_data)
            for i, (target_path, target_data) in enumerate(target_file_data.items()):
                target_hash = target_data.get('hash')
                if not target_hash: continue

                for ad_path, ad_data in ad_file_data.items():
                    ad_hash = ad_data.get('hash')
                    if not ad_hash: continue

                    diff = target_hash - ad_hash
                    if diff <= max_diff:
                        similarity = (1 - diff / (hash_size ** 2)) * 100
                        found_duplicates.append((ad_path, target_path, similarity))
                if (i + 1) % 100 == 0 or (i + 1) == total_comparisons:
                    progress = int((i + 1) / total_comparisons * 100)
                    self._update_progress(p_type='progress', value=progress, text=f"廣告比對中: {i+1}/{total_comparisons}")

        elif self.config['comparison_mode'] == 'mutual_comparison':
            items = list(target_file_data.items())
            n = len(items)

            for i in range(n):
                for j in range(i + 1, n):
                    path1, data1 = items[i]
                    path2, data2 = items[j]
                    
                    hash1 = data1.get('hash')
                    hash2 = data2.get('hash')
                    if not hash1 or not hash2: continue

                    diff = hash1 - hash2
                    if diff <= max_diff:
                        similarity = (1 - diff / (hash_size ** 2)) * 100
                        found_duplicates.append((path1, path2, similarity))

                if (i + 1) % 500 == 0 or (i + 1) == n:
                    progress = int(((i + 1) / n) * 100)
                    self._update_progress(p_type='progress', value=progress, text=f"互相比對中: {i+1}/{n}")

        self._update_progress(p_type='progress', value=100, text=f"比對完成，找到 {len(found_duplicates)} 對相似/重複項。")
        return found_duplicates, all_file_data

    def _detect_qr_codes(self, start_date_dt, end_date_dt):
        folder_cache_manager = FolderCreationCacheManager()
        files_to_process, _ = self._get_files_to_process(folder_cache_manager, start_date_dt, end_date_dt)
        folder_cache_manager.save_cache()
        
        self._update_progress(text="開始檢測圖片中的 QR Code...")
        found_qr_images = []
        all_file_data = self._calculate_hashes_in_parallel(files_to_process, "掃描目標")
        
        total_files = len(files_to_process)
        for i, image_path in enumerate(files_to_process):
            try:
                with Image.open(image_path) as pil_img:
                    pil_img = ImageOps.exif_transpose(pil_img)
                    pil_img = pil_img.convert('RGB')
                    img_cv = np.array(pil_img)
                    gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)
                retval, _, _, _ = cv2.QRCodeDetector().detectAndDecodeMulti(gray)
                if retval:
                    found_qr_images.append((image_path, "N/A", 100.0))
            except Exception:
                pass
            
            if (i + 1) % 100 == 0 or (i + 1) == total_files:
                progress = int((i + 1) / total_files * 100)
                self._update_progress(p_type='progress', value=progress, text=f"QR Code 檢測中: {i+1}/{total_files}")
        
        self._update_progress(text=f"QR Code 檢測完成。找到 {len(found_qr_images)} 個包含 QR Code 的圖片。")
        return found_qr_images, all_file_data

# === 10. GUI 類別 ===
class Tooltip:
    def __init__(self, widget, text):
        self.widget, self.text, self.tooltip_window, self.id = widget, text, None, None
    def enter(self, event=None): self.schedule(event)
    def leave(self, event=None): self.unschedule(); self.hidetip()
    def schedule(self, event=None):
        self.unschedule()
        if event: self.x, self.y = event.x_root + 15, event.y_root + 10
        self.id = self.widget.after(500, self.showtip)
    def unschedule(self):
        if self.id: self.widget.after_cancel(self.id); self.id = None
    def showtip(self):
        if self.tooltip_window: return
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True); tw.wm_geometry(f"+{self.x}+{self.y}")
        tk.Label(tw, text=self.text, justify='left', background="#ffffe0", relief='solid', borderwidth=1, font=("tahoma", "8", "normal")).pack(ipadx=1)
    def hidetip(self):
        if self.tooltip_window: self.tooltip_window.destroy(); self.tooltip_window = None

class SettingsGUI(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.config = master.config
        
        self.title(f"{APP_NAME_TC} v{APP_VERSION} - 設定")
        self.geometry("700x550")
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()
        
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_columnconfigure(1, weight=1)
        
        self._create_widgets(main_frame)
        self._load_settings_into_gui()
        self._setup_bindings()
        
        self.wait_window(self)

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
        ttk.Label(basic_settings_frame, text="(從每個資料夾末尾提取N張圖片)").grid(row=1, column=2, sticky="w", padx=5)

        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=2, column=0, sticky="w", pady=2)
        self.similarity_threshold_var = tk.DoubleVar()
        ttk.Scale(basic_settings_frame, from_=80, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label).grid(row=2, column=1, sticky="w", padx=5)
        self.threshold_label = ttk.Label(basic_settings_frame, text="")
        self.threshold_label.grid(row=2, column=2, sticky="w", padx=5)
        
        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (換行分隔):").grid(row=3, column=0, sticky="w", pady=2)
        self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=3)
        self.excluded_folders_text.grid(row=3, column=1, columnspan=2, sticky="ew", padx=5)
        scrollbar = ttk.Scrollbar(basic_settings_frame, command=self.excluded_folders_text.yview)
        scrollbar.grid(row=3, column=3, sticky="ns")
        self.excluded_folders_text.config(yscrollcommand=scrollbar.set)
        
        row_idx += 1
        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10")
        mode_frame.grid(row=row_idx, column=0, sticky="nsew", pady=5, padx=5)
        self.comparison_mode_var = tk.StringVar()
        ttk.Radiobutton(mode_frame, text="廣告比對", variable=self.comparison_mode_var, value="ad_comparison").pack(anchor="w", pady=2)
        ttk.Radiobutton(mode_frame, text="互相比對", variable=self.comparison_mode_var, value="mutual_comparison").pack(anchor="w", pady=2)
        self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測", variable=self.comparison_mode_var, value="qr_detection")
        self.qr_mode_radiobutton.pack(anchor="w", pady=2)
        if not QR_SCAN_ENABLED:
            self.qr_mode_radiobutton.config(state=tk.DISABLED)
            ttk.Label(mode_frame, text="(缺少依賴)", foreground="red").pack(anchor="w", padx=5)
        self.comparison_mode_var.trace_add("write", self._toggle_ad_folder_entry_state)
        
        cache_time_frame = ttk.LabelFrame(frame, text="快取與時間篩選", padding="10")
        cache_time_frame.grid(row=row_idx, column=1, columnspan=2, sticky="nsew", pady=5, padx=5)
        
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
        ttk.Button(cache_time_frame, text="清空資料夾時間快取", command=self._rebuild_folder_cache).grid(row=3, column=0, columnspan=3, sticky="w", pady=5)
        
        row_idx += 1
        button_frame = ttk.Frame(frame, padding="10")
        button_frame.grid(row=row_idx, column=0, columnspan=3, sticky="ew", pady=10)
        ttk.Button(button_frame, text="保存並關閉", command=self._save_and_close).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT)

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
        if not QR_SCAN_ENABLED and comparison_mode_cfg == 'qr_detection':
            comparison_mode_cfg = 'mutual_comparison'
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
        state = tk.NORMAL if self.enable_time_filter_var.get() else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)

    def _toggle_extract_count_fields(self):
        state = tk.NORMAL if self.enable_extract_count_limit_var.get() else tk.DISABLED
        self.extract_count_spinbox.config(state=state)

    def _browse_folder(self, entry_widget):
        folder_selected = filedialog.askdirectory(parent=self)
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
        state = tk.NORMAL if self.comparison_mode_var.get() == "ad_comparison" else tk.DISABLED
        self.ad_folder_entry.config(state=state)

    def _save_and_close(self):
        if self._save_settings():
            self.destroy()

    def _save_settings(self):
        try:
            extract_count_val = int(self.extract_count_var.get())
            if self.enable_extract_count_limit_var.get() and extract_count_val <= 0:
                messagebox.showerror("輸入錯誤", "提取數量必須是大於0的整數！", parent=self)
                return False
        except ValueError:
            messagebox.showerror("輸入錯誤", "提取數量必須是有效數字！", parent=self)
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
            messagebox.showerror("錯誤", "漫畫掃描根資料夾無效或不存在！", parent=self)
            return False
        if config_to_save["comparison_mode"] == "ad_comparison" and (not config_to_save["ad_folder_path"] or not os.path.isdir(config_to_save["ad_folder_path"])):
            messagebox.showerror("錯誤", "在廣告比對模式下，廣告圖片資料夾無效或不存在！", parent=self)
            return False
        if config_to_save["enable_time_filter"]:
            if not self._validate_date(config_to_save["start_date_filter"]) or not self._validate_date(config_to_save["end_date_filter"]):
                messagebox.showerror("輸入錯誤", "日期格式無效 (YYYY-MM-DD)。", parent=self)
                return False

        self.master.config = config_to_save
        save_config(config_to_save, CONFIG_FILE)
        messagebox.showinfo("設定已保存", "您的設定已成功保存。", parent=self)
        return True

    def _rebuild_folder_cache(self):
        if messagebox.askyesno("重建快取", "這將清空並重建資料夾建立時間快取，確定嗎？", parent=self):
            try:
                manager = FolderCreationCacheManager()
                manager.invalidate_cache()
                manager.save_cache()
                messagebox.showinfo("操作成功", "資料夾時間快取已清空。", parent=self)
            except Exception as e:
                messagebox.showerror("錯誤", f"清空快取失敗: {e}", parent=self)

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.config = load_config(CONFIG_FILE)
        self.all_similar_files = []
        self.all_file_data = {}
        self.selected_files = set()
        self.banned_ad_images = set()
        
        self.pil_img_target, self.pil_img_compare = None, None
        self.img_tk_target, self.img_tk_compare = None, None
        
        self.scan_thread = None
        self.progress_queue = Queue()
        self._after_id = None
        self._preview_delay = 250
        
        self._setup_main_window()
        self._create_widgets()
        self._bind_keys()
        self.check_queue()

    def _setup_main_window(self):
        self.title(f"{APP_NAME_TC} v{APP_VERSION}")
        self.geometry("1600x900")
        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        sys.excepthook = self.custom_excepthook
        self.bold_font = self._create_bold_font()

    def custom_excepthook(self, exc_type, exc_value, exc_traceback):
        log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", include_traceback=True)
        if self.winfo_exists():
             messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt' 獲取詳細資訊。")
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        self.destroy()

    def _create_bold_font(self):
        try:
            default_font = ttk.Style().lookup("TLabel", "font")
            font_family = self.tk.call('font', 'actual', default_font, '-family')
            font_size = self.tk.call('font', 'actual', default_font, '-size')
            return (font_family, abs(int(font_size)), 'bold')
        except (ValueError, IndexError, tk.TclError):
            return ("TkDefaultFont", 9, 'bold')

    def _create_widgets(self):
        top_frame = ttk.Frame(self, padding="5")
        top_frame.pack(side=tk.TOP, fill=tk.X)
        self.settings_button = ttk.Button(top_frame, text="設定", command=self.open_settings)
        self.settings_button.pack(side=tk.LEFT, padx=5)
        self.start_button = ttk.Button(top_frame, text="開始執行", command=self.start_scan, style="Accent.TButton")
        self.start_button.pack(side=tk.LEFT, padx=5)
        ttk.Style(self).configure("Accent.TButton", font=self.bold_font, foreground='blue')

        main_pane = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        left_frame = ttk.Frame(main_pane)
        main_pane.add(left_frame, weight=3)
        self._create_treeview(left_frame)
        
        right_frame = ttk.Frame(main_pane)
        main_pane.add(right_frame, weight=2)
        self._create_preview_panels(right_frame)

        bottom_button_container = ttk.Frame(self)
        bottom_button_container.pack(fill=tk.X, expand=False, padx=10, pady=(0, 5))
        self._create_bottom_buttons(bottom_button_container)

        status_frame = ttk.Frame(self, relief=tk.SUNKEN, padding=2)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X)
        self.status_label = ttk.Label(status_frame, text="準備就緒")
        self.status_label.pack(side=tk.LEFT, padx=5)
        self.progress_bar = ttk.Progressbar(status_frame, orient='horizontal', mode='determinate')
        self.progress_bar.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=5)

    def _create_treeview(self, parent_frame):
        columns = ("checkbox", "filename", "path", "count", "size", "ctime", "similarity")
        self.tree = ttk.Treeview(parent_frame, columns=columns, show="headings", selectmode="extended")
        
        self.tree.heading("checkbox", text="")
        self.tree.heading("filename", text="群組 - 重複/相似圖片")
        self.tree.heading("path", text="路徑")
        self.tree.heading("count", text="數量")
        self.tree.heading("size", text="大小")
        self.tree.heading("ctime", text="建立日期")
        self.tree.heading("similarity", text="相似度")

        self.tree.column("checkbox", width=40, minwidth=40, stretch=tk.NO, anchor=tk.W)
        self.tree.column("filename", width=300, stretch=tk.YES, anchor=tk.W)
        self.tree.column("path", width=300, stretch=tk.YES, anchor=tk.W)
        self.tree.column("count", width=50, minwidth=50, stretch=tk.NO, anchor=tk.CENTER)
        self.tree.column("size", width=100, minwidth=90, stretch=tk.NO, anchor=tk.E)
        self.tree.column("ctime", width=150, minwidth=140, stretch=tk.NO, anchor=tk.CENTER)
        self.tree.column("similarity", width=80, minwidth=70, stretch=tk.NO, anchor=tk.CENTER)
        
        self.tree.tag_configure('child_item', foreground='#555555')
        self.tree.tag_configure('source_copy_item', background='lightyellow')
        self.tree.tag_configure('ad_parent_item', font=self.bold_font, background='#FFFACD')
        self.tree.tag_configure('parent_item', font=self.bold_font)

        vscroll = ttk.Scrollbar(parent_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vscroll.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        
    def _create_preview_panels(self, parent_frame):
        right_pane = ttk.Panedwindow(parent_frame, orient=tk.VERTICAL)
        right_pane.pack(fill=tk.BOTH, expand=True)

        self.target_image_frame = ttk.LabelFrame(right_pane, text="選中圖片預覽", padding="5")
        right_pane.add(self.target_image_frame, weight=1)
        self.target_image_label = ttk.Label(self.target_image_frame, cursor="hand2")
        self.target_image_label.pack(fill=tk.BOTH, expand=True)
        self.target_path_label = ttk.Label(self.target_image_frame, text="", wraplength=500)
        self.target_path_label.pack(fill=tk.X)
        self.target_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=True))
        
        self.compare_image_frame = ttk.LabelFrame(right_pane, text="群組基準圖片預覽", padding="5")
        right_pane.add(self.compare_image_frame, weight=1)
        self.compare_image_label = ttk.Label(self.compare_image_frame, cursor="hand2")
        self.compare_image_label.pack(fill=tk.BOTH, expand=True)
        self.compare_path_label = ttk.Label(self.compare_image_frame, text="", wraplength=500)
        self.compare_path_label.pack(fill=tk.X)
        self.compare_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=False))

        self.target_image_label.bind("<Configure>", self._on_preview_resize)
        self.compare_image_label.bind("<Configure>", self._on_preview_resize)
        self._create_context_menu()

    def _create_bottom_buttons(self, parent_frame):
        button_frame = ttk.Frame(parent_frame)
        button_frame.pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="選取建議", command=self._select_suggested_for_deletion).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="刪除選中(回收桶)", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=2)
        
        actions_frame = ttk.Frame(parent_frame)
        actions_frame.pack(side=tk.RIGHT, padx=5, pady=5)
        ttk.Button(actions_frame, text="開啟選中資料夾", command=self._open_selected_folder_single).pack(side=tk.LEFT, padx=2)
        ttk.Button(actions_frame, text="開啟回收桶", command=self._open_recycle_bin).pack(side=tk.LEFT, padx=2)
    
    def _bind_keys(self):
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select)
        self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection)
        self.tree.bind("<Return>", self._toggle_selection)
        self.tree.bind("<Delete>", lambda e: self._delete_selected_from_disk())
        self.tree.bind("<BackSpace>", lambda e: self._delete_selected_from_disk())
        self.tree.bind("<Motion>", self._on_mouse_motion)
        self.tooltip = None
        self.tree.bind("<Up>", lambda e: self._navigate_image(e, "Up"))
        self.tree.bind("<Down>", lambda e: self._navigate_image(e, "Down"))

    def open_settings(self):
        self.settings_button.config(state=tk.DISABLED)
        SettingsGUI(self)
        self.settings_button.config(state=tk.NORMAL)

    def start_scan(self):
        if self.scan_thread and self.scan_thread.is_alive():
            messagebox.showwarning("正在執行", "掃描任務正在執行中，請稍候。")
            return
        
        self.start_button.config(state=tk.DISABLED)
        self.settings_button.config(state=tk.DISABLED)
        self.tree.delete(*self.tree.get_children())
        self.all_similar_files.clear()
        self.all_file_data.clear()
        
        self.scan_thread = threading.Thread(target=self._run_scan_in_thread, daemon=True)
        self.scan_thread.start()

    def check_queue(self):
        try:
            while True:
                msg = self.progress_queue.get_nowait()
                msg_type = msg.get('type')
                if msg_type == 'progress':
                    self.progress_bar['value'] = msg.get('value', 0)
                    if 'text' in msg: self.status_label['text'] = msg['text']
                elif msg_type == 'text':
                    self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'result':
                    self.all_similar_files = msg.get('data', [])
                    self.all_file_data = msg.get('meta', {})
                    self._populate_listbox()
                elif msg_type == 'finish':
                    self.status_label['text'] = msg.get('text', "任務完成")
                    self.progress_bar['value'] = 0
                    self.start_button.config(state=tk.NORMAL)
                    self.settings_button.config(state=tk.NORMAL)
                    if not self.all_similar_files:
                        messagebox.showinfo("掃描結果", "未找到符合條件的相似或廣告圖片。")
        except Empty:
            pass
        finally:
            self.after(100, self.check_queue)
    
    def _run_scan_in_thread(self):
        try:
            engine = ImageComparisonEngine(self.config, self.progress_queue)
            similar_files, all_file_data = engine.find_duplicates()
            
            self.progress_queue.put({'type': 'result', 'data': similar_files, 'meta': all_file_data})
            self.progress_queue.put({'type': 'finish', 'text': f"掃描完成。找到 {len(similar_files)} 對相似項。"})
        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", include_traceback=True)
            self.progress_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})
            if self.winfo_exists():
                messagebox.showerror("執行錯誤", f"程式執行時發生錯誤: {e}")
            
    def _populate_listbox(self):
        self.tree.delete(*self.tree.get_children())
        current_selection = self.selected_files.copy()
        self.selected_files.clear()
        
        sim_map = {}
        adj = defaultdict(list)
        nodes = set()
        
        for path1, path2, sim in self.all_similar_files:
            nodes.add(path1); nodes.add(path2)
            adj[path1].append(path2); adj[path2].append(path1)
            sim_map[tuple(sorted((path1, path2)))] = sim

        visited = set()
        all_components = []
        for node in sorted(list(nodes)): 
            if node not in visited:
                component = set(); q = deque([node]); visited.add(node)
                while q:
                    current = q.popleft(); component.add(current)
                    for neighbor in adj.get(current, []):
                        if neighbor not in visited: visited.add(neighbor); q.append(neighbor)
                if len(component) > 1:
                    all_components.append(sorted(list(component)))
        
        unique_id_counter = 0
        for component in all_components:
            group_key = component[0]
            if group_key in self.banned_ad_images: continue
            
            parent_id = f"group_{unique_id_counter}"; unique_id_counter += 1
            is_ad_group = self.config['comparison_mode'] == 'ad_comparison'
            parent_tags = ('ad_parent_item' if is_ad_group else 'parent_item', group_key)
            parent_data = self.all_file_data.get(group_key, {})
            parent_size = f"{parent_data.get('size', 0):,}" if 'size' in parent_data else "N/A"
            parent_ctime_ts = parent_data.get('ctime')
            parent_ctime = datetime.datetime.fromtimestamp(parent_ctime_ts).strftime('%Y/%m/%d %H:%M') if parent_ctime_ts else "N/A"

            self.tree.insert("", "end", iid=parent_id, 
                             values=("☐" if not is_ad_group else "", os.path.basename(group_key), 
                                     os.path.dirname(group_key), len(component) if not is_ad_group else len(component)-1, 
                                     parent_size, parent_ctime, "基準"), 
                             tags=parent_tags, open=True)

            for path in component:
                if is_ad_group and path == group_key: continue

                child_tags = ['child_item', path, group_key]
                if path == group_key: child_tags.append('source_copy_item')
                
                item_id = f"item_{unique_id_counter}"; unique_id_counter += 1
                similarity_val = sim_map.get(tuple(sorted((group_key, path))), 100.0)
                child_data = self.all_file_data.get(path, {})
                child_size = f"{child_data.get('size', 0):,}" if 'size' in child_data else "N/A"
                child_ctime_ts = child_data.get('ctime')
                child_ctime = datetime.datetime.fromtimestamp(child_ctime_ts).strftime('%Y/%m/%d %H:%M') if child_ctime_ts else "N/A"
                
                self.tree.insert(parent_id, "end", iid=item_id, 
                                 values=("☑" if path in current_selection else "☐", f"  └─ {os.path.basename(path)}", 
                                         os.path.dirname(path), "", child_size, child_ctime, f"{similarity_val:.1f}%"),
                                 tags=tuple(child_tags))
        
        if self.tree.get_children():
            first_item = self.tree.get_children()[0]
            self.tree.selection_set(first_item); self.tree.focus(first_item)

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
        if self._after_id: self.after_cancel(self._after_id)
        self._after_id = self.after(self._preview_delay, self._load_and_display_selected_image)

    def _load_and_display_selected_image(self):
        self._after_id = None
        selected = self.tree.selection()
        if not selected:
            self.target_image_label.config(image=""); self.compare_image_label.config(image="")
            self.target_path_label.config(text=""); self.compare_path_label.config(text="")
            return

        tags = self.tree.item(selected[0], "tags")
        selected_path, compare_path = (tags[1], tags[1]) if 'parent_item' in tags or 'ad_parent_item' in tags else (tags[1], tags[2]) if 'child_item' in tags else (None, None)

        self.pil_img_target = self._load_pil_image(selected_path, self.target_path_label)
        self.pil_img_compare = self._load_pil_image(compare_path, self.compare_path_label) if compare_path != "N/A" else None

        self._update_all_previews()
    
    def _load_pil_image(self, path, label_widget):
        if not path:
            label_widget.config(text=""); return None
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img)
                label_widget.config(text=f"路徑: {path}")
                return img.copy()
        except Exception:
            label_widget.config(text=f"無法載入圖片: {os.path.basename(path)}"); return None

    def _update_all_previews(self):
        self._resize_and_display(self.target_image_label, self.pil_img_target, True)
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)

    def _on_preview_resize(self, event):
        is_target = (event.widget == self.target_image_label)
        pil_image = self.pil_img_target if is_target else self.pil_img_compare
        self._resize_and_display(event.widget, pil_image, is_target)

    def _resize_and_display(self, label, pil_image, is_target):
        if pil_image is None:
            label.config(image=""); 
            if is_target: self.img_tk_target = None
            else: self.img_tk_compare = None
            return

        w, h = label.winfo_width(), label.winfo_height()
        if w <= 1 or h <= 1: return
            
        img_copy = pil_image.copy()
        img_copy.thumbnail((w - 10, h - 10), Image.Resampling.LANCZOS)
        img_tk = ImageTk.PhotoImage(img_copy)
        
        label.config(image=img_tk)
        if is_target: self.img_tk_target = img_tk
        else: self.img_tk_compare = img_tk
    
    def _on_preview_image_click(self, event, is_target_image):
        path_label = self.target_path_label if is_target_image else self.compare_path_label
        text = path_label.cget("text")
        if text.startswith("路徑: "):
            path = text[len("路徑: "):].strip()
            if path and os.path.exists(path):
                self._open_folder(os.path.dirname(path))

    def _navigate_image(self, event, direction):
        selected_id = self.tree.selection()
        if not selected_id: return "break"
        current_id = selected_id[0]
        
        target_id = None
        if direction == "Down":
            parent = self.tree.parent(current_id)
            if parent == "" and self.tree.item(current_id, "open"): # Is an open parent
                children = self.tree.get_children(current_id)
                if children: target_id = children[0]
                else: target_id = self.tree.next(current_id)
            elif parent != "": # Is a child
                siblings = self.tree.get_children(parent)
                idx = siblings.index(current_id)
                if idx < len(siblings) - 1: target_id = siblings[idx+1]
                else: target_id = self.tree.next(parent) # Next parent
            else: # Is a closed parent
                target_id = self.tree.next(current_id)
        
        elif direction == "Up":
            parent = self.tree.parent(current_id)
            if parent != "": # Is a child
                siblings = self.tree.get_children(parent)
                if current_id == siblings[0]: target_id = parent # Go to parent
                else: target_id = self.tree.prev(current_id)
            else: # Is a parent
                prev_sibling = self.tree.prev(current_id)
                if prev_sibling and self.tree.item(prev_sibling, "open"):
                    children = self.tree.get_children(prev_sibling)
                    if children: target_id = children[-1] # Go to last child of prev open parent
                    else: target_id = prev_sibling
                else:
                    target_id = prev_sibling

        if target_id:
            self.tree.selection_set(target_id)
            self.tree.focus(target_id)
            self.tree.see(target_id)
        
        return "break"

    def _toggle_selection_by_item_id(self, item_id):
        if not self.tree.exists(item_id): return
        tags = self.tree.item(item_id, "tags")

        if 'parent_item' in tags:
            select_all = self.tree.set(item_id, "checkbox") == "☐"
            self.tree.set(item_id, column="checkbox", value="☑" if select_all else "☐")
            for child_id in self.tree.get_children(item_id):
                if 'source_copy_item' not in self.tree.item(child_id, "tags"):
                    self._update_child_selection(child_id, select_all)
        elif 'child_item' in tags:
            is_selected = self.tree.item(item_id, "tags")[1] in self.selected_files
            self._update_child_selection(item_id, not is_selected)
            parent_id = self.tree.parent(item_id)
            if parent_id and 'ad_parent_item' not in self.tree.item(parent_id, "tags"):
                self._update_parent_checkbox(parent_id)

    def _update_child_selection(self, child_id, select):
        path_to_toggle = self.tree.item(child_id, "tags")[1]
        if select:
            self.selected_files.add(path_to_toggle)
            self.tree.set(child_id, column="checkbox", value="☑")
        else:
            self.selected_files.discard(path_to_toggle)
            self.tree.set(child_id, column="checkbox", value="☐")

    def _update_parent_checkbox(self, parent_id):
        children = self.tree.get_children(parent_id)
        selectable = [cid for cid in children if 'source_copy_item' not in self.tree.item(cid, "tags")]
        selected_count = sum(1 for cid in selectable if self.tree.set(cid, "checkbox") == "☑")
        all_selected = selectable and selected_count == len(selectable)
        self.tree.set(parent_id, column="checkbox", value="☑" if all_selected else "☐")

    def _toggle_selection(self, event=None):
        for item_id in self.tree.selection(): self._toggle_selection_by_item_id(item_id)

    def _update_all_checkboxes_based_on_selection_set(self):
        for parent_id in self.tree.get_children(""):
            for child_id in self.tree.get_children(parent_id):
                path = self.tree.item(child_id, "tags")[1]
                if 'source_copy_item' not in self.tree.item(child_id, "tags"):
                    self._update_child_selection(child_id, path in self.selected_files)
            self._update_parent_checkbox(parent_id)

    def _select_all(self):
        self.selected_files.clear()
        for parent_id in self.tree.get_children(""):
            for child_id in self.tree.get_children(parent_id):
                if 'source_copy_item' not in self.tree.item(child_id, "tags"):
                    self.selected_files.add(self.tree.item(child_id, "tags")[1])
        self._update_all_checkboxes_based_on_selection_set()

    def _select_suggested_for_deletion(self): self._select_all()
    def _deselect_all(self):
        self.selected_files.clear()
        self._update_all_checkboxes_based_on_selection_set()

    def _invert_selection(self):
        all_paths = {self.tree.item(cid, "tags")[1] for pid in self.tree.get_children("") for cid in self.tree.get_children(pid) if 'source_copy_item' not in self.tree.item(cid, "tags")}
        self.selected_files = all_paths - self.selected_files
        self._update_all_checkboxes_based_on_selection_set()

    def _delete_selected_from_disk(self):
        if not self.selected_files or not messagebox.askyesno("確認刪除", f"確定要將 {len(self.selected_files)} 個圖片移至回收桶嗎？"): return
        
        deleted_paths = set()
        for path in list(self.selected_files):
            try:
                send2trash.send2trash(os.path.abspath(path)); deleted_paths.add(path)
            except Exception as e:
                log_error(f"移至回收桶失敗 {path}: {e}", include_traceback=True)
        
        if len(deleted_paths) < len(self.selected_files):
            messagebox.showerror("刪除失敗", f"有 {len(self.selected_files) - len(deleted_paths)} 個檔案刪除失敗。")

        if deleted_paths:
            self.all_similar_files = [(p1, p2, sim) for p1, p2, sim in self.all_similar_files if p1 not in deleted_paths and p2 not in deleted_paths]
            for path in deleted_paths:
                if path in self.all_file_data: del self.all_file_data[path]
            self.selected_files.clear()
            self._populate_listbox()
            messagebox.showinfo("刪除完成", f"成功將 {len(deleted_paths)} 個文件移至回收桶。")

    def _open_recycle_bin(self):
        try:
            if sys.platform == "win32": subprocess.run(['explorer.exe', 'shell:RecycleBinFolder'])
            elif sys.platform == "darwin": subprocess.run(['open', os.path.expanduser("~/.Trash")])
            else: subprocess.run(['xdg-open', "trash:/"])
        except Exception as e:
            messagebox.showerror("開啟失敗", f"無法自動開啟回收桶: {e}")

    def _open_folder(self, folder_path):
        try:
            if not os.path.isdir(folder_path):
                messagebox.showwarning("路徑無效", f"資料夾不存在: {folder_path}")
                return
            if sys.platform == "win32": os.startfile(folder_path)
            elif sys.platform == "darwin": subprocess.Popen(["open", folder_path])
            else: subprocess.Popen(["xdg-open", folder_path])
        except Exception as e:
            log_error(f"開啟資料夾失敗 {folder_path}: {e}", include_traceback=True)

    def _open_selected_folder_single(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("提示", "請先在列表中選中一個項目。")
            return
        path = self.tree.item(selected[0], "tags")[1]
        if path and os.path.exists(path): self._open_folder(os.path.dirname(path))

    def _create_context_menu(self):
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="臨時隱藏此群組", command=self._ban_ad_image)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="取消所有隱藏", command=self._unban_all_ads)

    def _show_context_menu(self, event):
        if self.tree.identify_row(event.y): self.context_menu.tk_popup(event.x_root, event.y_root)

    def _ban_ad_image(self):
        selected = self.tree.selection()
        if not selected: return
        item_id = self.tree.parent(selected[0]) or selected[0]
        key = self.tree.item(item_id, "tags")[1]
        if key: self.banned_ad_images.add(key); self._populate_listbox()
    
    def _unban_all_ads(self):
        self.banned_ad_images.clear(); self._populate_listbox()
    
    def _on_mouse_motion(self, event):
        item_id = self.tree.identify_row(event.y)
        if hasattr(self, 'tooltip_item_id') and self.tooltip_item_id == item_id: return
        if self.tooltip: self.tooltip.leave(); self.tooltip = None
        
        self.tooltip_item_id = item_id
        if item_id:
            if 'ad_parent_item' in self.tree.item(item_id, "tags"):
                self.tooltip = Tooltip(self.tree, "廣告圖片 (基準，不會被刪除)")
                self.tooltip.enter(event)
    
    def _on_closing(self):
        if messagebox.askokcancel("關閉程式", "確定要關閉程式嗎？"): self.destroy()

# === 11. 主程式入口 ===
def main():
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