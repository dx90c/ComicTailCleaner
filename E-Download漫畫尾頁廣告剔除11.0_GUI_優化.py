# ======================================================================
# 檔案名稱：E-Download漫畫尾頁廣告剔除11.0_GUI_優化.py
# 版本號：11.0v78
#
# === 程式說明 ===
# 這是一個專為清理 E-Download 資料夾中漫畫檔案尾頁廣告的工具。
# 它能自動檢測並移除與廣告圖片相似或相互重複的圖片，提升漫畫閱讀體驗。
# 適用於處理大量漫畫檔案，節省手動篩選時間。
# 支援三種比對模式：廣告比對、互相比對和 QR Code 檢測。
#
# === 11.0v78 版本更新內容 (基於 11.0v77 穩定版) ===
# 本次更新為重大功能整合，旨在提升性能、易用性和介面穩定性。
#
# 1. **【性能】引入增量比對快取 (Comparison Result Cache)**:
#    - 新增 `comparison_cache.json` 快取檔案，用於儲存圖片對的比對結果。
#    - 在後續掃描中，若圖片對未曾變動，程式會直接從快取讀取相似度，
#      極大地減少了重複計算的耗時，對大型圖庫的二次掃描速度有顯著提升。
#    - 設定視窗中加入了「重建比對結果快取」的選項。
#
# 2. **【功能】新增右鍵選單隱藏功能 (Ban Ad Image)**:
#    - 在結果列表中的項目上按右鍵，會彈出上下文選單。
#    - 可選擇「臨時隱藏此廣告的所有結果」，將由特定廣告圖片匹配到的所有項目
#      從當前列表中暫時隱藏，方便使用者專注於處理其他結果。
#    - 提供「取消所有隱藏」選項以恢復顯示。
#
# 3. **【佈局】徹底修復圖片預覽區佈局問題**:
#    - 右側的兩個圖片預覽區現在被放置在一個垂直的 `PanedWindow` 中，
#      使用者可以手動拖動分隔條來調整兩個預覽區的大小。
#    - **核心修復**: 採用 `<Configure>` 事件綁定機制。圖片的縮放不再依賴不可靠的
#      `after` 延遲或 `winfo` 瞬間值，而是在預覽區的尺寸實際發生改變後
#      (如視窗初始化、使用者拖動分隔條或改變視窗大小)，才觸發圖片的重新縮放。
#    - 此修改徹底解決了之前版本中因佈局計算時序問題導致的
#      「圖片顯示不全」、「互相擠壓」以及「scale must be > 0」的錯誤。
#
# === 11.0v77 版本更新內容 (基於 11.0v76 修正) ===
# - **版本號更新**: 將程式版本號從 `11.0v76` 更新為 `11.0v77`。
# - **強化路徑處理**:
#   - 在 `_open_folder` 方法中，再次確認並強化了路徑正規化邏輯。
#   - 使用 `os.path.abspath(os.path.normpath(folder_path))` 來確保傳遞給
#     系統 shell 的是一個絕對且符合該作業系統標準格式的路徑。
# - **快取使用情況可視化**:
#   - 在 `_calculate_hashes_multiprocess` 方法中，加入了詳細的日誌輸出。
# ======================================================================


# === 1. 標準庫導入 (Python Built-in Libraries) ===
import os
import sys
import json
import datetime
import traceback
import subprocess
from collections import deque
from multiprocessing import set_start_method, Pool, cpu_count
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

# === 3. Tkinter GUI 庫導入 ===
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from tkinter import messagebox

# === 4. 全局常量和設定 ===
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
    try:
        if 'PIL' not in sys.modules or 'imagehash' not in sys.modules:
            raise ImportError("Pillow 或 imagehash 未成功導入")
        print("Pillow 和 imagehash 套件檢查通過。", flush=True)
    except ImportError as e:
        messagebox.showerror("缺少核心依賴", f"請安裝必要的 Python 套件：Pillow 和 imagehash。\n"
                                             f"錯誤詳情: {e}\n"
                                             "可以使用 'pip install Pillow imagehash' 命令安裝。")
        sys.exit(1)
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
    'comparison_mode': 'ad_comparison',
    'similarity_threshold': 85,
    'rebuild_ad_cache': False,
    'enable_time_filter': False,
    'start_date_filter': '',
    'end_date_filter': '',
    'rebuild_comparison_cache': False # v78 新增
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


# === 7. 快取管理相關 ===
class ComparisonResultCacheManager:
    # v78 新增: 用於儲存比對結果的快取管理器
    def __init__(self, cache_file_path="comparison_cache.json"):
        self.cache_file_path = cache_file_path
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    # Key is "path1||path2", value is similarity
                    loaded_cache = json.load(f)
                    print(f"比對結果快取 '{self.cache_file_path}' 已成功載入。", flush=True)
                    return loaded_cache
            except json.JSONDecodeError:
                log_error(f"比對結果快取檔案 '{self.cache_file_path}' 格式不正確，將重建。", include_traceback=True)
            except Exception as e:
                log_error(f"載入比對結果快取時發生錯誤: {e}，將重建。", include_traceback=True)
        return {}

    def save_cache(self):
        try:
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=4, ensure_ascii=False)
            print(f"比對結果快取已成功保存到 '{self.cache_file_path}'。", flush=True)
        except Exception as e:
            log_error(f"保存比對結果快取時發生錯誤: {e}", include_traceback=True)

    def get_result(self, path1, path2):
        key = f"{path1}||{path2}"
        return self.cache.get(key)

    def update_result(self, path1, path2, similarity):
        key = f"{path1}||{path2}"
        self.cache[key] = similarity
        
    def invalidate_cache(self):
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try:
                os.remove(self.cache_file_path)
                print(f"比對結果快取檔案 '{self.cache_file_path}' 已刪除。", flush=True)
            except Exception as e:
                log_error(f"刪除比對結果快取檔案 '{self.cache_file_path}' 時發生錯誤: {e}", include_traceback=True)
        print("比對結果快取已失效。", flush=True)


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


class ScannedImageHashesCacheManager:
    def __init__(self, root_scan_folder):
        normalized_path = os.path.normpath(root_scan_folder).replace('\\', '/')
        hash_object = hashlib.sha256(normalized_path.encode('utf-8'))
        self.cache_file_path = f"scanned_hashes_cache_{hash_object.hexdigest()}.json"
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    converted_cache = {}
                    for path, data in loaded_data.items():
                        if isinstance(data, dict) and 'hash' in data and 'mtime' in data:
                            try:
                                phash_obj = imagehash.hex_to_hash(data['hash'])
                                converted_cache[path] = {
                                    'hash': phash_obj,
                                    'mtime': float(data['mtime'])
                                }
                            except (ValueError, TypeError, AttributeError) as e:
                                log_error(f"快取檔案 '{self.cache_file_path}' 中 '{path}' 的數據格式不正確或哈希值無效 ({e})，將忽略此項。", include_traceback=True)
                                continue
                        else:
                            log_error(f"快取檔案 '{self.cache_file_path}' 中 '{path}' 的格式不正確 (非字典或缺少鍵)，將忽略此項。", include_traceback=True)
                    print(f"掃描圖片哈希快取 '{self.cache_file_path}' 已成功載入。", flush=True)
                    return converted_cache
            except json.JSONDecodeError:
                log_error(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 格式不正確，將重建快取。", include_traceback=True)
            except Exception as e:
                log_error(f"載入掃描圖片哈希快取時發生錯誤: {e}，將重建快取。", include_traceback=True)
        print(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 不存在或載入失敗，將從空快取開始。", flush=True)
        return {}

    def save_cache(self):
        try:
            serializable_cache = {}
            for path, data in self.cache.items():
                if data and 'hash' in data and 'mtime' in data and data['hash'] is not None:
                    serializable_cache[path] = {
                        'hash': str(data['hash']),
                        'mtime': str(data['mtime'])
                    }
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_cache, f, indent=4, ensure_ascii=False)
            print(f"掃描圖片哈希快取已成功保存到 '{self.cache_file_path}'。", flush=True)
        except Exception as e:
            log_error(f"保存掃描圖片哈希快取時發生錯誤: {e}", include_traceback=True)

    def get_hash(self, file_path):
        if file_path in self.cache:
            cached_data = self.cache[file_path]
            try:
                current_mtime = os.path.getmtime(file_path)
                if abs(current_mtime - cached_data['mtime']) < 0.001:
                    return cached_data['hash']
            except FileNotFoundError:
                log_error(f"快取中文件 '{file_path}' 不存在，將重新計算。", include_traceback=False)
            except Exception as e:
                log_error(f"檢查文件 '{file_path}' 修改時間時發生錯誤: {e}，將重新計算。", include_traceback=True)
        return None

    def update_hash(self, file_path, phash, mtime):
        try:
            if phash is not None:
                self.cache[file_path] = {'hash': phash, 'mtime': mtime}
            else:
                log_error(f"跳過更新無效哈希值的快取: {file_path}", include_traceback=False)
        except Exception as e:
            log_error(f"更新哈希快取時發生錯誤: {e}", include_traceback=True)

    def invalidate_cache(self):
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try:
                os.remove(self.cache_file_path)
                print(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 已刪除。", flush=True)
            except Exception as e:
                log_error(f"刪除掃描圖片哈希快取檔案 '{self.cache_file_path}' 時發生錯誤: {e}", include_traceback=True)
        print("掃描圖片哈希快取已失效。", flush=True)

AD_HASH_CACHE_FILE = "ad_hashes.json"

def load_ad_hashes(ad_folder_path, rebuild_cache=False):
    ad_hashes = {}
    if os.path.exists(AD_HASH_CACHE_FILE) and not rebuild_cache:
        try:
            with open(AD_HASH_CACHE_FILE, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                ad_hashes = {path: imagehash.hex_to_hash(phash_str) for path, phash_str in loaded_data.items()}
            print(f"廣告圖片哈希快取 '{AD_HASH_CACHE_FILE}' 已成功載入。", flush=True)
            return ad_hashes
        except json.JSONDecodeError:
            log_error(f"廣告哈希快取檔案 '{AD_HASH_CACHE_FILE}' 格式不正確，將重建快取。", include_traceback=True)
        except Exception as e:
            log_error(f"載入廣告哈希快取時發生錯誤: {e}，將重建快取。", include_traceback=True)
    
    print(f"正在重建廣告圖片哈希快取，掃描資料夾: {ad_folder_path}", flush=True)
    if os.path.isdir(ad_folder_path):
        all_ad_images = []
        for root, _, files in os.walk(ad_folder_path):
            for file in files:
                if file.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp')):
                    all_ad_images.append(os.path.normpath(os.path.join(root, file)).replace('\\', '/'))
        for img_path in all_ad_images:
            img_hash = calculate_image_hash(img_path)
            if img_hash:
                ad_hashes[img_path] = img_hash
    serializable_ad_hashes = {path: str(phash) for path, phash in ad_hashes.items()}
    try:
        with open(AD_HASH_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(serializable_ad_hashes, f, indent=4, ensure_ascii=False)
        print(f"廣告圖片哈希快取已保存到 '{AD_HASH_CACHE_FILE}'。", flush=True)
    except Exception as e:
        log_error(f"保存廣告哈希快取時發生錯誤: {e}", include_traceback=True)
    return ad_hashes

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
        if current_folder != root_folder and enable_time_filter and creation_cache_manager:
            folder_ctime_timestamp = creation_cache_manager.get_creation_time(current_folder)
            if folder_ctime_timestamp is not None:
                folder_ctime = datetime.datetime.fromtimestamp(folder_ctime_timestamp)
                if (start_date and folder_ctime < start_date) or \
                   (end_date and folder_ctime > end_date.replace(hour=23, minute=59, second=59, microsecond=999999)):
                    continue
            else:
                log_error(f"無法獲取資料夾建立時間，跳過時間篩選: {current_folder}", include_traceback=False)
                continue
        if current_folder != root_folder:
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
            for f in os.listdir(folder_path):
                file_path = os.path.join(folder_path, f)
                if os.path.isfile(file_path) and f.lower().endswith(image_extensions):
                    image_files.append((file_path, os.path.getmtime(file_path)))
            image_files.sort(key=lambda x: x[0])
            if enable_limit:
                extracted_files[folder_path] = [item[0] for item in image_files[-count:]]
            else:
                extracted_files[folder_path] = [item[0] for item in image_files]
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

def _pool_worker_hash_and_mtime(image_path):
    phash = calculate_image_hash(image_path)
    if phash is not None:
        try:
            mtime = os.path.getmtime(image_path)
            return image_path, phash, mtime
        except Exception as e:
            log_error(f"獲取文件修改時間失敗 {image_path}: {e}", include_traceback=True)
            return image_path, None, None
    return image_path, None, None


class ImageComparisonEngine:
    def __init__(self, root_scan_folder, ad_folder_path, extract_count, excluded_folders,
                 enable_time_filter, start_date_filter, end_date_filter,
                 similarity_threshold, comparison_mode, rebuild_ad_cache, system_qr_scan_capability,
                 scanned_hashes_cache_manager, comparison_result_cache_manager, # v78: 新增
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
        self.rebuild_ad_cache = rebuild_ad_cache
        self.system_qr_scan_capability = system_qr_scan_capability
        self.image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
        self.target_hashes = {}
        self.ad_hashes_cache = {}
        self.scanned_hashes_cache_manager = scanned_hashes_cache_manager
        self.comparison_result_cache_manager = comparison_result_cache_manager # v78: 新增
        self.folder_count = 0
        self.file_count = 0
        self.processed_folders_display_interval = 1000
        print(f"ImageComparisonEngine initialized with config: {self.__dict__}", flush=True)

    def generate_extracted_files(self, folder_creation_cache_manager):
        print(f"正在生成目標檔案清單...", flush=True)
        print(f"開始處理資料夾: {self.root_scan_folder}", flush=True)
        if self.enable_extract_count_limit:
            print(f"每個資料夾抽取最後 {self.extract_count} 個檔案", flush=True)
        else:
            print("每個資料夾將掃描所有圖片 (未限制抽取數量)", flush=True)
        if self.enable_time_filter:
            print(f"注意: 時間篩選功能已啟用，範圍從 {self.start_date_filter.strftime('%Y-%m-%d')} 到 {self.end_date_filter.strftime('%Y-%m-%d') if self.end_date_filter else '無結束日期'}。", flush=True)
        else:
            print("注意: 時間篩選功能未啟用，所有資料夾都將被掃描。", flush=True)
        all_folders = get_all_subfolders(
            self.root_scan_folder,
            self.excluded_folders,
            self.enable_time_filter,
            self.start_date_filter,
            self.end_date_filter,
            folder_creation_cache_manager
        )
        print(f"過濾後找到 {len(all_folders)} 個資料夾。", flush=True)
        extracted_files_dict = extract_last_n_files_from_folders(all_folders, self.extract_count, self.enable_extract_count_limit)
        total_extracted_files = sum(len(files) for files in extracted_files_dict.values())
        print(f"總共抽取了 {total_extracted_files} 個檔案。", flush=True)
        extracted_file_paths_list = [f_fpath for folder_files in extracted_files_dict.values() for f_fpath in folder_files]
        try:
            with open("extracted_files.txt", "w", encoding="utf-8") as f:
                for filepath in extracted_file_paths_list:
                    f.write(f"{filepath}\n")
            print("結果已儲存至: extracted_files.txt (此文件主要為兼容目的，程式內部使用字典數據)", flush=True)
        except Exception as e:
            log_error(f"寫入 extracted_files.txt 失敗: {e}", include_traceback=True)
        return extracted_files_dict

    def _calculate_hashes_multiprocess(self, file_paths, description="圖片"):
        """使用多進程計算圖片哈希值，並提供詳細的快取命中/未命中報告。"""
        hashes = {}  # 存儲最終的 ImageHash 物件
        paths_to_rehash = []  # 存儲需要重新計算哈希的路徑
        cache_hits = 0
        total_files = len(file_paths)

        print(f"正在檢查 {total_files} 個{description}的快取狀態...", flush=True)

        # 步驟 1: 檢查快取，分離快取命中和未命中的圖片
        for path in file_paths:
            cached_hash = self.scanned_hashes_cache_manager.get_hash(path)
            if cached_hash:
                hashes[path] = cached_hash  # 直接從快取加入結果
                cache_hits += 1
            else:
                paths_to_rehash.append(path)  # 加入待重新計算列表

        # 步驟 2: 打印詳細的快取統計報告
        cache_misses = len(paths_to_rehash)
        hit_rate_percent = (cache_hits / total_files * 100) if total_files > 0 else 0

        print("==================================================")
        print("快取檢查完成:")
        print(f"- 總共檔案: {total_files}")
        print(f"- 快取命中 (無需重算): {cache_hits} ({hit_rate_percent:.2f}%)")
        print(f"- 快取未命中 (新增/已修改): {cache_misses}")
        print("==================================================", flush=True)

        # 如果所有檔案都已在快取中，則直接返回
        if not paths_to_rehash:
            print(f"所有 {description} 檔案均從快取載入，無需重新計算。", flush=True)
            return hashes

        # 步驟 3: 對於快取未命中的檔案，使用多進程進行哈希計算
        num_processes = cpu_count()
        print(f"正在計算 {len(paths_to_rehash)} 個新增/已修改的{description}的哈希值 (多進程，使用 {num_processes} 個進程)...", flush=True)
        
        try:
            with Pool(processes=num_processes) as pool:
                results_iterator = pool.imap_unordered(_pool_worker_hash_and_mtime, paths_to_rehash)
                
                processed_count = 0
                total_to_process = len(paths_to_rehash)
                for path, phash, mtime in results_iterator:
                    if phash is not None:  # 僅當哈希計算成功時處理
                        hashes[path] = phash  # 將新計算的哈希加入結果字典
                        self.scanned_hashes_cache_manager.update_hash(path, phash, mtime)  # 更新主進程的快取管理器
                    processed_count += 1
                    # 打印進度
                    if processed_count % self.processed_folders_display_interval == 0 or processed_count == total_to_process:
                        print(f"  已完成計算 {processed_count}/{total_to_process} 個新增/已修改的{description}的哈希值...", flush=True)
        except Exception as e:
            error_message = f"多進程哈希計算過程中發生嚴重錯誤: {e}"
            log_error(error_message, include_traceback=True)
            print(f"\n!!!! 多進程錯誤 !!!! {error_message}", flush=True)
            try:
                if tk._default_root and tk._default_root.winfo_exists():
                    messagebox.showerror("程式錯誤 - 多進程", f"多進程計算過程中發生錯誤，程式將關閉。\n錯誤: {e}\n請查看 'error_log.txt'。", parent=tk._default_root)
                else: 
                    messagebox.showerror("程式錯誤 - 多進程", f"多進程計算過程中發生錯誤，程式將關閉。\n錯誤: {e}\n請查看 'error_log.txt'。")
            except Exception:
                pass 
            sys.exit(1)

        # 步驟 4: 保存快取並返回結果
        self.scanned_hashes_cache_manager.save_cache()
        print(f"完成計算 {len(hashes)} 個{description}的哈希值 (包含快取與新增)。", flush=True)
        return hashes

    def compare_images(self, files_to_process_dict, ad_hashes_from_main):
        similar_files = []
        all_target_file_paths = [f_fpath for folder_files in files_to_process_dict.values() for f_fpath in folder_files]
        if not all_target_file_paths:
            print("沒有找到任何目標圖片檔案進行比對。", flush=True)
            return []
        print(f"已收集 {len(all_target_file_paths)} 個目標圖片檔案。", flush=True)
        self.target_hashes = self._calculate_hashes_multiprocess(all_target_file_paths, "目標圖片")
        print(f"啟動圖片比對，模式: {self.comparison_mode}", flush=True)
        threshold_diff = (100 - self.similarity_threshold) / 100.0 * 64
        if threshold_diff < 0: threshold_diff = 0
        if threshold_diff > 64: threshold_diff = 64
        if self.comparison_mode == "ad_comparison":
            self.ad_hashes_cache = ad_hashes_from_main
            if not self.ad_hashes_cache:
                print("沒有可用的廣告圖片哈希值進行比對。", flush=True)
                return []
            similar_files = self._compare_with_ads(threshold_diff)
        elif self.comparison_mode == "mutual_comparison":
            similar_files = self._compare_mutually(threshold_diff)
        elif self.comparison_mode == "qr_detection":
            if self.system_qr_scan_capability:
                similar_files = self._detect_qr_codes()
            else:
                print("QR Code 掃描功能因缺少依賴而被禁用，無法執行 QR Code 檢測模式。", flush=True)
                return []
        else:
            print("無效的比對模式，請檢查設定。", flush=True)
            return []
        
        self.comparison_result_cache_manager.save_cache() # v78: 保存比對結果快取
        print("比對完成。", flush=True)
        return similar_files

    def _compare_with_ads(self, threshold_diff):
        print(f"開始與廣告圖片進行比對，相似度閾值: {self.similarity_threshold:.1f}% (哈希差異 <= {int(threshold_diff)})", flush=True)
        found_similar = []
        progress_interval = max(1, len(self.target_hashes) // 20)
        cache_hits = 0
        total_comparisons = 0

        for i, (target_path, target_phash) in enumerate(self.target_hashes.items()):
            if target_phash is None: continue
            for ad_path, ad_phash in self.ad_hashes_cache.items():
                if ad_phash is None: continue
                
                total_comparisons += 1
                cached_similarity = self.comparison_result_cache_manager.get_result(target_path, ad_path)
                
                if cached_similarity is not None:
                    similarity = cached_similarity
                    cache_hits += 1
                else:
                    diff = target_phash - ad_phash
                    similarity = (1 - diff / 64) * 100
                    self.comparison_result_cache_manager.update_result(target_path, ad_path, similarity)
                
                if similarity >= self.similarity_threshold:
                    found_similar.append((target_path, ad_path, similarity))

            if (i + 1) % progress_interval == 0 or (i + 1) == len(self.target_hashes):
                print(f"  已比對 {i + 1}/{len(self.target_hashes)} 個目標圖片...", flush=True)

        if total_comparisons > 0:
            hit_rate = (cache_hits / total_comparisons * 100) if total_comparisons > 0 else 0
            print(f"比對快取命中率: {hit_rate:.2f}% ({cache_hits}/{total_comparisons})")
        
        print(f"廣告比對完成。找到 {len(found_similar)} 個相似圖片。", flush=True)
        return found_similar

    def _compare_mutually(self, threshold_diff):
        print(f"開始在抽取出的圖片之間進行互相比對，相似度閾值: {self.similarity_threshold:.1f}% (哈希差異 <= {int(threshold_diff)})", flush=True)
        found_similar = []
        target_paths_list = list(self.target_hashes.keys())
        target_phashes_list = list(self.target_hashes.values())
        n = len(target_paths_list)
        progress_interval = max(1, n // 20)
        cache_hits = 0
        total_comparisons = 0

        for i in range(n):
            path1 = target_paths_list[i]
            phash1 = target_phashes_list[i]
            if phash1 is None: continue
            for j in range(i + 1, n):
                path2 = target_paths_list[j]
                phash2 = target_phashes_list[j]
                if phash2 is None: continue

                total_comparisons += 1
                # 確保快取鍵的順序一致
                sorted_paths = tuple(sorted((path1, path2)))
                cached_similarity = self.comparison_result_cache_manager.get_result(sorted_paths[0], sorted_paths[1])

                if cached_similarity is not None:
                    similarity = cached_similarity
                    cache_hits += 1
                else:
                    diff = phash1 - phash2
                    similarity = (1 - diff / 64) * 100
                    self.comparison_result_cache_manager.update_result(sorted_paths[0], sorted_paths[1], similarity)

                if similarity >= self.similarity_threshold:
                    found_similar.append((path1, path2, similarity))

            if (i + 1) % progress_interval == 0 or (i + 1) == n:
                print(f"  已比對 {i + 1}/{n} 個目標圖片...", flush=True)
        
        if total_comparisons > 0:
            hit_rate = (cache_hits / total_comparisons * 100) if total_comparisons > 0 else 0
            print(f"比對快取命中率: {hit_rate:.2f}% ({cache_hits}/{total_comparisons})")
            
        print(f"互相比對完成。找到 {len(found_similar)} 對相似圖片。", flush=True)
        return found_similar

    def _detect_qr_codes(self):
        print("開始檢測圖片中的 QR Code...", flush=True)
        found_qr_images = []
        progress_interval = max(1, len(self.target_hashes) // 20)
        qr_detector = cv2.QRCodeDetector()
        for i, (image_path, _) in enumerate(self.target_hashes.items()):
            try:
                with Image.open(image_path) as pil_img:
                    pil_img = ImageOps.exif_transpose(pil_img)
                    pil_img = pil_img.convert('RGB')
                    img_cv = np.array(pil_img)
                    img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR)
                if img_cv is None:
                    log_error(f"無法將圖片轉換為 OpenCV 格式或圖片內容為空: {image_path}", include_traceback=False)
                    continue
                gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
                retval, decoded_info, points, straight_qrcode = qr_detector.detectAndDecodeMulti(gray)
                if retval:
                    found_qr_images.append((image_path, "N/A", 100.0))
            except FileNotFoundError:
                log_error(f"QR Code 檢測失敗: 文件未找到 - {image_path}", include_traceback=False)
            except UnidentifiedImageError:
                log_error(f"QR Code 檢測失敗: 無法識別圖片格式或文件已損壞 - {image_path}", include_traceback=False)
            except Exception as e:
                log_error(f"檢測 QR Code 時發生錯誤於圖片 {image_path}: {e}", include_traceback=True)
            if (i + 1) % progress_interval == 0 or (i + 1) == len(self.target_hashes):
                print(f"  已檢測 {i + 1}/{len(self.target_hashes)} 個圖片...", flush=True)
        print(f"QR Code 檢測完成。找到 {len(found_qr_images)} 個包含 QR Code 的圖片。", flush=True)
        return found_qr_images

class SettingsGUI:
    def __init__(self, master, config_file_path, qr_scan_feature_enabled_global):
        self.master = master
        self.config_file_path = config_file_path
        self.qr_scan_feature_enabled_global = qr_scan_feature_enabled_global
        self.result_config = None
        self.should_proceed = False
        self.rebuild_folder_cache_result = False
        self.rebuild_scanned_cache_result = False
        self.rebuild_comparison_cache_result = False # v78 新增
        self.config = load_config(self.config_file_path)
        self.settings_window = tk.Toplevel(master)
        self.settings_window.title("E-Download 漫畫尾頁廣告剔除 - 設定")
        self.settings_window.geometry("700x750") # 增加高度以容納新選項
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
        basic_settings_frame.grid(row=row_idx+1, column=0, columnspan=3, sticky="ew", pady=5, padx=5)
        basic_settings_frame.grid_columnconfigure(1, weight=1)
        self.enable_extract_count_limit_var = tk.BooleanVar()
        self.chk_enable_extract_count = ttk.Checkbutton(basic_settings_frame, text="啟用圖片抽取數量限制", variable=self.enable_extract_count_limit_var)
        self.chk_enable_extract_count.grid(row=0, column=0, sticky="w", pady=2)
        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2)
        self.extract_count_var = tk.StringVar()
        self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5)
        self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="(以檔名為判斷基準，從每個資料夾末尾提取N張圖片進行比對)").grid(row=1, column=2, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=2, column=0, sticky="w", pady=2)
        self.similarity_threshold_var = tk.DoubleVar()
        ttk.Scale(basic_settings_frame, from_=50, to=100, orient="horizontal", variable=self.similarity_threshold_var, length=200, command=self._update_threshold_label).grid(row=2, column=1, sticky="w", padx=5)
        self.threshold_label = ttk.Label(basic_settings_frame, text="")
        self.threshold_label.grid(row=2, column=2, sticky="w", padx=5)
        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (逗號分隔):").grid(row=3, column=0, sticky="w", pady=2)
        self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=4)
        self.excluded_folders_text.grid(row=3, column=1, columnspan=2, sticky="ew", padx=5)
        scrollbar = ttk.Scrollbar(basic_settings_frame, command=self.excluded_folders_text.yview)
        scrollbar.grid(row=3, column=3, sticky="ns")
        self.excluded_folders_text.config(yscrollcommand=scrollbar.set)
        row_idx += 4
        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10")
        mode_frame.grid(row=row_idx+1, column=0, sticky="ew", pady=5, padx=5)
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
        row_idx += 1
        cache_time_frame = ttk.LabelFrame(frame, text="快取與時間篩選", padding="10")
        cache_time_frame.grid(row=row_idx+1, column=1, columnspan=2, sticky="ew", pady=5, padx=5)
        cache_time_frame.grid_columnconfigure(1, weight=1)
        self.rebuild_ad_cache_var = tk.BooleanVar()
        ttk.Checkbutton(cache_time_frame, text="重建廣告圖片哈希快取", variable=self.rebuild_ad_cache_var).grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        self.rebuild_comparison_cache_var = tk.BooleanVar() # v78 新增
        ttk.Checkbutton(cache_time_frame, text="重建比對結果快取 (下次掃描更快)", variable=self.rebuild_comparison_cache_var).grid(row=1, column=0, columnspan=3, sticky="w", pady=2) # v78 新增
        self.enable_time_filter_var = tk.BooleanVar()
        self.enable_time_filter_checkbox = ttk.Checkbutton(cache_time_frame, text="啟用資料夾建立時間篩選", variable=self.enable_time_filter_var)
        self.enable_time_filter_checkbox.grid(row=2, column=0, columnspan=3, sticky="w", pady=2)
        ttk.Label(cache_time_frame, text="從:").grid(row=3, column=0, sticky="w", padx=5)
        self.start_date_var = tk.StringVar()
        self.start_date_entry = ttk.Entry(cache_time_frame, textvariable=self.start_date_var, width=15)
        self.start_date_entry.grid(row=3, column=1, sticky="ew", padx=5)
        ttk.Label(cache_time_frame, text="(YYYY-MM-DD)").grid(row=3, column=2, sticky="w")
        ttk.Label(cache_time_frame, text="到:").grid(row=4, column=0, sticky="w", padx=5)
        self.end_date_var = tk.StringVar()
        self.end_date_entry = ttk.Entry(cache_time_frame, textvariable=self.end_date_var, width=15)
        self.end_date_entry.grid(row=4, column=1, sticky="ew", padx=5)
        ttk.Label(cache_time_frame, text="(YYYY-MM-DD)").grid(row=4, column=2, sticky="w")
        ttk.Button(cache_time_frame, text="重建資料夾時間快取", command=self._rebuild_folder_cache).grid(row=5, column=0, columnspan=3, sticky="w", pady=5)
        ttk.Button(cache_time_frame, text="重建掃描圖片哈希快取", command=self._rebuild_scanned_cache).grid(row=6, column=0, columnspan=3, sticky="w", pady=5)
        row_idx += 1
        button_frame = ttk.Frame(frame, padding="10")
        button_frame.grid(row=row_idx+1, column=0, columnspan=3, sticky="ew", pady=10, padx=5)
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
        self.similarity_threshold_var.set(self.config.get('similarity_threshold', 85.0))
        self._update_threshold_label(self.similarity_threshold_var.get())
        comparison_mode_cfg = self.config.get('comparison_mode', 'ad_comparison')
        self.comparison_mode_var.set(comparison_mode_cfg)
        self._toggle_ad_folder_entry_state()
        self.rebuild_ad_cache_var.set(self.config.get('rebuild_ad_cache', False))
        self.rebuild_comparison_cache_var.set(self.config.get('rebuild_comparison_cache', False)) # v78 新增
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
                'rebuild_ad_cache': self.rebuild_ad_cache_var.get(),
                'rebuild_comparison_cache': self.rebuild_comparison_cache_var.get(), # v78 新增
                'enable_time_filter': self.enable_time_filter_var.get(),
                'start_date_filter': self.start_date_var.get(),
                'end_date_filter': self.end_date_var.get()
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
        if messagebox.askyesno("重建快取", "這將清空並重建資料夾建立時間快取，此操作可能需要一些時間，確定要繼續嗎？"):
            self.rebuild_folder_cache_result = True
            messagebox.showinfo("快取重建提示", "資料夾建立時間快取已標記為需要重建。下次運行程式時將自動處理此操作。")
    
    def _rebuild_comparison_cache(self): # v78 新增
        if messagebox.askyesno("重建快取", "這將清空並重建圖片比對結果快取，確定要繼續嗎？"):
            self.rebuild_comparison_cache_result = True
            messagebox.showinfo("快取重建提示", "比對結果快取已標記為需要重建。下次運行程式時將自動處理此操作。")

    def _rebuild_scanned_cache(self):
        if messagebox.askyesno("重建快取", "這將清空並重建掃描圖片哈希快取，此操作可能需要一些時間，確定要繼續嗎？"):
            self.rebuild_scanned_cache_result = True
            messagebox.showinfo("快取重建提示", "掃描圖片哈希快取已標記為需要重建。下次運行程式時將自動處理此操作。")

    def _start_execution(self):
        if self._save_settings():
            self.result_config = self.config
            self.should_proceed = True
            self.settings_window.destroy()

    def _on_closing(self):
        if messagebox.askokcancel("關閉程式", "確定要關閉設定視窗並退出程式嗎？"):
            self.should_proceed = False
            self.settings_window.destroy()


class MainWindow:
    def __init__(self, master, similar_files=None, comparison_mode="N/A", initial_similarity_threshold=85.0):
        self.root = master
        self.all_similar_files = similar_files if similar_files is not None else []
        self.displayed_similar_files = []
        self.selected_files = set()
        self.comparison_mode = comparison_mode
        self.deleted_history = deque(maxlen=10)
        self.max_preview_size = (400, 400) # 預設值，實際會被<Configure>覆蓋
        self.current_display_threshold = tk.DoubleVar(value=initial_similarity_threshold)
        self.original_scan_threshold = initial_similarity_threshold

        # v78 佈局和功能變更
        self.banned_ad_images = set()
        self.pil_img_target = None
        self.pil_img_compare = None
        self.img_tk_target = None
        self.img_tk_compare = None
        
        try:
            self.root.title("圖片比對結果 - 廣告/相似圖片清理工具 v11.0v78")
            self.root.geometry("1400x850")
            self.root.deiconify()
            self.root.lift()
            self.root.focus_force()
            self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
            self._create_widgets()
            self._populate_listbox()
            self._bind_keys()
            self.root.update_idletasks()
            print("MainWindow: 介面已成功建立並初始化。", flush=True)
            if self.displayed_similar_files and self.tree.get_children():
                first_item_id = self.tree.get_children()[0]
                self.tree.selection_set(first_item_id)
                self.tree.focus(first_item_id)
                self._on_item_select(None)
        except Exception as e:
            log_error(f"MainWindow 初始化失敗: {e}\n{traceback.format_exc()}")
            messagebox.showerror("GUI 錯誤", f"無法啟動圖片比對結果介面: {e}\n請查看 error_log.txt 獲取詳細信息。")
            if self.root: self.root.destroy()
            sys.exit(1)

    def _create_widgets(self):
        for widget in self.root.winfo_children():
            widget.destroy()
        main_pane = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        left_frame = ttk.Frame(main_pane)
        main_pane.add(left_frame, weight=1)
        right_frame = ttk.Frame(main_pane)
        main_pane.add(right_frame, weight=2)
        columns = ("Selected", "PrimaryImage", "ComparisonInfo", "Similarity", "OpenFolder")
        self.tree = ttk.Treeview(left_frame, columns=columns, show="headings", selectmode="extended")
        self.tree.heading("Selected", text="選取", anchor=tk.CENTER)
        self.tree.heading("PrimaryImage", text="主要圖片", anchor=tk.W)
        self.tree.heading("ComparisonInfo", text="比對資訊", anchor=tk.W)
        self.tree.heading("Similarity", text="相似度", anchor=tk.CENTER)
        self.tree.heading("OpenFolder", text="打開資料夾", anchor=tk.CENTER)
        self.tree.column("Selected", width=60, minwidth=50, stretch=tk.NO, anchor=tk.CENTER)
        self.tree.column("PrimaryImage", width=200, minwidth=150, stretch=tk.YES, anchor=tk.W)
        self.tree.column("ComparisonInfo", width=200, minwidth=150, stretch=tk.YES, anchor=tk.W)
        self.tree.column("Similarity", width=90, minwidth=80, stretch=tk.NO, anchor=tk.CENTER)
        self.tree.column("OpenFolder", width=100, minwidth=80, stretch=tk.NO, anchor=tk.CENTER)
        vscroll = ttk.Scrollbar(left_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vscroll.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select)
        self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<Button-3>", self._show_context_menu) # v78: 右鍵選單
        self.tree.bind("<space>", self._toggle_selection)
        self.tree.bind("<Return>", self._toggle_selection)
        self.tree.bind("<Delete>", self._delete_selected_from_disk)
        self.tree.bind("<BackSpace>", self._delete_selected_from_disk)
        self.root.bind("<Control-z>", self._undo_delete_gui)

        # v78: 右側佈局大改，使用垂直 PanedWindow
        right_pane = ttk.Panedwindow(right_frame, orient=tk.VERTICAL)
        right_pane.pack(fill=tk.BOTH, expand=True)

        self.target_image_frame = ttk.LabelFrame(right_pane, text="目標圖片預覽", padding="10")
        right_pane.add(self.target_image_frame, weight=1)
        self.target_image_label = ttk.Label(self.target_image_frame, cursor="hand2")
        self.target_image_label.pack(fill=tk.BOTH, expand=True)
        self.target_path_label = ttk.Label(self.target_image_frame, text="", wraplength=600)
        self.target_path_label.pack(fill=tk.X)
        self.target_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=True))
        
        self.compare_image_frame = ttk.LabelFrame(right_pane, text="比對圖片預覽", padding="10")
        right_pane.add(self.compare_image_frame, weight=1)
        self.compare_image_label = ttk.Label(self.compare_image_frame, cursor="hand2")
        self.compare_image_label.pack(fill=tk.BOTH, expand=True)
        self.compare_path_label = ttk.Label(self.compare_image_frame, text="", wraplength=600)
        self.compare_path_label.pack(fill=tk.X)
        self.compare_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=False))

        # v78: 為圖片預覽區綁定 Configure 事件，以實現可靠的縮放
        self.target_image_label.bind("<Configure>", self._on_preview_resize)
        self.compare_image_label.bind("<Configure>", self._on_preview_resize)

        # v78: 建立右鍵選單
        self._create_context_menu()
        
        bottom_button_container = ttk.Frame(self.root)
        bottom_button_container.pack(fill=tk.X, expand=False, padx=10, pady=10)
        button_frame = ttk.Frame(bottom_button_container)
        button_frame.pack(fill=tk.X, expand=True, padx=5, pady=5)
        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="刪除選中", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="撤銷刪除 (Ctrl+Z)", command=self._undo_delete_gui).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="開啟選中資料夾", command=self._open_selected_folder_single).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="關閉", command=self._on_closing).pack(side=tk.RIGHT, padx=5, pady=5)
        filter_frame = ttk.LabelFrame(bottom_button_container, text="相似度篩選", padding="10")
        filter_frame.pack(fill=tk.X, expand=True, padx=5, pady=5)
        ttk.Label(filter_frame, text="最小相似度 (%):").pack(side=tk.LEFT, pady=2)
        ttk.Scale(filter_frame, from_=50, to=100, orient="horizontal", variable=self.current_display_threshold, length=200, command=self._update_display_threshold).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.display_threshold_label = ttk.Label(filter_frame, text=f"{self.current_display_threshold.get():.1f}%")
        self.display_threshold_label.pack(side=tk.LEFT, padx=5)
        self.current_display_threshold.trace_add("write", self._update_display_threshold)

    def _create_context_menu(self): # v78 新增
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="臨時隱藏此廣告的所有結果", command=self._ban_ad_image)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="取消所有隱藏", command=self._unban_all_ads)

    def _show_context_menu(self, event): # v78 新增
        if self.comparison_mode != 'ad_comparison':
            return
        
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return
        
        # 確保點擊的項目被選中，以便後續操作
        if item_id not in self.tree.selection():
            self.tree.selection_set(item_id)

        try:
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()

    def _ban_ad_image(self): # v78 新增
        selected_items = self.tree.selection()
        if not selected_items:
            return
        
        item_id = selected_items[0]
        _, ad_path, _ = self.tree.item(item_id, "tags")

        if ad_path and ad_path != "N/A":
            self.banned_ad_images.add(ad_path)
            print(f"已臨時隱藏廣告圖片: {os.path.basename(ad_path)}", flush=True)
            self._populate_listbox()
    
    def _unban_all_ads(self): # v78 新增
        if not self.banned_ad_images:
            messagebox.showinfo("提示", "目前沒有被隱藏的廣告。")
            return
        self.banned_ad_images.clear()
        print("已取消所有臨時隱藏。", flush=True)
        self._populate_listbox()

    def _update_display_threshold(self, *args):
        # v78: 增加鎖定機制，防止無限遞迴
        if hasattr(self, '_update_threshold_lock') and self._update_threshold_lock:
            return
        self._update_threshold_lock = True
        try:
            current_val = self.current_display_threshold.get()
            self.display_threshold_label.config(text=f"{round(current_val):d}%")
            self._populate_listbox()
        finally:
            self._update_threshold_lock = False
            
    def _populate_listbox(self):
        for iid in self.tree.get_children(): self.tree.delete(iid)
        self.selected_files.clear()
        current_threshold = round(self.current_display_threshold.get())
        self.displayed_similar_files = []
        for path1, path2, similarity in self.all_similar_files:
            # v78: 篩選被ban的廣告和相似度
            if similarity < current_threshold or path2 in self.banned_ad_images:
                continue

            self.displayed_similar_files.append((path1, path2, similarity))
            primary_image_basename = os.path.basename(path1)
            comparison_info_text = ""
            if self.comparison_mode == "ad_comparison":
                display_path2_basename = os.path.basename(path2) if path2 and path2 != "N/A" else "N/A"
                comparison_info_text = f"(廣告: {display_path2_basename})"
            elif self.comparison_mode == "mutual_comparison":
                display_path2_basename = os.path.basename(path2) if path2 and path2 != "N/A" else "N/A"
                comparison_info_text = f"(與: {display_path2_basename})"
            elif self.comparison_mode == "qr_detection":
                comparison_info_text = "(QR Code 已偵測)"
            unique_item_id = f"item_{abs(hash(path1))}_{abs(hash(path2))}_{similarity}"
            checkbox_char = "☑" if path1 in self.selected_files else "☐"
            item_id = self.tree.insert("", "end", iid=unique_item_id, values=(checkbox_char, primary_image_basename, comparison_info_text, f"{similarity:.2f}%", "開啟"))
            self.tree.item(item_id, tags=(path1, path2, similarity))
        
        banned_count = len(self.banned_ad_images)
        banned_info = f" (另有 {banned_count} 個廣告結果被隱藏)" if banned_count > 0 else ""
        print(f"清單已根據最小相似度 {current_threshold:.1f}% 篩選。顯示 {len(self.displayed_similar_files)} 個項目。{banned_info}", flush=True)

    def _on_treeview_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading": return
        item_id = self.tree.identify_row(event.y)
        if not item_id: return
        column_id = self.tree.identify_column(event.x)
        if column_id == "#1":
            self._toggle_selection_by_item_id(item_id)
        elif column_id in ("#2", "#3"):
            self.tree.selection_set(item_id)
            self.tree.focus(item_id)
            self._on_item_select(None)
            self._toggle_selection_by_item_id(item_id)
        elif column_id == "#5":
            original_path = self.tree.item(item_id, "tags")[0]
            if original_path and os.path.exists(original_path):
                self._open_folder(os.path.dirname(original_path))
            else:
                messagebox.showwarning("路徑無效", f"檔案路徑不存在或無效:\n{original_path}")
        else:
            self.tree.selection_set(item_id)
            self.tree.focus(item_id)
            self._on_item_select(None)

    def _on_item_select(self, event): # v78: 邏輯簡化，只負責加載PIL對象
        selected_items = self.tree.selection()
        if not selected_items:
            # 清空圖像和路徑
            self.pil_img_target = None
            self.pil_img_compare = None
            self.target_image_label.config(image="")
            self.compare_image_label.config(image="")
            self.target_path_label.config(text="")
            self.compare_path_label.config(text="")
            self.img_tk_target = self.img_tk_compare = None
            return

        item_id = selected_items[0]
        path1, path2, similarity = self.tree.item(item_id, "tags")

        # 加載目標圖片
        self.pil_img_target = self._load_pil_image(path1, self.target_path_label)
        
        # 加載比對圖片
        if self.comparison_mode != "qr_detection" and path2 and path2 != "N/A":
            self.pil_img_compare = self._load_pil_image(path2, self.compare_path_label)
        else:
            self.pil_img_compare = None
            self.compare_image_label.config(image="")
            self.compare_path_label.config(text="（QR Code 檢測模式下無比對圖片）" if self.comparison_mode == "qr_detection" else "（無比對圖片）")
            self.img_tk_compare = None
            
        # 觸發一次手動的預覽更新
        self._update_all_previews()

    def _load_pil_image(self, image_path, path_label_widget): # v78: 新的輔助函數
        try:
            with Image.open(image_path) as img:
                img = ImageOps.exif_transpose(img)
                path_label_widget.config(text=f"路徑: {image_path}")
                return img.copy() # 返回PIL Image對象副本
        except Exception as e:
            error_text = f"無法載入圖片: {os.path.basename(image_path)}"
            if isinstance(e, FileNotFoundError): error_text = f"圖片文件未找到: {image_path}"
            elif isinstance(e, UnidentifiedImageError): error_text = f"圖片格式無法識別或文件已損壞: {image_path}"
            path_label_widget.config(text=error_text)
            log_error(f"載入圖片預覽失敗 '{image_path}': {e}", include_traceback=False)
            return None

    def _update_all_previews(self): # v78: 手動更新兩個預覽
        self._resize_and_display(self.target_image_label, self.pil_img_target, is_target=True)
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, is_target=False)

    def _on_preview_resize(self, event): # v78: 核心佈局修復
        # 根據觸發事件的組件來更新對應的圖片預覽
        widget = event.widget
        if widget == self.target_image_label:
            self._resize_and_display(widget, self.pil_img_target, is_target=True)
        elif widget == self.compare_image_label:
            self._resize_and_display(widget, self.pil_img_compare, is_target=False)

    def _resize_and_display(self, label_widget, pil_image, is_target): # v78: 統一的縮放和顯示函數
        if pil_image is None:
            label_widget.config(image="")
            if is_target: self.img_tk_target = None
            else: self.img_tk_compare = None
            return

        width = label_widget.winfo_width()
        height = label_widget.winfo_height()

        if width <= 1 or height <= 1: # 窗口還未完全繪製
            return
            
        try:
            img_copy = pil_image.copy()
            img_copy.thumbnail((width - 10, height - 10), Image.Resampling.LANCZOS)
            img_tk = ImageTk.PhotoImage(img_copy)
            
            label_widget.config(image=img_tk)
            # 必須保持對PhotoImage對象的引用，否則會被垃圾回收
            if is_target:
                self.img_tk_target = img_tk
            else:
                self.img_tk_compare = img_tk
        except Exception as e:
            log_error(f"預覽圖縮放時出錯: {e}", include_traceback=True)


    def _on_preview_image_click(self, event, is_target_image):
        image_path_label = self.target_path_label if is_target_image else self.compare_path_label
        full_path_text = image_path_label.cget("text")
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
        if event.keysym == "Up":
            item_to_select = self.tree.prev(current_item) or current_item
        elif event.keysym == "Down":
            item_to_select = self.tree.next(current_item) or current_item
        self.tree.selection_set(item_to_select)
        self.tree.focus(item_to_select)
        self.tree.see(item_to_select)
        return "break"

    def _toggle_selection_by_item_id(self, item_id):
        path1, _, _ = self.tree.item(item_id, "tags")
        current_values = list(self.tree.item(item_id, "values"))
        if path1 in self.selected_files:
            self.selected_files.remove(path1)
            current_values[0] = "☐"
        else:
            self.selected_files.add(path1)
            current_values[0] = "☑"
        self.tree.item(item_id, values=current_values)

    def _toggle_selection(self, event):
        for item_id in self.tree.selection():
            self._toggle_selection_by_item_id(item_id)

    def _update_all_checkboxes(self, select=True):
        checkbox_char = "☑" if select else "☐"
        action_set = self.selected_files.add if select else self.selected_files.clear
        if not select: self.selected_files.clear()

        for item_id in self.tree.get_children():
            path1, _, _ = self.tree.item(item_id, "tags")
            if select: self.selected_files.add(path1)
            current_values = list(self.tree.item(item_id, "values"))
            current_values[0] = checkbox_char
            self.tree.item(item_id, values=current_values)

    def _select_all(self):
        self._update_all_checkboxes(select=True)
        print("已選擇所有項目。", flush=True)

    def _deselect_all(self):
        self._update_all_checkboxes(select=False)
        print("已取消選擇所有項目。", flush=True)

    def _invert_selection(self):
        all_item_paths = {self.tree.item(item_id, "tags")[0] for item_id in self.tree.get_children()}
        new_selected_files = all_item_paths - self.selected_files
        self.selected_files = new_selected_files
        for item_id in self.tree.get_children():
            path1, _, _ = self.tree.item(item_id, "tags")
            current_values = list(self.tree.item(item_id, "values"))
            current_values[0] = "☑" if path1 in self.selected_files else "☐"
            self.tree.item(item_id, values=current_values)
        print("已反轉選擇。", flush=True)

    def _delete_selected_from_disk(self):
        if not self.selected_files:
            messagebox.showinfo("提示", "沒有選中的圖片。")
            return
        if not messagebox.askyesno("確認刪除", f"確定要刪除這 {len(self.selected_files)} 個選中的圖片嗎？此操作不可撤銷！"):
            return
        deleted_paths = []
        for path1 in list(self.selected_files):
            try:
                os.remove(path1)
                deleted_paths.append(path1)
                print(f"已刪除文件: {path1}", flush=True)
            except OSError as e:
                log_error(f"刪除文件失敗 {path1}: {e}", include_traceback=True)
                messagebox.showerror("刪除失敗", f"無法刪除文件: {path1}\n錯誤: {e}")
        if deleted_paths:
            self.deleted_history.append(deleted_paths)
            self.all_similar_files = [item for item in self.all_similar_files if item[0] not in deleted_paths]
            self.selected_files.clear()
            self._populate_listbox()
            messagebox.showinfo("刪除完成", f"成功刪除 {len(deleted_paths)} 個文件。")

    def _undo_delete_gui(self, event=None):
        if not self.deleted_history:
            messagebox.showinfo("提示", "沒有可撤銷的刪除操作。")
            return
        messagebox.showwarning("無法撤銷實際刪除", "已刪除的文件無法從硬碟中恢復。此功能僅為示意，提醒您刪除操作的不可逆性，並清空歷史記錄。")
        self.deleted_history.pop()

    def _open_folder(self, folder_path):
            """以最穩定的方式開啟資料夾，使用 os.startfile 繞過 shell 解析問題。"""
            try:
                stable_path = os.path.abspath(os.path.normpath(folder_path))

                if not os.path.isdir(stable_path):
                    messagebox.showwarning("路徑無效", f"資料夾不存在或無效:\n{stable_path}")
                    return

                print(f"嘗試開啟資料夾 (最終方案): {stable_path}", flush=True)

                if sys.platform == "win32":
                    os.startfile(stable_path)
                elif sys.platform == "darwin": # macOS
                    subprocess.Popen(["open", stable_path])
                else: # Linux
                    subprocess.Popen(["xdg-open", stable_path])
                
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
        path1, _, _ = self.tree.item(item_id, "tags")
        if path1 and os.path.exists(path1):
            self._open_folder(os.path.dirname(path1))
        else:
            messagebox.showwarning("路徑無效", f"選中的圖片文件路徑不存在或無效:\n{path1}")

    def _on_closing(self):
        if messagebox.askokcancel("關閉", "確定要關閉比對結果視窗嗎？"):
            self.root.destroy()
            sys.exit(0)

def custom_excepthook(exc_type, exc_value, exc_traceback):
    log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", include_traceback=True)
    try:
        root_exists = False
        try:
            if tk._default_root and tk._default_root.winfo_exists():
                root_exists = True
        except: pass
        if not root_exists:
            temp_root = tk.Tk()
            temp_root.withdraw()
            messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt' 獲取詳細資訊。")
            temp_root.destroy()
        else:
            messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt' 獲取詳細資訊。")
    except Exception as mb_e:
        print(f"顯示錯誤訊息框失敗 (可能Tkinter環境問題): {mb_e}", flush=True)
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

def main():
    sys.excepthook = custom_excepthook
    print("--- 程式啟動，main 函數開始執行 ---", flush=True)
    if sys.platform.startswith('win'):
        try:
            set_start_method('spawn', force=True)
            print("多進程啟動方法已設置為 'spawn'。", flush=True)
        except RuntimeError:
            print("多進程啟動方法已在其他地方設置，或無法設置。", flush=True)
        except Exception as e:
            log_error(f"設置多進程啟動方法時發生錯誤: {e}", include_traceback=True)
    
    print("=== E-Download 漫畫尾頁廣告剔除 v11.0v78 - 啟動中 ===", flush=True)
    check_and_install_packages()
    print("套件檢查完成。", flush=True)
    
    root = tk.Tk()
    folder_creation_cache_manager = FolderCreationCacheManager()
    comparison_result_cache_manager = ComparisonResultCacheManager() # v78: 初始化
    
    settings_gui = SettingsGUI(root, CONFIG_FILE, QR_SCAN_ENABLED)
    root.wait_window(settings_gui.settings_window)

    final_config_from_settings = settings_gui.result_config
    should_proceed_with_main_app = settings_gui.should_proceed
    rebuild_folder_cache_flag = settings_gui.rebuild_folder_cache_result
    rebuild_scanned_cache_flag = settings_gui.rebuild_scanned_cache_result
    rebuild_comparison_cache_flag = settings_gui.rebuild_comparison_cache_result # v78 新增

    if should_proceed_with_main_app and final_config_from_settings:
        if rebuild_folder_cache_flag:
            folder_creation_cache_manager.invalidate_cache()
            folder_creation_cache_manager.save_cache()
            print("資料夾建立時間快取已清空。下次運行時將重新建立。", flush=True)

        if rebuild_comparison_cache_flag: # v78 新增
            comparison_result_cache_manager.invalidate_cache()
            comparison_result_cache_manager.save_cache()
            print("比對結果快取已清空。下次運行時將重新建立。", flush=True)
        
        if final_config_from_settings['root_scan_folder']:
            scanned_hashes_cache_manager = ScannedImageHashesCacheManager(final_config_from_settings['root_scan_folder'])
        else:
            messagebox.showerror("錯誤", "未設定根掃描資料夾，無法初始化圖片哈希快取。程式將退出。")
            root.destroy()
            sys.exit(1)
            
        if rebuild_scanned_cache_flag:
            scanned_hashes_cache_manager.invalidate_cache()
            scanned_hashes_cache_manager.save_cache()
            print("掃描圖片哈希快取已清空。下次運行時將重新建立。", flush=True)

        main_app_config = final_config_from_settings
        start_date_dt, end_date_dt = None, None
        if main_app_config.get('enable_time_filter'):
            try:
                if main_app_config.get('start_date_filter'):
                    start_date_dt = datetime.datetime.strptime(main_app_config['start_date_filter'], "%Y-%m-%d")
                if main_app_config.get('end_date_filter'):
                    end_date_dt = datetime.datetime.strptime(main_app_config['end_date_filter'], "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                log_error("時間篩選日期格式錯誤，將禁用時間篩選。", include_traceback=False)
                messagebox.showwarning("日期格式錯誤", "時間篩選日期格式不正確，將禁用時間篩選。")
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
                rebuild_ad_cache=main_app_config['rebuild_ad_cache'],
                system_qr_scan_capability=QR_SCAN_ENABLED,
                scanned_hashes_cache_manager=scanned_hashes_cache_manager,
                comparison_result_cache_manager=comparison_result_cache_manager, # v78: 傳入
                enable_extract_count_limit=main_app_config['enable_extract_count_limit']
            )

            files_to_process_dict = engine.generate_extracted_files(folder_creation_cache_manager)
            folder_creation_cache_manager.save_cache()
            ad_hashes = {}
            if engine.comparison_mode == 'ad_comparison':
                rebuild_ad_flag = main_app_config.get('rebuild_ad_cache', False) or main_app_config.get('rebuild_comparison_cache', False)
                ad_hashes = load_ad_hashes(main_app_config['ad_folder_path'], rebuild_ad_flag)
            
            similar_files = engine.compare_images(files_to_process_dict, ad_hashes)

            if similar_files:
                MainWindow(root, similar_files, engine.comparison_mode, initial_similarity_threshold=main_app_config['similarity_threshold'])
            else:
                messagebox.showinfo("掃描結果", "未找到相似或廣告圖片，或沒有檢測到 QR Code。")
                root.destroy()
                sys.exit(0)
            
            print("任務完成。", flush=True)

        except Exception as e:
            raise
    else:
        root.destroy()
        sys.exit(0)

    root.mainloop()

if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()