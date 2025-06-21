# ======================================================================
# 檔案名稱：E-Download漫畫尾頁廣告剔除11.0_GUI_優化.py
# 版本號：11.0v7
#
# === 程式說明 ===
# 這是一個專為清理 E-Download 資料夾中漫畫檔案尾頁廣告的工具。
# 它能自動檢測並移除與廣告圖片相似或相互重複的圖片，提升漫畫閱讀體驗。
# 適用於處理大量漫畫檔案，節省手動篩選時間。
# 支援三種比對模式：廣告比對、互相比對和 QR Code 檢測。
#
# === 11.0v7 版本更新內容 ===
# - **版本號更新**: 將版本號從 `11.0v6` 更新為 `11.0v7`。
# - **修正時間篩選邏輯**: 調整 `get_all_subfolders` 函數，確保時間篩選只應用於
#   `root_scan_folder` (根掃描資料夾) 下的子資料夾，而不是根資料夾本身。
#   這解決了當根資料夾建立時間不在篩選範圍內時，導致所有子資料夾都被跳過的問題。
# - **哈希演算法優化**: 將圖片感知哈希比對演算法從 `average_hash` (ahash)
#   更新為 `perceptual_hash` (phash)，以增加圖片相似度比對的準確度。
# - **快取優化**: 掃描圖片哈希快取檔案 (`scanned_hashes_cache.json`) 現在會根據
#   「根掃描資料夾」的路徑動態生成一個專屬的檔案名稱。
#   例如：`scanned_hashes_cache_{根掃描資料夾路徑的SHA256哈希值}.json`。
#   這確保了每個不同根掃描資料夾的快取相互獨立，避免數據混淆。
# - **功能調整**: 將「開啟所有選中資料夾」功能修改為「開啟選中資料夾」。
#   現在只會開啟列表中第一個被反白選中（滑鼠選中）的圖片所在的資料夾，避免同時開啟過多視窗。
# - **修正錯誤**: 修正了「打開資料夾」功能在某些情況下錯誤地開啟「我的文件」資料夾的問題。
#   現在使用更穩健的 `start` 命令透過 shell 開啟資料夾，以確保路徑正確解析。
# - **基礎版本**: 此版本基於 "1140614谷歌版-可用版-只有調整排序.PY" 進行組織與命名更新。
# - **功能強化**: 正式啟用並實作資料夾「建立時間」篩選功能。
# - **性能優化**: 引入資料夾建立時間快取機制 (JSON 檔案)，大幅提升後續掃描效率。
# - **程式碼重構**: 統一導入語句，並對部分程式碼邏輯進行整理，提高可讀性和維護性。
# - **核心邏輯實作**: 將 ImageComparisonEngine 中的圖片哈希計算、相似度比對和 QR Code 偵測邏輯從模擬替換為實際功能。
# - **新增掃描圖片哈希快取**: 實作了掃描圖片的哈希快取功能，包括讀取、寫入、增量更新和強制重建，進一步提升效率。
# - **錯誤修復 (閃退問題)**: 修正 `extract_last_n_files_from_folders` 函數中 `log_error` 呼叫的語法錯誤，
#   並增強 `log_error` 函數的寫入即時性，以更好地捕捉早期錯誤。
#
# === 導入必要套件 ===
# 1. Pillow (PIL): 圖片處理庫
# 2. imagehash: 圖片哈希值計算，用於相似度比對
# 3. tkinter: Python 標準 GUI 庫
# 4. pyzbar (可選): QR Code 掃描 (如果啟用了 QR Code 掃描功能)
# 5. opencv-python (cv2, 可選): 圖片處理和 QR Code 掃描的底層庫 (如果啟用了 QR Code 掃描功能)
# 6. hashlib: 用於生成文件路徑的哈希值
#
# 安裝方式：pip install Pillow imagehash pyzbar opencv-python
#
# === 使用方法 ===
# 1. 運行腳本。
# 2. 程式啟動後會彈出設定視窗，配置所需參數（路徑、數量、排除規則、比對模式等）。
#    注意：現在可使用「建立時間」篩選功能。
# 3. 點擊「開始執行」按鈕。
# 4. 程式將執行掃描、比對或檢測任務。
# 5. 如果找到相似圖片或 QR Code，會彈出 GUI 介面，允許預覽、選擇並刪除圖片。
# 6. 如資料夾結構或圖片內容有變動，可在設定中選擇「重建快取」，以確保數據的準確性。
# ======================================================================


# === 1. 標準庫導入 (Python Built-in Libraries) ===
# (所有程式碼中用到的標準庫，都應在此處導入一次)
import os                    # 用於文件系統操作 (路徑、文件判斷)
import sys                   # 用於系統相關操作 (如 sys.platform, sys.exit)
import json                  # 用於讀取和寫入 JSON 格式的設定檔和快取
import datetime              # 用於日期和時間操作 (時間篩選，用於 log_error 和其他日期處理)
import traceback             # 用於獲取錯誤堆疊信息 (除錯，如果需要詳細錯誤記錄)
import subprocess            # 用於啟動外部程式 (如打開資料夾)
from collections import deque # 用於雙端佇列，可能用於歷史記錄或撤銷功能
# 多進程相關模塊，通常需要在 main 或最頂層進行設置
from multiprocessing import set_start_method, Pool, cpu_count # 引入 Pool 和 cpu_count
import hashlib               # 新增：用於生成資料夾路徑的哈希值

# === 2. 第三方庫導入 (Third-party Libraries) ===
# (所有程式碼中用到的第三方庫，都應在此處導入一次)
# Pillow (PIL) 相關導入
from PIL import Image, ImageTk, ImageOps, UnidentifiedImageError # Image: 圖片處理核心; ImageTk: 讓PIL圖片能在Tkinter中顯示; ImageOps: 圖片操作 (如自動旋轉); UnidentifiedImageError: 新增捕獲此特定錯誤

# 嘗試導入 imagehash, cv2 (opencv-python), pyzbar, numpy
# 這裡的 try-except pass 確保即使這些可選依賴不存在，程式也能啟動，
# 實際的可用性會在 check_and_install_packages 函數中檢查並處理。
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
# (通常將Tkinter相關的導入放在一起，方便管理)
import tkinter as tk         # Tkinter 主模塊，常用別名 tk
from tkinter import ttk      # Tkinter 的主題化部件 (美化控件)
from tkinter import filedialog # 用於文件和資料夾選擇對話框
from tkinter import messagebox # 用於彈出訊息、警告和錯誤對話框


# === 4. 全局常量和設定 ===
# (應在所有導入後定義，確保其值在腳本的任何地方都可見)
CONFIG_FILE = "config.json" # 程式設定檔的名稱
# QR_SCAN_ENABLED 的初始值設置為 False。它的最終值將由 check_and_install_packages 設置
QR_SCAN_ENABLED = False


# === 5. 工具函數 (Helper Functions) ===
# (通用性的、不屬於特定類別或主要業務邏輯的函數)

def log_error(message, include_traceback=False):
    """
    將錯誤訊息記錄到檔案 'error_log.txt' 並打印到控制台。
    Args:
        message (str): 要記錄的錯誤訊息。
        include_traceback (bool): 如果為 True，則包含當前的堆疊追蹤信息。
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Use datetime.datetime.now()
    log_content = f"[{timestamp}] ERROR: {message}\n"
    if include_traceback:
        log_content += traceback.format_exc() + "\n" # Get full traceback
    
    # Print to console
    print(log_content, end='', flush=True) # Ensure console output is flushed
    
    # Write to log file, with line buffering (buffering=1) for immediate write after each line
    try:
        with open("error_log.txt", "a", encoding="utf-8", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"Failed to write to error log: {e}\nOriginal error: {message}", flush=True)


def check_and_install_packages():
    """
    檢查並提示安裝必要的 Python 套件。
    如果缺少核心套件，則會彈出錯誤訊息並退出程式。
    如果缺少可選套件，則會打印警告並禁用相關功能。
    這個函數的作用是 "檢查" 而不是 "導入"。導入動作已在腳本頂部完成。
    """
    print("正在檢查必要的 Python 套件...", flush=True)

    # 檢查核心依賴：Pillow 和 imagehash
    try:
        # Check if modules are successfully imported (exist in sys.modules)
        if 'PIL' not in sys.modules or 'imagehash' not in sys.modules:
            raise ImportError("Pillow 或 imagehash 未成功導入")
        print("Pillow 和 imagehash 套件檢查通過。", flush=True)
    except ImportError as e:
        messagebox.showerror("缺少核心依賴", f"請安裝必要的 Python 套件：Pillow 和 imagehash。\n"
                                             f"錯誤詳情: {e}\n"
                                             "可以使用 'pip install Pillow imagehash' 命令安裝。")
        sys.exit(1) # Force exit

    # 檢查 Tkinter 依賴 (通常內建於 Python)
    try:
        if 'tkinter' not in sys.modules:
            raise ImportError("Tkinter 未成功導入")
        print(f"Tkinter Version: {tk.TkVersion}, Tcl Version: {tk.TclVersion}", flush=True)
    except ImportError as e:
        messagebox.showerror("Tkinter 錯誤", f"無法找到 Tkinter ({e})。您的 Python 安裝可能不完整或損壞。")
        sys.exit(1)

    # 檢查可選依賴：QR Code 掃描功能 (OpenCV 和 pyzbar)
    global QR_SCAN_ENABLED # Declare modification of global variable
    QR_SCAN_ENABLED = False # Reset to False before each check

    try:
        # Check if these modules are successfully imported and exist in sys.modules
        if 'cv2' not in sys.modules or 'pyzbar' not in sys.modules or 'numpy' not in sys.modules:
            raise ImportError("opencv-python 或 pyzbar 或 numpy 未成功導入")
            
        QR_SCAN_ENABLED = True
        print("OpenCV 和 pyzbar (QR Code 掃描) 套件檢查通過。QR Code 掃描功能已啟用。", flush=True)
    except ImportError as e:
        print(f"警告: 缺少 'opencv-python' 或 'pyzbar' 或 'numpy' ({e})。QR Code 掃描功能將被禁用。", flush=True)
    except Exception as e: # Catch other possible errors, e.g., missing DLLs
        log_error(f"導入 QR Code 相關套件時發生未知錯誤: {e}。QR Code 掃描功能將被禁用。", include_traceback=True)


# === 6. 配置管理相關函數 ===
# (負責程式設定檔的載入與保存)

# Default configuration values for the program.
# Used when the config file does not exist or fails to load.
default_config = {
    'root_scan_folder': '',          # Root scan folder path
    'ad_folder_path': '',            # Ad image folder path
    'extract_count': 5,              # Number of images to extract from the end of each folder
    'enable_extract_count_limit': True, # New: Whether to limit extraction count (True = limit, False = scan all)
    'excluded_folders': [],          # List of folder names to exclude from scanning
    'comparison_mode': 'ad_comparison', # Comparison mode: 'ad_comparison' or 'mutual_comparison' or 'qr_detection'
    'similarity_threshold': 85,      # Image similarity threshold (0-100)
    'rebuild_ad_cache': False,       # Whether to rebuild ad image hash cache
    'qr_scan_enabled': False,        # Whether QR Code scanning is enabled
    'enable_time_filter': False,     # Whether time filtering is enabled
    'start_date_filter': '',         # Start date for time filter (format:YYYY-MM-DD)
    'end_date_filter': ''            # End date for time filter (format:YYYY-MM-DD)
}

def load_config(config_path):
    """
    從指定的 JSON 檔案載入程式設定。
    如果檔案不存在或讀取失敗，則返回預設設定。
    
    Args:
        config_path (str): 設定檔案的路徑。
    
    Returns:
        dict: 載入的設定字典。
    """
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Merge default settings with loaded settings to ensure all keys exist
                # For settings added in a new version, if missing in old config, use default value
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
        
    return default_config.copy() # Return a copy of default settings

def save_config(config, config_path):
    """
    將程式設定儲存到指定的 JSON 檔案。
    
    Args:
        config (dict): 要儲存的設定字典。
        config_path (str): 設定檔案的路徑。
    """
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False) # indent=4 for readability, ensure_ascii=False for Chinese support
        print(f"設定已成功保存到 '{config_path}'。", flush=True) # Added flush=True
    except Exception as e:
        log_error(f"保存設定檔 '{config_path}' 時發生錯誤: {e}", include_traceback=True)


# === 7. 快取管理相關 ===
# (New feature: Manage folder creation time cache)
class FolderCreationCacheManager:
    """
    管理資料夾建立時間的快取。
    將資料夾路徑與其建立時間儲存為 JSON 檔案，以提高重複掃描的效率。
    """
    def __init__(self, cache_file_path="folder_creation_cache.json"):
        """
        初始化快取管理器。
        Args:
            cache_file_path (str): 快取檔案的名稱。
        """
        self.cache_file_path = cache_file_path
        self.cache = self._load_cache() # Load cache upon initialization

    def _load_cache(self):
        """
        從快取檔案載入資料夾建立時間的快取。
        如果檔案不存在或載入失敗，則返回一個空的字典。
        """
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    loaded_cache = json.load(f)
                    converted_cache = {}
                    for folder_path, timestamp_str in loaded_cache.items():
                        try:
                            # Attempt to convert the stored string to a float timestamp, skip if failed
                            converted_cache[folder_path] = float(timestamp_str)
                        except (ValueError, TypeError):
                            log_error(f"快取檔案 '{self.cache_file_path}' 中 '{folder_path}' 的建立時間格式不正確，將忽略此項。", include_traceback=True)
                            continue
                    
                    print(f"資料夾建立時間快取 '{self.cache_file_path}' 已成功載入。", flush=True) # Added flush=True
                    return converted_cache
            except json.JSONDecodeError:
                log_error(f"資料夾建立時間快取檔案 '{self.cache_file_path}' 格式不正確，將重建快取。", include_traceback=True)
            except Exception as e:
                log_error(f"載入資料夾建立時間快取時發生錯誤: {e}，將重建快取。", include_traceback=True)
        print(f"資料夾建立時間快取檔案 '{self.cache_file_path}' 不存在或載入失敗，將從空快取開始。", flush=True) # Added flush=True
        return {} # Return empty dictionary on load failure or non-existence

    def save_cache(self):
        """
        將當前快取內容儲存到快取檔案。
        """
        try:
            # Convert timestamps to strings for JSON storage
            serializable_cache = {path: str(timestamp) for path, timestamp in self.cache.items()}
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_cache, f, indent=4, ensure_ascii=False)
            print(f"資料夾建立時間快取已成功保存到 '{self.cache_file_path}'。", flush=True) # Added flush=True
        except Exception as e:
            log_error(f"保存資料夾建立時間快取時發生錯誤: {e}", include_traceback=True)

    def get_creation_time(self, folder_path):
        """
        獲取指定資料夾的建立時間（秒數時間戳）。
        優先從快取中獲取，如果快取中沒有，則從檔案系統獲取並加入快取。
        
        Args:
            folder_path (str): 資料夾的絕對路徑。
            
        Returns:
            float: 資料夾的建立時間時間戳 (秒)，如果無法獲取則返回 None。
        """
        if folder_path in self.cache:
            return self.cache[folder_path]
        
        # If not in cache, get from file system
        try:
            # os.path.getctime returns a float timestamp
            ctime = os.path.getctime(folder_path)
            self.cache[folder_path] = ctime # Add to cache
            return ctime
        except FileNotFoundError:
            log_error(f"資料夾不存在，無法獲取建立時間: {folder_path}", include_traceback=False) # Added include_traceback=False for common FileNotFoundError
            return None
        except Exception as e:
            log_error(f"獲取資料夾建立時間失敗: {folder_path}, 錯誤: {e}", include_traceback=True)
            return None

    def invalidate_cache(self):
        """
        清空快取，下次獲取時將強制從檔案系統重新讀取。
        通常用於用戶請求重建快取時。
        """
        self.cache = {}
        # Option to delete the cache file, or just clear it
        if os.path.exists(self.cache_file_path):
            try:
                os.remove(self.cache_file_path)
                print(f"資料夾建立時間快取檔案 '{self.cache_file_path}' 已刪除。", flush=True) # Added flush=True
            except Exception as e:
                log_error(f"刪除快取檔案 '{self.cache_file_path}' 時發生錯誤: {e}", include_traceback=True)
        print("資料夾建立時間快取已失效。", flush=True) # Added flush=True


# New: Scanned image hash cache manager
# SCANNED_HASH_CACHE_FILE = "scanned_hashes_cache.json" # Removed global constant

class ScannedImageHashesCacheManager:
    """
    管理掃描圖片的哈希值快取，包括圖片路徑、哈希值和檔案修改時間 (mtime)。
    用於避免重複計算已處理圖片的哈希。
    這個快取檔案現在會根據 root_scan_folder 產生一個獨特的名稱。
    """
    def __init__(self, root_scan_folder): # Modified: added root_scan_folder parameter
        # Generate a unique cache file path based on the root_scan_folder
        # Use SHA256 hash of the normalized root folder path to ensure valid and unique filename
        # Added .encode('utf-8') for hashing string data
        normalized_path = os.path.normpath(root_scan_folder).replace('\\', '/')
        hash_object = hashlib.sha256(normalized_path.encode('utf-8'))
        self.cache_file_path = f"scanned_hashes_cache_{hash_object.hexdigest()}.json"
        
        self.cache = self._load_cache()

    def _load_cache(self):
        """Load scanned image hash cache from JSON file."""
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    converted_cache = {}
                    for path, data in loaded_data.items():
                        if isinstance(data, dict) and 'hash' in data and 'mtime' in data:
                            try:
                                # Ensure mtime is float, hash is ImageHash object
                                phash_obj = imagehash.hex_to_hash(data['hash'])
                                converted_cache[path] = {
                                    'hash': phash_obj,
                                    'mtime': float(data['mtime'])
                                }
                            except (ValueError, TypeError, AttributeError) as e: # Add AttributeError for imagehash.hex_to_hash(None)
                                log_error(f"快取檔案 '{self.cache_file_path}' 中 '{path}' 的數據格式不正確或哈希值無效 ({e})，將忽略此項。", include_traceback=True)
                                continue
                        else:
                            log_error(f"快取檔案 '{self.cache_file_path}' 中 '{path}' 的格式不正確 (非字典或缺少鍵)，將忽略此項。", include_traceback=True)
                    print(f"掃描圖片哈希快取 '{self.cache_file_path}' 已成功載入。", flush=True) # Added flush=True
                    return converted_cache
            except json.JSONDecodeError:
                log_error(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 格式不正確，將重建快取。", include_traceback=True)
            except Exception as e:
                log_error(f"載入掃描圖片哈希快取時發生錯誤: {e}，將重建快取。", include_traceback=True)
        print(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 不存在或載入失敗，將從空快取開始。", flush=True) # Added flush=True
        return {}

    def save_cache(self):
        """Save current scanned image hash cache to JSON file."""
        try:
            # Convert ImageHash objects to strings, mtime to strings for JSON storage
            serializable_cache = {}
            for path, data in self.cache.items():
                # Only save if hash exists and is not None
                if data and 'hash' in data and 'mtime' in data and data['hash'] is not None:
                    serializable_cache[path] = {
                        'hash': str(data['hash']), # ImageHash object to string
                        'mtime': str(data['mtime']) # Float timestamp to string
                    }
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_cache, f, indent=4, ensure_ascii=False)
            print(f"掃描圖片哈希快取已成功保存到 '{self.cache_file_path}'。", flush=True) # Added flush=True
        except Exception as e:
            log_error(f"保存掃描圖片哈希快取時發生錯誤: {e}", include_traceback=True)

    def get_hash(self, file_path):
        """
        從快取中獲取指定檔案的哈希值，並檢查檔案修改時間是否匹配。
        如果快取中沒有或檔案已修改，則返回 None。
        """
        if file_path in self.cache:
            cached_data = self.cache[file_path]
            try:
                current_mtime = os.path.getmtime(file_path)
                # Comparison of floats might need tolerance, e.g., 0.001 seconds
                if abs(current_mtime - cached_data['mtime']) < 0.001: 
                    return cached_data['hash'] # Hash is ImageHash object
            except FileNotFoundError:
                log_error(f"快取中文件 '{file_path}' 不存在，將重新計算。", include_traceback=False)
            except Exception as e:
                log_error(f"檢查文件 '{file_path}' 修改時間時發生錯誤: {e}，將重新計算。", include_traceback=True)
        return None

    def update_hash(self, file_path, phash, mtime):
        """將新計算的哈希值和指定的修改時間更新到快取中。"""
        try:
            # Only update cache if phash is valid
            if phash is not None:
                self.cache[file_path] = {'hash': phash, 'mtime': mtime}
            else:
                log_error(f"跳過更新無效哈希值的快取: {file_path}", include_traceback=False)
        except Exception as e:
            log_error(f"更新哈希快取時發生錯誤: {e}", include_traceback=True)

    def invalidate_cache(self):
        """清空掃描圖片哈希快取，通常用於強制重建。"""
        self.cache = {}
        if os.path.exists(self.cache_file_path):
            try:
                os.remove(self.cache_file_path)
                print(f"掃描圖片哈希快取檔案 '{self.cache_file_path}' 已刪除。", flush=True) # Added flush=True
            except Exception as e:
                log_error(f"刪除掃描圖片哈希快取檔案 '{self.cache_file_path}' 時發生錯誤: {e}", include_traceback=True)
        print("掃描圖片哈希快取已失效。", flush=True) # Added flush=True


# Ad hash cache file path
AD_HASH_CACHE_FILE = "ad_hashes.json" 

def calculate_image_hash(image_path, hash_size=8):
    """
    計算圖片的感知哈希值。
    Args:
        image_path (str): 圖片檔案路徑。
        hash_size (int): 哈希的大小 (例如 8x8 像素)。
    Returns:
        imagehash.ImageHash: 計算出的感知哈希對象，如果失敗則返回 None。
    """
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img) # Process EXIF orientation of the image
            # Convert to grayscale for hash calculation
            img = img.convert("L").resize((hash_size, hash_size), Image.Resampling.LANCZOS)
            # Changed from imagehash.average_hash to imagehash.phash
            return imagehash.phash(img) # Return ImageHash object
    except FileNotFoundError:
        log_error(f"圖片檔案未找到: {image_path}", include_traceback=False) 
        return None
    except UnidentifiedImageError: # Add specific error for unrecognized image formats
        log_error(f"圖片格式無法識別或文件已損壞: {image_path}", include_traceback=False)
        return None
    except OSError as e: # Catch broader OS-related errors for image opening
        log_error(f"打開圖片檔案時發生操作系統錯誤 '{image_path}': {e}", include_traceback=False)
        return None
    except Exception as e:
        log_error(f"處理圖片 '{image_path}' 時發生未知錯誤: {e}", include_traceback=True)
        return None

def load_ad_hashes(ad_folder_path, rebuild_cache=False):
    """
    載入或重新計算廣告圖片的哈希值。
    Args:
        ad_folder_path (str): 廣告圖片資料夾路徑。
        rebuild_cache (bool): 是否強制重建快取。
    Returns:
        dict: 廣告圖片檔案路徑到哈希值的映射。哈希值為 imagehash.ImageHash 物件。
    """
    ad_hashes = {}
    if os.path.exists(AD_HASH_CACHE_FILE) and not rebuild_cache:
        try:
            with open(AD_HASH_CACHE_FILE, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                # Convert from string back to ImageHash object
                ad_hashes = {path: imagehash.hex_to_hash(phash_str) for path, phash_str in loaded_data.items()}
            print(f"廣告圖片哈希快取 '{AD_HASH_CACHE_FILE}' 已成功載入。", flush=True) # Added flush=True
            return ad_hashes
        except json.JSONDecodeError:
            log_error(f"廣告哈希快取檔案 '{AD_HASH_CACHE_FILE}' 格式不正確，將重建快取。", include_traceback=True)
        except Exception as e:
                log_error(f"載入廣告哈希快取時發生錯誤: {e}，將重建快取。", include_traceback=True)
    
    print(f"正在重建廣告圖片哈希快取，掃描資料夾: {ad_folder_path}", flush=True) # Added flush=True
    if os.path.isdir(ad_folder_path):
        all_ad_images = []
        for root, _, files in os.walk(ad_folder_path):
            for file in files:
                if file.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp')):
                    # IMPORTANT FIX: Normalize path to use forward slashes for consistency
                    # This ensures paths like 'E:/path\\to\\file.jpg' become 'E:/path/to/file.jpg'
                    all_ad_images.append(os.path.normpath(os.path.join(root, file)).replace('\\', '/'))
        
        # Process directly using calculate_image_hash function, avoiding ImageComparisonEngine instance
        for img_path in all_ad_images:
            img_hash = calculate_image_hash(img_path)
            if img_hash: # If hash calculation successful
                ad_hashes[img_path] = img_hash # Store as ImageHash object
    
    # Save hash values (convert ImageHash objects to strings)
    serializable_ad_hashes = {path: str(phash) for path, phash in ad_hashes.items()}
    try:
        with open(AD_HASH_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(serializable_ad_hashes, f, indent=4, ensure_ascii=False)
        print(f"廣告圖片哈希快取已保存到 '{AD_HASH_CACHE_FILE}'。", flush=True) # Added flush=True
    except Exception as e:
        log_error(f"保存廣告哈希快取時發生錯誤: {e}", include_traceback=True)
    
    return ad_hashes

def get_all_subfolders(root_folder, excluded_folders=None, enable_time_filter=False, start_date=None, end_date=None, creation_cache_manager=None):
    """
    遞歸獲取根資料夾下的所有子資料夾，並應用排除和時間篩選。
    
    Args:
        root_folder (str): 根資料夾路徑。
        excluded_folders (list): 需要排除的資料夾名稱列表。
        enable_time_filter (bool): 是否啟用時間篩選。
        start_date (datetime.datetime): 時間篩選的起始日期。
        end_date (datetime.datetime): 時間篩選的結束日期。
        creation_cache_manager (FolderCreationCacheManager): 資料夾建立時間快取管理器實例。
        
    Returns:
        list: 符合條件的所有子資料夾路徑。
    """
    if excluded_folders is None:
        excluded_folders = []
    
    all_subfolders_to_return = []
    
    if not os.path.isdir(root_folder):
        log_error(f"根掃描資料夾不存在: {root_folder}", include_traceback=False)
        return []

    excluded_norm_paths = {os.path.normpath(f) for f in excluded_folders}

    # Use deque for breadth-first traversal to prevent recursion depth limits for deep paths
    folders_to_process_queue = deque([root_folder])
    processed_folders_for_traversal = set() # To prevent re-processing same path (e.g., due to symlinks)

    while folders_to_process_queue:
        current_folder = folders_to_process_queue.popleft()

        # Normalize and add to processed set to prevent infinite loops or redundant processing
        norm_current_folder = os.path.normpath(current_folder)
        if norm_current_folder in processed_folders_for_traversal:
            continue
        processed_folders_for_traversal.add(norm_current_folder)

        # Check if the current folder itself (or any of its parents) is excluded by name
        # This exclusion applies to any folder encountered during traversal
        if any(norm_current_folder.startswith(excluded_path) for excluded_path in excluded_norm_paths):
            continue

        # Apply time filter ONLY if it's a subfolder AND time filter is enabled
        # The root_folder (current_folder == root_folder) is always traversed to find its children,
        # but its own creation time doesn't affect its subfolders' inclusion.
        # Only actual subfolders that are candidates for image extraction will be added to all_subfolders_to_return.
        if current_folder != root_folder and enable_time_filter and creation_cache_manager:
            folder_ctime_timestamp = creation_cache_manager.get_creation_time(current_folder)
            if folder_ctime_timestamp is not None:
                folder_ctime = datetime.datetime.fromtimestamp(folder_ctime_timestamp)
                # Set end_date to end of the day to include that date
                if (start_date and folder_ctime < start_date) or \
                   (end_date and folder_ctime > end_date.replace(hour=23, minute=59, second=59, microsecond=999999)):
                    continue # Not within time range, skip this folder and its subfolders
            else:
                log_error(f"無法獲取資料夾建立時間，跳過時間篩選: {current_folder}", include_traceback=False)
                continue # If time cannot be retrieved and time filter is enabled, this folder will be skipped

        # If it's a subfolder (not the root_folder itself) and passed all filters, add it to the final list.
        # The root_folder itself is used for traversal, but not included in the result list based on user's need
        # to scan "E-Download以下所有符合時間的子資料夾".
        if current_folder != root_folder:
            all_subfolders_to_return.append(current_folder)

        try:
            # Iterate through direct subfolders of the current folder
            for entry in os.listdir(current_folder):
                entry_path = os.path.join(current_folder, entry)
                if os.path.isdir(entry_path):
                    # Add to queue for further processing
                    folders_to_process_queue.append(entry_path)
        except PermissionError:
            log_error(f"無權限訪問資料夾: {current_folder}", include_traceback=False)
        except Exception as e:
            log_error(f"遍歷資料夾 '{current_folder}' 時發生錯誤: {e}", include_traceback=True)
            
    return all_subfolders_to_return

def extract_last_n_files_from_folders(folder_paths, count, enable_limit): # Modified: Added enable_limit parameter
    """
    從每個資料夾中提取圖片檔案的路徑。
    Args:
        folder_paths (list): 資料夾路徑列表。
        count (int): 要提取的最後 N 個檔案數量 (當 enable_limit 為 True 時有效)。
        enable_limit (bool): 是否啟用圖片抽取數量限制。
    Returns:
        dict: 每個資料夾路徑到其圖片檔案路徑列表的映射。
    """
    extracted_files = {}
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff') # Define image extensions
    for folder_path in folder_paths:
        image_files = []
        try:
            for f in os.listdir(folder_path):
                file_path = os.path.join(folder_path, f)
                if os.path.isfile(file_path) and f.lower().endswith(image_extensions):
                    image_files.append((file_path, os.path.getmtime(file_path))) # Store path and modification time
            
            # Sort by modification time (or filename), filename sorting is usually more stable
            image_files.sort(key=lambda x: x[0]) # Sort by filename
            
            # Apply limit based on enable_limit
            if enable_limit:
                extracted_files[folder_path] = [item[0] for item in image_files[-count:]]
            else:
                extracted_files[folder_path] = [item[0] for item in image_files] # Extract all files
        except PermissionError:
            # FIX: Corrected the log_error call for PermissionError
            log_error(f"無權限訪問資料夾中的檔案: {folder_path}", include_traceback=False) 
        except Exception as e:
            log_error(f"處理資料夾 '{folder_path}' 時發生錯誤: {e}", include_traceback=True)
    return extracted_files

# New global helper function for multiprocessing pool
def _pool_worker_hash_and_mtime(image_path):
    """
    多進程池的工作函數：計算圖片哈希值和獲取修改時間。
    這個函數必須是全局的或靜態方法，才能被 multiprocessing 正確序列化。
    """
    phash = calculate_image_hash(image_path) # Returns Im