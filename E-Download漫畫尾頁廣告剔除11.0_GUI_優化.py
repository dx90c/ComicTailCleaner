# ======================================================================
# 檔案名稱：E-Download漫畫尾頁廣告剔除11.0_GUI_優化.py
# 版本號：11.0v75
#
# === 程式說明 ===
# 這是一個專為清理 E-Download 資料夾中漫畫檔案尾頁廣告的工具。
# 它能自動檢測並移除與廣告圖片相似或相互重複的圖片，提升漫畫閱讀體驗。
# 適用於處理大量漫畫檔案，節省手動篩選時間。
# 支援三種比對模式：廣告比對、互相比對和 QR Code 檢測。
#
# === 11.0v75 版本更新內容 (基於 11.0v74 修正) ===
# - **版本號更新**: 將程式版本號從 `11.0v74` 更新為 `11.0v75`。
# - **預覽圖片點擊開啟資料夾**:
#   - 在 `MainWindow` 中，現在可以點擊右側的「目標圖片預覽」和「比對圖片預覽」
#     區域內的圖片，直接開啟該圖片所在的資料夾。
# - **列表「選取」欄位顯示優化**:
#   - 將左側 Treeview 列表中的「選取」欄位顯示從文字「是/否」變更為視覺化的
#     「☑」(打勾) 和「☐」(空框)，讓介面更加清晰美觀。
# - **QR Code 檢測邏輯修復與優化**: (已在 11.0v74 修正)
#   - 修正了 `AttributeError: module 'pyzbar' has no attribute 'decode'` 錯誤。
#   - 在 `ImageComparisonEngine` 的 `_detect_qr_codes` 方法中，QR Code 檢測
#     現在改為使用 OpenCV 內建的 `cv2.QRCodeDetector()`。
#   - 程式仍然會使用 Pillow (PIL) 載入圖片並進行 EXIF 方向處理與色彩模式轉換，
#     確保圖片預處理的穩健性，然後再傳遞給 OpenCV 進行 QR Code 檢測。
# - **修正 ImageComparisonEngine 方法調用錯誤**: (已在 11.0v72 修正)
#   - 修正 `_compare_with_ads`、`_compare_mutually` 和 `_detect_qr_codes`
#     方法在 `ImageComparisonEngine` 類別中的縮排問題。這解決了之前程式報告的
#     `AttributeError: 'ImageComparisonEngine' object has no attribute '_detect_qr_codes'` 錯誤，
#     確保這些方法能被正確識別並調用。
# - **QR Code 檢測模式獨立化**: (已在 11.0v71 修正)
#   - 將「啟用 QR Code 掃描」從設定介面的一個獨立核取方塊，調整為「比對模式」中的一個獨立單選按鈕。
#   - 使用者現在可以明確選擇「廣告比對」、「互相比對」或「QR Code 檢測」作為主要比對模式。
#   - 當選擇「廣告比對」以外的模式時，程式會自動禁用「廣告圖片資料夾」的輸入框。
#   - 如果系統缺少 QR Code 掃描所需的核心依賴（`opencv-python`、`pyzbar`），
#     則「QR Code 檢測」選項將會被禁用，並顯示提示訊息。
# - **滑動條步進與顯示優化**: (已在 11.0v71 修正)
#   - 移除 `ttk.Scale` 中不兼容的 `-resolution` 選項，解決 `TclError` 錯誤。
#   - 修正設定介面與結果顯示介面中的相似度閾值滑動條，使其數值顯示強制四捨五入到整數百分比，
#     達到視覺上 1% 步進的效果。同時，篩選邏輯也將基於四捨五入後的整數值。
# - **列表方向鍵導航修正**: (已在 11.0v71 修正)
#   - 修正 `MainWindow` 中圖片列表使用方向鍵（上下箭頭）導航時，一次移動兩格的問題。
#     現在每次按鍵只會移動到上一個或下一個單一項目，並透過事件中斷機制防止重複觸發。
# - **GUI 佈局健壯性強化**: (已在 11.0v71 修正)
#   - 徹底解決了 `MainWindow` 初始化時因 `grid_row_configure`
#     和 `grid_column_configure` 導致的 `AttributeError` 錯誤。現在 `MainWindow`
#     及其子框架內所有元件的佈局都統一使用 `pack` 佈局管理器，
#     或僅在 Tkinter 根視窗上應用 `grid` 佈局管理器，避免了潛在的屬性衝突。
# - **修正 `NameError`**: (已在 11.0v71 修正)
#   - 在 `load_ad_hashes` 函數中，將 `AD_HASH_FILE` 修正為
#     正確的全局常量 `AD_HASH_CACHE_FILE`，解決了 `NameError: name 'AD_HASH_FILE' is not定義`。
# - **修正 `AttributeError` (SettingsGUI)**: (已在 11.0v71 修正)
#   - 調整 `SettingsGUI` 類中 `self.enable_extract_count_checkbox` 變數名稱為
#     `self.chk_enable_extract_count`，以排除潛在的命名衝突或屬性賦值問題。
# - **新增圖片抽取數量限制開關**: (已在 11.0v71 修正)
#   - 在設定介面中增加一個選項，允許使用者選擇是「提取末尾 N 張圖片」
#     還是「掃描資料夾內所有圖片」，提供更靈活的掃描控制。
# - **相似度閾值動態調整**: (已在 11.0v71 修正)
#   - 在結果顯示介面 (`MainWindow`) 中加入一個滑塊和數值顯示，
#     讓使用者可以直接調整相似度閾值，並即時篩選顯示的結果，無需重新運行掃描。
# - **修正時間篩選邏輯**: (已在 11.0v71 修正)
#   - 調整 `get_all_subfolders` 函數，確保時間篩選只應用於
#     `root_scan_folder` (根掃描資料夾) 下的子資料夾，而不是根資料夾本身。
#     這解決了當根資料夾建立時間不在篩選範圍內時，導致所有子資料夾都被跳過的問題。
# - **哈希演算法優化**: (已在 11.0v71 修正)
#   - 將圖片感知哈希比對演算法從 `average_hash` (ahash)
#     更新為 `perceptual_hash` (phash)，以增加圖片相似度比對的準確度。
# - **快取優化**: (已在 11.0v71 修正)
#   - 掃描圖片哈希快取檔案 (`scanned_hashes_cache.json`) 現在會根據
#     「根掃描資料夾」的路徑動態生成一個專屬的檔案名稱，確保每個不同根掃描資料夾的快取相互獨立。
# - **功能調整**: (已在 11.0v71 修正)
#   - 將「開啟所有選中資料夾」功能修改為「開啟選中資料夾」。
#     現在只會開啟列表中第一個被反白選中（滑鼠選中）的圖片所在的資料夾。
# - **修正錯誤**: (已在 11.0v71 修正)
#   - 修正了「打開資料夾」功能在某些情況下錯誤地開啟「我的文件」資料夾的問題，
#     現在使用更穩健的 `start` 命令透過 shell 開啟資料夾。
# - **基礎版本**: (已在 11.0v71 修正)
#   - 此版本基於 "1140614谷歌版-可用版-只有調整排序.PY" 進行組織與命名更新。
# - **功能強化**: (已在 11.0v71 修正)
#   - 正式啟用並實作資料夾「建立時間」篩選功能。
# - **性能優化**: (已在 11.0v71 修正)
#   - 引入資料夾建立時間快取機制 (JSON 檔案)，大幅提升後續掃描效率。
# - **程式碼重構**: (已在 11.0v71 修正)
#   - 統一導入語句，並對部分程式碼邏輯進行整理，提高可讀性和維護性。
# - **核心邏輯實作**: (已在 11.0v71 修正)
#   - 將 ImageComparisonEngine 中的圖片哈希計算、相似度比對和 QR Code 偵測邏輯從模擬替換為實際功能。
# - **新增掃描圖片哈希快取**: (已在 11.0v71 修正)
#   - 實作了掃描圖片的哈希快取功能，包括讀取、寫入、增量更新和強制重建，進一步提升效率。
# - **錯誤修復 (閃退問題)**: (已在 11.0v71 修正)
#   - 修正 `extract_last_n_files_from_folders` 函數中 `log_error` 呼叫的語法錯誤，
#     並增強 `log_error` 函數的寫入即時性。
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
    # 'qr_scan_enabled': False,      # Removed: Now part of comparison_mode
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


# Ad hash cache file path
AD_HASH_CACHE_FILE = "ad_hashes.json" 

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
            with open(AD_HASH_CACHE_FILE, 'r', encoding='utf-8') as f: # Corrected variable name from AD_HASH_FILE
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

def calculate_image_hash(image_path, hash_size=8):
    """
    計算圖片的感知哈希值 (Perceptual Hash)。
    Args:
        image_path (str): 圖片檔案的路徑。
        hash_size (int): 哈希值的尺寸，例如 8 代表生成 8x8 的哈希矩陣。
    Returns:
        imagehash.ImageHash: 圖片的感知哈希物件，如果處理失敗則返回 None。
    """
    try:
        with Image.open(image_path) as img:
            # 使用 ImageOps.exif_transpose 處理圖片的 EXIF 方向，確保正確旋轉
            img = ImageOps.exif_transpose(img)
            # 使用 perceptual_hash (phash) 演算法
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

# New global helper function for multiprocessing pool
def _pool_worker_hash_and_mtime(image_path):
    """
    多進程池的工作函數：計算圖片哈希值和獲取修改時間。
    這個函數必須是全局的或靜態方法，才能被 multiprocessing 正確序列化。
    """
    phash = calculate_image_hash(image_path) # Returns ImageHash object
    if phash is not None:
        try:
            mtime = os.path.getmtime(image_path)
            return image_path, phash, mtime
        except Exception as e:
            log_error(f"獲取文件修改時間失敗 {image_path}: {e}", include_traceback=True) # Changed to True
            return image_path, None, None # Return None for hash if mtime fails
    return image_path, None, None # Return None for hash if hashing fails


class ImageComparisonEngine:
    def __init__(self, root_scan_folder, ad_folder_path, extract_count, excluded_folders,
                 enable_time_filter, start_date_filter, end_date_filter,
                 similarity_threshold, comparison_mode, rebuild_ad_cache, system_qr_scan_capability, # Renamed qr_scan_enabled to system_qr_scan_capability
                 scanned_hashes_cache_manager, enable_extract_count_limit): # Modified: Added enable_extract_count_limit
        self.root_scan_folder = root_scan_folder
        self.ad_folder_path = ad_folder_path
        self.extract_count = extract_count
        self.enable_extract_count_limit = enable_extract_count_limit # New: Store the new setting
        # Standardize excluded paths for comparison with os.path.normpath(dirpath)
        self.excluded_folders = [os.path.normpath(f) for f in excluded_folders]
        self.enable_time_filter = enable_time_filter
        self.start_date_filter = start_date_filter
        self.end_date_filter = end_date_filter
        self.similarity_threshold = similarity_threshold
        # comparison_mode now directly contains 'ad_comparison', 'mutual_comparison', or 'qr_detection'
        self.comparison_mode = comparison_mode 
        self.rebuild_ad_cache = rebuild_ad_cache
        self.system_qr_scan_capability = system_qr_scan_capability # Store the global capability flag
        
        self.image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
        
        self.target_hashes = {} # Stores hashes of target images
        self.ad_hashes_cache = {} # Stores hashes of ad images (obtained from load_ad_hashes)
        self.scanned_hashes_cache_manager = scanned_hashes_cache_manager # Store instance

        # Progress counters
        self.folder_count = 0
        self.file_count = 0
        self.processed_folders_display_interval = 1000 # Print progress every 1000 folders
        print(f"ImageComparisonEngine initialized with config: {self.__dict__}", flush=True) # Added flush=True


    def generate_extracted_files(self, folder_creation_cache_manager):
        """
        Scan root folder, extract last N images from each subfolder,
        and save paths to extracted_files.txt (now directly returns dictionary).
        Also calculates the number of skipped folders.
        Uses get_all_subfolders and extract_last_n_files_from_folders functions.
        """
        print(f"正在生成目標檔案清單...", flush=True) # Added flush=True
        print(f"開始處理資料夾: {self.root_scan_folder}", flush=True) # Added flush=True
        
        if self.enable_extract_count_limit:
            print(f"每個資料夾抽取最後 {self.extract_count} 個檔案", flush=True) # Added flush=True
        else:
            print("每個資料夾將掃描所有圖片 (未限制抽取數量)", flush=True)

        if self.enable_time_filter:
            print(f"注意: 時間篩選功能已啟用，範圍從 {self.start_date_filter.strftime('%Y-%m-%d')} 到 {self.end_date_filter.strftime('%Y-%m-%d') if self.end_date_filter else '無結束日期'}。", flush=True) # Added flush=True
        else:
            print("注意: 時間篩選功能未啟用，所有資料夾都將被掃描。", flush=True) # Added flush=True

        # Get all eligible subfolders using existing get_all_subfolders
        all_folders = get_all_subfolders(
            self.root_scan_folder,
            self.excluded_folders,
            self.enable_time_filter,
            self.start_date_filter,
            self.end_date_filter,
            folder_creation_cache_manager
        )
        print(f"過濾後找到 {len(all_folders)} 個資料夾。", flush=True) # Added flush=True

        # Extract images from these folders using existing extract_last_n_files_from_folders
        # Modified: Pass enable_extract_count_limit
        extracted_files_dict = extract_last_n_files_from_folders(all_folders, self.extract_count, self.enable_extract_count_limit)
        
        total_extracted_files = sum(len(files) for files in extracted_files_dict.values())
        print(f"總共抽取了 {total_extracted_files} 個檔案。", flush=True) # Added flush=True
        
        # For compatibility with old logic that depended on extracted_files.txt, regenerate it here.
        # In MainWindow, we will directly use the returned similar_files data, no longer relying on this file.
        # FIX: Changed f_path to f_fpath to match the inner loop variable
        extracted_file_paths_list = [f_fpath for folder_files in extracted_files_dict.values() for f_fpath in folder_files]
        try:
            with open("extracted_files.txt", "w", encoding="utf-8") as f:
                for filepath in extracted_file_paths_list:
                    f.write(f"{filepath}\n")
            print("結果已儲存至: extracted_files.txt (此文件主要為兼容目的，程式內部使用字典數據)", flush=True) # Added flush=True
        except Exception as e:
            log_error(f"寫入 extracted_files.txt 失敗: {e}", include_traceback=True)

        return extracted_files_dict # Return dictionary format

    def _calculate_hashes_multiprocess(self, file_paths, description="圖片"):
        """Calculate image hashes using multiprocessing"""
        hashes = {} # This will store ImageHash objects (for return value)
        paths_to_hash_with_mtime = [] # This list will only contain paths that need hashing
        
        # Step 1: Check cache, separate cached and unhashed images
        for path in file_paths:
            cached_hash = self.scanned_hashes_cache_manager.get_hash(path)
            if cached_hash:
                hashes[path] = cached_hash # Add valid cached hash directly to results
            else:
                paths_to_hash_with_mtime.append(path) # Paths to be hashed
        
        if not paths_to_hash_with_mtime:
            print(f"所有 {description} 檔案均已在快取中且有效，無需重新計算哈希值。", flush=True) # Added flush=True
            return hashes # If all hashes are already in cache, return directly

        num_processes = cpu_count()
        print(f"正在計算 {len(paths_to_hash_with_mtime)} 個{description}的哈希值 (多進程，使用 {num_processes} 個進程)...", flush=True) # Added flush=True
        
        try:
            with Pool(processes=num_processes) as pool:
                print("多進程池已成功創建。開始映射任務...", flush=True) # Added flush=True
                results_iterator = pool.imap_unordered(_pool_worker_hash_and_mtime, paths_to_hash_with_mtime)
                
                processed_count = 0
                for path, phash, mtime in results_iterator:
                    if phash is not None: # Only process if hash calculation successful
                        hashes[path] = phash # Add newly calculated hash to results dictionary
                        self.scanned_hashes_cache_manager.update_hash(path, phash, mtime) # Update main process's cache manager
                    processed_count += 1
                    # Print progress
                    if processed_count % self.processed_folders_display_interval == 0 or processed_count == len(paths_to_hash_with_mtime):
                        print(f"  已完成計算 {processed_count}/{len(paths_to_hash_with_mtime)} 個{description}的哈希值...", flush=True) # Added flush=True
        except Exception as e:
            error_message = f"多進程哈希計算過程中發生嚴重錯誤: {e}"
            log_error(error_message, include_traceback=True)
            # Try to show a messagebox, but also print for immediate console visibility
            print(f"\n!!!! 多進程錯誤 !!!! {error_message}", flush=True)
            messagebox.showerror("程式錯誤 - 多進程", f"多進程計算過程中發生錯誤，程式將關閉。\n錯誤: {e}\n請查看 'error_log.txt'。", parent=self.root) # Added parent for messagebox
            sys.exit(1) # Exit immediately if multiprocessing fails critically

        # Step 3: Save cache after all multiprocessing tasks are complete
        self.scanned_hashes_cache_manager.save_cache() 
        print(f"完成計算 {len(hashes)} 個{description}的哈希值 (包含快取)。", flush=True) # Added flush=True
        return hashes

    def compare_images(self, files_to_process_dict, ad_hashes_from_main):
        """Execute image comparison based on selected comparison mode"""
        similar_files = [] # Structure: [(filepath1, filepath2, similarity), ...]

        # Flatten all file paths from files_to_process_dict into a single list
        # FIX: Changed f_path to f_fpath to match the inner loop variable
        all_target_file_paths = [f_fpath for folder_files in files_to_process_dict.values() for f_fpath in folder_files]
        
        if not all_target_file_paths:
            print("沒有找到任何目標圖片檔案進行比對。", flush=True) # Added flush=True
            return []

        print(f"已收集 {len(all_target_file_paths)} 個目標圖片檔案。", flush=True) # Added flush=True

        # Calculate target image hashes (this will use or update cache via _calculate_hashes_multiprocess)
        self.target_hashes = self._calculate_hashes_multiprocess(all_target_file_paths, "目標圖片")

        print(f"啟動圖片比對，模式: {self.comparison_mode}", flush=True) # Added flush=True
        
        # Calculate hash difference threshold
        # imagehash library returns Hamming distance (0-64), smaller distance means higher similarity.
        # similarity = (1 - diff / 64) * 100
        # then diff = (1 - similarity / 100) * 64
        threshold_diff = (100 - self.similarity_threshold) / 100.0 * 64
        # Ensure threshold is within a reasonable range
        if threshold_diff < 0: threshold_diff = 0
        if threshold_diff > 64: threshold_diff = 64

        if self.comparison_mode == "ad_comparison":
            self.ad_hashes_cache = ad_hashes_from_main # Use ad_hashes passed from main
            if not self.ad_hashes_cache:
                print("沒有可用的廣告圖片哈希值進行比對。", flush=True) # Added flush=True
                return []
            similar_files = self._compare_with_ads(threshold_diff)
        elif self.comparison_mode == "mutual_comparison":
            similar_files = self._compare_mutually(threshold_diff)
        elif self.comparison_mode == "qr_detection":
            if self.system_qr_scan_capability: # Check the system capability here
                similar_files = self._detect_qr_codes()
            else:
                print("QR Code 掃描功能因缺少依賴而被禁用，無法執行 QR Code 檢測模式。", flush=True) # Added flush=True
                return []
        else:
            print("無效的比對模式，請檢查設定。", flush=True) # Added flush=True
            return []

        print("比對完成。", flush=True) # Added flush=True
        return similar_files

    def _compare_with_ads(self, threshold_diff):
        """將目標圖片與廣告圖片進行比對"""
        print(f"開始與廣告圖片進行比對，相似度閾值: {self.similarity_threshold:.1f}% (哈希差異 <= {int(threshold_diff)})", flush=True) # Added flush=True
        found_similar = []
        progress_interval = max(1, len(self.target_hashes) // 20) # 每 5% 進度打印一次

        for i, (target_path, target_phash) in enumerate(self.target_hashes.items()):
            if target_phash is None: # 跳過哈希計算失敗的圖片
                continue

            for ad_path, ad_phash in self.ad_hashes_cache.items():
                if ad_phash is None: # 跳過哈希計算失敗的圖片
                    continue

                diff = target_phash - ad_phash
                if diff <= threshold_diff:
                    similarity = (1 - diff / 64) * 100
                    found_similar.append((target_path, ad_path, similarity))
            
            if (i + 1) % progress_interval == 0 or (i + 1) == len(self.target_hashes):
                print(f"  已比對 {i + 1}/{len(self.target_hashes)} 個目標圖片...", flush=True) # Added flush=True
        
        print(f"廣告比對完成。找到 {len(found_similar)} 個相似圖片。", flush=True) # Added flush=True
        return found_similar

    def _compare_mutually(self, threshold_diff):
        """在抽取出的圖片之間進行互相比對"""
        print(f"開始在抽取出的圖片之間進行互相比對，相似度閾值: {self.similarity_threshold:.1f}% (哈希差異 <= {int(threshold_diff)})", flush=True) # Added flush=True
        found_similar = []
        # 將哈希值和路徑轉換為列表以便按索引遍歷
        target_paths_list = list(self.target_hashes.keys())
        target_phashes_list = list(self.target_hashes.values())
        
        n = len(target_paths_list)
        progress_interval = max(1, n // 20) # 每 5% 進度打印一次

        for i in range(n):
            path1 = target_paths_list[i]
            phash1 = target_phashes_list[i]

            if phash1 is None:
                continue

            # 只與後面的圖片比對，避免重複和自己與自己比對
            for j in range(i + 1, n):
                path2 = target_paths_list[j] # Corrected: should be from target_paths_list, not target_phashes_list
                phash2 = target_phashes_list[j]

                if phash2 is None:
                    continue

                diff = phash1 - phash2
                if diff <= threshold_diff:
                    similarity = (1 - diff / 64) * 100
                    # 互相比對模式下，記錄兩張圖片的路徑
                    found_similar.append((path1, path2, similarity))
            
            if (i + 1) % progress_interval == 0 or (i + 1) == n:
                print(f"  已比對 {i + 1}/{n} 個目標圖片...", flush=True) # Added flush=True

        print(f"互相比對完成。找到 {len(found_similar)} 對相似圖片。", flush=True) # Added flush=True
        return found_similar

    def _detect_qr_codes(self):
        """檢測圖片中的 QR Code (使用 OpenCV 內建檢測器)"""
        print("開始檢測圖片中的 QR Code...", flush=True) # Added flush=True
        found_qr_images = []
        progress_interval = max(1, len(self.target_hashes) // 20) # 每 5% 進度打印一次

        # 初始化 OpenCV 的 QRCodeDetector
        qr_detector = cv2.QRCodeDetector()

        for i, (image_path, _) in enumerate(self.target_hashes.items()):
            try:
                # 1. 使用 Pillow 載入圖片，處理 EXIF 方向並轉換為 RGB 格式 (移除 alpha 通道)
                with Image.open(image_path) as pil_img:
                    pil_img = ImageOps.exif_transpose(pil_img) # 處理 EXIF 方向
                    pil_img = pil_img.convert('RGB') # 確保是 RGB 格式

                    # 2. 將 Pillow 圖片轉換為 NumPy 陣列 (OpenCV 預設 BGR 格式)
                    img_cv = np.array(pil_img)
                    img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR) # 從 PIL 的 RGB 轉換為 OpenCV 的 BGR

                if img_cv is None:
                    log_error(f"無法將圖片轉換為 OpenCV 格式或圖片內容為空: {image_path}", include_traceback=False)
                    continue

                # 將圖片轉換為灰度圖，有助於 QR Code 檢測
                gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
                
                # 使用 OpenCV 的 QRCodeDetector 進行檢測和解碼
                # detectAndDecodeMulti 返回多個結果 (如果有)
                retval, decoded_info, points, straight_qrcode = qr_detector.detectAndDecodeMulti(gray)

                if retval: # 如果檢測到並解碼了 QR Code
                    # 為了與其他模式的 similar_files 結構一致，第二個路徑設為 'N/A'，相似度 100%
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
        """Initializes the settings interface."""
        self.master = master
        self.config_file_path = config_file_path
        self.qr_scan_feature_enabled_global = qr_scan_feature_enabled_global

        self.result_config = None
        self.should_proceed = False
        self.rebuild_folder_cache_result = False
        self.rebuild_scanned_cache_result = False # Corrected: This is the actual attribute name

        self.config = load_config(self.config_file_path)

        self.settings_window = tk.Toplevel(master)
        self.settings_window.title("E-Download 漫畫尾頁廣告剔除 - 設定")
        self.settings_window.geometry("700x700")
        self.settings_window.resizable(False, False)

        self.settings_window.transient(master)
        self.settings_window.grab_set()
        self.settings_window.focus_force()
        self.settings_window.update_idletasks() # Force update to display the window
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
        """Creates all GUI widgets in the settings interface."""
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
        
        # New: Checkbutton for enabling/disabling extract count limit
        self.enable_extract_count_limit_var = tk.BooleanVar()
        # Renamed to chk_enable_extract_count to avoid potential attribute error due to name clash or subtle issue
        self.chk_enable_extract_count = ttk.Checkbutton(basic_settings_frame, text="啟用圖片抽取數量限制", variable=self.enable_extract_count_limit_var)
        self.chk_enable_extract_count.grid(row=0, column=0, sticky="w", pady=2) # Using the new name

        ttk.Label(basic_settings_frame, text="提取末尾圖片數量:").grid(row=1, column=0, sticky="w", pady=2) # Shifted row index
        self.extract_count_var = tk.StringVar()
        self.extract_count_spinbox = ttk.Spinbox(basic_settings_frame, from_=1, to=100, textvariable=self.extract_count_var, width=5)
        self.extract_count_spinbox.grid(row=1, column=1, sticky="w", padx=5) # Shifted row index
        ttk.Label(basic_settings_frame, text="(以檔名為判斷基準，從每個資料夾末尾提取N張圖片進行比對)").grid(row=1, column=2, sticky="w", padx=5) # Shifted row index

        ttk.Label(basic_settings_frame, text="相似度閾值 (%):").grid(row=2, column=0, sticky="w", pady=2) # Shifted row index
        self.similarity_threshold_var = tk.DoubleVar()
        # 移除 resolution=1.0
        ttk.Scale(basic_settings_frame, from_=50, to=100, orient="horizontal",
                  variable=self.similarity_threshold_var, length=200,
                  command=self._update_threshold_label).grid(row=2, column=1, sticky="w", padx=5) # Shifted row index
        self.threshold_label = ttk.Label(basic_settings_frame, text="")
        self.threshold_label.grid(row=2, column=2, sticky="w", padx=5) # Shifted row index

        ttk.Label(basic_settings_frame, text="排除資料夾名稱 (逗號分隔):").grid(row=3, column=0, sticky="w", pady=2) # Shifted row index
        self.excluded_folders_text = tk.Text(basic_settings_frame, width=40, height=4)
        self.excluded_folders_text.grid(row=3, column=1, columnspan=2, sticky="ew", padx=5) # Shifted row index
        scrollbar = ttk.Scrollbar(basic_settings_frame, command=self.excluded_folders_text.yview)
        scrollbar.grid(row=3, column=3, sticky="ns") # Shifted row index
        self.excluded_folders_text.config(yscrollcommand=scrollbar.set)
        
        row_idx += 4 # Adjusted row index for subsequent frames

        mode_frame = ttk.LabelFrame(frame, text="比對模式", padding="10")
        mode_frame.grid(row=row_idx+1, column=0, sticky="ew", pady=5, padx=5)
        mode_frame.grid_columnconfigure(0, weight=1)

        self.comparison_mode_var = tk.StringVar()
        ttk.Radiobutton(mode_frame, text="廣告比對 (廣告圖 vs 掃描圖)", variable=self.comparison_mode_var, value="ad_comparison").pack(anchor="w", pady=2)
        ttk.Radiobutton(mode_frame, text="互相比對 (掃描圖 vs 掃描圖)", variable=self.comparison_mode_var, value="mutual_comparison").pack(anchor="w", pady=2)
        
        # QR Code 檢測模式作為獨立選項
        self.qr_mode_radiobutton = ttk.Radiobutton(mode_frame, text="QR Code 檢測 (僅掃描圖)", variable=self.comparison_mode_var, value="qr_detection")
        self.qr_mode_radiobutton.pack(anchor="w", pady=2)

        # 根據全局 QR_SCAN_ENABLED 狀態禁用或啟用 QR Code 相關選項
        if not self.qr_scan_feature_enabled_global:
            self.qr_mode_radiobutton.config(state=tk.DISABLED)
            # 如果預設配置是 QR_detection 但依賴不滿足，則將模式設置為 ad_comparison
            if self.config.get('comparison_mode') == 'qr_detection':
                self.comparison_mode_var.set('ad_comparison') 
            ttk.Label(mode_frame, text="(QR Code 檢測功能禁用，缺少依賴)", foreground="red").pack(anchor="w", padx=5)
        
        # 綁定 comparison_mode_var 追蹤器，控制廣告資料夾輸入框的啟用/禁用狀態
        self.comparison_mode_var.trace_add("write", self._toggle_ad_folder_entry_state)
        
        row_idx += 1

        cache_time_frame = ttk.LabelFrame(frame, text="快取與時間篩選", padding="10")
        cache_time_frame.grid(row=row_idx+1, column=1, columnspan=2, sticky="ew", pady=5, padx=5)
        cache_time_frame.grid_columnconfigure(1, weight=1)

        self.rebuild_ad_cache_var = tk.BooleanVar()
        ttk.Checkbutton(cache_time_frame, text="重建廣告圖片哈希快取", variable=self.rebuild_ad_cache_var,
                        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=2)
        
        self.enable_time_filter_var = tk.BooleanVar()
        self.enable_time_filter_checkbox = ttk.Checkbutton(cache_time_frame, text="啟用資料夾建立時間篩選", variable=self.enable_time_filter_var)
        self.enable_time_filter_checkbox.grid(row=1, column=0, columnspan=3, sticky="w", pady=2)

        ttk.Label(cache_time_frame, text="從:").grid(row=2, column=0, sticky="w", padx=5)
        self.start_date_var = tk.StringVar()
        self.start_date_entry = ttk.Entry(cache_time_frame, textvariable=self.start_date_var, width=15)
        self.start_date_entry.grid(row=2, column=1, sticky="ew", padx=5)
        ttk.Label(cache_time_frame, text="(YYYY-MM-DD)").grid(row=2, column=2, sticky="w")

        ttk.Label(cache_time_frame, text="到:").grid(row=3, column=0, sticky="w", padx=5)
        self.end_date_var = tk.StringVar()
        self.end_date_entry = ttk.Entry(cache_time_frame, textvariable=self.end_date_var, width=15)
        self.end_date_entry.grid(row=3, column=1, sticky="ew", padx=5)
        ttk.Label(cache_time_frame, text="(YYYY-MM-DD)").grid(row=3, column=2, sticky="w")

        ttk.Button(cache_time_frame, text="重建資料夾時間快取", command=self._rebuild_folder_cache).grid(row=4, column=0, columnspan=3, sticky="w", pady=5)
        ttk.Button(cache_time_frame, text="重建掃描圖片哈希快取", command=self._rebuild_scanned_cache).grid(row=5, column=0, columnspan=3, sticky="w", pady=5)
        
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
        """Populates GUI widgets with loaded settings."""
        self.root_scan_folder_entry.insert(0, self.config.get('root_scan_folder', ''))
        self.ad_folder_entry.insert(0, self.config.get('ad_folder_path', ''))
        self.extract_count_var.set(str(self.config.get('extract_count', 5)))

        excluded_folders_str = "\n".join(self.config.get('excluded_folders', []))
        self.excluded_folders_text.delete("1.0", tk.END)
        self.excluded_folders_text.insert(tk.END, excluded_folders_str)

        self.similarity_threshold_var.set(self.config.get('similarity_threshold', 85.0))
        self._update_threshold_label(self.similarity_threshold_var.get())

        comparison_mode_cfg = self.config.get('comparison_mode', 'ad_comparison')
        # Load the comparison mode directly
        self.comparison_mode_var.set(comparison_mode_cfg)
        # Manually call the toggle function to set the initial state of ad_folder_entry
        self._toggle_ad_folder_entry_state() 

        self.rebuild_ad_cache_var.set(self.config.get('rebuild_ad_cache', False))

        # Load new setting and set initial state
        self.enable_extract_count_limit_var.set(self.config.get('enable_extract_count_limit', True))
        self._toggle_extract_count_fields() # Call to set initial state

        self.enable_time_filter_var.set(self.config.get('enable_time_filter', False))
        self.start_date_var.set(self.config.get('start_date_filter', ''))
        self.end_date_var.set(self.config.get('end_date_filter', ''))
        
        self._toggle_time_filter_fields()

        # No longer loading qr_scan_enabled_var from config, as it's now part of comparison_mode_var
        # The state of qr_mode_radiobutton is set during widget creation based on qr_scan_feature_enabled_global

    def _setup_bindings(self):
        """Sets up event bindings."""
        self.enable_time_filter_var.trace_add("write", lambda *args: self._toggle_time_filter_fields())
        # New: Bind the extract count limit checkbox
        # Using the new name self.chk_enable_extract_count to bind the trace
        self.enable_extract_count_limit_var.trace_add("write", lambda *args: self._toggle_extract_count_fields())

    def _toggle_time_filter_fields(self):
        """Enables/disables date entry fields based on time filter checkbox."""
        is_enabled = self.enable_time_filter_var.get()
        state = tk.NORMAL if is_enabled else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)
        if not is_enabled:
            self.start_date_var.set("")
            self.end_date_var.set("")

    def _toggle_extract_count_fields(self): # New method
        """Enables/disables extract count spinbox based on enable_extract_count_limit checkbox."""
        is_enabled = self.enable_extract_count_limit_var.get()
        state = tk.NORMAL if is_enabled else tk.DISABLED
        self.extract_count_spinbox.config(state=state)
        if not is_enabled:
            # Optionally clear the spinbox value or set to a placeholder when disabled
            # self.extract_count_var.set("") # Could be confusing if it clears valid input
            pass # Keep the value when disabled, just prevent editing

    def _browse_folder(self, entry_widget):
        """Opens a folder selection dialog."""
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, folder_selected)

    def _update_threshold_label(self, val):
        """Updates the similarity threshold label."""
        # 使用 round 函數確保顯示的值是整數，因為 resolution 設為 1.0
        self.threshold_label.config(text=f"{round(float(val)):d}%") 

    def _validate_date(self, date_str):
        """Validates date string format (YYYY-MM-DD)."""
        if not date_str:
            return True
        try:
            datetime.datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
            
    def _toggle_ad_folder_entry_state(self, *args):
        """Enables/disables the ad folder entry based on selected comparison mode."""
        selected_mode = self.comparison_mode_var.get()
        if selected_mode == "ad_comparison":
            self.ad_folder_entry.config(state=tk.NORMAL)
        else:
            self.ad_folder_entry.config(state=tk.DISABLED)
            # Optionally clear the ad_folder_entry if it's disabled and not the current mode.
            # self.ad_folder_entry.delete(0, tk.END)

    def _save_settings(self):
        """Gets current settings from GUI and saves to JSON file."""
        try:
            # Validate extract_count only if the limit is enabled
            extract_count_val = 0 # Default if disabled
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
                'extract_count': extract_count_val, # Use validated value
                'enable_extract_count_limit': self.enable_extract_count_limit_var.get(), # Save the new setting
                'excluded_folders': [f.strip() for f in self.excluded_folders_text.get("1.0", tk.END).splitlines() if f.strip()],
                'similarity_threshold': self.similarity_threshold_var.get(),
                'comparison_mode': self.comparison_mode_var.get(), # Now directly saves the selected mode
                'rebuild_ad_cache': self.rebuild_ad_cache_var.get(),
                # 'qr_scan_enabled': self.qr_scan_enabled_var.get() if self.qr_scan_feature_enabled_global else False, # Removed
                'enable_time_filter': self.enable_time_filter_var.get(),
                'start_date_filter': self.start_date_var.get(),
                'end_date_filter': self.end_date_var.get()
            }

            if not config_to_save["root_scan_folder"]:
                messagebox.showerror("錯誤", "漫畫掃描根資料夾不能為空！")
                return False
            if not os.path.isdir(config_to_save["root_scan_folder"]):
                messagebox.showerror("錯誤", "漫畫掃描根資料夾無效或不存在！")
                return False
            
            if config_to_save["comparison_mode"] == "ad_comparison":
                if not config_to_save["ad_folder_path"]:
                    messagebox.showerror("錯誤", "在廣告比對模式下，廣告圖片資料夾不能為空！")
                    return False
                if not os.path.isdir(config_to_save["ad_folder_path"]):
                    messagebox.showerror("錯誤", "廣告圖片資料夾無效或不存在！")
                    return False

            if config_to_save["enable_time_filter"]:
                if not self._validate_date(config_to_save["start_date_filter"]):
                    messagebox.showerror("輸入錯誤", "開始日期格式無效。請使用YYYY-MM-DD 格式。")
                    return False
                if not self._validate_date(config_to_save["end_date_filter"]):
                    messagebox.showerror("輸入錯誤", "結束日期格式無效。請使用YYYY-MM-DD 格式。")
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
            messagebox.showerror("輸入錯誤", "相似度閾值必須是有效數字！") # Removed extract count part as it's now handled specifically
            return False
        except Exception as e:
            log_error(f"保存或處理設定時發生錯誤: {e}", include_traceback=True)
            messagebox.showerror("錯誤", f"保存或處理設定時發生錯誤: {e}\n{traceback.format_exc()}")
            return False

    def _rebuild_folder_cache(self):
        """Handles click event for rebuilding folder creation time cache."""
        response = messagebox.askyesno("重建快取", "這將清空並重建資料夾建立時間快取，此操作可能需要一些時間，確定要繼續嗎？")
        if response:
            self.rebuild_folder_cache_result = True
            messagebox.showinfo("快取重建提示", "資料夾建立時間快取已標記為需要重建。下次運行程式時將自動處理此操作。")

    def _rebuild_scanned_cache(self):
        """Handles click event for rebuilding scanned image hash cache."""
        response = messagebox.askyesno("重建快取", "這將清空並重建掃描圖片哈希快取，此操作可能需要一些時間，確定要繼續嗎？")
        if response:
            self.rebuild_scanned_cache_result = True
            messagebox.showinfo("快取重建提示", "掃描圖片哈希快取已標記為需要重建。下次運行程式時將自動處理此操作。")

    def _start_execution(self):
        """Handles 'Start Execution' button click: saves settings and sets flag to proceed."""
        if self._save_settings():
            self.result_config = self.config
            self.should_proceed = True
            self.settings_window.destroy()

    def _on_closing(self):
        """Handles user attempting to close the settings window."""
        if messagebox.askokcancel("關閉程式", "確定要關閉設定視窗並退出程式嗎？"):
            self.should_proceed = False
            self.settings_window.destroy()

class MainWindow:
    def __init__(self, master, similar_files=None, comparison_mode="N/A", initial_similarity_threshold=85.0): # Added initial_similarity_threshold
        self.root = master
        self.all_similar_files = similar_files if similar_files is not None else [] # Store all found similar files
        self.displayed_similar_files = [] # Files currently displayed after filtering
        self.selected_files = set()
        self.comparison_mode = comparison_mode
        self.deleted_history = deque(maxlen=10)

        self.img_tk_target = None
        self.img_tk_compare = None
        self.max_preview_size = (400, 400)
        self.current_display_threshold = tk.DoubleVar(value=initial_similarity_threshold) # New: For dynamic filtering
        self.original_scan_threshold = initial_similarity_threshold # Store original scan threshold for reference

        try:
            self.root.title("圖片比對結果 - 廣告/相似圖片清理工具")
            self.root.geometry("1400x850")
            self.root.deiconify() # Ensure the main window is visible
            self.root.lift()
            self.root.focus_force()
            self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

            self._create_widgets()
            self._populate_listbox() # This will now filter based on initial threshold
            self._bind_keys() # Re-enabled and modified for single-step navigation
            
            self.root.update_idletasks()

            print("MainWindow: 介面已成功建立並初始化。", flush=True)

            if self.displayed_similar_files: # Check displayed files
                if self.tree.get_children():
                    first_item_id = self.tree.get_children()[0]
                    self.tree.selection_set(first_item_id)
                    self.tree.focus(first_item_id)
                    self._on_item_select(None)
        except Exception as e:
            log_error(f"MainWindow 初始化失敗: {e}\n{traceback.format_exc()}")
            messagebox.showerror("GUI 錯誤", f"無法啟動圖片比對結果介面: {e}\n請查看 error_log.txt 獲取詳細信息。")
            if self.root:
                self.root.destroy()
            sys.exit(1)

    def _create_widgets(self):
        """Creates all GUI widgets for the main window."""
        for widget in self.root.winfo_children():
            widget.destroy()

        # Create main paned window and pack it into the root
        main_pane = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10) # Using pack here

        # Left and Right frames for the paned window
        left_frame = ttk.Frame(main_pane)
        main_pane.add(left_frame, weight=1)

        right_frame = ttk.Frame(main_pane)
        main_pane.add(right_frame, weight=2)

        # Treeview and scrollbar inside left_frame
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
        
        # Changed from grid to pack for treeview and scrollbar
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vscroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<<TreeviewSelect>>", self._on_item_select)
        self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<space>", self._toggle_selection)
        self.tree.bind("<Return>", self._toggle_selection)
        self.tree.bind("<Delete>", self._delete_selected_from_disk)
        self.tree.bind("<BackSpace>", self._delete_selected_from_disk)
        self.root.bind("<Control-z>", self._undo_delete_gui)

        self.target_image_frame = ttk.LabelFrame(right_frame, text="目標圖片預覽", padding="10")
        self.target_image_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5) 
        self.target_image_label = ttk.Label(self.target_image_frame)
        self.target_image_label.pack(fill=tk.BOTH, expand=True) 
        self.target_path_label = ttk.Label(self.target_image_frame, text="", wraplength=600)
        self.target_path_label.pack(fill=tk.X) 
        # Bind click event to target image label for opening folder
        self.target_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=True))

        self.compare_image_frame = ttk.LabelFrame(right_frame, text="比對圖片預覽", padding="10")
        self.compare_image_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5) 
        self.compare_image_label = ttk.Label(self.compare_image_frame)
        self.compare_image_label.pack(fill=tk.BOTH, expand=True) 
        self.compare_path_label = ttk.Label(self.compare_image_frame, text="", wraplength=600)
        self.compare_path_label.pack(fill=tk.X) 
        # Bind click event to compare image label for opening folder
        self.compare_image_label.bind("<Button-1>", lambda event: self._on_preview_image_click(event, is_target_image=False))


        # Create bottom button container and pack it into the root
        bottom_button_container = ttk.Frame(self.root)
        bottom_button_container.pack(fill=tk.X, expand=False, padx=10, pady=10) 

        button_frame = ttk.Frame(bottom_button_container)
        button_frame.pack(fill=tk.X, expand=True, padx=5, pady=5) # Added padx/pady here

        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="刪除選中", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="撤銷刪除 (Ctrl+Z)", command=self._undo_delete_gui).pack(side=tk.LEFT, padx=5, pady=5)
        # Modified button text and command to open only the currently selected folder
        ttk.Button(button_frame, text="開啟選中資料夾", command=self._open_selected_folder_single).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(button_frame, text="關閉", command=self._on_closing).pack(side=tk.RIGHT, padx=5, pady=5)
        
        # New: Similarity Filter Section
        filter_frame = ttk.LabelFrame(bottom_button_container, text="相似度篩選", padding="10")
        filter_frame.pack(fill=tk.X, expand=True, padx=5, pady=5) 
        
        # Changed from grid to pack for children within filter_frame
        ttk.Label(filter_frame, text="最小相似度 (%):").pack(side=tk.LEFT, pady=2)
        # 移除 resolution=1.0
        ttk.Scale(filter_frame, from_=50, to=100, orient="horizontal",
                  variable=self.current_display_threshold, length=200,
                  command=self._update_display_threshold).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.display_threshold_label = ttk.Label(filter_frame, text=f"{self.current_display_threshold.get():.1f}%")
        self.display_threshold_label.pack(side=tk.LEFT, padx=5)
        
        # Bind the trace to update the listbox dynamically when the slider changes
        self.current_display_threshold.trace_add("write", self._update_display_threshold)


    def _update_display_threshold(self, *args):
        """Callback for the similarity threshold slider."""
        current_val = self.current_display_threshold.get()
        # 使用 round 函數確保顯示的值是整數，因為 resolution 設為 1.0
        self.display_threshold_label.config(text=f"{round(current_val):d}%")
        self._populate_listbox() # Re-populate the listbox with the new filter

    def _populate_listbox(self):
        """Populates the Treeview with similar files, applying the current filter."""
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        self.selected_files.clear() # Clear selections when repopulating

        # 將閾值四捨五入到整數，確保篩選邏輯符合 1% 步進
        current_threshold = round(self.current_display_threshold.get()) 
        self.displayed_similar_files = [] # Reset displayed list

        for path1, path2, similarity in self.all_similar_files:
            if similarity >= current_threshold:
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
                
                unique_item_id = f"item_{abs(hash(path1))}_{abs(hash(path2))}_{similarity}" # Make ID more robust
                
                # Use checkbox characters for the "Selected" column
                checkbox_char = "☑" if path1 in self.selected_files else "☐"
                
                item_id = self.tree.insert("", "end", iid=unique_item_id,
                                            values=(checkbox_char, primary_image_basename, comparison_info_text, f"{similarity:.2f}%", "開啟"))
                self.tree.item(item_id, tags=(path1, path2, similarity))
        
        print(f"清單已根據最小相似度 {current_threshold:.1f}% 篩選。顯示 {len(self.displayed_similar_files)} 個項目。", flush=True)


    def _on_treeview_click(self, event):
        """Handles Treeview click events."""
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            return
        
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return

        column_id = self.tree.identify_column(event.x)
        
        if column_id == "#1": # "Selected" column
            self._toggle_selection_by_item_id(item_id)
        elif column_id == "#2" or column_id == "#3": # "PrimaryImage" or "ComparisonInfo" column
            self.tree.selection_set(item_id) # Ensure this item is selected for preview
            self.tree.focus(item_id)
            self._on_item_select(None) # Force update preview
            self._toggle_selection_by_item_id(item_id) # Then toggle checkbox
        elif column_id == "#5": # "OpenFolder" column
            original_path = self.tree.item(item_id, "tags")[0]
            if original_path and os.path.exists(original_path):
                folder_path = os.path.dirname(original_path)
                self._open_folder(folder_path)
            else:
                messagebox.showwarning("路徑無效", f"檔案路徑不存在或無效:\n{original_path}")
        else:
            # Default selection behavior for other clicks
            self.tree.selection_set(item_id)
            self.tree.focus(item_id)
            self._on_item_select(None) # Ensure preview updates on selection

    def _on_item_select(self, event):
        """Handles item selection in Treeview."""
        selected_items = self.tree.selection()
        if not selected_items:
            self.target_image_label.config(image="")
            self.compare_image_label.config(image="")
            self.target_path_label.config(text="")
            self.compare_path_label.config(text="")
            self.img_tk_target = None
            self.img_tk_compare = None
            return

        item_id = selected_items[0]
        path1, path2, similarity = self.tree.item(item_id, "tags")

        self._load_and_display_image(path1, self.target_image_label, self.target_path_label, is_target=True)
        if self.comparison_mode != "qr_detection" and path2 and path2 != "N/A":
            self._load_and_display_image(path2, self.compare_image_label, self.compare_path_label, is_target=False)
        else:
            self.compare_image_label.config(image="")
            self.compare_path_label.config(text="（QR Code 檢測模式下無比對圖片）" if self.comparison_mode == "qr_detection" else "（無比對圖片）")
            self.img_tk_compare = None

    def _load_and_display_image(self, image_path, label_widget, path_label_widget, is_target):
        """Loads and displays an image in the preview area."""
        try:
            self.root.update_idletasks()
            
            available_width = label_widget.winfo_width()
            available_height = label_widget.winfo_height()

            if available_width <= 1 or available_height <= 1:
                display_width, display_height = self.max_preview_size
            else:
                display_width = max(available_width - 20, 100)
                display_height = max(available_height - 20, 100)

            with Image.open(image_path) as img:
                img = ImageOps.exif_transpose(img)
                img.thumbnail((display_width, display_height), Image.Resampling.LANCZOS)
                
                img_tk = ImageTk.PhotoImage(img)

                label_widget.config(image=img_tk)
                label_widget.image = img_tk
                path_label_widget.config(text=f"路徑: {image_path}")

                if is_target:
                    self.img_tk_target = img_tk
                else:
                    self.img_tk_compare = img_tk

        except FileNotFoundError:
            label_widget.config(image="")
            path_label_widget.config(text=f"圖片文件未找到: {image_path}")
            log_error(f"圖片文件未找到: {image_path}", include_traceback=False)
        except UnidentifiedImageError:
            label_widget.config(image="")
            path_label_widget.config(text=f"圖片格式無法識別或文件已損壞: {image_path}")
            log_error(f"圖片格式無法識別或文件已損壞: {image_path}", include_traceback=False)
        except OSError as e:
            label_widget.config(image="")
            path_label_widget.config(text=f"無法載入圖片 (操作系統錯誤): {image_path}\n錯誤: {e}")
            log_error(f"打開圖片檔案時發生操作系統錯誤 '{image_path}': {e}", include_traceback=False)
        except Exception as e:
            label_widget.config(image="")
            path_label_widget.config(text=f"無法載入圖片: {image_path}\n錯誤: {e}")
            log_error(f"載入圖片 {image_path} 錯誤: {e}", include_traceback=True)

    def _on_preview_image_click(self, event, is_target_image):
        """Opens the folder of the clicked preview image."""
        if is_target_image:
            image_path_label = self.target_path_label
        else:
            image_path_label = self.compare_path_label
        
        full_path_text = image_path_label.cget("text")
        
        # Extract the actual path from the label text "路徑: <image_path>"
        if full_path_text.startswith("路徑: "):
            image_path = full_path_text[len("路徑: "):].strip()
        else:
            image_path = None

        if image_path and os.path.exists(image_path):
            folder_path = os.path.dirname(image_path)
            self._open_folder(folder_path)
        else:
            messagebox.showwarning("路徑無效", "無法開啟資料夾，圖片路徑無效或未載入。")


    def _bind_keys(self):
        """Binds keyboard navigation keys."""
        # 重新綁定上下方向鍵，並確保只移動一格
        self.tree.bind("<Up>", self._navigate_image)
        self.tree.bind("<Down>", self._navigate_image)

    def _navigate_image(self, event):
        """Navigates through the image list using arrow keys."""
        current_selection = self.tree.selection()
        if not current_selection:
            return "break" # Break if no selection

        current_item = current_selection[0]
        
        if event.keysym == "Up":
            prev_item = self.tree.prev(current_item)
            if prev_item:
                self.tree.selection_set(prev_item)
                self.tree.focus(prev_item)
                self.tree.see(prev_item)
            else: # If at the first item, keep selection on first item
                self.tree.selection_set(current_item)
                self.tree.focus(current_item)
                self.tree.see(current_item)
        elif event.keysym == "Down":
            next_item = self.tree.next(current_item)
            if next_item:
                self.tree.selection_set(next_item)
                self.tree.focus(next_item)
                self.tree.see(next_item)
            else: # If at the last item, keep selection on last item
                self.tree.selection_set(current_item)
                self.tree.focus(current_item)
                self.tree.see(current_item)
        
        # 關鍵：返回 "break" 以阻止事件進一步傳播，避免 Tkinter 默認行為的額外移動
        return "break" 


    def _toggle_selection_by_item_id(self, item_id):
        """Toggles selection for a given item ID."""
        # Unpack tags correctly
        path1, path2_tag, similarity_tag = self.tree.item(item_id, "tags")
        current_values = list(self.tree.item(item_id, "values"))
        
        if path1 in self.selected_files:
            self.selected_files.remove(path1)
            current_values[0] = "☐" # Set to unchecked box
        else:
            self.selected_files.add(path1)
            current_values[0] = "☑" # Set to checked box
        
        self.tree.item(item_id, values=current_values)

    def _toggle_selection(self, event):
        """Toggles selection for currently selected items."""
        selected_items = self.tree.selection()
        if not selected_items:
            return
        for item_id in selected_items:
            self._toggle_selection_by_item_id(item_id)

    def _select_all(self):
        """Selects all items in the Treeview."""
        for item_id in self.tree.get_children():
            # Unpack tags correctly
            path1, path2_tag, similarity_tag = self.tree.item(item_id, "tags")
            if path1 not in self.selected_files:
                self.selected_files.add(path1)
            
            current_values = list(self.tree.item(item_id, "values"))
            current_values[0] = "☑" # Set to checked box
            self.tree.item(item_id, values=current_values)
        print("已選擇所有項目。", flush=True)

    def _deselect_all(self):
        """Deselects all items in the Treeview."""
        self.selected_files.clear()
        for item_id in self.tree.get_children():
            current_values = list(self.tree.item(item_id, "values"))
            current_values[0] = "☐" # Set to unchecked box
            self.tree.item(item_id, values=current_values)
        print("已取消選擇所有項目。", flush=True)

    def _invert_selection(self):
        """Inverts the current selection."""
        all_items = self.tree.get_children()
        temp_selected_paths = set()
        for item_id in all_items:
            # Unpack tags correctly
            path1, path2_tag, similarity_tag = self.tree.item(item_id, "tags")
            current_values = list(self.tree.item(item_id, "values"))
            
            if path1 not in self.selected_files:
                temp_selected_paths.add(path1)
                current_values[0] = "☑" # Set to checked box
            else:
                current_values[0] = "☐" # Set to unchecked box
            self.tree.item(item_id, values=current_values)
        self.selected_files = temp_selected_paths
        print("已反轉選擇。", flush=True)

    def _delete_selected_from_disk(self):
        """Deletes selected files from disk."""
        if not self.selected_files:
            messagebox.showinfo("提示", "沒有選中的圖片。")
            return
        files_to_delete = list(self.selected_files)
        if not messagebox.askyesno("確認刪除", f"確定要刪除這 {len(files_to_delete)} 個選中的圖片嗎？此操作不可撤銷！"):
            return
        deleted_paths = []
        processed_path1_for_deletion = set()
        for item_id in list(self.tree.get_children()): # Iterate over currently displayed items
            path1, path2, similarity = self.tree.item(item_id, "tags")
            if path1 in self.selected_files and path1 not in processed_path1_for_deletion:
                try:
                    os.remove(path1)
                    deleted_paths.append(path1)
                    processed_path1_for_deletion.add(path1)
                    print(f"已刪除文件: {path1}", flush=True)
                except OSError as e:
                    log_error(f"刪除文件失敗 {path1}: {e}", include_traceback=True)
                    messagebox.showerror("刪除失敗", f"無法刪除文件: {path1}\n錯誤: {e}")
                except Exception as e:
                    log_error(f"刪除文件時發生意外錯誤 {path1}: {e}", include_traceback=True)
                    messagebox.showerror("刪除失敗", f"刪除文件時發生意外錯誤: {path1}\n錯誤: {e}")
        
        if deleted_paths:
            self.deleted_history.append(deleted_paths)
            
            # Update self.all_similar_files by removing deleted items
            self.all_similar_files = [item for item in self.all_similar_files if item[0] not in deleted_paths]
            
            self.selected_files.clear()
            self._populate_listbox() # Re-populate to reflect changes
            messagebox.showinfo("刪除完成", f"成功刪除 {len(deleted_paths)} 個文件。")


    def _undo_delete_gui(self, event=None):
        """Placeholder for undo delete functionality."""
        if not self.deleted_history:
            messagebox.showinfo("提示", "沒有可撤銷的刪除操作。")
            return
        messagebox.showwarning("無法撤銷實際刪除", "已刪除的文件無法從硬碟中恢復。此功能僅為示意，提醒您刪除操作的不可逆性，並清空歷史記錄。")
        self.deleted_history.pop()
        return

    def _open_folder(self, folder_path):
        """Opens a folder in the file explorer."""
        print(f"嘗試開啟資料夾: {folder_path}", flush=True) # Added debug print
        if not os.path.isdir(folder_path):
            messagebox.showwarning("路徑無效", f"資料夾不存在或無效:\n{folder_path}")
            return
        try:
            if sys.platform == "win32":
                # Use 'start' command with shell=True for better path handling, especially with spaces
                subprocess.Popen(f'start "" "{folder_path}"', shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", folder_path])
            else:
                subprocess.Popen(["xdg-open", folder_path])
            print(f"已開啟資料夾: {folder_path}", flush=True)
        except Exception as e:
            log_error(f"開啟資料夾失敗 {folder_path}: {e}", include_traceback=True)
            messagebox.showerror("開啟失敗", f"無法開啟資料夾: {folder_path}\n錯誤: {e}")

    def _open_selected_folder_single(self):
        """Opens the folder of the single currently selected (highlighted) image."""
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showinfo("提示", "請先在列表中選中一個圖片。")
            return
        
        # Take the first selected item only
        item_id = selected_items[0]
        # Unpack tags correctly
        path1, path2_tag, similarity_tag = self.tree.item(item_id, "tags")
        
        if path1 and os.path.exists(path1):
            folder_path = os.path.dirname(path1)
            self._open_folder(folder_path)
        else:
            messagebox.showwarning("路徑無效", f"選中的圖片文件路徑不存在或無效:\n{path1}")

    def _on_closing(self):
        """Handles closing the main window."""
        if messagebox.askokcancel("關閉", "確定要關閉比對結果視窗嗎？"):
            self.root.destroy()
            sys.exit(0)

def custom_excepthook(exc_type, exc_value, exc_traceback):
    """Custom global exception handler."""
    log_error(f"捕獲到未處理的錯誤: {exc_type.__name__}: {exc_value}", include_traceback=True)
    
    try:
        # Check if a Tkinter root is already defined and still exists (not destroyed)
        # tk._default_root is an internal variable, safer to use a check like below
        # or simply create a new Toplevel if the main root is not ready/visible
        root_exists = False
        try:
            if tk._default_root and tk._default_root.winfo_exists():
                root_exists = True
        except:
            pass # Ignore errors if _default_root isn't properly initialized yet

        if not root_exists:
            # Create a temporary root for the messagebox if no main root is available
            temp_root = tk.Tk()
            temp_root.withdraw() # Hide the main window
            messagebox.showerror("致命錯誤", f"程式發生未預期的錯誤並將關閉。\n錯誤類型: {exc_type.__name__}\n請檢查 'error_log.txt' 獲取詳細資訊。")
            temp_root.destroy() # Destroy the temporary root
        else:
            # Use the existing root to display the messagebox
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

    print("=== E-Download 漫畫尾頁廣告剔除 v11.0v75 - 啟動中 ===", flush=True) # Changed version to 11.0v75
    check_and_install_packages()
    print("套件檢查完成。", flush=True)
    
    root = tk.Tk()
    # Removed root.withdraw() as it was causing the SettingsGUI not to show immediately
    # print(f"DEBUG: Type of root before SettingsGUI: {type(root)}", flush=True) # Diagnostic print

    folder_creation_cache_manager = FolderCreationCacheManager()
    
    settings_gui = SettingsGUI(root, CONFIG_FILE, QR_SCAN_ENABLED)
    # The `settings_gui.settings_window.update()` and `settings_gui.settings_window.wait_window(...)`
    # combination should ensure the settings window is shown and blocks execution until closed.
    root.wait_window(settings_gui.settings_window)

    # print(f"DEBUG: Type of root after SettingsGUI closes: {type(root)}", flush=True) # Diagnostic print

    final_config_from_settings = settings_gui.result_config
    should_proceed_with_main_app = settings_gui.should_proceed
    rebuild_folder_cache_flag = settings_gui.rebuild_folder_cache_result
    rebuild_scanned_cache_flag = settings_gui.rebuild_scanned_cache_result # Corrected: used wrong variable name

    if should_proceed_with_main_app and final_config_from_settings:
        if rebuild_folder_cache_flag:
            folder_creation_cache_manager.invalidate_cache()
            folder_creation_cache_manager.save_cache()
            print("資料夾建立時間快取已清空。下次運行時將重新建立。", flush=True)

        # Initialize ScannedImageHashesCacheManager AFTER getting root_scan_folder from config
        # This ensures the cache file name is correctly generated based on the user's selection
        if final_config_from_settings['root_scan_folder']: # Ensure root_scan_folder is not empty
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

        start_date_dt = None
        end_date_dt = None
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
                comparison_mode=main_app_config['comparison_mode'], # Now directly from config
                rebuild_ad_cache=main_app_config['rebuild_ad_cache'],
                system_qr_scan_capability=QR_SCAN_ENABLED, # Pass the global capability directly
                scanned_hashes_cache_manager=scanned_hashes_cache_manager,
                enable_extract_count_limit=main_app_config['enable_extract_count_limit'] # Pass the new setting
            )

            files_to_process_dict = engine.generate_extracted_files(folder_creation_cache_manager)
            folder_creation_cache_manager.save_cache()

            ad_hashes = {}
            if engine.comparison_mode == 'ad_comparison':
                ad_hashes = load_ad_hashes(main_app_config['ad_folder_path'], main_app_config['rebuild_ad_cache'])

            similar_files = engine.compare_images(files_to_process_dict, ad_hashes)

            if similar_files:
                # print(f"DEBUG: Type of root before MainWindow creation: {type(root)}", flush=True) # Diagnostic print
                # Pass the original similarity threshold used for scanning to MainWindow
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
