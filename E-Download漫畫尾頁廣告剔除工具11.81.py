import os
import sys
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from PIL import Image, ImageTk, ImageFile
import imagehash
import json
import datetime
import hashlib # For checking file modification
import platform
import threading # For running long tasks in a separate thread
import webbrowser # For opening folders
import tkinter.font # For dynamic font width calculation

# 啟用處理損壞 JPEG 圖像的機制，防止因圖片損壞導致程序崩潰
ImageFile.LOAD_TRUNCATED_IMAGES = True

# 嘗試導入 OpenCV 和 Pyzbar for QR code scanning
# 如果無法導入，QR code 功能將被禁用，並在控制台打印警告
QR_SCAN_ENABLED = False
try:
    import cv2
    from pyzbar import pyzbar
    QR_SCAN_ENABLED = True
    print("OpenCV 和 Pyzbar 已成功導入，QR Code 掃描功能已啟用。")
except ImportError:
    print("警告：無法導入 OpenCV 或 Pyzbar。QR Code 掃描功能將被禁用。")
    print("請確保已安裝 'opencv-python' 和 'pyzbar' (pip install opencv-python pyzbar)。")
except Exception as e:
    print(f"警告：導入 OpenCV 或 Pyzbar 時發生未知錯誤: {e}。QR Code 掃描功能將被禁用。")

# === 1. 腳本資訊與初始化 ===
# -----------------------------------------------------------------------------
# 腳本版本號，便於追蹤更新
SCRIPT_VERSION = "11.81" # 版本號更新為 11.81
# 腳本名稱
SCRIPT_NAME = "E-Download漫畫尾頁廣告剔除工具"
# 版本備註
VERSION_NOTE = "新增：只掃描每個資料夾的最後N頁功能；優化：狀態訊息顯示" 

# 確保在 Windows 上使用 'spawn' 啟動方法來避免多進程閃退問題
# 這段代碼只會在主進程中執行一次，提高打包後的兼容性和穩定性
if sys.platform.startswith('win'):
    try:
        from multiprocessing import freeze_support, set_start_method
        # 為了兼容不同 Python 版本，進行版本判斷
        if platform.python_version() >= '3.8':
            # 在 Python 3.8+，可以通過 set_start_method 的返回值判斷是否已設置
            # 如果已經設置過，再次調用 with force=True 可能會拋出 RuntimeError，所以使用 try-except
            set_start_method('spawn', force=True)
            print("多進程啟動方法已設置為 'spawn'。")
        else:
            set_start_method('spawn', force=True)
            print("多進程啟程方法已設置為 'spawn'。")
        freeze_support() # 這是 Windows 上用於打包的可選，但對運行時穩定性也有幫助
    except RuntimeError:
        print("多進程啟動方法已在其他地方設置，或無法設置。")
    except Exception as e:
        print(f"設置多進程啟動方法時發生錯誤: {e}")

# 設置緩存文件目錄和各個緩存文件的路徑
# 緩存文件和配置檔將儲存在與腳本同級的 'cache' 資料夾中
CACHE_DIR = "cache"
CONFIG_FILE = os.path.join(CACHE_DIR, "config.json") # 新增配置檔路徑
FOLDER_CREATION_CACHE_FILE = os.path.join(CACHE_DIR, "folder_creation_times.json")
IMAGE_HASH_CACHE_FILE = os.path.join(CACHE_DIR, "image_hashes_cache.json")
AD_IMAGE_HASH_CACHE_FILE = os.path.join(CACHE_DIR, "ad_image_hashes.json")

# 確保緩存目錄存在，如果不存在則創建
os.makedirs(CACHE_DIR, exist_ok=True)

# === 2. 輔助函數與工具 (所有被 AppConfig 或其他類別/函式早期呼叫的函式都移動到此處) ===
# -----------------------------------------------------------------------------

def log_message(message, level="info"):
    """
    統一的日誌輸出函數。當前輸出到控制台，未來可擴展到文件日誌或GUI日誌區。
    方便調試和用戶了解程序運行狀態。
    """
    timestamp = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} [{level.upper()}] {message}")

def get_file_modification_time_and_hash(filepath):
    """
    獲取文件的修改時間作為一個簡易的哈希，用於判斷文件是否可能已更改。
    並返回格式化的修改時間字符串。
    """
    try:
        mod_timestamp = os.path.getmtime(filepath)
        mod_time_hash = str(mod_timestamp)
        # 將時間戳轉換為人類可讀的格式
        mod_time_formatted = datetime.datetime.fromtimestamp(mod_timestamp).strftime("%Y-%m-%d %H:%M")
        return mod_time_hash, mod_time_formatted
    except Exception as e:
        log_message(f"無法獲取文件 '{filepath}' 的修改時間: {e}", "error")
        return None, None

def load_json(file_path):
    """
    安全地載入 JSON 檔案。
    如果檔案不存在、為空或損壞，則返回一個空的字典或列表，並打印警告/錯誤信息。
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        log_message(f"JSON 緩存文件 '{file_path}' 不存在或為空，將返回空數據。")
        return {} # 默認返回字典，因為哈希緩存是字典
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            log_message(f"成功載入 JSON 緩存文件: {file_path}")
            return data
    except json.JSONDecodeError as e:
        log_message(f"錯誤：無法解析 JSON 緩存文件 '{file_path}'。文件可能損壞。錯誤: {e}", "error")
        return {}
    except Exception as e:
        log_message(f"載入 JSON 緩存文件 '{file_path}' 時發生未知錯誤: {e}", "error")
        return {}

def save_json(data, file_path):
    """
    安全地儲存數據到 JSON 檔案。
    確保目標目錄存在，並使用UTF-8編碼和美觀的縮進。
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True) # 確保目錄存在
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log_message(f"成功儲存數據到 JSON 緩存文件: {file_path}")
    except Exception as e:
        log_message(f"錯誤：儲存數據到 JSON 緩存文件 '{file_path}' 失敗: {e}", "error")

def save_app_config(config_obj, file_path):
    """
    將 AppConfig 物件的設定保存到 JSON 文件。
    """
    config_dict = config_obj.__dict__
    save_json(config_dict, file_path)
    log_message(f"應用程式設定已保存到: {file_path}")

def load_app_config(config_obj, file_path):
    """
    從 JSON 文件加載設定到 AppConfig 物件。
    """
    if not os.path.exists(file_path):
        log_message(f"設定文件 '{file_path}' 不存在，使用默認設定。")
        return

    loaded_config = load_json(file_path)
    if loaded_config:
        for key, value in loaded_config.items():
            if hasattr(config_obj, key): # 只更新 AppConfig 中已有的屬性
                setattr(config_obj, key, value)
        log_message(f"應用程式設定已從 '{file_path}' 加載。")

def calculate_image_hash_and_dimensions(image_path, hash_size=8):
    """
    計算圖片的感知哈希 (pHash) 並獲取圖片尺寸。
    處理圖片加載錯誤，對於無法處理的圖片返回 None。
    返回一個元組 (hash_string, dimensions_string)。
    """
    try:
        with Image.open(image_path) as img:
            # 獲取尺寸
            dimensions = f"{img.width}x{img.height}"
            # 轉換為灰度圖以獲得更穩定的哈希，並調整大小以適應哈希算法要求
            img_for_hash = img.convert("L").resize((hash_size, hash_size), Image.LANCZOS)
            img_hash = str(imagehash.phash(img_for_hash))
            return img_hash, dimensions
    except Exception as e:
        log_message(f"無法處理圖片 '{image_path}' 的哈希或尺寸: {e}", "error")
        return None, None

def get_folder_creation_time(folder_path):
    """
    獲取資料夾的建立時間。在 Windows 上是建立時間，在 Unix/Linux 上則是最後元數據變更時間。
    為了跨平台兼容性，我們在 Windows 上用 ctime，其他平台用 mtime 作為近似。
    返回 ISO 8601 格式的字符串，便於存儲和比較。
    """
    try:
        if sys.platform.startswith('win'):
            # 在 Windows 上，os.path.getctime 確實返回創建時間
            timestamp = os.path.getctime(folder_path)
        else:
            # 在 Unix/Linux 上，getctime 返回 inode 修改時間，getmtime 返回內容修改時間。
            # 對於資料夾，內容修改可能指其內部文件增刪。我們仍用 ctime 盡量模擬創建，
            # 但需注意其平台差異。若無特殊需求，用 mtime 也是常見做法。
            timestamp = os.path.getmtime(folder_path) # 使用 mtime 作為 Unix/Linux 的備用
        return datetime.datetime.fromtimestamp(timestamp).isoformat()
    except Exception as e:
        log_message(f"無法獲取資料夾 '{folder_path}' 的建立時間: {e}", "error")
        return None

def is_image_file(filename):
    """
    判斷文件是否為常見的圖片格式。
    支持多種圖片擴展名，確保能識別大多數漫畫圖片。
    """
    return filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.tiff'))

def get_image_paths_in_folder(folder_path, recursive=False):
    """
    獲取指定資料夾（可選遞歸）下所有圖片文件的絕對路徑列表。
    """
    image_paths = []
    if recursive:
        # os.walk 會遞歸遍歷所有子目錄
        for root, _, files in os.walk(folder_path):
            for file in files:
                if os.path.isfile(os.path.join(root, file)) and is_image_file(file):
                    image_paths.append(os.path.join(root, file))
    else:
        # 只遍歷當前資料夾
        if os.path.isdir(folder_path): # 確保是有效的資料夾
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                if os.path.isfile(file_path) and is_image_file(file):
                    image_paths.append(file_path)
    return image_paths

def get_filtered_image_paths_for_scanning(root_folder, scan_recursive, scan_last_n_pages):
    """
    獲取指定根資料夾下需要掃描的圖片路徑列表。
    - 如果 scan_recursive 為 True，則會遞歸掃描子資料夾。
    - 如果 scan_last_n_pages > 0，則只返回每個子資料夾（或根資料夾本身，如果非遞歸）的最後 N 張圖片。
    """
    all_image_paths_raw = []
    
    if scan_recursive:
        # 遞歸模式下，將每個最底層的漫畫資料夾視為一個獨立的單元進行「最後N頁」的篩選
        for root, dirs, files in os.walk(root_folder):
            current_folder_images = []
            for file in files:
                if is_image_file(file):
                    current_folder_images.append(os.path.join(root, file))
            
            if current_folder_images: # 如果當前資料夾有圖片
                # 對當前資料夾的圖片按文件名排序
                current_folder_images.sort(key=lambda p: os.path.basename(p))
                
                if scan_last_n_pages > 0:
                    # 只取最後 N 張圖片
                    all_image_paths_raw.extend(current_folder_images[-scan_last_n_pages:])
                else:
                    all_image_paths_raw.extend(current_folder_images)
    else:
        # 非遞歸模式，只處理根資料夾下的圖片
        current_folder_images = []
        if os.path.isdir(root_folder):
            for file in os.listdir(root_folder):
                file_path = os.path.join(root_folder, file)
                if os.path.isfile(file_path) and is_image_file(file):
                    current_folder_images.append(file_path)
        
        if current_folder_images:
            current_folder_images.sort(key=lambda p: os.path.basename(p))
            if scan_last_n_pages > 0:
                all_image_paths_raw.extend(current_folder_images[-scan_last_n_pages:])
            else:
                all_image_paths_raw.extend(current_folder_images)
    
    log_message(f"根據掃描設定（遞歸: {scan_recursive}, 最後N頁: {scan_last_n_pages}），篩選出 {len(all_image_paths_raw)} 張圖片進行掃描。")
    return all_image_paths_raw

def open_file_explorer(path):
    """
    打開文件瀏覽器並定位到指定路徑。
    根據操作系統使用不同的命令。
    """
    if not os.path.exists(path):
        messagebox.showerror("錯誤", f"路徑不存在: {path}")
        return
    
    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin": # macOS
        webbrowser.open(f'file://{path}')
    else: # Linux and other Unix-like systems
        try:
            webbrowser.open(f'file://{path}')
        except Exception:
            # Fallback for Linux if webbrowser.open doesn't work as expected
            os.system(f'xdg-open "{path}"')
    log_message(f"已嘗試打開路徑: {path}")

# === 3. JSON 緩存管理函數 (實際定義) ===
# -----------------------------------------------------------------------------

def update_folder_creation_cache(root_folder, cache_file_path):
    """
    掃描指定根資料夾下的所有直接子資料夾，更新其建立時間到 JSON 緩存。
    這個緩存用於後續按時間篩選資料夾。
    """
    log_message(f"開始更新資料夾建立時間緩存：{cache_file_path}")
    existing_cache = load_json(cache_file_path) # 載入現有緩存
    updated_cache = {}
    
    # 遍歷根資料夾下的所有項目，只處理資料夾
    if not os.path.isdir(root_folder):
        log_message(f"根資料夾 '{root_folder}' 不存在，無法更新資料夾緩存。", "error")
        return {}

    for item_name in os.listdir(root_folder):
        item_path = os.path.join(root_folder, item_name)
        if os.path.isdir(item_path):
            creation_time = get_folder_creation_time(item_path)
            if creation_time:
                # 如果緩存中已有該資料夾且建立時間未變，則直接復用舊數據
                if item_path in existing_cache and existing_cache[item_path].get("creation_time") == creation_time:
                    updated_cache[item_path] = existing_cache[item_path]
                else:
                    # 否則，更新或添加新數據
                    updated_cache[item_path] = {
                        "name": item_name,
                        "creation_time": creation_time
                    }
                    # log_message(f"更新資料夾緩存：'{item_name}' (建立時間: {creation_time})") # 暫時關閉詳細日誌，避免過多輸出
            else:
                log_message(f"警告：無法獲取資料夾 '{item_name}' 的建立時間，將跳過。", "warning")

    # 清理緩存中已不存在的資料夾路徑，保持緩存的準確性
    keys_to_remove = [p for p in existing_cache if p not in updated_cache]
    for p in keys_to_remove:
        log_message(f"從資料夾時間緩存中移除不存在的資料夾：'{os.path.basename(p)}'")

    save_json(updated_cache, cache_file_path) # 保存更新後的緩存
    log_message("資料夾建立時間緩存更新完成。")
    return updated_cache

def filter_folders_by_time(root_folder, start_date_str, end_date_str, cache_file_path):
    """
    根據用戶輸入的時間範圍和資料夾建立時間緩存來篩選資料夾。
    只返回符合條件的資料夾路徑列表。
    """
    log_message(f"開始根據時間篩選資料夾。範圍：{start_date_str} 至 {end_date_str}")
    folder_cache = load_json(cache_file_path) # 載入資料夾建立時間緩存
    filtered_folders = []

    # 解析開始和結束日期字符串
    start_date = None
    end_date = None
    try:
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        if end_date_str:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError:
        # 在 GUI 中處理錯誤，這裡只記錄
        log_message("日期格式錯誤，請使用YYYY-MM-DD 格式。", "error")
        return []

    for folder_path, info in folder_cache.items():
        if not os.path.isdir(folder_path): # 檢查資料夾是否存在於文件系統中
            # log_message(f"緩存中資料夾 '{folder_path}' 不存在，將跳過。", "warning") # 避免過多輸出
            continue

        creation_time_str = info.get("creation_time")
        if not creation_time_str:
            log_message(f"資料夾 '{folder_path}' 緩存中缺少建立時間信息，將跳過。", "warning")
            continue

        try:
            # 將 ISO 格式的建立時間字符串轉換為日期對象進行比較
            folder_date = datetime.datetime.fromisoformat(creation_time_str).date()
            
            # 判斷資料夾建立日期是否在指定範圍內
            is_after_start = True if start_date is None else folder_date >= start_date
            is_before_end = True if end_date is None else folder_date <= end_date

            if is_after_start and is_before_end:
                filtered_folders.append(folder_path)
        except ValueError:
            log_message(f"資料夾 '{folder_path}' 的建立時間格式 '{creation_time_str}' 無法解析，將跳過。", "error")
    
    log_message(f"時間篩選完成。找到 {len(filtered_folders)} 個符合條件的資料夾。")
    return filtered_folders

def update_image_hashes_cache(image_paths_to_hash, cache_file_path, hash_size):
    """
    掃描指定圖片路徑列表中的所有圖片，更新其哈希值、尺寸和修改時間到 JSON 緩存。
    支援增量更新：只計算新圖片或已修改圖片的哈希，大幅減少重複計算。
    此函數不再負責 GUI 進度更新，僅進行核心數據處理。
    """
    log_message(f"開始更新圖片哈希緩存：{cache_file_path}")
    existing_cache = load_json(cache_file_path) # 載入現有緩存
    updated_cache = {}

    total_images = len(image_paths_to_hash)
    log_message(f"圖片哈希更新：總計 {total_images} 張圖片。")

    for image_path in image_paths_to_hash:
        if not os.path.exists(image_path):
            continue

        file_mod_time_hash, file_mod_time_formatted = get_file_modification_time_and_hash(image_path) # 使用修改時間作為版本標識
        if file_mod_time_hash is None:
            continue # 如果無法獲取修改時間，則跳過

        # 檢查緩存中是否存在該圖片的路徑，並且修改時間一致
        if image_path in existing_cache and existing_cache[image_path].get("mod_time_hash") == file_mod_time_hash:
            updated_cache[image_path] = existing_cache[image_path] # 直接復用舊數據
        else:
            # 文件不存在於緩存或已修改，重新計算哈希、獲取尺寸和格式化修改時間
            image_hash, dimensions = calculate_image_hash_and_dimensions(image_path, hash_size)
            if image_hash:
                updated_cache[image_path] = {
                    "hash": image_hash,
                    "mod_time_hash": file_mod_time_hash, # 用於判斷緩存是否過期
                    "mod_time_formatted": file_mod_time_formatted, # 用於顯示
                    "dimensions": dimensions # 儲存圖片尺寸
                }
            else:
                log_message(f"警告：無法計算圖片 '{image_path}' 的哈希值，將跳過。", "warning")

    # 清理緩存中已不存在的圖片路徑，保持緩存的有效性
    keys_to_remove = [p for p in existing_cache if p not in updated_cache]
    for p in keys_to_remove:
        log_message(f"從圖片哈希緩存中移除不存在的圖片：'{os.path.basename(p)}'")

    save_json(updated_cache, cache_file_path) # 保存更新後的緩存
    log_message("圖片哈希緩存更新完成。")
    return updated_cache

def update_ad_image_hashes_cache(ad_folders, cache_file_path, hash_size):
    """
    掃描廣告資料夾中的所有圖片，更新其哈希值和尺寸到 JSON 緩存。
    這個緩存將儲存哈希到圖片路徑的映射，以便於追溯。
    此函數不再負責 GUI 進度更新，僅進行核心數據處理。
    """
    log_message(f"開始更新廣告圖片哈希緩存：{cache_file_path if cache_file_path else '（不保存文件）'}")
    
    # Ad hashes will be stored as a dictionary: {hash_string: image_path}
    ad_hashes_map = {} 
    all_ad_image_paths = []
    
    for folder in ad_folders:
        if os.path.isdir(folder):
            all_ad_image_paths.extend(get_image_paths_in_folder(folder, recursive=True))
        else:
            log_message(f"警告：廣告資料夾 '{folder}' 不存在或無法訪問。", "warning")

    if not all_ad_image_paths:
        log_message("沒有找到廣告圖片，廣告哈希緩存將為空。", "warning")
        if cache_file_path: # 如果指定了緩存文件路徑，則保存空字典
            save_json({}, cache_file_path)
        return {}

    total_images = len(all_ad_image_paths)
    log_message(f"廣告圖片哈希更新：總計 {total_images} 張圖片。")

    for image_path in all_ad_image_paths:
        ad_hash, dimensions = calculate_image_hash_and_dimensions(image_path, hash_size) # 也獲取尺寸
        if ad_hash:
            ad_hashes_map[ad_hash] = {
                "path": image_path, # 儲存哈希到路徑的映射
                "dimensions": dimensions # 儲存廣告圖的尺寸
            }
        else:
            log_message(f"警告：無法計算廣告圖片 '{image_path}' 的哈希值，將跳過。", "warning")
    
    if cache_file_path: # 如果指定了緩存文件路徑，則保存
        save_json(ad_hashes_map, cache_file_path)
    log_message(f"廣告圖片哈希緩存更新完成。共 {len(ad_hashes_map)} 個唯一廣告哈希。")
    return ad_hashes_map

# 全局配置變量類，用於在應用程序的各個模塊之間傳遞和管理用戶設定
# 這使得配置集中化，易於讀取和修改
class AppConfig:
    def __init__(self):
        # 基本路徑設定
        self.root_folder = "" # 漫畫根資料夾路徑
        self.output_folder = "" # 輸出資料夾路徑（用於移動或複製，目前主要用於邏輯上的目標）
        self.ad_image_folder = "" # 廣告圖片資料夾路徑

        # 比對參數設定
        self.compare_mode = "ad" # 比對模式："ad"(廣告比對), "self"(自身比對), "qr"(QR碼檢測)
        self.hash_size = 8 # 感知哈希計算的哈希大小
        self.threshold = 10 # 圖片相似度閾值，越小越嚴格
        self.max_delete_count = 0 # 每個資料夾最大刪除圖片數量 (0為不限制)

        # GUI 和操作行為設定
        self.preview_height = 200 # 預覽圖顯示高度，調整為更小以適應雙預覽
        self.excluded_subfolders = "" # 排除掃描的子資料夾名稱關鍵字，逗號分隔
        self.excluded_filenames = "" # 排除處理的文件名關鍵字，逗號分隔
        self.excluded_hash_values = "" # 排除特定哈希值的圖片，逗號分隔
        self.scan_recursive = False # 是否遞歸掃描子資料夾
        self.scan_last_n_pages = 0 # 新增：只掃描每個資料夾的最後 N 張圖片 (0為不限制)
        self.delete_immediately = False # 是否掃描後立即刪除 (危險選項)
        self.confirm_delete = True # 每次刪除前是否確認
        self.process_parallel = True # 是否啟用並行處理 (目前未完全實現，留作擴展)
        self.auto_open_folder = False # 處理完畢後是否自動打開輸出資料夾 (目前未完全實現)
        self.list_pane_width_ratio = 0.75 # 新增：列表面板在 Panedwindow 中的寬度佔比 (例如 0.75 表示 3:1)

        # 新增的緩存相關配置
        self.use_folder_time_cache = False # 是否啟用資料夾建立時間篩選
        self.folder_time_start_date = "" # 資料夾時間篩選的開始日期 (YYYY-MM-DD)
        self.folder_time_end_date = "" # 資料夾時間篩選的結束日期 (YYYY-MM-DD)
        self.use_image_hash_cache = False # 是否啟用所有圖片哈希緩存
        self.use_ad_hash_cache = False # 是否啟用廣告圖片哈希緩存
        
        # 在初始化時加載配置
        load_app_config(self, CONFIG_FILE)

# 創建 AppConfig 的實例，全局可用
config = AppConfig()


# === 4. 核心比對邏輯 ===
# -----------------------------------------------------------------------------

def calculate_similarity_percentage(difference, hash_size):
    """
    將哈希差異 (Hamming distance) 轉換為相似度百分比。
    """
    max_possible_difference = hash_size * hash_size
    if max_possible_difference == 0:
        return 0.0 # 避免除以零
    similarity = (1 - difference / max_possible_difference) * 100
    return round(similarity, 2) # 保留兩位小數

def find_similar_images_with_ads(all_image_data, ad_hashes_map, threshold, hash_size):
    """
    將所有待處理圖片的哈希與廣告圖片哈希進行比對。
    使用已載入的哈希數據，避免重複讀取文件，提高效率。
    參數：
    - all_image_data: 字典 {圖片路徑: {"hash": "圖片哈希字符串", "mod_time_formatted": "YYYY-MM-DD HH:MM", ...}}
    - ad_hashes_map: 字典 {廣告哈希字符串: {"path": "廣告圖片路徑", "dimensions": "WxH"}}
    - threshold: 相似度閾值
    - hash_size: 哈希大小，用於計算百分比相似度
    返回：相似圖片的路徑及相關信息列表。
    """
    log_message("開始進行廣告圖片比對。")
    similar_images = []
    
    # 將廣告哈希字典轉換為哈希對象列表以便比對，同時保留其原始路徑和尺寸信息
    ad_hash_objects = []
    for ad_hex_hash, ad_info in ad_hashes_map.items():
        try:
            ad_hash_objects.append((imagehash.hex_to_hash(ad_hex_hash), ad_info["path"], ad_info["dimensions"]))
        except ValueError as e:
            log_message(f"廣告哈希值 '{ad_hex_hash}' 無法轉換為哈希對象: {e}", "error")
            continue

    # 遍歷所有圖片數據
    for img_path, img_info in all_image_data.items():
        if img_info and img_info.get("hash"): # 確保圖片信息有效且包含哈希
            try:
                img_hash = imagehash.hex_to_hash(img_info["hash"]) # 將哈希字符串轉為哈希對象
            except ValueError as e:
                log_message(f"圖片 '{img_path}' 的哈希值 '{img_info['hash']}' 無法轉換為哈希對象: {e}", "error")
                continue # 跳過此圖片

            for ad_obj, ad_path, ad_dims in ad_hash_objects:
                difference = img_hash - ad_obj # 計算哈希差異
                if difference <= threshold:
                    similarity_percent = calculate_similarity_percentage(difference, hash_size)
                    # 找到相似圖片，記錄其路徑、修改時間、原因、哈希、相似度百分比和匹配廣告圖片路徑
                    similar_images.append({
                        'path': img_path,
                        'mod_time': img_info.get('mod_time_formatted', ''), # 新增修改時間
                        'reason': "與廣告相似", # 保留reason用於內部判斷，不在表格顯示
                        'img_hash': str(img_hash),
                        'matched_ad_hash': str(ad_obj),
                        'similarity': similarity_percent,
                        'matched_ad_path': ad_path # 儲存匹配廣告圖的實際路徑
                    })
                    break # 一旦找到一個廣告匹配，就停止比對，移到下一張圖片
    log_message(f"廣告圖片比對完成。找到 {len(similar_images)} 張相似圖片。")
    return similar_images

def find_similar_images_within_folders(all_image_data, threshold, hash_size):
    """
    在每個資料夾內尋找相似的圖片（互相比較）。
    這通常用於發現漫畫中的重複頁面或變體。
    參數：
    - all_image_data: 字典 {圖片路徑: {"hash": "圖片哈希字符串", "mod_time_formatted": "YYYY-MM-DD HH:MM", ...}}
    - threshold: 相似度閾值
    - hash_size: 哈希大小，用於計算百分比相似度
    返回：相似圖片的路徑及相關信息列表。
    """
    log_message("開始進行資料夾內圖片互相比對。")
    raw_similar_pairs = [] # 儲存所有相似對，可能包含重複
    
    # 首先按資料夾路徑將圖片數據分組以便於在每個資料夾內進行比對
    images_by_folder = {}
    for img_path, img_info in all_image_data.items():
        if img_info and img_info.get("hash"):
            folder = os.path.dirname(img_path)
            if folder not in images_by_folder:
                images_by_folder[folder] = []
            try:
                # 將哈希字符串轉為哈希對象，並儲存路徑、哈希和修改時間
                images_by_folder[folder].append({
                    'path': img_path,
                    'hash_obj': imagehash.hex_to_hash(img_info["hash"]),
                    'mod_time': img_info.get("mod_time_formatted", '')
                })
            except ValueError as e:
                log_message(f"圖片 '{img_path}' 的哈希值 '{img_info['hash']}' 無法轉換為哈希對象: {e}", "error")
                continue
    
    # 按資料夾處理，以確保「最大刪除數量」對每個資料夾單獨生效
    for folder in images_by_folder:
        img_list = images_by_folder[folder]
        # Sort images within the folder by path to ensure consistent comparison order
        img_list.sort(key=lambda x: x['path'])

        if len(img_list) < 2:
            continue # 少於兩張圖片無法進行互相比較

        # 對資料夾內的每對圖片進行比對
        for i in range(len(img_list)):
            img1 = img_list[i]
            for j in range(i + 1, len(img_list)): # 避免重複比較和與自身比較
                img2 = img_list[j]
                
                difference = img1['hash_obj'] - img2['hash_obj'] # 計算哈希差異
                if difference <= threshold:
                    similarity_percent = calculate_similarity_percentage(difference, hash_size)
                    
                    # 記錄兩個方向的相似關係，以便在列表中顯示兩張圖片
                    # 圖片1 (作為可疑圖片) 匹配 圖片2
                    raw_similar_pairs.append({
                        'path': img1['path'],
                        'mod_time': img1['mod_time'],
                        'reason': f"與 {os.path.basename(img2['path'])} 相似", # 提供比對對象的檔名
                        'img_hash': str(img1['hash_obj']),
                        'matched_ad_hash': str(img2['hash_obj']), # 這裡的 matched_ad_hash 實質是匹配的另一張本地圖的哈希
                        'similarity': similarity_percent,
                        'matched_ad_path': img2['path'] # 這裡的 matched_ad_path 實質是匹配的另一張本地圖的路徑
                    })
                    # 圖片2 (作為可疑圖片) 匹配 圖片1
                    raw_similar_pairs.append({
                        'path': img2['path'],
                        'mod_time': img2['mod_time'],
                        'reason': f"與 {os.path.basename(img1['path'])} 相似", # 提供比對對象的檔名
                        'img_hash': str(img2['hash_obj']),
                        'matched_ad_hash': str(img1['hash_obj']),
                        'similarity': similarity_percent,
                        'matched_ad_path': img1['path']
                    })
    
    # 對結果進行去重，確保每張圖片只被列出一次
    # 如果同一張圖片與多個圖片相似，只保留第一個發現的匹配對象
    unique_similar_images = {}
    for item in raw_similar_pairs:
        if item['path'] not in unique_similar_images:
            unique_similar_images[item['path']] = item
    
    final_similar_images = list(unique_similar_images.values())
    
    log_message(f"資料夾內圖片互相比對完成。找到 {len(final_similar_images)} 張相似圖片。")
    return final_similar_images

def scan_qr_codes(all_image_data):
    """
    掃描圖片中的 QR Code。
    這個函數需要實際讀取圖片文件，因為哈希值不包含 QR Code 的內容信息。
    它會遍歷所有圖片路徑，並使用 OpenCV 和 Pyzbar 進行檢測。
    參數：
    - all_image_data: 字典 {圖片路徑: {"hash": "...", "mod_time_formatted": "YYYY-MM-DD HH:MM", ...}}
    返回：包含 QR Code 的圖片路徑及相關信息列表 (列表元素為字典)。
    """
    if not QR_SCAN_ENABLED:
        log_message("QR Code 掃描功能未啟用，跳過。", "warning")
        return []

    log_message("開始進行 QR Code 掃描。")
    qr_images = []
    
    for img_path, img_info in all_image_data.items(): # 現在也獲取 img_info
        if not os.path.exists(img_path):
            # log_message(f"圖片文件 '{img_path}' 不存在，跳過 QR Code 掃描。", "warning") # 避免過多輸出
            continue
        try:
            # 使用 OpenCV 讀取圖片，並轉換為灰度圖以提高識別率
            image = cv2.imread(img_path)
            if image is None:
                log_message(f"警告：無法讀取圖片 '{img_path}' 進行 QR Code 掃描。", "warning")
                continue
            
            gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # 使用 pyzbar 掃描圖片中的條形碼和 QR Code
            decoded_objects = pyzbar.decode(gray_image)
            
            if decoded_objects:
                # 找到 QR Code，記錄圖片路徑、修改時間和原因
                qr_images.append({
                    'path': img_path,
                    'mod_time': img_info.get('mod_time_formatted', ''), # 包含圖片修改時間
                    'reason': "包含 QR Code",
                    'img_hash': '', # QR 掃描不基於哈希，所以留空
                    'matched_ad_hash': '',
                    'similarity': '',
                    'matched_ad_path': ''
                })
        except Exception as e:
            log_message(f"掃描圖片 '{img_path}' 的 QR Code 時發生錯誤: {e}", "error")
            
    log_message(f"QR Code 掃描完成。找到 {len(qr_images)} 張包含 QR Code 的圖片。")
    return qr_images

# === 5. GUI 介面設計與事件處理 ===
# -----------------------------------------------------------------------------

class SettingsWindow(tk.Toplevel):
    """
    應用程序的設定視窗，允許用戶配置各種參數，包括新增的緩存選項。
    """
    def __init__(self, master, app_config):
        super().__init__(master)
        self.master = master
        self.app_config = app_config
        self.title("設定")
        self.geometry("800x700") # 調整視窗大小以容納更多選項
        self.resizable(False, False) # 不允許用戶調整視窗大小
        self.grab_set() # 讓設定視窗獨佔，直到關閉，防止用戶操作主視窗

        self.create_widgets() # 創建所有UI控件
        self.load_settings() # 從 AppConfig 加載當前設定值

    def create_widgets(self):
        # 設置 Notebook (Tabbed interface) 實現多頁籤設定
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(padx=10, pady=10, fill="both", expand=True)

        # 創建三個主要的頁籤框架
        self.general_frame = ttk.Frame(self.notebook)
        self.cache_frame = ttk.Frame(self.notebook)
        self.advanced_frame = ttk.Frame(self.notebook)

        # 將框架添加到 Notebook 中，並設置頁籤名稱
        self.notebook.add(self.general_frame, text="一般設定")
        self.notebook.add(self.cache_frame, text="緩存設定")
        self.notebook.add(self.advanced_frame, text="高級設定")

        # 調用各自的函數來填充頁籤內容
        self.create_general_settings(self.general_frame)
        self.create_cache_settings(self.cache_frame)
        self.create_advanced_settings(self.advanced_frame)

        # 底部按鈕區域：保存設定和取消
        button_frame = ttk.Frame(self)
        button_frame.pack(pady=10)

        save_button = ttk.Button(button_frame, text="保存設定", command=self.save_settings)
        save_button.pack(side="left", padx=5)

        cancel_button = ttk.Button(button_frame, text="取消", command=self.destroy)
        cancel_button.pack(side="left", padx=5)

    def create_general_settings(self, parent_frame):
        """創建「一般設定」頁籤的 UI 控件。"""
        # 漫畫根資料夾路徑選擇
        tk.Label(parent_frame, text="漫畫根資料夾路徑:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.root_folder_entry = ttk.Entry(parent_frame, width=60)
        self.root_folder_entry.grid(row=0, column=1, padx=5, pady=5)
        ttk.Button(parent_frame, text="瀏覽", command=lambda: self.browse_folder(self.root_folder_entry)).grid(row=0, column=2, padx=5, pady=5)

        # 輸出資料夾路徑選擇
        tk.Label(parent_frame, text="輸出資料夾路徑 (選填):").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.output_folder_entry = ttk.Entry(parent_frame, width=60)
        self.output_folder_entry.grid(row=1, column=1, padx=5, pady=5)
        ttk.Button(parent_frame, text="瀏覽", command=lambda: self.browse_folder(self.output_folder_entry)).grid(row=1, column=2, padx=5, pady=5)

        # 廣告圖片資料夾路徑選擇 (新增)
        tk.Label(parent_frame, text="廣告圖片資料夾路徑 (選填):").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.ad_image_folder_entry = ttk.Entry(parent_frame, width=60)
        self.ad_image_folder_entry.grid(row=2, column=1, padx=5, pady=5)
        ttk.Button(parent_frame, text="瀏覽", command=lambda: self.browse_folder(self.ad_image_folder_entry)).grid(row=2, column=2, padx=5, pady=5)

        # 比對模式選擇 (單選按鈕)
        tk.Label(parent_frame, text="比對模式:").grid(row=3, column=0, sticky="w", padx=5, pady=5)
        self.compare_mode_var = tk.StringVar(value=self.app_config.compare_mode)
        ttk.Radiobutton(parent_frame, text="廣告比對 (單向)", variable=self.compare_mode_var, value="ad").grid(row=3, column=1, sticky="w")
        ttk.Radiobutton(parent_frame, text="互相排查 (雙向)", variable=self.compare_mode_var, value="self").grid(row=4, column=1, sticky="w")
        # QR Code 檢測按鈕的狀態會根據是否成功導入 pyzbar 和 opencv-python 而定
        ttk.Radiobutton(parent_frame, text="QR Code 檢測", variable=self.compare_mode_var, value="qr", state="normal" if QR_SCAN_ENABLED else "disabled").grid(row=5, column=1, sticky="w")

        # 哈希大小和相似度閾值輸入
        tk.Label(parent_frame, text="哈希大小 (數字，建議8或16):").grid(row=6, column=0, sticky="w", padx=5, pady=5)
        self.hash_size_entry = ttk.Entry(parent_frame, width=10)
        self.hash_size_entry.grid(row=6, column=1, sticky="w", padx=5, pady=5)

        tk.Label(parent_frame, text="相似度閾值 (數字，越小越嚴格):").grid(row=7, column=0, sticky="w", padx=5, pady=5)
        self.threshold_entry = ttk.Entry(parent_frame, width=10)
        self.threshold_entry.grid(row=7, column=1, sticky="w", padx=5, pady=5)

        # 最大刪除數量和預覽圖高度輸入
        tk.Label(parent_frame, text="每個資料夾最大刪除數量 (0為不限制):").grid(row=8, column=0, sticky="w", padx=5, pady=5)
        self.max_delete_count_entry = ttk.Entry(parent_frame, width=10)
        self.max_delete_count_entry.grid(row=8, column=1, sticky="w", padx=5, pady=5)

        tk.Label(parent_frame, text="預覽圖高度 (px，雙圖預覽時為單圖高度):").grid(row=9, column=0, sticky="w", padx=5, pady=5)
        self.preview_height_entry = ttk.Entry(parent_frame, width=10)
        self.preview_height_entry.grid(row=9, column=1, sticky="w", padx=5, pady=5)
        
        # 列表面板寬度比例
        tk.Label(parent_frame, text="列表面板寬度佔比 (0.0 - 1.0, 例如 0.75 為 3:1):").grid(row=10, column=0, sticky="w", padx=5, pady=5)
        self.list_pane_width_ratio_entry = ttk.Entry(parent_frame, width=10)
        self.list_pane_width_ratio_entry.grid(row=10, column=1, sticky="w", padx=5, pady=5)

        # 新增：只掃描最後 N 頁
        tk.Label(parent_frame, text="只掃描每個資料夾的最後N頁 (0為不限制):").grid(row=11, column=0, sticky="w", padx=5, pady=5)
        self.scan_last_n_pages_entry = ttk.Entry(parent_frame, width=10)
        self.scan_last_n_pages_entry.grid(row=11, column=1, sticky="w", padx=5, pady=5)


    def create_cache_settings(self, parent_frame):
        """創建「緩存設定」頁籤的 UI 控件，包含新添加的緩存功能。"""
        # 資料夾建立時間緩存及篩選
        self.use_folder_time_cache_var = tk.BooleanVar(value=self.app_config.use_folder_time_cache)
        ttk.Checkbutton(parent_frame, text="啟用資料夾建立時間篩選", variable=self.use_folder_time_cache_var, command=self.toggle_time_filter_widgets).grid(row=0, column=0, sticky="w", padx=5, pady=5, columnspan=2)
        
        # 日期篩選輸入框的容器框架
        self.time_filter_frame = ttk.Frame(parent_frame)
        self.time_filter_frame.grid(row=1, column=0, columnspan=3, sticky="ew", padx=10, pady=5)

        tk.Label(self.time_filter_frame, text="開始日期 (YYYY-MM-DD):").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        self.folder_time_start_date_entry = ttk.Entry(self.time_filter_frame, width=20)
        self.folder_time_start_date_entry.grid(row=0, column=1, sticky="w", padx=5, pady=2)
        tk.Label(self.time_filter_frame, text="結束日期 (YYYY-MM-DD):").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        self.folder_time_end_date_entry = ttk.Entry(self.time_filter_frame, width=20)
        self.folder_time_end_date_entry.grid(row=1, column=1, sticky="w", padx=5, pady=2)
        
        ttk.Button(parent_frame, text="更新資料夾時間緩存", command=self.update_folder_cache_gui).grid(row=2, column=0, sticky="w", padx=5, pady=10, columnspan=3)

        # 圖片哈希緩存 (總圖庫)
        self.use_image_hash_cache_var = tk.BooleanVar(value=self.app_config.use_image_hash_cache)
        ttk.Checkbutton(parent_frame, text="啟用圖片哈希緩存 (大幅提升效率)", variable=self.use_image_hash_cache_var).grid(row=3, column=0, sticky="w", padx=5, pady=5, columnspan=2)
        ttk.Button(parent_frame, text="更新圖片庫哈希緩存", command=self.update_image_hash_cache_gui).grid(row=4, column=0, sticky="w", padx=5, pady=10, columnspan=3)

        # 廣告圖片哈希緩存
        self.use_ad_hash_cache_var = tk.BooleanVar(value=self.app_config.use_ad_hash_cache)
        ttk.Checkbutton(parent_frame, text="啟用廣告圖片哈希緩存", variable=self.use_ad_hash_cache_var).grid(row=5, column=0, sticky="w", padx=5, pady=5, columnspan=2)
        ttk.Button(parent_frame, text="更新廣告庫哈希緩存", command=self.update_ad_hash_cache_gui).grid(row=6, column=0, sticky="w", padx=5, pady=10, columnspan=3)

        self.toggle_time_filter_widgets() # 根據初始設定值設置日期輸入框的狀態

    def create_advanced_settings(self, parent_frame):
        """創建「高級設定」頁籤的 UI 控件。"""
        # 排除設定 (排除子資料夾、文件名、特定哈希值)
        tk.Label(parent_frame, text="排除子資料夾 (多個用逗號分隔):").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.excluded_subfolders_entry = ttk.Entry(parent_frame, width=60)
        self.excluded_subfolders_entry.grid(row=0, column=1, padx=5, pady=5, columnspan=2)

        tk.Label(parent_frame, text="排除文件名關鍵字 (多個用逗號分隔):").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.excluded_filenames_entry = ttk.Entry(parent_frame, width=60)
        self.excluded_filenames_entry.grid(row=1, column=1, padx=5, pady=5, columnspan=2)

        tk.Label(parent_frame, text="排除特定哈希值 (多個用逗號分隔):").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.excluded_hash_values_entry = ttk.Entry(parent_frame, width=60)
        self.excluded_hash_values_entry.grid(row=2, column=1, padx=5, pady=5, columnspan=2)

        # 其他行為選項 (遞歸掃描、立即刪除、刪除確認、並行處理、自動打開資料夾)
        self.scan_recursive_var = tk.BooleanVar(value=self.app_config.scan_recursive)
        ttk.Checkbutton(parent_frame, text="遞歸掃描子資料夾 (深度掃描)", variable=self.scan_recursive_var).grid(row=3, column=0, sticky="w", padx=5, pady=5, columnspan=3)

        self.delete_immediately_var = tk.BooleanVar(value=self.app_config.delete_immediately)
        ttk.Checkbutton(parent_frame, text="掃描後立即刪除 (危險，不建議勾選)", variable=self.delete_immediately_var).grid(row=4, column=0, sticky="w", padx=5, pady=5, columnspan=3)

        self.confirm_delete_var = tk.BooleanVar(value=self.app_config.confirm_delete)
        ttk.Checkbutton(parent_frame, text="每次刪除前確認", variable=self.confirm_delete_var).grid(row=5, column=0, sticky="w", padx=5, pady=5, columnspan=3)

        self.process_parallel_var = tk.BooleanVar(value=self.app_config.process_parallel)
        ttk.Checkbutton(parent_frame, text="啟用並行處理 (提升速度，可能增加內存消耗)", variable=self.process_parallel_var).grid(row=6, column=0, sticky="w", padx=5, pady=5, columnspan=3)
        
        self.auto_open_folder_var = tk.BooleanVar(value=self.app_config.auto_open_folder)
        ttk.Checkbutton(parent_frame, text="處理完畢後自動打開輸出資料夾", variable=self.auto_open_folder_var).grid(row=7, column=0, sticky="w", padx=5, pady=5, columnspan=3)

    def toggle_time_filter_widgets(self):
        """
        根據「啟用資料夾建立時間篩選」勾選框的狀態，啟用或禁用日期輸入框。
        提供更好的用戶體驗，避免在功能未啟用時誤操作。
        """
        state = "normal" if self.use_folder_time_cache_var.get() else "disabled"
        for child in self.time_filter_frame.winfo_children():
            child.configure(state=state)

    def browse_folder(self, entry_widget):
        """
        打開文件對話框，讓用戶選擇資料夾，並將選定的路徑填充到指定的 Entry 控件中。
        """
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            entry_widget.delete(0, tk.END) # 清空現有內容
            entry_widget.insert(0, folder_selected) # 插入新路徑

    def load_settings(self):
        """
        從 AppConfig 對象中讀取當前設定值，並填充到設定視窗的各個 UI 控件中。
        """
        self.root_folder_entry.insert(0, self.app_config.root_folder)
        self.output_folder_entry.insert(0, self.app_config.output_folder)
        self.ad_image_folder_entry.insert(0, self.app_config.ad_image_folder)
        self.compare_mode_var.set(self.app_config.compare_mode)
        self.hash_size_entry.insert(0, str(self.app_config.hash_size))
        self.threshold_entry.insert(0, str(self.app_config.threshold))
        self.max_delete_count_entry.insert(0, str(self.app_config.max_delete_count))
        self.preview_height_entry.insert(0, str(self.app_config.preview_height))
        self.excluded_subfolders_entry.insert(0, self.app_config.excluded_subfolders)
        self.excluded_filenames_entry.insert(0, self.app_config.excluded_filenames)
        self.excluded_hash_values_entry.insert(0, self.app_config.excluded_hash_values)
        self.scan_recursive_var.set(self.app_config.scan_recursive)
        self.delete_immediately_var.set(self.app_config.delete_immediately)
        self.confirm_delete_var.set(self.app_config.confirm_delete)
        self.process_parallel_var.set(self.app_config.process_parallel)
        self.auto_open_folder_var.set(self.app_config.auto_open_folder)
        self.list_pane_width_ratio_entry.insert(0, str(self.app_config.list_pane_width_ratio)) # 載入寬度比例
        self.scan_last_n_pages_entry.insert(0, str(self.app_config.scan_last_n_pages)) # 載入掃描最後N頁設定

        # 載入緩存相關設置
        self.use_folder_time_cache_var.set(self.app_config.use_folder_time_cache)
        self.folder_time_start_date_entry.insert(0, self.app_config.folder_time_start_date)
        self.folder_time_end_date_entry.insert(0, self.app_config.folder_time_end_date)
        self.use_image_hash_cache_var.set(self.app_config.use_image_hash_cache)
        self.use_ad_hash_cache_var.set(self.app_config.use_ad_hash_cache)
        self.toggle_time_filter_widgets() # 確保載入後，日期輸入框的啟用狀態正確

    def save_settings(self):
        """
        將設定視窗中用戶輸入的值保存回 AppConfig 對象。
        執行簡單的輸入驗證，並提示用戶保存成功或失敗。
        同時保存到配置文件。
        """
        try:
            self.app_config.root_folder = self.root_folder_entry.get()
            self.app_config.output_folder = self.output_folder_entry.get()
            self.app_config.ad_image_folder = self.ad_image_folder_entry.get()
            self.app_config.compare_mode = self.compare_mode_var.get()
            self.app_config.hash_size = int(self.hash_size_entry.get())
            self.app_config.threshold = int(self.threshold_entry.get())
            self.app_config.max_delete_count = int(self.max_delete_count_entry.get())
            self.app_config.preview_height = int(self.preview_height_entry.get())
            self.app_config.excluded_subfolders = self.excluded_subfolders_entry.get()
            self.app_config.excluded_filenames = self.excluded_filenames_entry.get()
            self.app_config.excluded_hash_values = self.excluded_hash_values_entry.get()
            self.app_config.scan_recursive = self.scan_recursive_var.get()
            self.app_config.scan_last_n_pages = int(self.scan_last_n_pages_entry.get()) # 保存新增的N頁掃描設定
            self.app_config.delete_immediately = self.delete_immediately_var.get()
            self.app_config.confirm_delete = self.confirm_delete_var.get()
            self.app_config.process_parallel = self.process_parallel_var.get()
            self.app_config.auto_open_folder = self.auto_open_folder_var.get()
            
            # 驗證並保存寬度比例
            ratio_str = self.list_pane_width_ratio_entry.get()
            ratio = float(ratio_str)
            if not (0.1 <= ratio <= 0.9): # 限制比例在合理範圍
                raise ValueError("列表面板寬度佔比必須在 0.1 到 0.9 之間。")
            self.app_config.list_pane_width_ratio = ratio

            # 緩存相關設置的保存
            self.app_config.use_folder_time_cache = self.use_folder_time_cache_var.get()
            self.app_config.folder_time_start_date = self.folder_time_start_date_entry.get()
            self.app_config.folder_time_end_date = self.folder_time_end_date_entry.get()
            self.app_config.use_image_hash_cache = self.use_image_hash_cache_var.get()
            self.app_config.use_ad_hash_cache = self.use_ad_hash_cache_var.get()

            save_app_config(self.app_config, CONFIG_FILE) # 保存設定到文件

            messagebox.showinfo("設定", "設定已保存！")
            self.destroy() # 關閉設定視窗
        except ValueError as e:
            messagebox.showerror("輸入錯誤", f"請檢查您的輸入，確保數字欄位為有效數字: {e}")
        except Exception as e:
            messagebox.showerror("保存失敗", f"保存設定時發生錯誤: {e}")

    def update_folder_cache_gui(self):
        """
        GUI 事件處理函數：用戶點擊「更新資料夾時間緩存」按鈕時調用。
        此操作會在新線程中運行。
        """
        root_folder = self.root_folder_entry.get()
        if not os.path.isdir(root_folder):
            messagebox.showerror("錯誤", "請先選擇有效的漫畫根資料夾路徑！")
            return
        
        # 在新線程中執行緩存更新
        def run_update():
            try:
                self.master.update_status("正在更新資料夾建立時間緩存...")
                update_folder_creation_cache(root_folder, FOLDER_CREATION_CACHE_FILE)
                self.master.after(0, lambda: messagebox.showinfo("緩存更新", "資料夾建立時間緩存已更新完成！"))
                self.master.update_status("準備就緒...")
            except Exception as e:
                self.master.after(0, lambda: messagebox.showerror("錯誤", f"更新資料夾緩存失敗: {e}"))
                log_message(f"更新資料夾緩存 GUI 觸發失敗: {e}", "error")
                self.master.update_status("更新失敗。")

        threading.Thread(target=run_update).start()


    def update_image_hash_cache_gui(self):
        """
        GUI 事件處理函數：用戶點擊「更新圖片庫哈希緩存」按鈕時調用。
        此操作會在新線程中運行。
        """
        root_folder = self.root_folder_entry.get()
        if not os.path.isdir(root_folder):
            messagebox.showerror("錯誤", "請先選擇有效的漫畫根資料夾路徑！")
            return
        
        # 在新線程中執行緩存更新
        def run_update():
            try:
                self.master.update_status("正在獲取資料夾列表和圖片路徑...")
                
                # 這裡需要獲取所有符合篩選條件（包括scan_last_n_pages）的圖片路徑
                image_paths_to_hash = []
                if self.use_folder_time_cache_var.get():
                    start_date = self.folder_time_start_date_entry.get()
                    end_date = self.folder_time_end_date_entry.get()
                    folders_to_scan = filter_folders_by_time(root_folder, start_date, end_date, FOLDER_CREATION_CACHE_FILE)
                    
                    if not folders_to_scan:
                        self.master.after(0, lambda: messagebox.showinfo("提示", "根據時間篩選，沒有找到符合條件的資料夾進行哈希更新。"))
                        self.master.update_status("沒有找到資料夾。")
                        return
                    
                    for folder in folders_to_scan:
                        image_paths_to_hash.extend(
                            get_filtered_image_paths_for_scanning(folder, self.scan_recursive_var.get(), int(self.scan_last_n_pages_entry.get()))
                        )
                else:
                    # 如果不使用資料夾時間緩存，則直接從根目錄獲取所有圖片
                    image_paths_to_hash = get_filtered_image_paths_for_scanning(
                        root_folder, self.scan_recursive_var.get(), int(self.scan_last_n_pages_entry.get())
                    )

                if not image_paths_to_hash:
                    self.master.after(0, lambda: messagebox.showinfo("提示", "沒有找到任何圖片進行哈希更新。"))
                    self.master.update_status("沒有找到圖片。")
                    return

                current_hash_size = int(self.hash_size_entry.get())
                self.master.update_status("正在更新圖片庫哈希緩存 - 計算中...") # 統一為「計算中」
                # 這裡不再傳遞 progress_callback 給 update_image_hashes_cache
                update_image_hashes_cache(image_paths_to_hash, IMAGE_HASH_CACHE_FILE, current_hash_size)
                self.master.after(0, lambda: messagebox.showinfo("緩存更新", "圖片哈希緩存已更新完成！"))
                self.master.update_status("準備就緒...")
            except ValueError:
                self.master.after(0, lambda: messagebox.showerror("錯誤", "哈希大小或掃描頁數必須是有效數字。"))
                self.master.update_status("更新失敗。")
            except Exception as e:
                self.master.after(0, lambda: messagebox.showerror("錯誤", f"更新圖片庫緩存失敗: {e}"))
                log_message(f"更新圖片庫緩存 GUI 觸發失敗: {e}", "error")
                self.master.update_status("更新失敗。")

        threading.Thread(target=run_update).start()


    def update_ad_hash_cache_gui(self):
        """
        GUI 事件處理函數：用戶點擊「更新廣告庫哈希緩存」按鈕時調用。
        此操作會在新線程中運行。
        """
        ad_folder = self.ad_image_folder_entry.get()
        if not ad_folder or not os.path.isdir(ad_folder):
            messagebox.showerror("錯誤", "請先選擇有效的廣告圖片資料夾路徑！")
            return
        
        # 在新線程中執行緩存更新
        def run_update():
            try:
                current_hash_size = int(self.hash_size_entry.get())
                self.master.update_status("正在更新廣告圖片哈希緩存 - 計算中...") # 統一為「計算中」
                # 這裡不再傳遞 progress_callback 給 update_ad_image_hashes_cache
                update_ad_image_hashes_cache([ad_folder], AD_IMAGE_HASH_CACHE_FILE, current_hash_size)
                self.master.after(0, lambda: messagebox.showinfo("緩存更新", "廣告圖片哈希緩存已更新完成！"))
                self.master.update_status("準備就緒...")
            except ValueError:
                self.master.after(0, lambda: messagebox.showerror("錯誤", "哈希大小必須是有效數字。"))
                self.master.update_status("更新失敗。")
            except Exception as e:
                self.master.after(0, lambda: messagebox.showerror("錯誤", f"更新廣告緩存失敗: {e}"))
                log_message(f"更新廣告緩存 GUI 觸發失敗: {e}", "error")
                self.master.update_status("更新失敗。")

        threading.Thread(target=run_update).start()


class MainApplication(tk.Tk):
    """
    主應用程式視窗，負責顯示結果列表、圖片預覽和主要操作按鈕。
    """
    def __init__(self, app_config):
        super().__init__()
        self.app_config = app_config
        self.title(f"{SCRIPT_NAME} v{SCRIPT_VERSION} ({VERSION_NOTE})")
        self.geometry("1200x800") # 設置主視窗初始大小

        self.found_images_data = [] # 儲存所有找到的相似圖片及其信息 (列表，每個元素為字典)
        self.current_comic_preview_path = None # 當前預覽的漫畫圖片路徑
        self.current_ad_preview_path = None # 當前預覽的廣告圖片路徑

        # 保持 ImageTk.PhotoImage 的引用，防止被垃圾回收
        self.comic_tk_image = None 
        self.ad_tk_image = None

        # 用於 Treeview 勾選框的字符
        self.checked_char = "☑"
        self.unchecked_char = "☐"
        self.partial_char = "☒" # 新增：部分選中狀態的字符
        # 用於儲存 Treeview 每個圖片項目的勾選狀態 {圖片路徑: True/False}
        self.checkbox_state = {} 
        # 用於儲存資料夾節點到其子圖片項目的映射 {folder_iid: [image_iid1, image_iid2, ...]}
        self.folder_to_image_iids = {} 

        self.create_widgets() # 創建主視窗的所有 UI 控件
        self.protocol("WM_DELETE_WINDOW", self.on_closing) # 設置視窗關閉時的處理函數

    def calculate_datetime_width(self):
        """
        計算修改時間字符串的實際顯示寬度，以適應不同操作系統和字體。
        """
        sample_datetime = "2024-12-31 23:59:59"  # 典型或最長可能的時間格式
        # 獲取 Treeview 使用的默認字體
        # 注意：這可能需要先創建 Treeview 或在 after 函數中調用，以確保字體已初始化
        font_obj = tkinter.font.nametofont("TkDefaultFont")
        text_width = font_obj.measure(sample_datetime)
        # 添加一些額外空間作為 padding，並確保最小寬度
        return max(text_width + 20, 140) 

    def create_widgets(self):
        # 頂部控制面板，包含「開始執行」和「設定」按鈕，以及狀態顯示
        top_frame = ttk.Frame(self)
        top_frame.pack(side="top", fill="x", padx=10, pady=10)

        self.start_button = ttk.Button(top_frame, text="開始執行", command=self.start_execution_threaded)
        self.start_button.pack(side="left", padx=5)

        settings_button = ttk.Button(top_frame, text="設定", command=self.open_settings)
        settings_button.pack(side="left", padx=5)

        self.status_label = ttk.Label(top_frame, text="準備就緒...")
        self.status_label.pack(side="right", padx=5)

        # 主內容區：使用 ttk.Panedwindow 分割，左側顯示結果列表，右側顯示圖片預覽
        self.main_pane = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        self.main_pane.pack(fill="both", expand=True, padx=10, pady=10)

        # 左側：結果列表 (Treeview)
        list_frame = ttk.Frame(self.main_pane)
        self.main_pane.add(list_frame, weight=3) # 左側列表佔 3 份寬度

        # Treeview 設置列標題和顯示的列
        self.tree = ttk.Treeview(list_frame, columns=("Filename", "Path", "ModificationTime", "Similarity"), show="tree headings")
        
        # 設置列標題
        self.tree.heading("#0", text="選擇") # 默認第一列的標題
        self.tree.heading("Filename", text="文件名")
        self.tree.heading("Path", text="路徑") # 新增路徑欄位
        self.tree.heading("ModificationTime", text="修改時間") # 將「原因」替換為「修改時間」
        self.tree.heading("Similarity", text="相似度") 
        
        # 動態計算「修改時間」欄位寬度
        datetime_width = self.calculate_datetime_width()

        # 設置列寬
        self.tree.column("#0", width=100, minwidth=80, stretch=tk.NO) # 固定勾選框列寬，調整寬度
        self.tree.column("Filename", width=120, minwidth=100, stretch=tk.YES) # 可自適應寬度
        self.tree.column("Path", width=350, minwidth=250, stretch=tk.YES) # 路徑欄位寬度，現在也自適應
        self.tree.column("ModificationTime", width=datetime_width, minwidth=datetime_width, stretch=tk.NO) # 修改時間欄位動態寬度
        self.tree.column("Similarity", width=80, minwidth=60, stretch=tk.NO) # 相似度欄位寬度

        self.tree.pack(fill="both", expand=True, padx=5, pady=5)

        # Treeview 滾動條
        scrollbar = ttk.Scrollbar(self.tree, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")

        # 綁定 Treeview 的選擇事件、鍵盤刪除事件和右鍵菜單事件
        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)
        self.tree.bind("<Delete>", self.delete_selected_images) # 綁定 Delete 鍵進行刪除
        self.tree.bind("<Button-3>", self.show_context_menu) # 右鍵菜單
        
        # 綁定單擊事件，用於處理勾選框的狀態和資料夾全選/取消全選
        self.tree.bind("<Button-1>", self.on_tree_click) 
        # 綁定雙擊事件，用於開啟資料夾或收合/展開資料夾
        self.tree.bind("<Double-1>", self.on_tree_double_click)

        # 右側：圖片預覽區
        preview_panel_frame = ttk.Frame(self.main_pane)
        self.main_pane.add(preview_panel_frame, weight=1) # 右側預覽區佔 1 份寬度

        # 漫畫圖片預覽區
        tk.Label(preview_panel_frame, text="漫畫頁預覽:").pack(fill="x", padx=5, pady=2)
        self.comic_preview_canvas = tk.Canvas(preview_panel_frame, bg="lightgray", height=self.app_config.preview_height)
        self.comic_preview_canvas.pack(fill="x", padx=5, pady=5)
        self.comic_path_label = ttk.Label(preview_panel_frame, text="漫畫圖片路徑:")
        self.comic_path_label.pack(fill="x", padx=5, pady=2)
        # 綁定漫畫預覽 Canvas 的點擊事件
        self.comic_preview_canvas.bind("<Button-1>", self.on_comic_preview_click)

        # 廣告圖片預覽區 (僅在有匹配廣告圖時顯示)
        tk.Label(preview_panel_frame, text="匹配廣告圖預覽:").pack(fill="x", padx=5, pady=2)
        self.ad_preview_canvas = tk.Canvas(preview_panel_frame, bg="lightgray", height=self.app_config.preview_height)
        self.ad_preview_canvas.pack(fill="x", padx=5, pady=5)
        self.ad_path_label = ttk.Label(preview_panel_frame, text="廣告圖片路徑:")
        self.ad_path_label.pack(fill="x", padx=5, pady=2)
        # 綁定廣告預覽 Canvas 的點擊事件
        self.ad_preview_canvas.bind("<Button-1>", self.on_ad_preview_click)

        # 在所有組件佈局完成後設定 Panedwindow 的初始 sash 位置
        # 這裡使用 after(100) 稍微延遲，確保視窗和組件有足夠的時間完成渲染
        self.after(100, self.set_initial_sash_position)

        # 底部操作按鈕：刪除選中、刪除所有、全選、取消全選
        bottom_button_frame = ttk.Frame(self)
        bottom_button_frame.pack(side="bottom", fill="x", padx=10, pady=10)

        delete_button = ttk.Button(bottom_button_frame, text="刪除選中圖片", command=self.delete_selected_images)
        delete_button.pack(side="left", padx=5)
        
        delete_all_button = ttk.Button(bottom_button_frame, text="刪除所有結果", command=self.delete_all_images_confirmation)
        delete_all_button.pack(side="left", padx=5)

        select_all_button = ttk.Button(bottom_button_frame, text="全選", command=self.select_all_images)
        select_all_button.pack(side="right", padx=5)

        deselect_all_button = ttk.Button(bottom_button_frame, text="取消全選", command=self.deselect_all_images)
        deselect_all_button.pack(side="right", padx=5)

    def set_initial_sash_position(self):
        """
        設定 Panedwindow 的初始分割條位置，以實現比例佈局。
        """
        # 確保 Panedwindow 已有寬度
        self.main_pane.update_idletasks() 
        current_width = self.main_pane.winfo_width()
        
        if current_width > 0:
            # 計算左側面板的期望寬度
            sash_position = int(current_width * self.app_config.list_pane_width_ratio)
            # 設定 sash 的位置
            self.main_pane.sashpos(0, sash_position)
            log_message(f"Panedwindow 初始 sash 位置設定為 {sash_position}px (比例: {self.app_config.list_pane_width_ratio})")
        else:
            log_message("警告：Panedwindow 尚未渲染，無法設定初始 sash 位置。", "warning")


    def open_settings(self):
        """打開設定視窗。"""
        SettingsWindow(self, self.app_config)

    def on_closing(self):
        """處理應用程式關閉事件，提示用戶確認退出。"""
        if messagebox.askokcancel("退出", "確定要退出應用程式嗎？"):
            self.destroy() # 銷毀主視窗
            sys.exit() # 確保所有進程都終止，特別是對於多進程應用

    def update_status(self, message):
        """更新狀態欄消息並強制 GUI 刷新，以便用戶實時看到進度。"""
        self.status_label.config(text=message)
        self.update_idletasks()

    def start_execution_threaded(self):
        """
        在單獨的線程中啟動核心執行邏輯，防止 GUI 凍結。
        """
        self.start_button.config(state="disabled") # 執行期間禁用按鈕，防止重複點擊
        self.update_status("正在準備，請稍候...")
        
        # 清空之前的結果顯示
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.found_images_data = [] # 清空內部數據列表
        self.checkbox_state = {} # 清空勾選狀態
        self.folder_to_image_iids = {} # 清空資料夾映射
        self.clear_previews() # 清空所有預覽

        # 創建並啟動新線程
        thread = threading.Thread(target=self._run_analysis_logic)
        thread.daemon = True # 將線程設置為守護線程，隨主程序退出而退出
        thread.start()

    def _run_analysis_logic(self):
        """
        在單獨線程中運行的核心分析邏輯。
        所有 GUI 更新必須通過 `self.after()` 安排到主線程。
        """
        try:
            root_folder = self.app_config.root_folder
            if not os.path.isdir(root_folder):
                self.after(0, lambda: messagebox.showerror("錯誤", "請在設定中選擇有效的漫畫根資料夾路徑！"))
                return

            ad_image_folder = self.app_config.ad_image_folder
            if self.app_config.compare_mode == "ad" and (not ad_image_folder or not os.path.isdir(ad_image_folder)):
                self.after(0, lambda: messagebox.showerror("錯誤", "在廣告比對模式下，請在設定中選擇有效的廣告圖片資料夾路徑！"))
                return

            # --- 1. 處理資料夾時間緩存和篩選 ---
            # 如果啟用了資料夾時間緩存，則先篩選資料夾列表
            folders_to_scan_base = []
            if self.app_config.use_folder_time_cache:
                self.after(0, lambda: self.update_status("正在更新資料夾建立時間緩存..."))
                update_folder_creation_cache(root_folder, FOLDER_CREATION_CACHE_FILE)
                
                self.after(0, lambda: self.update_status("正在根據時間篩選資料夾..."))
                folders_to_scan_base = filter_folders_by_time(
                    root_folder,
                    self.app_config.folder_time_start_date,
                    self.app_config.folder_time_end_date,
                    FOLDER_CREATION_CACHE_FILE
                )
                if not folders_to_scan_base:
                    self.after(0, lambda: messagebox.showinfo("提示", "根據設定的時間範圍，沒有找到符合條件的資料夾。"))
                    self.after(0, lambda: self.update_status("沒有找到符合條件的資料夾。"))
                    return
                log_message(f"篩選後將處理 {len(folders_to_scan_base)} 個資料夾。")
            else:
                log_message("不使用資料夾時間篩選。")
                # 如果不使用資料夾時間緩存，則獲取根目錄下所有直接子資料夾 (或根目錄本身)
                if self.app_config.scan_recursive:
                    # 如果是遞歸掃描，則直接從 root_folder 開始遍歷
                    folders_to_scan_base = [root_folder] 
                else:
                    # 如果非遞歸掃描，只檢查 root_folder 本身
                    if os.path.isdir(root_folder):
                        folders_to_scan_base = [root_folder]
                    else:
                        self.after(0, lambda: messagebox.showinfo("提示", "指定的根資料夾無效。"))
                        self.after(0, lambda: self.update_status("無效的根資料夾。"))
                        return


            # --- 2. 獲取最終的圖片路徑列表 (應用 scan_last_n_pages 和 scan_recursive) ---
            all_image_paths = []
            for folder in folders_to_scan_base:
                all_image_paths.extend(
                    get_filtered_image_paths_for_scanning(folder, self.app_config.scan_recursive, self.app_config.scan_last_n_pages)
                )

            if not all_image_paths:
                self.after(0, lambda: messagebox.showinfo("提示", "根據設定的掃描範圍，沒有找到任何圖片進行處理。"))
                self.after(0, lambda: self.update_status("沒有找到圖片。"))
                return
            log_message(f"最終確定了 {len(all_image_paths)} 張圖片進行處理。")


            # --- 3. 處理圖片哈希緩存 (所有圖片) ---
            all_image_data = {} # 儲存所有待處理圖片的哈希數據 {圖片路徑: {"hash": "...", "mod_time_hash": "...", "mod_time_formatted": "...", "dimensions": "WxH"}}
            if self.app_config.use_image_hash_cache:
                self.after(0, lambda: self.update_status("正在載入圖片哈希..."))
                # 不再傳遞 progress_callback
                all_image_data = update_image_hashes_cache(
                    all_image_paths, IMAGE_HASH_CACHE_FILE, self.app_config.hash_size
                )
                if not all_image_data:
                    self.after(0, lambda: messagebox.showinfo("提示", "沒有找到任何圖片或圖片緩存為空。"))
                    self.after(0, lambda: self.update_status("沒有找到圖片。"))
                    return
                log_message(f"從緩存中載入 {len(all_image_data)} 張圖片的哈希數據。")
            else:
                self.after(0, lambda: self.update_status("正在計算圖片哈希..."))
                
                # 實時計算哈希，不再逐幀更新狀態，只在整體階段開始時設定一次狀態
                for img_path in all_image_paths:
                    img_hash, dimensions = calculate_image_hash_and_dimensions(img_path, self.app_config.hash_size)
                    file_mod_time_hash, file_mod_time_formatted = get_file_modification_time_and_hash(img_path)
                    if img_hash and file_mod_time_hash:
                        all_image_data[img_path] = {
                            "hash": img_hash, 
                            "mod_time_hash": file_mod_time_hash,
                            "mod_time_formatted": file_mod_time_formatted,
                            "dimensions": dimensions
                        }
                log_message(f"實時計算完成，共 {len(all_image_data)} 張圖片的哈希、尺寸和修改時間。")


            # --- 4. 處理廣告圖片哈希緩存 ---
            ad_hashes_map = {} # 儲存廣告圖片的哈希到路徑的映射
            if self.app_config.compare_mode == "ad": # 只有在廣告比對模式才需要廣告哈希
                self.after(0, lambda: self.update_status("正在計算廣告圖片哈希...")) # 統一為「計算中」
                # 不再傳遞 progress_callback
                ad_hashes_map = update_ad_image_hashes_cache(
                    [ad_image_folder], AD_IMAGE_HASH_CACHE_FILE, self.app_config.hash_size
                )
                if not ad_hashes_map:
                    self.after(0, lambda: messagebox.showinfo("提示", "沒有找到任何廣告圖片，廣告比對將無法進行。"))
                    self.after(0, lambda: self.update_status("廣告比對無法進行。"))
                    return
                log_message(f"從緩存中載入 {len(ad_hashes_map)} 個廣告哈希。")
            
            # 處理排除的哈希值、文件名和子資料夾
            excluded_hashes_set = set()
            if self.app_config.excluded_hash_values:
                for h_str in self.app_config.excluded_hash_values.split(','):
                    try:
                        excluded_hashes_set.add(h_str.strip())
                    except Exception as e:
                        log_message(f"解析排除哈希值 '{h_str.strip()}' 失敗: {e}", "warning")

            excluded_filenames_keywords = [k.strip().lower() for k in self.app_config.excluded_filenames.split(',') if k.strip()]
            excluded_subfolders_keywords = [k.strip().lower() for k in self.app_config.excluded_subfolders.split(',') if k.strip()]

            filtered_image_data = {}
            for path, data in all_image_data.items():
                if data and data.get("hash") and data["hash"] not in excluded_hashes_set:
                    skip_file = False
                    for keyword in excluded_filenames_keywords:
                        if keyword in os.path.basename(path).lower():
                            skip_file = True
                            # log_message(f"排除圖片 '{os.path.basename(path)}' 因文件名關鍵字 '{keyword}'。", "info") # 避免過多輸出
                            break
                    if skip_file:
                        continue

                    for keyword in excluded_subfolders_keywords:
                        if keyword in os.path.dirname(path).lower():
                            skip_file = True
                            # log_message(f"排除圖片 '{os.path.basename(path)}' 因其資料夾包含關鍵字 '{keyword}'。", "info") # 避免過多輸出
                            break
                    if skip_file:
                        continue
                    
                    filtered_image_data[path] = data
            log_message(f"排除過濾後，剩餘 {len(filtered_image_data)} 張圖片待比對。")

            # --- 5. 執行核心比對邏輯 ---
            self.after(0, lambda: self.update_status(f"開始執行 {self.app_config.compare_mode} 模式比對..."))
            
            if self.app_config.compare_mode == "ad":
                # find_similar_images_with_ads 已經修改為返回字典列表
                self.found_images_data = find_similar_images_with_ads(
                    filtered_image_data, ad_hashes_map, self.app_config.threshold, self.app_config.hash_size
                )
            elif self.app_config.compare_mode == "self":
                # find_similar_images_within_folders 已經修改為返回字典列表
                self.found_images_data = find_similar_images_within_folders(
                    filtered_image_data, self.app_config.threshold, self.app_config.hash_size
                )
            elif self.app_config.compare_mode == "qr":
                # scan_qr_codes 已經修改為返回字典列表
                self.found_images_data = scan_qr_codes(filtered_image_data)

            # 填充 Treeview 顯示結果 (在主線程中執行)
            self.after(0, lambda: self.populate_treeview(self.found_images_data))
            self.after(0, lambda: self.update_status(f"完成。找到 {len(self.found_images_data)} 張可疑圖片。"))

            # 如果啟用了立即刪除且有找到圖片，則自動執行刪除
            if self.app_config.delete_immediately and self.found_images_data:
                log_message("設定為立即刪除，正在執行自動刪除...", "info")
                images_to_auto_delete = [item['path'] for item in self.found_images_data]
                self.after(0, lambda: self.perform_deletion(images_to_auto_delete))
                self.after(0, lambda: messagebox.showinfo("自動刪除完成", f"已自動刪除 {len(images_to_auto_delete)} 張圖片。"))
                self.after(0, lambda: self.populate_treeview([])) # 清空列表
                self.after(0, lambda: self.update_status("自動刪除完成。"))
            elif not self.found_images_data:
                self.after(0, lambda: messagebox.showinfo("結果", "沒有找到任何可疑圖片。"))


        except Exception as e:
            # 確保 'e' 在 lambda 函數中可見，通常是因為它在外部作用域被捕獲了
            # 這裡的 NameError: free variable 'e' referenced before assignment 可能是因為實際的異常在 lambda 被定義之前發生
            # 但當函式定義順序正確後，這個問題應該會消失
            self.after(0, lambda current_e=e: messagebox.showerror("執行錯誤", f"執行過程中發生錯誤: {current_e}\n請檢查日誌獲取詳細信息。"))
            log_message(f"主執行邏輯錯誤: {e}", "error")
        finally:
            self.after(0, lambda: self.start_button.config(state="normal")) # 無論成功或失敗，重新啟用按鈕

    def populate_treeview(self, images_data):
        """
        將結果填充到 Treeview 中。
        結果會按資料夾分組顯示，提高可讀性。
        images_data: 列表，每個元素是一個字典 {'path': ..., 'mod_time': 'YYYY-MM-DD HH:MM', 'reason': ..., 'similarity': ...}
        """
        for item in self.tree.get_children(): # 清空現有內容
            self.tree.delete(item)
        
        self.checkbox_state = {} # 清空之前的勾選狀態
        self.folder_to_image_iids = {} # 清空資料夾映射

        # 按資料夾路徑將圖片數據分組
        images_by_folder = {}
        for img_info in images_data:
            folder = os.path.dirname(img_info['path'])
            if folder not in images_by_folder:
                images_by_folder[folder] = []
            images_by_folder[folder].append(img_info)
        
        # 遍歷每個資料夾，插入到 Treeview 作為父節點
        for folder, imgs in images_by_folder.items():
            folder_name_only = os.path.basename(folder)
            
            # 插入資料夾節點，初始顯示為未勾選，待所有子項插入後再更新其勾選狀態
            folder_id = self.tree.insert("", "end", 
                                         text=self.unchecked_char, # 初始為未勾選符號
                                         values=("", folder_name_only, "", ""), # Filename (空), Path (資料夾名)
                                         open=True, tags=("folder",))
            
            self.folder_to_image_iids[folder_id] = [] # 初始化資料夾的子圖片列表

            # 遍歷資料夾內的圖片，作為子節點插入
            for img_info in imgs:
                display_filename = os.path.basename(img_info['path'])
                full_image_path = img_info['path'] # 完整的圖片路徑
                display_mod_time = img_info.get('mod_time', '') # 獲取圖片修改時間
                display_similarity = f"{img_info.get('similarity', ''):.2f}%" if isinstance(img_info.get('similarity'), (int, float)) else str(img_info.get('similarity', ''))
                
                # 插入圖片項目， text 為勾選框符號，iid 為圖片路徑
                # values 順序: 文件名, 路徑, 修改時間, 相似度
                self.tree.insert(folder_id, "end", text=self.unchecked_char, 
                                 values=(display_filename, full_image_path, display_mod_time, display_similarity), 
                                 tags=("image_item",), iid=full_image_path) # iid 仍然是圖片路徑
                self.checkbox_state[full_image_path] = False # 初始化為未勾選
                self.folder_to_image_iids[folder_id].append(full_image_path) # 將圖片 iid 添加到資料夾映射中
            
            # 在所有子圖片插入後，更新資料夾節點的勾選狀態
            self.update_folder_checkbox_display(folder_id)

    def on_tree_select(self, event):
        """
        處理 Treeview 選擇事件。當用戶選擇列表中的圖片時，在預覽區顯示該圖片。
        """
        selected_items = self.tree.selection()
        if not selected_items:
            self.clear_previews()
            return

        selected_item_iid = selected_items[0]
        item_tags = self.tree.item(selected_item_iid, 'tags')
        
        if "image_item" in item_tags:
            comic_image_path = selected_item_iid # iid 就是漫畫圖片路徑
            self.current_comic_preview_path = comic_image_path
            
            # 根據選中的圖片信息，查找匹配的圖片路徑
            selected_img_info = next((item for item in self.found_images_data if item['path'] == comic_image_path), None)
            
            if selected_img_info and selected_img_info.get('matched_ad_path'):
                # 無論是廣告比對還是互相排查，matched_ad_path 都應該指向匹配的圖片
                self.current_ad_preview_path = selected_img_info['matched_ad_path']
            else:
                self.current_ad_preview_path = None # 如果沒有匹配圖片，則設為 None

            # 顯示漫畫圖片預覽
            self.show_image_preview(self.comic_preview_canvas, self.current_comic_preview_path, self.comic_path_label, "漫畫圖片路徑:")
            
            # 顯示匹配圖片預覽 (如果存在)
            if self.current_ad_preview_path:
                self.show_image_preview(self.ad_preview_canvas, self.current_ad_preview_path, self.ad_path_label, "匹配圖片路徑:")
            else:
                self.ad_preview_canvas.delete("all")
                self.ad_path_label.config(text="匹配圖片路徑: (無匹配圖片)") # 更新文字提示

        else:
            self.clear_previews()

    def on_tree_click(self, event):
        """
        處理 Treeview 單擊事件，特別是針對勾選框和資料夾全選/取消全選的點擊。
        """
        item_id = self.tree.identify_row(event.y)
        
        if not item_id:
            return

        # 判斷點擊的是否是展開/收合箭頭
        element = self.tree.identify_element(event.x, event.y)
        if item_id and element == "tree": # 如果點擊的是展開/收合圖示
            # 讓 Treeview 自己處理展開/收合，不觸發勾選狀態的改變
            return # 阻止後續的勾選邏輯

        item_tags = self.tree.item(item_id, 'tags')

        # 檢查是否點擊了 #0 欄 (選擇欄) 或 #1 欄 (文件名欄，此處為資料夾名/圖片名)
        column_id = self.tree.identify_column(event.x)
        if column_id in ("#0", "#1"): 
            if "image_item" in item_tags: # 如果是圖片項目，切換其勾選狀態
                current_state = self.checkbox_state.get(item_id, False) # 獲取當前勾選狀態
                new_state = not current_state # 切換狀態
                self.set_item_checked_state(item_id, new_state)
                # 更新父資料夾的勾選狀態
                parent_id = self.tree.parent(item_id)
                if parent_id:
                    self.update_folder_checkbox_display(parent_id)
                # 保持選中狀態，以便預覽正確顯示
                self.tree.selection_set(item_id) 

            elif "folder" in item_tags: # 如果是資料夾頂層節點，切換其所有子圖片的勾選狀態
                children_image_iids = self.folder_to_image_iids.get(item_id, [])
                if not children_image_iids: return # 沒有圖片子節點，不執行操作

                # 判斷是全選還是取消全選：如果當前資料夾下有任何未勾選的圖片，則點擊後全選；否則取消全選
                should_select_all = False
                # 遍歷檢查，如果任何一個子項目未勾選，則應該執行全選操作
                for child_iid in children_image_iids:
                    if not self.checkbox_state.get(child_iid, False):
                        should_select_all = True
                        break
                
                # 根據判斷結果，設定所有子圖片的勾選狀態
                for child_iid in children_image_iids:
                    self.set_item_checked_state(child_iid, should_select_all)
                
                # 更新資料夾本身的勾選狀態
                self.update_folder_checkbox_display(item_id)

                # 保持選中狀態，以便預覽正確顯示
                self.tree.selection_set(item_id) 

    def set_item_checked_state(self, item_id, state):
        """
        設定指定 Treeview 項目的勾選狀態 (True for checked, False for unchecked)。
        更新內部狀態字典和 Treeview 顯示。
        """
        if item_id in self.checkbox_state:
            self.checkbox_state[item_id] = state
            self.tree.item(item_id, text=self.checked_char if state else self.unchecked_char)

    def update_folder_checkbox_display(self, folder_iid):
        """
        根據子圖片的勾選狀態，更新資料夾節點的勾選符號。
        如果所有子圖片都勾選，資料夾顯示為'☑'；否則為'☐'。
        """
        children_image_iids = self.folder_to_image_iids.get(folder_iid, [])
        
        if not children_image_iids:
            # 如果沒有子圖片，資料夾仍顯示為未勾選
            self.tree.item(folder_iid, text=self.unchecked_char)
            return

        checked_count = 0
        total_children = len(children_image_iids)

        for child_iid in children_image_iids:
            if self.checkbox_state.get(child_iid, False):
                checked_count += 1
        
        if checked_count == 0:
            self.tree.item(folder_iid, text=self.unchecked_char) # 所有子項目都未選中
        elif checked_count == total_children:
            self.tree.item(folder_iid, text=self.checked_char) # 所有子項目都已選中
        else:
            self.tree.item(folder_iid, text=self.partial_char) # 部分子項目選中


    def on_tree_double_click(self, event):
        """
        處理 Treeview 雙擊事件。
        雙擊資料夾節點：只進行展開/收合，並確保選中狀態不變。
        雙擊圖片檔案：開啟其所在資料夾。
        """
        item_id = self.tree.identify('item', event.x, event.y) # 使用 identify('item', x, y)
        if not item_id:
            return

        item_tags = self.tree.item(item_id, 'tags')
        
        if "folder" in item_tags: # 雙擊的是資料夾頂層節點
            # 展開/收合節點
            current_open_state = self.tree.item(item_id, 'open')
            self.tree.item(item_id, open=not current_open_state)
            # 確保資料夾節點在雙擊後仍然保持選中狀態
            self.tree.selection_set(item_id) 
            return "break" # 阻止默認的雙擊行為 (例如，打開文件瀏覽器或選中項目)
        
        elif "image_item" in item_tags: # 雙擊圖片項目
            image_path = item_id # iid 就是圖片路徑
            open_file_explorer(os.path.dirname(image_path)) # 開啟圖片所在資料夾
            return "break" # 阻止事件繼續傳播，避免其他潛在行為


    def show_image_preview(self, canvas, image_path, label_widget, label_text_prefix):
        """
        在指定的 Canvas 上顯示圖片，並更新對應的標籤。
        """
        canvas.delete("all") # 清空 Canvas 上的所有內容
        # 這裡不清除 self.comic_tk_image 或 self.ad_tk_image 的引用
        # 因為它們需要在 show_image_preview 函數之外持久存在，防止被垃圾回收
        # 而是將新的 ImageTk.PhotoImage 對象賦值給它們

        if not os.path.exists(image_path):
            canvas.create_text(
                canvas.winfo_width()/2, canvas.winfo_height()/2,
                text="圖片不存在或已移動！", fill="red", font=("Arial", 12)
            )
            label_widget.config(text=f"{label_text_prefix} (圖片不存在)")
            # 根據是漫畫預覽還是廣告預覽，更新對應的引用
            if canvas == self.comic_preview_canvas:
                self.comic_tk_image = None
            elif canvas == self.ad_preview_canvas:
                self.ad_tk_image = None
            return

        try:
            img = Image.open(image_path)
            canvas_width = canvas.winfo_width()
            canvas_height = self.app_config.preview_height

            if canvas_width == 0: canvas_width = 400
            if canvas_height == 0: canvas_height = self.app_config.preview_height

            img_width, img_height = img.size
            
            ratio_w = canvas_width / img_width
            ratio_h = canvas_height / img_height
            ratio = min(ratio_w, ratio_h) 

            new_width = int(img_width * ratio)
            new_height = int(img_height * ratio)

            img = img.resize((new_width, new_height), Image.LANCZOS)
            
            # 將 PIL 圖片對象轉換為 Tkinter 可以顯示的 ImageTk.PhotoImage 對象
            if canvas == self.comic_preview_canvas:
                self.comic_tk_image = ImageTk.PhotoImage(img)
                canvas.create_image(canvas_width/2, canvas_height/2, anchor="center", image=self.comic_tk_image)
            elif canvas == self.ad_preview_canvas:
                self.ad_tk_image = ImageTk.PhotoImage(img)
                canvas.create_image(canvas_width/2, canvas_height/2, anchor="center", image=self.ad_tk_image)

            label_widget.config(text=f"{label_text_prefix} {image_path}")

        except Exception as e:
            canvas_width = canvas.winfo_width() if canvas.winfo_width() > 0 else 400
            canvas_height = canvas.winfo_height() if canvas.winfo_height() > 0 else self.app_config.preview_height
            canvas.create_text(
                canvas_width/2, canvas_height/2,
                text=f"無法預覽圖片: {e}", fill="red", font=("Arial", 12)
            )
            label_widget.config(text=f"{label_text_prefix} (無法載入)")
            log_message(f"預覽圖片 '{image_path}' 失敗: {e}", "error")

    def on_comic_preview_click(self, event):
        """
        處理漫畫預覽 Canvas 的點擊事件，開啟圖片所在資料夾。
        """
        if self.current_comic_preview_path and os.path.exists(self.current_comic_preview_path):
            open_file_explorer(os.path.dirname(self.current_comic_preview_path))
        else:
            messagebox.showinfo("提示", "無效的漫畫圖片路徑，無法打開資料夾。")

    def on_ad_preview_click(self, event):
        """
        處理匹配廣告圖預覽 Canvas 的點擊事件，開啟圖片所在資料夾。
        """
        if self.current_ad_preview_path and os.path.exists(self.current_ad_preview_path):
            open_file_explorer(os.path.dirname(self.current_ad_preview_path))
        else:
            messagebox.showinfo("提示", "無效的匹配圖片路徑，無法打開資料夾。")


    def clear_previews(self):
        """清空所有預覽區的內容。"""
        self.comic_preview_canvas.delete("all")
        self.ad_preview_canvas.delete("all")
        self.comic_path_label.config(text="漫畫圖片路徑:")
        self.ad_path_label.config(text="匹配圖片路徑:") # 修改提示文字
        self.comic_tk_image = None
        self.ad_tk_image = None
        self.current_comic_preview_path = None
        self.current_ad_preview_path = None

    def show_context_menu(self, event):
        """
        顯示 Treeview 右鍵上下文菜單。
        """
        # 識別點擊的項目
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return

        # 確保選中被右鍵點擊的項目
        self.tree.selection_set(item_id) 

        # 創建菜單
        context_menu = tk.Menu(self, tearoff=0)
        
        item_tags = self.tree.item(item_id, 'tags')
        
        if "image_item" in item_tags: # 如果是圖片項目
            image_path = item_id # iid 就是圖片路徑
            context_menu.add_command(label="開啟圖片所在資料夾", command=lambda: open_file_explorer(os.path.dirname(image_path)))
        elif "folder" in item_tags: # 如果是資料夾頂層節點
            folder_path = self.tree.item(item_id, 'values')[1] # 從 values 中獲取路徑
            context_menu.add_command(label="開啟資料夾", command=lambda: open_file_explorer(folder_path))
        
        context_menu.post(event.x_root, event.y_root) # 在鼠標位置顯示菜單

    def get_checked_image_paths(self):
        """
        返回所有勾選框為 'True' 的圖片路徑列表。
        """
        return [path for path, state in self.checkbox_state.items() if state]

    def select_all_images(self):
        """
        將 Treeview 中所有可勾選的圖片項目設定為勾選狀態。
        """
        for parent_item in self.tree.get_children(): # 遍歷所有頂層資料夾
            for item_id in self.tree.get_children(parent_item): # 遍歷子項目
                item_tags = self.tree.item(item_id, 'tags')
                if "image_item" in item_tags:
                    self.set_item_checked_state(item_id, True)
        # 更新所有資料夾的勾選狀態
        for parent_item in self.tree.get_children():
            self.update_folder_checkbox_display(parent_item)
        log_message("已全選所有圖片。")

    def deselect_all_images(self):
        """
        將 Treeview 中所有可勾選的圖片項目設定為未勾選狀態。
        """
        for parent_item in self.tree.get_children(): # 遍歷所有頂層資料夾
            for item_id in self.tree.get_children(parent_item): # 遍歷子項目
                item_tags = self.tree.item(item_id, 'tags')
                if "image_item" in item_tags:
                    self.set_item_checked_state(item_id, False)
        # 更新所有資料夾的勾選狀態
        for parent_item in self.tree.get_children():
            self.update_folder_checkbox_display(parent_item)
        log_message("已取消全選所有圖片。")


    def delete_selected_images(self, event=None):
        """
        刪除 Treeview 中已勾選的圖片。
        """
        images_to_delete = self.get_checked_image_paths() # 使用新函數獲取勾選的圖片

        if not images_to_delete:
            messagebox.showinfo("提示", "請先勾選要刪除的圖片。")
            return

        if self.app_config.confirm_delete:
            if not messagebox.askyesno("確認刪除", f"確定要刪除已勾選的 {len(images_to_delete)} 張圖片嗎？此操作不可逆！"):
                return
        
        self.perform_deletion(images_to_delete)

    def delete_all_images_confirmation(self):
        """
        提示用戶確認後，刪除所有在結果列表中顯示的圖片。
        """
        if not self.found_images_data:
            messagebox.showinfo("提示", "沒有可刪除的圖片結果。")
            return

        if self.app_config.confirm_delete:
            if not messagebox.askyesno("確認刪除所有", f"確定要刪除所有 {len(self.found_images_data)} 張結果圖片嗎？此操作不可逆！"):
                return
        
        # 從 self.found_images_data 列表中提取所有圖片路徑用於刪除
        images_to_delete = [item['path'] for item in self.found_images_data] 
        self.perform_deletion(images_to_delete)


    def perform_deletion(self, image_paths_to_delete):
        """
        執行實際的文件刪除操作，並更新 Treeview。
        """
        deleted_count = 0
        deleted_paths = set() # 用於記錄已成功刪除的路徑
        
        for image_path in image_paths_to_delete:
            if os.path.exists(image_path):
                try:
                    os.remove(image_path) # 執行文件刪除
                    deleted_count += 1
                    deleted_paths.add(image_path)
                    log_message(f"已刪除圖片: {image_path}")
                except Exception as e:
                    log_message(f"刪除圖片 '{image_path}' 失敗: {e}", "error")
            else:
                log_message(f"圖片 '{image_path}' 不存在，無法刪除。", "warning")
        
        messagebox.showinfo("刪除結果", f"已成功刪除 {deleted_count} 張圖片。")
        self.update_treeview_after_deletion(deleted_paths) # 更新 GUI 顯示 
        self.clear_previews() # 清空預覽區

    def update_treeview_after_deletion(self, deleted_paths):
        """
        從 Treeview 和內部 self.found_images_data 列表中移除已刪除的條目。
        同時檢查並移除變空的資料夾節點。
        """
        # 更新 self.found_images_data 列表，移除已刪除的圖片
        self.found_images_data = [img_info for img_info in self.found_images_data if img_info['path'] not in deleted_paths]

        # 更新 self.checkbox_state 列表，移除已刪除的圖片
        self.checkbox_state = {path: state for path, state in self.checkbox_state.items() if path not in deleted_paths}

        # 更新 Treeview 顯示
        self.populate_treeview(self.found_images_data)


# === 7. 程式入口 ===
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 在主程序啟動時，確保緩存目錄存在
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # 創建應用程序配置的實例
    app_config = AppConfig()

    # 創建並運行主應用程序視窗
    root = MainApplication(app_config)
    root.mainloop()

