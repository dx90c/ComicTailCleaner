# ======================================================================
# 檔案名稱：ComicTailCleaner_v14.2.1.py
# 版本號：14.2.1 (雙哈希LSH引擎與穩定性修正版)
# 專案名稱：ComicTailCleaner (漫畫尾頁廣告清理)
#
# === 程式說明 ===
# 一個專為清理漫畫檔案尾頁廣告或重複頁面的工具。
# 它能高效地掃描大量漫畫檔案，並通過感知哈希算法找出內容上
# 相似或完全重複的圖片，提升漫畫閱讀體驗。
#
# === 14.1.0 ~ 14.2.1 版本更新內容 ===
# - 【核心引擎升級】引入 LSH 雙哈希驗證機制，大幅提升比對精度與效率：
#   - pHash 海選：掃描階段只計算 pHash，速度極快。
#   - LSH 候選加速：利用局部敏感哈希 (LSH) 建立索引，將比對範圍從“全庫掃描”縮小為“少量高潛力候選”，性能指數級提升。
#   - wHash 懶算復核：僅在 pHash 相似度落入“不確定區” (85-93%) 時，才“懶加載”並計算原理不同的 wHash 進行二次驗證，精準剔除“看著不像”的誤報。
#   - 性能與精度兼得：用戶設定高相似度(>93%)時，流程極快；設定低相似度時，結果依然可靠。
#
# - 【廣告比對修正】引入“集團搜索”邏輯，解決相似廣告導致的重複與漏報問題：
#   - 廣告預處理：在比對前先對廣告庫內部分組，將相似廣告（如不同QR碼版本）視為一個整體。
#   - 集團驗證：使用廣告組“領隊”進行LSH初篩，但用組內“全體成員”對候選目標進行最終確認，確保不因代表差異而錯過匹配。
#
# - 【數據完整性修正】
#   - 徹底隔離掃描數據：修復了因共享變數導致漫畫庫數據被廣告庫數據意外覆蓋的致命Bug，確保了比對範圍的絕對正確。
#   - 全面路徑正規化：統一了所有從磁碟讀取路徑的格式，根除了因路徑分隔符 `\` 和 `/` 混用導致的重複顯示問題。
#
# - 【UI/UX 穩定性優化】
#   - 固定預覽面板佈局：透過固定高度容器，徹底解決了預覽圖因下方路徑文字換行而忽大忽小、畫面跳動的問題。
#   - 恢復原生鍵盤導航：移除了有缺陷的自訂導航，恢復了Treeview原生、流暢的上下方向鍵瀏覽體驗。
#   - 強化資料夾開啟邏輯：現在無論檔案是否已被刪除，雙擊路徑或單擊預覽圖都能成功打開其所在的資料夾。
#
# - 【健壯性修正】
#   - 完善刪除邏輯：確保刪除操作（無論成功或失敗）後，能正確更新UI、圖片快取和資料夾狀態快取，杜絕“鬼影”項目殘留。
#   - 強化設定檔讀寫：增加了防呆設計，即使設定檔缺少特定項目或UI輸入為空，程式也能安全讀寫，避免崩潰。
#
# === 14.0.0 版本歷史 ===
# - 【UI交互最終重構】基於 v13.3.x 奠定的穩定架構，實現了一套完美的 AllDup 風格交互。
#
# === 13.x 版本歷史 ===
# - 13.3.0: 建立UI架構基線，簡化交互以確保穩定性。
# - 13.2.x: 引入UI架構的早期嘗試與迭代。
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
from multiprocessing import set_start_method, Pool, Manager, Event, cpu_count
from functools import partial
import platform
import threading
import time
from queue import Queue, Empty
import re

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
from tkinter import ttk, filedialog, messagebox, font # <--- 也可以這樣合併
from tkinter import filedialog
from tkinter import messagebox

# === 4. 全局常量和設定 (更新) ===
APP_VERSION = "14.2.1" # 更新版本號以標記雙哈希LSH優化
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"
CONFIG_FILE = "config.json"
QR_SCAN_ENABLED = False
PERFORMANCE_LOGGING_ENABLED = False
CACHE_LOCK = threading.Lock()

# === 5. 雙哈希 LSH 相關常數和工具函數 (新增) ===
HASH_BITS = 64
PHASH_FAST_THRESH   = 0.80   # <--- 修正於此，與UI的下限保持一致
PHASH_STRICT_SKIP   = 0.93
WHASH_TIER_1        = 0.90   # pHash 0.90~0.93 區間，wHash 需 >= 0.90
WHASH_TIER_2        = 0.92   # pHash 0.88~0.90 區間，wHash 需 >= 0.92
WHASH_TIER_3        = 0.95   # pHash 0.80~0.88 區間，wHash 需 >= 0.95
WHASH_TIER_4        = 0.98   # [新增] 對應 pHash 區間: 0.80 <= sim_p < 0.85

LSH_BANDS = 4  # 4 × 16bit 分段

def sim_from_hamming(d: int, bits: int = HASH_BITS) -> float:
    """將海明距離轉換為相似度 (0.0 到 1.0)"""
    return 1.0 - (d / bits)

def hamming_from_sim(sim: float, bits: int = HASH_BITS) -> int:
    """將相似度轉換為海明距離（用於計算相似度下限對應的距離上限）"""
    return max(0, int(round((1.0 - sim) * bits)))

# === 5. 工具函數 (Helper Functions) ===
def log_error(message: str, include_traceback: bool = False):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] ERROR: {message}\n"
    if include_traceback:
        log_content += traceback.format_exc() + "\n"
    
    log_file = "error_log.txt"
    print(log_content, end='', flush=True)
    try:
        file_exists = os.path.exists(log_file)
        with open(log_file, "a", encoding="utf-8-sig", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"Failed to write to error log: {e}\nOriginal error: {message}", flush=True)

def log_info(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] INFO: {message}\n"

    log_file = "info_log.txt"
    print(log_content, end='', flush=True)
    try:
        file_exists = os.path.exists(log_file)
        # 使用 utf-8-sig 會在文件開頭寫入BOM (如果文件不存在)
        with open(log_file, "a", encoding="utf-8-sig", buffering=1) as f:
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
###
def check_and_install_packages():
    # [核心修正] 确保 global 声明在函式的最顶部
    global QR_SCAN_ENABLED, PERFORMANCE_LOGGING_ENABLED

    # 如果在打包后的EXE环境中运行，则完全跳过依赖检查
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        print("在打包环境中运行，跳过依赖检查。")
        # 在EXE中，我们假设所有可选依赖都已打包
        QR_SCAN_ENABLED = True 
        PERFORMANCE_LOGGING_ENABLED = True
        return

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
                messagebox.showerror("安裝失敗", f"自動安裝套-件失敗：{e}\n請手動打開命令提示字元並執行 'pip install {package_str}'")
                sys.exit(1)
        else:
            messagebox.showerror("缺少核心依賴", f"請手動安裝必要套件：{', '.join(missing_core)}。\n命令：pip install {package_str}")
            sys.exit(1)
            
    # 現在可以安全地赋值
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
###
def _pool_worker_process_image(image_path: str) -> tuple[str, dict | None]:
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})
    try:
        with Image.open(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                return (image_path, {'error': f"圖片尺寸異常或無法讀取: {image_path}"})
            img = ImageOps.exif_transpose(img)
            phash_val = imagehash.phash(img, hash_size=8)
            stat_info = os.stat(image_path)
            return (image_path, {
                'phash': phash_val, 'size': stat_info.st_size,
                'ctime': stat_info.st_ctime, 'mtime': stat_info.st_mtime
            })
    except UnidentifiedImageError:
        return (image_path, {'error': f"無法識別圖片格式: {image_path}"})
    except Exception as e:
        return (image_path, {'error': f"處理圖片失敗 {image_path}: {e}"})

def _detect_qr_on_image(img: Image.Image) -> list | None:
    img_cv = np.array(img.convert('RGB'))
    if img_cv.shape[0] == 0 or img_cv.shape[1] == 0:
        raise ValueError("圖像尺寸異常，無法進行 OpenCV 處理")
    qr_detector = cv2.QRCodeDetector()
    retval, decoded_info, points, _ = qr_detector.detectAndDecodeMulti(img_cv)
    if retval and decoded_info and any(info for info in decoded_info if info):
        return points.tolist()
    return None

def _pool_worker_detect_qr_code(image_path: str, resize_size: int) -> tuple[str, dict | None]:
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})
    try:
        with Image.open(image_path) as pil_img:
            if not pil_img or pil_img.width == 0 or pil_img.height == 0:
                return (image_path, {'error': f"圖片尺寸異常或無法讀取: {image_path}"})
            pil_img = ImageOps.exif_transpose(pil_img)
            resized_img = pil_img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            points = _detect_qr_on_image(resized_img)
            if not points:
                points = _detect_qr_on_image(pil_img)
            return (image_path, {'qr_points': points})
    except UnidentifiedImageError:
        return (image_path, {'error': f"無法識別圖片格式: {image_path}"})
    except (cv2.error, ValueError) as e:
        return (image_path, {'error': f"OpenCV 處理失敗 {image_path}: {e}"})
    except Exception as e:
        return (image_path, {'error': f"QR檢測失敗 {image_path}: {e}"})
##12
def _pool_worker_process_image_full(image_path: str, resize_size: int) -> tuple[str, dict | None]:
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})
    try:
        with Image.open(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                return (image_path, {'error': f"圖片尺寸異常或無法讀取: {image_path}"})
            
            img = ImageOps.exif_transpose(img)
            
            # 計算 pHash
            phash_val = imagehash.phash(img, hash_size=8)
            
            # 檢測 QR Code
            resized_img = img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            qr_points_val = _detect_qr_on_image(resized_img)
            if not qr_points_val:
                qr_points_val = _detect_qr_on_image(img)
                
        # 獲取檔案資訊
        stat_info = os.stat(image_path)
        
        return (image_path, {
            'phash': phash_val, 
            'qr_points': qr_points_val,
            'size': stat_info.st_size, 
            'ctime': stat_info.st_ctime, 
            'mtime': stat_info.st_mtime
        })
    except UnidentifiedImageError:
        return (image_path, {'error': f"無法識別圖片格式: {image_path}"})
    except (cv2.error, ValueError) as e:
        return (image_path, {'error': f"OpenCV 處理失敗 {image_path}: {e}"})
    except Exception as e:
        return (image_path, {'error': f"完整圖片處理失敗 {image_path}: {e}"})

# 替換原有的 _pool_worker_process_image
def _pool_worker_process_image_phash_only(image_path: str):
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})
    try:
        with Image.open(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                return (image_path, {'error': f"圖片尺寸異常或無法讀取: {image_path}"})
            img = ImageOps.exif_transpose(img)
            ph = imagehash.phash(img, hash_size=8)  # 64-bit
            st = os.stat(image_path)
            return (image_path, {
                'phash': ph, 'size': st.st_size, 'ctime': st.st_ctime, 'mtime': st.st_mtime
            })
    except Exception as e:
        return (image_path, {'error': f"處理 pHash 失敗 {image_path}: {e}"})

##12

##12
# === 6. 配置管理相關函數 ===
default_config = {
    'root_scan_folder': '', 'ad_folder_path': '', 'extract_count': 5,
    'enable_extract_count_limit': True, 'excluded_folders': [],
    'comparison_mode': 'mutual_comparison', 'similarity_threshold': 98,
    'enable_time_filter': False, 'start_date_filter': '', 'end_date_filter': '',
    'enable_qr_hybrid_mode': True, 'qr_resize_size': 800,
    'worker_processes': 0,
    'ux_scan_start_delay': 0.1,
    'compare_chunk_factor': 16
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
def _sanitize_path_for_filename(path: str) -> str:
    """清理路徑字串，使其可用於檔名。"""
    if not path:
        return ""
    # 取得最後一個目錄名
    basename = os.path.basename(os.path.normpath(path))
    # 移除或替換不合法字元
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', basename)
    return sanitized
###12
class ScannedImageCacheManager:
    def __init__(self, root_scan_folder: str, ad_folder_path: str | None = None):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        sanitized_ad = _sanitize_path_for_filename(ad_folder_path) if ad_folder_path else None
        
        base_name = f"scanned_hashes_cache_{sanitized_root}"
        if sanitized_ad:
            base_name += f"_{sanitized_ad}"
        
        self.cache_file_path = f"{base_name}.json"
        
        counter = 1
        while os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    first_key = next(iter(data), None)
                    # 檢查快取是否與當前路徑匹配
                    if first_key and os.path.normpath(first_key).startswith(os.path.normpath(root_scan_folder)):
                        break
            except (json.JSONDecodeError, StopIteration, TypeError):
                break

            self.cache_file_path = f"{base_name}_{counter}.json"
            counter += 1
            if counter > 10:
                 log_error("快取檔名衝突過多，可能存在問題。")
                 break

        self.cache = self._load_cache()

    def _normalize_loaded_data(self, data: dict) -> dict:
        converted_data = data.copy()
        for key in ['phash', 'whash']:
            if key in converted_data and converted_data[key] is not None:
                if not isinstance(converted_data[key], imagehash.ImageHash):
                    try:
                        converted_data[key] = imagehash.hex_to_hash(str(converted_data[key]))
                    except (TypeError, ValueError):
                        converted_data[key] = None
        return converted_data

    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    converted_cache = {}
                    for path, data in loaded_data.items():
                        # 所有載入的路徑都進行標準化
                        norm_path = os.path.normpath(path)
                        if isinstance(data, dict):
                            converted_cache[norm_path] = self._normalize_loaded_data(data)
                        elif isinstance(data, str):
                            converted_cache[norm_path] = self._normalize_loaded_data({'phash': data})
                    log_info(f"掃描圖片快取 '{self.cache_file_path}' 已成功載入。")
                    return converted_cache
            except (json.JSONDecodeError, Exception) as e:
                log_info(f"掃描圖片快取檔案 '{self.cache_file_path}' 格式不正確或讀取失敗 ({e})，將重建。")
        return {}

    def save_cache(self) -> None:
        with CACHE_LOCK:
            max_retries = 3; retry_delay = 0.5
            serializable_cache = {}
            for path, data in self.cache.items():
                if data:
                    serializable_data = {k: str(v) if isinstance(v, imagehash.ImageHash) else v for k, v in data.items()}
                    serializable_cache[path] = serializable_data

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
                        if 'messagebox' in globals():
                            messagebox.showerror("快取保存失敗", f"無法保存快取檔案 '{self.cache_file_path}'，請檢查檔案權限。\n錯誤: {e}")
                        break

    def get_data(self, file_path: str) -> dict | None:
        return self.cache.get(os.path.normpath(file_path))
        
    def update_data(self, file_path: str, data: dict) -> None:
        if data and 'error' not in data:
            norm_path = os.path.normpath(file_path)
            if self.cache.get(norm_path):
                self.cache[norm_path].update(data)
            else:
                self.cache[norm_path] = data

    def remove_data(self, file_path: str) -> bool:
        """從快取中移除單一檔案的紀錄"""
        with CACHE_LOCK:
            normalized_path = os.path.normpath(file_path)
            if normalized_path in self.cache:
                del self.cache[normalized_path]
                log_info(f"[快取清理] 已從圖片快取中移除條目: {normalized_path}")
                return True
            return False

    def remove_entries_from_folder(self, folder_path: str) -> int:
        with CACHE_LOCK:
            count = 0
            norm_folder_path = os.path.normpath(folder_path) + os.sep
            keys_to_delete = [key for key in self.cache if key.startswith(norm_folder_path)]
            for key in keys_to_delete:
                del self.cache[key]
                count += 1
            if count > 0:
                log_info(f"[快取清理] 已從圖片快取中移除屬於 '{folder_path}' 的 {count} 個條目。")
            return count

    def invalidate_cache(self) -> None:
        with CACHE_LOCK:
            self.cache = {}
            if os.path.exists(self.cache_file_path):
                try: 
                    log_info(f"[快取清理] 準備將圖片快取檔案 '{self.cache_file_path}' 移至回收桶。")
                    send2trash.send2trash(self.cache_file_path)
                except Exception as e: 
                    log_error(f"刪除掃描快取檔案時發生錯誤: {e}", True)
                    try:
                        os.remove(self.cache_file_path)
                    except Exception as e2:
                        log_error(f"Fallback 刪除掃描快取檔案失敗: {e2}", True)

###12
class FolderStateCacheManager:
    def __init__(self, root_scan_folder: str):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        base_name = f"folder_state_cache_{sanitized_root}"
        self.cache_file_path = f"{base_name}.json"
        
        # 處理檔名衝突
        counter = 1
        while os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    first_key = next(iter(data), None)
                    if first_key and os.path.normpath(first_key).startswith(os.path.normpath(root_scan_folder)):
                        break
            except (json.JSONDecodeError, StopIteration, TypeError):
                break
            
            self.cache_file_path = f"{base_name}_{counter}.json"
            counter += 1
            if counter > 10:
                log_error("資料夾快取檔名衝突過多，可能存在問題。")
                break
                
        self.cache = self._load_cache()

    def _load_cache(self) -> dict:
        if not os.path.exists(self.cache_file_path):
            return {}
        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                loaded_cache = json.load(f)
            
            converted_cache = {}
            needs_saving = False
            for path, state in loaded_cache.items():
                if isinstance(state, (int, float)): 
                    converted_cache[path] = {'mtime': state, 'ctime': None}
                    needs_saving = True
                elif isinstance(state, dict) and 'mtime' in state:
                    converted_cache[path] = state
            
            if needs_saving:
                log_info(f"檢測到舊版資料夾快取格式，將自動轉換...")
                self.cache = converted_cache
                self.save_cache()

            log_info(f"資料夾狀態快取 '{self.cache_file_path}' 已成功載入 ({len(converted_cache)} 筆)。")
            return converted_cache
        except Exception as e:
            log_error(f"載入或轉換資料夾狀態快取時發生錯誤: {e}", True)
            return {}

    def save_cache(self) -> None:
        with CACHE_LOCK:
            max_retries = 3; retry_delay = 0.5
            for attempt in range(max_retries):
                try:
                    os.makedirs(os.path.dirname(self.cache_file_path) or '.', exist_ok=True)
                    temp_file_path = self.cache_file_path + f".tmp{os.getpid()}"
                    with open(temp_file_path, 'w', encoding='utf-8') as f:
                        json.dump(self.cache, f, indent=2)
                    os.replace(temp_file_path, self.cache_file_path)
                    return
                except (IOError, OSError) as e:
                    log_error(f"保存資料夾快取失敗 (嘗試 {attempt + 1}/{max_retries}): {e}", True)
                    if attempt < max_retries - 1: time.sleep(retry_delay)
                    else:
                        if 'messagebox' in globals(): messagebox.showerror("快取保存失敗", f"無法保存資料夾快取檔案 '{self.cache_file_path}'，請檢查檔案權限。\n錯誤: {e}")
                        break
    
    def get_folder_state(self, folder_path: str) -> dict | None:
        return self.cache.get(folder_path)

    def update_folder_state(self, folder_path: str, mtime: float, ctime: float | None):
        if folder_path not in self.cache:
            self.cache[folder_path] = {}
        self.cache[folder_path]['mtime'] = mtime
        if ctime is not None:
            self.cache[folder_path]['ctime'] = ctime

    def remove_folders(self, folder_paths: list[str]):
        for path in folder_paths:
            if path in self.cache:
                del self.cache[path]

    def invalidate_cache(self) -> None:
        with CACHE_LOCK:
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
def _update_progress(queue: Queue, **kwargs):
    if queue:
        queue.put({'type': 'text', **kwargs})

def _full_scan_traversal(root_folder: str, excluded_paths: set, progress_queue: Queue) -> dict:
    _update_progress(progress_queue, text="正在執行全量掃描 (os.walk)，此過程無法中斷...")
    log_info("採用 os.walk 進行高效能全量掃描。")
    live_folders = {}
    for dirpath, dirnames, _ in os.walk(root_folder):
        if any(os.path.normpath(dirpath).startswith(ex) for ex in excluded_paths):
            dirnames[:] = []
            continue
        try:
            stat = os.stat(dirpath)
            live_folders[dirpath] = {'mtime': stat.st_mtime, 'ctime': stat.st_ctime}
        except OSError:
            continue
    return live_folders

def _incremental_scan_traversal(root_folder: str, excluded_paths: set, time_filter: dict, progress_queue: Queue, control_events: dict) -> dict:
    log_info("採用 deque 進行可中斷的增量掃描。")
    live_folders = {}
    queue = deque([root_folder])
    scanned_count = 0

    while queue:
        if control_events['cancel'].is_set():
            return {}
        current_dir = queue.popleft()

        if any(os.path.normpath(current_dir).startswith(ex) for ex in excluded_paths):
            continue

        scanned_count += 1
        if scanned_count % 50 == 0:
            _update_progress(progress_queue, text=f"📁 正在掃描資料夾結構... ({scanned_count})")

        try:
            stat_info = os.stat(current_dir)
            live_folders[current_dir] = {'mtime': stat_info.st_mtime, 'ctime': stat_info.st_ctime}
        except OSError:
            continue
            
        try:
            with os.scandir(current_dir) as it:
                for entry in it:
                    if control_events['cancel'].is_set():
                        return {}
                    if entry.is_dir():
                        # 【核心性能修正 v13.1.0】在此處進行時間篩選
                        if time_filter.get('enabled'):
                            try:
                                entry_ctime_dt = datetime.datetime.fromtimestamp(entry.stat().st_ctime)
                                if (time_filter['start'] and entry_ctime_dt < time_filter['start']) or \
                                   (time_filter['end'] and entry_ctime_dt > time_filter['end']):
                                    continue  # 不符合時間範圍，跳過此資料夾及其所有子資料夾
                            except OSError:
                                continue # 無法獲取狀態，跳過
                        
                        queue.append(entry.path)
        except OSError:
            continue
    
    return live_folders
###

def get_files_to_process(config: dict, image_cache: ScannedImageCacheManager, progress_queue: Queue | None = None, control_events: dict | None = None) -> list[str]:
    root_folder = config['root_scan_folder']
    if not os.path.isdir(root_folder): return []
    
    folder_cache = FolderStateCacheManager(root_folder)
    _update_progress(progress_queue, text=f"📂 已載入 {len(folder_cache.cache)} 筆資料夾快取。")

    is_full_scan = not config.get('enable_time_filter')
    excluded_paths = {os.path.normpath(f) for f in config.get('excluded_folders', [])}
    
    time_filter = {'enabled': config.get('enable_time_filter')}
    if time_filter['enabled']:
        try:
            start_str, end_str = config.get('start_date_filter'), config.get('end_date_filter')
            time_filter['start'] = datetime.datetime.strptime(start_str, "%Y-%m-%d") if start_str else None
            time_filter['end'] = datetime.datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if end_str else None
        except ValueError:
            log_error("時間篩選日期格式錯誤，將被忽略。")
            time_filter['enabled'] = False
            is_full_scan = True

    if is_full_scan:
        live_folders = _full_scan_traversal(root_folder, excluded_paths, progress_queue)
    else:
        live_folders = _incremental_scan_traversal(root_folder, excluded_paths, time_filter, progress_queue, control_events)

    if control_events and control_events['cancel'].is_set(): return []

    log_info(f"實體掃描資料夾總數：{len(live_folders)}")
    _update_progress(progress_queue, text=f"掃描完成，找到 {len(live_folders)} 個有效資料夾。正在比對快取...")

    cached_states = folder_cache.cache
    live_folder_set, cached_folder_set = set(live_folders.keys()), set(cached_states.keys())
    
    new_folders = live_folder_set - cached_folder_set
    deleted_folders = cached_folder_set - live_folder_set if is_full_scan else set()
    
    changed_folders = set()
    for path in live_folder_set.intersection(cached_folder_set):
        cached_entry = cached_states.get(path, {})
        old_mtime = cached_entry.get('mtime') if isinstance(cached_entry, dict) else cached_entry
        if old_mtime is None or abs(live_folders[path]['mtime'] - old_mtime) > 1e-6:
            changed_folders.add(path)

    unchanged_folders = live_folder_set - new_folders - changed_folders
    
    log_info(f"[資料夾快取] 模式: {'全量' if is_full_scan else '增量'} | 新/變更: {len(new_folders) + len(changed_folders)} | 未變更: {len(unchanged_folders)} | 待清理: {len(deleted_folders)}")
    _update_progress(progress_queue, text=f"快取比對完成，新/變更: {len(new_folders) + len(changed_folders)}，未變更: {len(unchanged_folders)}")

    if deleted_folders:
        folder_cache.remove_folders(list(deleted_folders))
        deleted_count = len(deleted_folders)
        for i, folder in enumerate(list(deleted_folders)):
            if control_events and control_events['cancel'].is_set(): break
            if (i+1) % 10 == 0 or (i+1) == deleted_count:
                _update_progress(progress_queue, text=f"正在清理過時快取...({i+1}/{deleted_count})")
            image_cache.remove_entries_from_folder(folder)
        if control_events and control_events['cancel'].is_set(): return []

    final_file_list = []
    exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    count, enable_limit = config['extract_count'], config['enable_extract_count_limit']
    
    folders_to_scan = sorted(list(new_folders.union(changed_folders)))
    files_from_scan = 0
    for folder in folders_to_scan:
        if control_events and control_events['cancel'].is_set(): break
        try:
            files_in_folder = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(exts) and os.path.isfile(os.path.join(folder, f))]
            files_in_folder.sort()
            
            # [核心修正] 在添加路徑時進行標準化
            to_extract = files_in_folder[-count:] if enable_limit else files_in_folder
            extracted = [os.path.normpath(p) for p in to_extract]
            
            final_file_list.extend(extracted)
            files_from_scan += len(extracted)
            folder_cache.update_folder_state(folder, live_folders[folder]['mtime'], live_folders[folder]['ctime'])
        except OSError: continue
    if control_events and control_events['cancel'].is_set(): return []

    files_from_cache = 0
    if unchanged_folders:
        _update_progress(progress_queue, text=f"從快取讀取 {len(unchanged_folders)} 個資料夾的檔案列表...")
        norm_unchanged_paths = {os.path.normpath(p) for p in unchanged_folders}
        unchanged_files_by_folder = defaultdict(list)
        for path in image_cache.cache.keys():
            parent_dir = os.path.normpath(os.path.dirname(path))
            if parent_dir in norm_unchanged_paths:
                unchanged_files_by_folder[parent_dir].append(path)
        
        for folder, files in unchanged_files_by_folder.items():
            files.sort()

            # [核心修正] 從快取恢復路徑時也進行標準化
            to_extract = files[-count:] if enable_limit else files
            extracted = [os.path.normpath(p) for p in to_extract]
            
            final_file_list.extend(extracted)
            files_from_cache += len(extracted)

    folder_cache.save_cache()
    # image_cache.save_cache() # 在此函數中此行是多餘的，已在後面處理

    log_info(f"檔案提取完成。從掃描獲取: {files_from_scan}，從快取恢復: {files_from_cache}。總計: {len(final_file_list)}")
    _update_progress(progress_queue, text=f"檔案提取完成，共 {len(final_file_list)} 個檔案待處理。")
    
    # [最終保障] 返回前確保整個列表是唯一的且標準化的
    return sorted(list(set(final_file_list)))
    #####

    if unchanged_folders:
        _update_progress(progress_queue, text=f"從快取讀取 {len(unchanged_folders)} 個資料夾的檔案列表...")
        norm_unchanged_paths = {os.path.normpath(p) for p in unchanged_folders}
        unchanged_files_by_folder = defaultdict(list)
        for path in image_cache.cache.keys():
            parent_dir = os.path.normpath(os.path.dirname(path))
            if parent_dir in norm_unchanged_paths:
                unchanged_files_by_folder[parent_dir].append(path)

        removed_any = False
        for folder, files in unchanged_files_by_folder.items():
            files.sort()

            # 先挑出要回填的清單（與你原本的 count / enable_limit 一致）
            candidates = files[-count:] if enable_limit else files

            # 對每個 path 檢查是否存在；不存在就從快取移除
            filtered = []
            for path in candidates:
                if os.path.exists(path):            # ← 這兩行就是你問的「對每個 path 加」
                    filtered.append(path)
                else:
                    image_cache.remove_data(path)   # ← 幽靈路徑同步從快取剔除
                    removed_any = True

            final_file_list.extend(filtered)
            files_from_cache += len(filtered)

        if removed_any:
            image_cache.save_cache()  # 有刪才存，避免多餘 I/O

    #####
    folder_cache.save_cache()
    image_cache.save_cache()
    
    log_info(f"檔案提取完成。從掃描獲取: {files_from_scan}，從快取恢復: {files_from_cache}。總計: {len(final_file_list)}")
    _update_progress(progress_queue, text=f"檔案提取完成，共 {len(final_file_list)} 個檔案待處理。")
    return final_file_list
###
# === 9. 核心比對引擎 ===
class ImageComparisonEngine:
    def __init__(self, config: dict, progress_queue: Queue | None = None, control_events: dict | None = None):
        self.config = config
        self.progress_queue = progress_queue
        self.control_events = control_events
        self.system_qr_scan_capability = QR_SCAN_ENABLED
        self.pool = None
        
        self.file_data = {}
        self.tasks_to_process = []
        self.total_task_count = 0
        self.completed_task_count = 0
        self.failed_tasks = []
        
        log_performance("[初始化] 掃描引擎實例")

    def _check_control(self) -> str:
        if self.control_events:
            if self.control_events['cancel'].is_set(): return 'cancel'
            if self.control_events['pause'].is_set(): return 'pause'
        return 'continue'

    def _update_progress(self, p_type: str = 'text', value: int | None = None, text: str | None = None) -> None:
        if self.progress_queue: self.progress_queue.put({'type': p_type, 'value': value, 'text': text})

    def _cleanup_pool(self):
        if self.pool:
            log_info("正在終結現有進程池...")
            self.progress_queue.put({'type': 'status_update', 'text': "正在終止背景任務..."})
            self.pool.terminate()
            self.pool.join()
            log_info("進程池已成功終結。")
            self.pool = None
            self.progress_queue.put({'type': 'status_update', 'text': "任務已暫停"})

    def find_duplicates(self) -> tuple[list, dict, list] | None:
        try:
            self._update_progress(text="任務開始...")
            log_performance("[開始] 掃描任務")
            
            # 根據模式決定根掃描資料夾和廣告資料夾路徑
            root_scan_folder = self.config['root_scan_folder']
            ad_folder_path = self.config.get('ad_folder_path') if self.config['comparison_mode'] in ['ad_comparison', 'qr_detection'] else None
            scan_cache_manager = ScannedImageCacheManager(root_scan_folder, ad_folder_path)
            
            if not self.tasks_to_process:
                initial_files = get_files_to_process(self.config, scan_cache_manager, self.progress_queue, self.control_events)
                if self.control_events and self.control_events['cancel'].is_set(): return None

                self.tasks_to_process = initial_files
                self.total_task_count = len(initial_files)
                self.completed_task_count = 0
                self.file_data = {}
                self.failed_tasks = []
            else:
                log_info(f"從上次暫停點恢復，剩餘 {len(self.tasks_to_process)} 個檔案待處理。")
            
            if not self.tasks_to_process:
                self._update_progress(text="在指定路徑下未找到任何圖片檔案。")
                return [], {}, []
            
            if self.config['comparison_mode'] == "qr_detection":
                result = self._detect_qr_codes_pure(self.tasks_to_process, scan_cache_manager) if not self.config.get('enable_qr_hybrid_mode') else self._detect_qr_codes_hybrid(self.tasks_to_process, scan_cache_manager)
            else:
                result = self._find_similar_images(self.tasks_to_process, scan_cache_manager)

            if result is None: return None
            
            found, data = result
            return found, data, self.failed_tasks
        
        finally:
            self._cleanup_pool()

    def _process_images_with_cache(self, current_task_list: list[str], cache_manager: ScannedImageCacheManager, description: str, worker_function: callable, data_key: str) -> bool:
        if not current_task_list: return True
        
        ux_delay = self.config.get('ux_scan_start_delay', 0.1)
        time.sleep(ux_delay)
        
        self._update_progress(text=f"📂 正在檢查 {len(current_task_list)} 個{description}的快取...")
        paths_to_recalc, cache_hits = [], 0
        for path in current_task_list:
            try:
                cached_data = cache_manager.get_data(path)
                
                # 檢查快取是否有效 (哈希鍵存在且文件時間戳匹配)
                if cached_data and data_key in cached_data and cached_data[data_key] and \
                   abs(os.path.getmtime(path) - cached_data.get('mtime', 0)) < 1e-6:
                    self.file_data[path] = cached_data
                    cache_hits += 1
                    self.completed_task_count += 1
                else:
                    paths_to_recalc.append(path)
                    if cached_data: self.file_data[path] = cached_data # 保留舊快取中的其他數據
            except FileNotFoundError:
                log_info(f"檔案在處理過程中被移除: {path}")
                try:
                    cache_manager.remove_data(path)
                except Exception: 
                    pass
                self.total_task_count = max(0, self.total_task_count - 1)
                continue

        if self.total_task_count > 0:
            log_info(f"圖片哈希快取檢查 - 命中: {cache_hits}/{len(current_task_list)} | 總體進度: {self.completed_task_count}/{self.total_task_count}")
            self._update_progress(text=f"📂 快取命中：{cache_hits} 張圖片")
        
        if not paths_to_recalc:
            log_performance(f"[完成] {description}計算 (無新檔案)")
            cache_manager.save_cache()
            return True

        user_proc_setting = self.config.get('worker_processes', 0)
        is_qr_mode = self.config.get('comparison_mode') == 'qr_detection'
        if user_proc_setting == 0:
            pool_size = max(1, min(cpu_count() - 2, 12)) if is_qr_mode else max(1, min(cpu_count() // 2, 8))
        else:
            pool_size = max(1, min(user_proc_setting, cpu_count()))
        
        if not self.pool:
            log_info(f"創建一個新的進程池，大小為 {pool_size}...")
            self.pool = Pool(processes=pool_size)

        self._update_progress(text=f"⚙️ 使用 {pool_size} 進程計算 {len(paths_to_recalc)} 個新檔案...")
        
        async_results = []
        path_map = {}
        # 根據 worker_function 的名稱來決定是否傳遞額外參數
        worker_args = {}
        if 'full' in worker_function.__name__ or 'qr_code' in worker_function.__name__:
            worker_args['resize_size'] = self.config.get('qr_resize_size', 800)

        for path in paths_to_recalc:
            res = self.pool.apply_async(worker_function, args=(path,), kwds=worker_args)
            async_results.append(res)
            path_map[res] = path
        
        while async_results:
            control_action = self._check_control()
            if control_action in ['cancel', 'pause']:
                uncompleted_paths = [path_map[res] for res in async_results if not res.ready()]
                log_info(f"檢測到 '{control_action}' 信號。剩餘 {len(uncompleted_paths)} 個任務未完成。")
                if control_action == 'pause':
                    self.tasks_to_process = uncompleted_paths
                self._cleanup_pool()
                return False

            remaining_results = []
            for res in async_results:
                if res.ready():
                    try:
                        path, data = res.get()
                        if data.get('error'):
                            self.failed_tasks.append((path, data['error']))
                        else:
                            # 更新 self.file_data 並讓 cache_manager 知道數據已更新
                            self.file_data.setdefault(path, {}).update(data)
                            cache_manager.update_data(path, self.file_data[path])
                        self.completed_task_count += 1
                    except Exception as e:
                        path = path_map.get(res, "未知路徑")
                        error_msg = f"從子進程獲取結果失敗: {e}"
                        log_error(error_msg, True)
                        self.failed_tasks.append((path, error_msg))
                        self.completed_task_count += 1
                else:
                    remaining_results.append(res)
            
            async_results = remaining_results
            
            if self.total_task_count > 0:
                current_progress = int(self.completed_task_count / self.total_task_count * 100)
                self._update_progress(p_type='progress', value=current_progress, text=f"⚙️ 計算{description}中... ({self.completed_task_count}/{self.total_task_count})")

            time.sleep(0.05)
        
        log_performance(f"[完成] {description}計算")
        cache_manager.save_cache()
        return True

    def _build_phash_band_index(self, gallery_file_data: dict, bands=LSH_BANDS):
        seg_bits = HASH_BITS // bands
        mask = (1 << seg_bits) - 1
        index = [defaultdict(list) for _ in range(bands)]
        
        for path, ent in gallery_file_data.items():
            phash_obj = ent.get('phash')
            if not phash_obj: continue
            try:
                v = int(str(phash_obj), 16)
            except (ValueError, TypeError):
                continue
            
            for b in range(bands):
                key = (v >> (b * seg_bits)) & mask
                index[b][key].append(path)
        return index

    def _lsh_candidates_for(self, ad_path: str, ad_hash_obj: imagehash.ImageHash, index: list, bands=LSH_BANDS):
        seg_bits = HASH_BITS // bands
        mask = (1 << seg_bits) - 1
        v = int(str(ad_hash_obj), 16)
        cand = set()
        for b in range(bands):
            key = (v >> (b * seg_bits)) & mask
            cand.update(index[b].get(key, []))
        if ad_path in cand:
            cand.remove(ad_path)
        return cand

    def _get_or_compute_whash(self, path: str, image_cache: ScannedImageCacheManager) -> imagehash.ImageHash | None:
        ent = image_cache.get_data(path) or {}
        whash_obj = ent.get('whash')
        if whash_obj and isinstance(whash_obj, imagehash.ImageHash):
            return whash_obj
            
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img)
                wh = imagehash.whash(img, hash_size=8, mode='haar', remove_max_haar_ll=True)
            image_cache.update_data(path, {'whash': wh, 'mtime': os.path.getmtime(path)})
            # 同時更新自身的 file_data
            self.file_data.setdefault(path, {}).update({'whash': wh})
            return wh
        except Exception as e:
            log_error(f"懶算 wHash 失敗: {path}: {e}")
            return None

    def _accept_pair_with_dual_hash(self, ad_hash_obj: imagehash.ImageHash, g_hash_obj: imagehash.ImageHash, ad_w_hash: imagehash.ImageHash, g_w_hash: imagehash.ImageHash) -> tuple[bool, float]:
        d_p = ad_hash_obj - g_hash_obj
        sim_p = sim_from_hamming(d_p)

        if sim_p < PHASH_FAST_THRESH: return False, sim_p
        if sim_p >= PHASH_STRICT_SKIP: return True, sim_p

        if not (ad_w_hash and g_w_hash): return False, sim_p
        
        d_w = ad_w_hash - g_w_hash
        sim_w = sim_from_hamming(d_w)

        if sim_p >= 0.90:
            ok = sim_w >= WHASH_TIER_1
        elif sim_p >= 0.88:
            ok = sim_w >= WHASH_TIER_2
        elif sim_p >= 0.85:
            ok = sim_w >= WHASH_TIER_3
        else: # 此處現在只處理 0.80 <= sim_p < 0.85 的區間
            ok = sim_w >= WHASH_TIER_4
        
        return (ok, min(sim_p, sim_w) if ok else sim_p)
##12
    def _find_similar_images(self, target_files: list[str], scan_cache_manager: ScannedImageCacheManager) -> tuple[list, dict] | None:
        # === 步驟 1: 獨立掃描主要漫畫文件 (Gallery) ===
        if not self._process_images_with_cache(target_files, scan_cache_manager, "目標雜湊", _pool_worker_process_image_phash_only, 'phash'):
            return None
        gallery_data = self.file_data.copy()

        # === 步驟 2: 獨立掃描並預處理廣告文件 (Ads) ===
        ad_data = {}
        ad_cache_manager = None
        leader_to_ad_group = {} # 用於存儲廣告組

        if self.config['comparison_mode'] == 'ad_comparison':
            ad_folder_path = self.config['ad_folder_path']
            if not os.path.isdir(ad_folder_path):
                self._update_progress(text="錯誤：廣告圖片資料夾無效。"); return [], {}
            
            ad_paths = [os.path.normpath(os.path.join(r, f)) for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg','.webp'))]
            ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
            
            self.file_data.clear()
            if not self._process_images_with_cache(ad_paths, ad_cache_manager, "廣告圖片雜湊", _pool_worker_process_image_phash_only, 'phash'):
                return None
            ad_data = self.file_data.copy()

            # --- [核心修正] 廣告預處理與分組 ---
            self._update_progress(text="🔍 正在預處理廣告庫，分組相似廣告...")
            ad_path_to_leader = {path: path for path in ad_data}
            ad_paths_sorted = sorted(list(ad_data.keys()))
            
            # [核心修正] 使用更合理的 PHASH_STRICT_SKIP (93%) 作為廣告分組閾值
            grouping_threshold = hamming_from_sim(PHASH_STRICT_SKIP)
            
            # 使用一個較高的固定閾值 (95%) 來分組廣告
            #grouping_threshold = hamming_from_sim(0.95)
            for i in range(len(ad_paths_sorted)):
                for j in range(i + 1, len(ad_paths_sorted)):
                    p1_path, p2_path = ad_paths_sorted[i], ad_paths_sorted[j]
                    h1, h2 = ad_data.get(p1_path, {}).get('phash'), ad_data.get(p2_path, {}).get('phash')

                    if h1 and h2 and (h1 - h2) <= grouping_threshold:
                        leader1, leader2 = ad_path_to_leader[p1_path], ad_path_to_leader[p2_path]
                        final_leader = min(leader1, leader2)
                        # 更新並查集
                        for path, leader in list(ad_path_to_leader.items()):
                            if leader == leader1 or leader == leader2:
                                ad_path_to_leader[path] = final_leader
            
            # 建立從領隊到整個群組的映射
            for path, leader in ad_path_to_leader.items():
                if leader not in leader_to_ad_group:
                    leader_to_ad_group[leader] = []
                leader_to_ad_group[leader].append(path)
            
            # 用於比對的 "ad_data" 現在是領隊的集合
            ad_data_representatives = {path: data for path, data in ad_data.items() if path in leader_to_ad_group}
            self._update_progress(text=f"🔍 廣告庫預處理完成，找到 {len(ad_data_representatives)} 個獨立廣告組。")
            # --- 修正結束 ---

        elif self.config['comparison_mode'] == 'mutual_comparison':
            ad_data_representatives = gallery_data

        # === 步驟 3: 建立 LSH 索引 ===
        self._update_progress(text="🔍 建立 LSH 索引中...")
        phash_index = self._build_phash_band_index(gallery_data)

        temp_found_pairs = []
        user_thresh = self.config.get('similarity_threshold', 95.0) / 100.0
        total_ad_count = len(ad_data_representatives)
        
        log_performance("[開始] LSH 雙哈希比對階段")
        
        # === 步驟 4: 在隔離的、乾淨的數據集上執行比對 ===
        for i, (ad_leader_path, ad_ent) in enumerate(ad_data_representatives.items()):
            if self._check_control() != 'continue': return None
            if (i + 1) % 50 == 0:
                self._update_progress(p_type='progress', value=int((i+1)/total_ad_count*100), text=f"🔍 雙哈希 LSH 比對中... ({i+1}/{total_ad_count})")

            ad_p_hash = ad_ent.get('phash')
            if not ad_p_hash: continue
            
            candidate_paths = self._lsh_candidates_for(ad_leader_path, ad_p_hash, phash_index)

            for g_path in candidate_paths:
                if self.config['comparison_mode'] == 'mutual_comparison' and g_path <= ad_leader_path:
                    continue
                if self.config['comparison_mode'] == 'ad_comparison' and g_path in ad_data:
                    continue

                g_ent = gallery_data.get(g_path)
                if not g_ent or not g_ent.get('phash'): continue
                
                # --- [核心修正] 集團搜索邏輯 ---
                is_match_found = False
                best_sim_val = 0.0
                
                # 獲取當前領隊代表的整個廣告組
                ad_group_paths = leader_to_ad_group.get(ad_leader_path, [ad_leader_path])
                
                for ad_member_path in ad_group_paths:
                    # 在廣告比對模式下，ad_data 來自 ad_cache_manager
                    # 在互相比對模式下，ad_data 來自 gallery_data (scan_cache_manager)
                    current_ad_cache = ad_cache_manager if self.config['comparison_mode'] == 'ad_comparison' else scan_cache_manager
                    ad_member_ent = current_ad_cache.get_data(ad_member_path)
                    if not ad_member_ent: continue
                    
                    ad_member_p_hash = ad_member_ent.get('phash')
                    g_p_hash = g_ent.get('phash')

                    if not ad_member_p_hash or not g_p_hash: continue

                    d_p = ad_member_p_hash - g_p_hash
                    sim_p = sim_from_hamming(d_p)

                    if sim_p < PHASH_FAST_THRESH: continue
                    
                    is_accepted, final_sim_val = True, sim_p
                    if sim_p < PHASH_STRICT_SKIP:
                        ad_member_w_hash = self._get_or_compute_whash(ad_member_path, current_ad_cache)
                        g_w_hash = self._get_or_compute_whash(g_path, scan_cache_manager)
                        is_accepted, final_sim_val = self._accept_pair_with_dual_hash(ad_member_p_hash, g_p_hash, ad_member_w_hash, g_w_hash)

                    if is_accepted and final_sim_val >= user_thresh:
                        is_match_found = True
                        best_sim_val = max(best_sim_val, final_sim_val) # 記錄最高的相似度
                
                if is_match_found:
                    if self.config['comparison_mode'] == 'ad_comparison':
                        temp_found_pairs.append((ad_leader_path, g_path, f"{best_sim_val * 100:.1f}%"))
                    else: # mutual_comparison
                        temp_found_pairs.append((min(ad_leader_path, g_path), max(ad_leader_path, g_path), f"{best_sim_val * 100:.1f}%"))
                # --- 修正結束 ---

        # === 步驟 5: 後處理與返回 ===
        if self.config['comparison_mode'] == 'mutual_comparison':
            path_to_group_leader = {}
            for p1, p2, _ in sorted(temp_found_pairs):
                leader1 = path_to_group_leader.get(p1, p1)
                leader2 = path_to_group_leader.get(p2, p2)
                final_leader = min(leader1, leader2)
                for path, leader in list(path_to_group_leader.items()):
                    if leader == leader1 or leader == leader2:
                         path_to_group_leader[path] = final_leader
                path_to_group_leader[p1] = final_leader
                path_to_group_leader[p2] = final_leader
            
            final_groups = defaultdict(list)
            all_paths_in_pairs = set(p for pair in temp_found_pairs for p in pair[:2])
            for path in all_paths_in_pairs:
                leader = path_to_group_leader.get(path, path)
                if path != leader:
                    final_groups[leader].append(path)

            found_items = []
            for leader, children in final_groups.items():
                for child in set(children):
                    leader_hash = gallery_data.get(leader, {}).get('phash')
                    child_hash = gallery_data.get(child, {}).get('phash')
                    if leader_hash and child_hash:
                        sim = sim_from_hamming(leader_hash - child_hash) * 100
                        found_items.append((leader, child, f"{sim:.1f}%"))
        else: # 廣告比對模式
            found_items = temp_found_pairs

        scan_cache_manager.save_cache()
        if ad_cache_manager:
            ad_cache_manager.save_cache()
            
        log_performance("[完成] LSH 雙哈希比對階段")
        
        self.file_data = {**gallery_data, **ad_data}
        return found_items, self.file_data
##12
    def _detect_qr_codes_pure(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> tuple[list, dict] | None:
        if not self._process_images_with_cache(files_to_process, scan_cache_manager, "QR Code 檢測", _pool_worker_detect_qr_code, 'qr_points'):
            return None
        
        found_qr_images = []
        for image_path, data in self.file_data.items():
            if data and data.get('qr_points'):
                found_qr_images.append((image_path, image_path, "QR Code 檢出"))
        return found_qr_images, self.file_data
##12
    def _detect_qr_codes_hybrid(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> tuple[list, dict] | None:
        ad_folder_path = self.config['ad_folder_path']
        if not os.path.isdir(ad_folder_path):
            self._update_progress(text="混合模式錯誤：廣告資料夾無效。轉為純粹 QR 掃描...")
            log_info("退回純 QR 掃描，因廣告資料夾無效。")
            return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        
        # [核心修正] 在此處對掃描到的廣告路徑進行正規化
        ad_paths = [os.path.normpath(os.path.join(r, f)) for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg','.webp'))]
        ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
        
        # 廣告圖片需要 pHash 和 qr_points
        if not self._process_images_with_cache(ad_paths, ad_cache_manager, "廣告圖片屬性", _pool_worker_process_image_full, 'qr_points'):
            return None
        ad_file_data = self.file_data.copy()

        ad_hashes = {path: data['phash'] for path, data in ad_file_data.items() if data and data.get('phash')}
        if not ad_hashes:
            log_info("廣告資料夾無有效哈希，退回純 QR 掃描模式。")
            return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        self._update_progress(text=f"🧠 廣告快取載入完成 ({len(ad_hashes)} 筆)")

        # 目標圖片只需要 pHash
        self.file_data.clear()
        if not self._process_images_with_cache(files_to_process, scan_cache_manager, "目標雜湊", _pool_worker_process_image_phash_only, 'phash'):
            return None
        target_file_data = self.file_data.copy()

        found_items, remaining_files_for_qr = [], []
        max_diff = hamming_from_sim(PHASH_STRICT_SKIP)
        
        for path, data in target_file_data.items():
            if self._check_control() != 'continue': return None
            target_hash = data.get('phash')
            if not target_hash:
                remaining_files_for_qr.append(path); continue
            
            match_found = False
            for ad_path, ad_hash in ad_hashes.items():
                if ad_hash and target_hash - ad_hash <= max_diff:
                    ad_has_qr = ad_file_data.get(ad_path, {}).get('qr_points')
                    if ad_has_qr:
                        found_items.append((ad_path, path, "廣告匹配(快速)"))
                        target_file_data.setdefault(path, {})['qr_points'] = ad_file_data[ad_path]['qr_points']
                        match_found = True
                        break 
            if not match_found:
                remaining_files_for_qr.append(path)
        
        ad_match_count = len(found_items)
        self._update_progress(text=f"快速匹配完成，找到 {ad_match_count} 個廣告。對 {len(remaining_files_for_qr)} 個檔案進行 QR 掃描...")
        
        if remaining_files_for_qr:
            if self._check_control() != 'continue': return None
            self.file_data = {p: d for p, d in target_file_data.items() if p in remaining_files_for_qr}
            qr_result_tuple = self._detect_qr_codes_pure(remaining_files_for_qr, scan_cache_manager)
            if qr_result_tuple is None: return None
            
            qr_results, qr_data = qr_result_tuple
            found_items.extend(qr_results)
            target_file_data.update(qr_data)

        self.file_data = {**ad_file_data, **target_file_data}
        return found_items, self.file_data
##12
#接續14.0.0第二部分

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
                # 根據新邏輯實例化
                cache_manager = ScannedImageCacheManager(root_scan_folder, ad_folder_path)
                cache_manager.invalidate_cache()
                if ad_folder_path and os.path.isdir(ad_folder_path):
                    # 廣告庫自己有獨立的快取
                    ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
                    ad_cache_manager.invalidate_cache()
                messagebox.showinfo("清理成功", "所有相關圖片快取檔案已移至回收桶。", parent=self)
            except Exception as e:
                log_error(f"清理圖片快取時發生錯誤: {e}", True)
                messagebox.showerror("清理失敗", f"清理圖片快取時發生錯誤：\n{e}", parent=self)

    def _clear_folder_cache(self):
        root_scan_folder = self.root_scan_folder_entry.get().strip()
        if not root_scan_folder:
            messagebox.showwarning("無法清理", "請先在「路徑設定」中指定根掃描資料夾。", parent=self)
            return
        if messagebox.askyesno("確認清理", "確定要將資料夾狀態快取移至回收桶嗎？\n下次掃描時將會重新掃描所有資料夾的結構。", parent=self):
            try:
                cache_manager = FolderStateCacheManager(root_scan_folder)
                cache_manager.invalidate_cache()
                messagebox.showinfo("清理成功", "資料夾狀態快取檔案已移至回收桶。", parent=self)
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
                # --- 新的、更健-壯的程式碼 ---
                'similarity_threshold': float(self.similarity_threshold_var.get()) if self.similarity_threshold_var.get() else 95.0,
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
                    if config['start_date_filter']: datetime.datetime.strptime(config['start_date_filter'], "%Y-%m-%d")
                    if config['end_date_filter']: datetime.datetime.strptime(config['end_date_filter'], "%Y-%m-%d")
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

from concurrent.futures import ThreadPoolExecutor

class MainWindow(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = load_config(CONFIG_FILE)
        
        self.all_found_items, self.all_file_data = [], {}
        self.selected_files, self.banned_groups = set(), set()
        
        # 狀態變數
        self.protected_paths = set()
        self.child_to_parent = {}
        self.parent_to_children = defaultdict(list)
        self.item_to_path = {}

        self.pil_img_target, self.pil_img_compare = None, None
        self.img_tk_target, self.img_tk_compare = None, None
        self.scan_thread, self._after_id = None, None
        self.cancel_event, self.pause_event = threading.Event(), threading.Event()
        
        self.scan_queue, self.preview_queue = Queue(), Queue()
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.sorted_groups = []
        self.current_page = 0
        self.page_size = 100
        self.is_loading_page = False
        self._preview_delay = 150

        self.scan_start_time = None
        self.final_status_text = ""
        self._widgets_initialized = False
        
        self.engine_instance = None
        self.is_paused = False

        self._setup_main_window()
        
    def deiconify(self):
        super().deiconify()
        if not self._widgets_initialized:
            self._init_widgets()
            self._check_queues()

    def _setup_main_window(self) -> None:
        self.title(f"{APP_NAME_TC} v{APP_VERSION}")
        self.geometry("1600x900")
        
        self.update_idletasks()
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        final_y = max(20, y - 50)
        self.geometry(f'{width}x{height}+{x}+{final_y}')

        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        sys.excepthook = self.custom_excepthook
        
    def _init_widgets(self):
        if self._widgets_initialized:
            return
        self.bold_font = self._create_bold_font()
        self._create_widgets()
        self._bind_keys()
        self._widgets_initialized = True

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
        style = ttk.Style(self)
        style.configure("Accent.TButton", font=self.bold_font, foreground='blue')
        style.configure("Danger.TButton", font=self.bold_font, foreground='red')
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
        columns=("status","filename","path","count","size","ctime","similarity")
        self.tree=ttk.Treeview(parent_frame,columns=columns,show="tree headings",selectmode="extended")
        
        self.tree.heading("#0", text="", anchor='center')
        self.tree.column("#0", width=25, stretch=False, anchor='center')

        headings={"status":"狀態","filename":"群組/圖片","path":"路徑","count":"數量","size":"大小","ctime":"建立日期","similarity":"相似度/類型"}; widths={"status":40,"filename":300,"path":300,"count":50,"size":100,"ctime":150,"similarity":80}
        for col,text in headings.items():self.tree.heading(col,text=text)
        for col,width in widths.items():self.tree.column(col,width=width,minwidth=width,stretch=(col in["filename","path"]))
        
        self.tree.tag_configure('child_item',foreground='#555555')
        self.tree.tag_configure('parent_item',font=self.bold_font)
        self.tree.tag_configure('parent_partial_selection', foreground='#00008B') # DarkBlue for partial state
        self.tree.tag_configure('qr_item', background='#E0FFFF')
        self.tree.tag_configure('protected_item', background='#FFFACD')
        
        vscroll=ttk.Scrollbar(parent_frame,orient="vertical",command=self.tree.yview);self.tree.configure(yscrollcommand=vscroll.set)
        vscroll.bind("<B1-Motion>", self._on_scroll)
        self.tree.pack(side=tk.LEFT,fill=tk.BOTH,expand=True);vscroll.pack(side=tk.RIGHT,fill=tk.Y)
#####
    # def _create_preview_panels(self, parent_frame: ttk.Frame) -> None:
        # right_pane=ttk.Panedwindow(parent_frame,orient=tk.VERTICAL);right_pane.pack(fill=tk.BOTH,expand=True)
        # self.target_image_frame=ttk.LabelFrame(right_pane,text="選中圖片預覽",padding="5");right_pane.add(self.target_image_frame,weight=1); self.target_image_label=ttk.Label(self.target_image_frame,cursor="hand2");self.target_image_label.pack(fill=tk.BOTH,expand=True); self.target_path_label=ttk.Label(self.target_image_frame,text="",wraplength=500);self.target_path_label.pack(fill=tk.X); self.target_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,True))
        # self.compare_image_frame=ttk.LabelFrame(right_pane,text="群組基準圖片預覽",padding="5");right_pane.add(self.compare_image_frame,weight=1); self.compare_image_label=ttk.Label(self.compare_image_frame,cursor="hand2");self.compare_image_label.pack(fill=tk.BOTH,expand=True); self.compare_path_label=ttk.Label(self.compare_image_frame,text="",wraplength=500);self.compare_path_label.pack(fill=tk.X); self.compare_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,False))
        # self.target_image_label.bind("<Configure>",self._on_preview_resize);self.compare_image_label.bind("<Configure>",self._on_preview_resize)
        # self._create_context_menu()
#####
    def _create_preview_panels(self, parent_frame: ttk.Frame) -> None:
        right_pane=ttk.Panedwindow(parent_frame,orient=tk.VERTICAL);right_pane.pack(fill=tk.BOTH,expand=True)
        
        # --- 創建選中圖片預覽面板 ---
        self.target_image_frame=ttk.LabelFrame(right_pane,text="選中圖片預覽",padding="5")
        right_pane.add(self.target_image_frame,weight=1)
        
        self.target_image_label=ttk.Label(self.target_image_frame,cursor="hand2")
        self.target_image_label.pack(fill=tk.BOTH,expand=True)
        
        # [核心修正] 創建一個固定高度的Frame來容納路徑標籤
        # 1. 延遲獲取字體，確保窗口已初始化
        try:
            # 使用正確的 font 模組
            label_font = font.nametofont(self.target_image_label.cget("font"))
            line_height = label_font.metrics("linespace")
        except tk.TclError:
            # Fallback for initial setup
            line_height = 16 
        path_frame_height = line_height * 2 + 4 # 兩行文字的高度再加一點邊距
        
        # 2. 創建Frame並設定固定高度
        target_path_container = tk.Frame(self.target_image_frame, height=path_frame_height)
        target_path_container.pack(fill=tk.X, expand=False, pady=(5,0))
        target_path_container.pack_propagate(False) # 关键：阻止Frame縮小以適應內容

        # 3. 將路徑標籤放入固定高度的Frame中
        self.target_path_label=ttk.Label(target_path_container,text="",wraplength=500, anchor="w", justify=tk.LEFT)
        self.target_path_label.pack(fill=tk.BOTH, expand=True)
        
        self.target_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,True))

        # --- 創建基準圖片預覽面板 (做同樣的修正) ---
        self.compare_image_frame=ttk.LabelFrame(right_pane,text="群組基準圖片預覽",padding="5")
        right_pane.add(self.compare_image_frame,weight=1)
        
        self.compare_image_label=ttk.Label(self.compare_image_frame,cursor="hand2")
        self.compare_image_label.pack(fill=tk.BOTH,expand=True)

        # 同样创建一个固定高度的Frame
        compare_path_container = tk.Frame(self.compare_image_frame, height=path_frame_height)
        compare_path_container.pack(fill=tk.X, expand=False, pady=(5,0))
        compare_path_container.pack_propagate(False) # 关键

        self.compare_path_label=ttk.Label(compare_path_container,text="",wraplength=500, anchor="w", justify=tk.LEFT)
        self.compare_path_label.pack(fill=tk.BOTH, expand=True)

        self.compare_image_label.bind("<Button-1>",lambda e:self._on_preview_image_click(e,False))
        
        # --- 綁定事件 ---
        self.target_image_label.bind("<Configure>",self._on_preview_resize)
        self.compare_image_label.bind("<Configure>",self._on_preview_resize)
        self._create_context_menu()

#####
    def _create_bottom_buttons(self, parent_frame: ttk.Frame) -> None:
        button_frame = ttk.Frame(parent_frame)
        button_frame.pack(side=tk.LEFT, padx=5, pady=5)

        ttk.Button(button_frame, text="全選", command=self._select_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="選取建議", command=self._select_suggested_for_deletion).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="取消全選", command=self._deselect_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="反選", command=self._invert_selection).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="刪除選中(回收桶)", command=self._delete_selected_from_disk).pack(side=tk.LEFT, padx=2)

        self.mark_new_ad_button = ttk.Button(button_frame, text="標記新廣告圖", command=self._mark_new_ads)
        self.move_to_ad_library_button = ttk.Button(button_frame, text="移入廣告庫", command=self._move_selected_to_ad_library)
        
        self.mark_new_ad_button.pack(side=tk.LEFT, padx=5)
        self.move_to_ad_library_button.pack(side=tk.LEFT, padx=2)
        self.mark_new_ad_button.pack_forget()

        actions_frame=ttk.Frame(parent_frame)
        actions_frame.pack(side=tk.RIGHT,padx=5,pady=5)
        ttk.Button(actions_frame,text="開啟選中資料夾",command=self._open_selected_folder_single).pack(side=tk.LEFT,padx=2)
        ttk.Button(actions_frame,text="開啟回收桶",command=self._open_recycle_bin).pack(side=tk.LEFT,padx=2)

    def _bind_keys(self) -> None:
        self.tree.bind("<<TreeviewSelect>>", self._on_item_select)
        self.tree.bind("<Button-1>", self._on_treeview_click)
        self.tree.bind("<Double-1>", self._on_treeview_double_click)
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<space>", self._toggle_selection_with_space)
        self.tree.bind("<Return>", self._handle_return_key)
        self.tree.bind("<Delete>", lambda e: self._delete_selected_from_disk())
        self.tree.bind("<BackSpace>", lambda e: self._delete_selected_from_disk())
        self.tree.bind("<Motion>", self._on_mouse_motion)
        self.tooltip = None


    def open_settings(self) -> None:
        self.settings_button.config(state=tk.DISABLED)
        SettingsGUI(self)
        self.settings_button.config(state=tk.NORMAL)

    def start_scan(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            messagebox.showwarning("正在執行", "掃描任務正在執行中。")
            return
        
        if not self.is_paused:
            self._reset_scan_state()
            self.scan_start_time = time.time()
        
        if self.engine_instance is None:
            log_info("創建新的 ImageComparisonEngine 實例。")
            self.engine_instance = ImageComparisonEngine(self.config.copy(), self.scan_queue, {'cancel': self.cancel_event, 'pause': self.pause_event})

        self.start_button.config(state=tk.DISABLED); self.settings_button.config(state=tk.NORMAL)
        self.pause_button.config(text="暫停", state=tk.NORMAL); self.cancel_button.config(state=tk.NORMAL)
        
        if not self.is_paused:
            self.tree.delete(*self.tree.get_children())
            self.all_found_items.clear()
        
        self.is_paused = False
        self.scan_thread = threading.Thread(target=self._run_scan_in_thread, daemon=True)
        self.scan_thread.start()

    def _reset_scan_state(self) -> None:
        self.final_status_text = "" 
        self.cancel_event.clear()
        self.pause_event.clear()
        self.is_paused = False
        self.engine_instance = None
        
        self.protected_paths.clear()
        self.child_to_parent.clear()
        self.parent_to_children.clear()
        self.item_to_path.clear()
        
    def cancel_scan(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askyesno("確認終止", "確定要終止目前的掃描任務嗎？"):
                log_info("使用者請求取消任務。")
                self.cancel_event.set()
                if self.is_paused:
                    self.pause_event.set()
                self.scan_thread.join(timeout=5)
                if self.scan_thread.is_alive():
                    log_error("掃描執行緒在取消後 5 秒內未正常終止。")

    def toggle_pause(self) -> None:
        if self.is_paused:
            log_info("使用者請求恢復任務。")
            self.pause_event.clear()
            self.pause_button.config(text="暫停")
            self.status_label.config(text="正在恢復任務...")
            self.start_scan()
        else:
            log_info("使用者請求暫停任務。")
            self.is_paused = True
            self.pause_event.set()
            self.pause_button.config(text="恢復")
            self.status_label.config(text="正在請求暫停...")

    def _reset_control_buttons(self, final_status_text: str = "任務完成") -> None:
        self.status_label.config(text=final_status_text)
        self.progress_bar['value'] = 0
        self.start_button.config(state=tk.NORMAL)
        self.settings_button.config(state=tk.NORMAL)
        self.pause_button.config(state=tk.DISABLED, text="暫停")
        self.cancel_button.config(state=tk.DISABLED)
        # 這裡不重置 self.engine_instance 等狀態，因為任務可能只是完成而不是被重置
#接續14.0.0第三部分

    def _check_queues(self) -> None:
        try:
            while True:
                msg = self.scan_queue.get_nowait()
                msg_type = msg.get('type')
                if msg_type == 'progress':
                    self.progress_bar['value'] = msg.get('value', 0)
                    if not self.is_paused:
                        self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'text':
                    if not self.is_paused:
                        self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'status_update':
                     self.status_label['text'] = msg.get('text', '')
                elif msg_type == 'result':
                    self.all_found_items, self.all_file_data, failed_tasks = msg.get('data', []), msg.get('meta', {}), msg.get('errors', [])
                    self._process_scan_results(failed_tasks)
                elif msg_type == 'finish':
                    self.final_status_text = msg.get('text', '任務完成')
                    self._reset_control_buttons(self.final_status_text)
                    if self.scan_start_time:
                        duration = time.time() - self.scan_start_time
                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        log_performance(f"[{now}] 掃描任務完成，總耗時: {duration:.2f} 秒。")
                    if not self.all_found_items and "取消" not in self.final_status_text and "暫停" not in self.final_status_text:
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

    def _run_scan_in_thread(self) -> None:
        try:
            result = self.engine_instance.find_duplicates()
            
            if result is None:
                if self.cancel_event.is_set():
                    self.scan_queue.put({'type': 'finish', 'text': "任務已取消"})
                else: # Paused
                    self.scan_queue.put({'type': 'status_update', 'text': "任務已暫停"})
                return

            found_items, all_file_data, failed_tasks = result
            self.scan_queue.put({'type': 'result', 'data': found_items, 'meta': all_file_data, 'errors': failed_tasks})
            
            unique_targets = len(set(p[1] for p in found_items))
            base_text = f"✅ 掃描完成！找到 {unique_targets} 個不重複的目標。" if self.config['comparison_mode'] != 'qr_detection' else f"✅ 掃描完成！共找到 {len(found_items)} 個目標。"
            
            if failed_tasks:
                error_message = f" (有 {len(failed_tasks)} 張圖片處理失敗，詳情請見 error_log.txt)"
                final_text = base_text + error_message
                log_info(f"下列 {len(failed_tasks)} 個檔案處理失敗：")
                for path, error in failed_tasks:
                    log_error(f"檔案: {path}, 錯誤: {error}")
            else:
                final_text = base_text

            self.scan_queue.put({'type': 'finish', 'text': final_text})
        except Exception as e:
            log_error(f"核心邏輯執行失敗: {e}", True)
            self.scan_queue.put({'type': 'finish', 'text': f"執行錯誤: {e}"})
            if self.winfo_exists():
                messagebox.showerror("執行錯誤", f"程式執行時發生錯誤: {e}")

    def _process_scan_results(self, failed_tasks: list) -> None:
        self.tree.delete(*self.tree.get_children())
        
        # Keep selected_files, but clear UI-related state
        self._reset_scan_state()
        # Repopulate selected_files in case it was a re-process
        # selected_files should be persistent across re-draws unless a new scan starts
        
        groups = defaultdict(list)
        for group_key, item_path, value_str in self.all_found_items:
            groups[group_key].append((item_path, value_str))
        
        self.sorted_groups = sorted(groups.items(), key=lambda item: item[0])
        
        is_ad_mode = self.config['comparison_mode'] == 'ad_comparison'
        is_hybrid_qr = self.config['comparison_mode'] == 'qr_detection' and self.config['enable_qr_hybrid_mode']
        if is_ad_mode or is_hybrid_qr:
            for group_key, items in self.sorted_groups:
                is_ad_match = any(item[1] == "廣告匹配(快速)" for item in items)
                if is_ad_mode or is_ad_match:
                    self.protected_paths.add(group_key)

        self.is_loading_page = False
        self.current_page = 0
        self._load_next_page()
        
        if self.config.get('comparison_mode') == 'qr_detection' and not self.config['enable_qr_hybrid_mode']:
            self.mark_new_ad_button.pack(side=tk.LEFT, padx=5)
        else:
            self.mark_new_ad_button.pack_forget()

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
        mode = self.config['comparison_mode']
        
        for group_key, items in groups_to_load:
            if group_key in self.banned_groups: continue

            # Handle simple QR code list items (no parent)
            is_qr_item = items and items[0][1] == "QR Code 檢出"
            if is_qr_item and mode == 'qr_detection' and not self.config.get('enable_qr_hybrid_mode'):
                item_id = f"item_{uid}"; uid += 1
                p_data = self.all_file_data.get(group_key, {})
                p_size = f"{p_data.get('size', 0):,}" if 'size' in p_data else "N/A"
                p_ctime = datetime.datetime.fromtimestamp(p_data.get('ctime')).strftime('%Y/%m/%d %H:%M') if p_data.get('ctime') else "N/A"
                
                is_selected = group_key in self.selected_files
                status_char = "☑" if is_selected else "☐"

                self.tree.insert("", "end", iid=item_id, values=(status_char, os.path.basename(group_key), group_key, "", p_size, p_ctime, items[0][1]), tags=('qr_item',))
                self.item_to_path[item_id] = group_key
                continue

            # Handle parent-child groups
            parent_id = f"group_{uid}"; uid += 1
            
            if mode == 'mutual_comparison':
                display_list = [(group_key, "基準 (自身)")] + sorted(items, key=lambda x: x[0])
            else: # ad_comparison or hybrid_qr
                display_list = [(group_key, "基準廣告")] + sorted(items, key=lambda x: x[0])
            
            group_title = os.path.basename(group_key)
            count = len(display_list)
            self.tree.insert("", "end", iid=parent_id, open=True,
                             values=("", group_title, "", count, "", "", ""), 
                             tags=('parent_item',))
            
            for path, value_str in display_list:
                child_id = f"item_{uid}"; uid += 1
                tags = ['child_item']
                
                is_protected = path in self.protected_paths
                if is_protected:
                    tags.append('protected_item')

                c_data = self.all_file_data.get(path, {})
                c_size = f"{c_data.get('size', 0):,}" if 'size' in c_data else "N/A"
                c_ctime = datetime.datetime.fromtimestamp(c_data.get('ctime')).strftime('%Y/%m/%d %H:%M') if c_data.get('ctime') else "N/A"
                
                is_selected = path in self.selected_files
                status_char = "🔒" if is_protected else ("☑" if is_selected else "☐")
                
                self.tree.insert(parent_id, "end", iid=child_id, 
                                 values=(status_char, f"  └─ {os.path.basename(path)}", path, "", c_size, c_ctime, value_str), 
                                 tags=tuple(tags))
                
                # Populate mappings
                self.child_to_parent[child_id] = parent_id
                self.parent_to_children[parent_id].append(child_id)
                self.item_to_path[child_id] = path

            self._update_group_checkbox(parent_id)
            
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
        if not self.is_paused:
            self.status_label.config(text=f"正在載入第 {self.current_page + 1} 頁...")
        self._populate_treeview_page(self.current_page)
        self.current_page += 1

    def _on_treeview_click(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if not item_id or not self.tree.exists(item_id): return
        
        tags = self.tree.item(item_id, "tags")
        column = self.tree.identify_column(event.x)
        
        # Checkbox/Filename click area triggers selection
        if column in ("#1", "#2"): # Status or Filename column
            if 'parent_item' in tags:
                self._toggle_group_selection(item_id)
            elif 'child_item' in tags or 'qr_item' in tags:
                self._toggle_selection_by_item_id(item_id)
        # Any other click just focuses the item
        else:
            self.tree.selection_set(item_id)
            self.tree.focus(item_id)
######    
    # def _on_treeview_double_click(self, event: tk.Event) -> None:
        # item_id = self.tree.identify_row(event.y)
        # if not item_id or not self.tree.exists(item_id): return
        # if 'parent_item' in self.tree.item(item_id, "tags"):
            # self.tree.item(item_id, open=not self.tree.item(item_id, "open"))
###
    def _on_treeview_double_click(self, event: tk.Event) -> None:
        """處理在Treeview上的雙擊事件。"""
        # 識別雙擊發生的區域
        region = self.tree.identify_region(event.x, event.y)
        
        # 確保雙擊發生在一個有效的儲存格上
        if region == "cell":
            item_id = self.tree.identify_row(event.y)
            if not item_id: return

            # 情況一：如果雙擊在虛擬父項上，執行展開/收合 (此行為不變)
            if 'parent_item' in self.tree.item(item_id, "tags"):
                self.tree.item(item_id, open=not self.tree.item(item_id, "open"))
                return

            # 情況二：如果雙擊在子項或QR項的路徑欄上
            column_id = self.tree.identify_column(event.x)
            
            # Treeview的欄位ID從#1開始計數，"path"是我們的第3個顯示欄位
            if column_id == "#3": 
                path_value = self.tree.item(item_id, "values")[2]
                
                # [核心修正] 不再檢查檔案是否存在，而是直接處理路徑字串
                if path_value:
                    # 從路徑字串中獲取資料夾名稱。
                    # 這一步驟即使檔案已被刪除也能成功執行。
                    folder_path = os.path.dirname(path_value)
                    
                    # 作為一個健壯性檢查，我們只驗證目標“資料夾”是否存在。
                    if os.path.isdir(folder_path):
                        self._open_folder(folder_path)
                    else:
                        # 如果連資料夾都不存在了，記錄一下日誌，但不打擾使用者。
                        log_info(f"無法開啟資料夾，因為路徑 '{folder_path}' 不存在。")    
######
    def _handle_return_key(self, event: tk.Event) -> str:
        selected_ids = self.tree.selection()
        if not selected_ids: return "break"
        item_id = selected_ids[0]
        if 'parent_item' in self.tree.item(item_id, "tags"):
            self.tree.item(item_id, open=not self.tree.item(item_id, "open"))
        return "break"
        
    def _on_item_select(self, event: tk.Event) -> None:
        if self._after_id:
            self.after_cancel(self._after_id)
        self._after_id = self.after(self._preview_delay, self._trigger_async_preview)

####        

    def _trigger_async_preview(self) -> None:
        self._after_id = None
        selected = self.tree.selection()
        if not selected or not self.tree.exists(selected[0]):
            self.target_path_label.config(text="")
            self.compare_path_label.config(text="")
            self.pil_img_target = None
            self.pil_img_compare = None
            self._update_all_previews()
            return
            
        item_id = selected[0]
        
        preview_path, compare_path = None, None
        tags = self.tree.item(item_id, "tags")

        if 'parent_item' in tags:
            children = self.tree.get_children(item_id)
            if children:
                preview_path = self.item_to_path.get(children[0])
                compare_path = None # Don't show compare image when parent is selected
        else: # child or qr_item
            preview_path = self.item_to_path.get(item_id)
            parent_id = self.child_to_parent.get(item_id)
            if parent_id:
                base_child_id = self.tree.get_children(parent_id)[0]
                compare_path = self.item_to_path.get(base_child_id)

        if preview_path: 
            self.executor.submit(self._load_image_worker, preview_path, self.target_path_label, True)
        else:
            self.target_path_label.config(text="")
            self.pil_img_target = None
            self._update_all_previews()

        if compare_path:
            self.executor.submit(self._load_image_worker, compare_path, self.compare_path_label, False)
        else:
            self.compare_path_label.config(text="")
            self.pil_img_compare = None
            self._update_all_previews()
####
    def _load_image_worker(self, path: str, label_widget: tk.Label, is_target: bool) -> None:
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img).convert('RGB')
                self.preview_queue.put({'type': 'image_loaded', 'image': img.copy(), 'is_target': is_target})
                label_widget.after(0, lambda: label_widget.config(text=f"路徑: {path}"))
        except Exception as e:
            label_widget.after(0, lambda: label_widget.config(text=f"無法載入: {os.path.basename(path)}"))
            log_error(f"載入圖片預覽失敗 '{path}': {e}", True)
            self.preview_queue.put({'type': 'image_loaded', 'image': None, 'is_target': is_target})

    def _update_all_previews(self) -> None:
        self._resize_and_display(self.target_image_label, self.pil_img_target, True)
        self._resize_and_display(self.compare_image_label, self.pil_img_compare, False)

    def _on_preview_resize(self, event: tk.Event) -> None:
        try:
            is_target = (event.widget.master == self.target_image_frame)
            self._resize_and_display(event.widget, self.pil_img_target if is_target else self.pil_img_compare, is_target)
        except Exception as e:
            log_error(f"調整預覽面板大小時發生錯誤: {e}", True)

    def _resize_and_display(self, label: tk.Label, pil_image: Image.Image | None, is_target: bool) -> None:
        img_tk_ref = self.img_tk_target if is_target else self.img_tk_compare
        if not pil_image:
            label.config(image="")
            img_tk_ref = None
            return
        
        w, h = label.winfo_width(), label.winfo_height()
        if w <= 1 or h <= 1: return
        
        img_copy = pil_image.copy()
        img_copy.thumbnail((w - 10, h - 10), Image.Resampling.LANCZOS)
        img_tk = ImageTk.PhotoImage(img_copy)
        label.config(image=img_tk)
        
        if is_target: self.img_tk_target = img_tk
        else: self.img_tk_compare = img_tk
#####
    # def _on_preview_image_click(self, event: tk.Event, is_target_image: bool) -> None:
        # text = (self.target_path_label if is_target_image else self.compare_path_label).cget("text")
        # if text.startswith("路徑: "):
            # path = text[len("路徑: "):].strip()
            # if path and os.path.exists(path): self._open_folder(os.path.dirname(path))
#####
    def _on_preview_image_click(self, event: tk.Event, is_target_image: bool) -> None:
        text = (self.target_path_label if is_target_image else self.compare_path_label).cget("text")
        if text.startswith("路徑: "):
            path_value = text[len("路徑: "):].strip()
            
            # [核心修正] 與雙擊路徑的邏輯保持完全一致
            if path_value:
                # 直接從路徑字串獲取資料夾，即使檔案已不存在
                folder_path = os.path.dirname(path_value)
                
                # 只檢查資料夾是否存在
                if os.path.isdir(folder_path):
                    self._open_folder(folder_path)
                else:
                    log_info(f"無法開啟資料夾，因為路徑 '{folder_path}' 不存在。")
#####

####
    def _toggle_selection_by_item_id(self, item_id: str) -> None:
        tags = self.tree.item(item_id, "tags")
        if 'protected_item' in tags: return
        
        path = self.item_to_path.get(item_id)
        if not path: return

        if path in self.selected_files:
            self.selected_files.discard(path)
            self.tree.set(item_id, "status", "☐")
        else:
            self.selected_files.add(path)
            self.tree.set(item_id, "status", "☑")
        
        parent_id = self.child_to_parent.get(item_id)
        if parent_id:
            self._update_group_checkbox(parent_id)
######        

    def _toggle_group_selection(self, parent_id: str):
        children = self.parent_to_children.get(parent_id, [])
        if not children: return

        # 獲取本群組內所有“可勾選”的子項路徑
        selectable_paths_in_group = [
            self.item_to_path.get(child_id)
            for child_id in children
            if 'protected_item' not in self.tree.item(child_id, "tags") and self.item_to_path.get(child_id)
        ]
        if not selectable_paths_in_group: return

        # 計算本群組內“已勾選”的數量
        selected_count_in_group = sum(1 for path in selectable_paths_in_group if path in self.selected_files)

        # 判斷本群組是否已全選
        is_fully_selected = selected_count_in_group == len(selectable_paths_in_group)

        # 根據本群組的狀態，執行“純粹”的添加或移除操作
        if is_fully_selected:
            # 意圖：取消全選。從總列表中只移除本群組的路徑。
            for path in selectable_paths_in_group:
                self.selected_files.discard(path)
        else:
            # 意圖：全選。向總列表中只添加本群組的路徑。
            for path in selectable_paths_in_group:
                self.selected_files.add(path)

        # 最後，根據新的狀態刷新UI
        self._update_group_checkbox(parent_id)
######
    def _update_group_checkbox(self, parent_id: str):
        if not self.tree.exists(parent_id): return
        
        children = self.parent_to_children.get(parent_id, [])
        selectable_children = [
            child_id for child_id in children 
            if 'protected_item' not in self.tree.item(child_id, "tags")
        ]
        if not selectable_children: 
            self.tree.set(parent_id, "status", "") # No selectable children, no checkbox
            return

        selected_count = sum(1 for child_id in selectable_children if self.item_to_path.get(child_id) in self.selected_files)

        # Update children checkboxes first
        for child_id in selectable_children:
            path = self.item_to_path.get(child_id)
            self.tree.set(child_id, "status", "☑" if path in self.selected_files else "☐")

        # Update parent aggregate checkbox
        current_tags = list(self.tree.item(parent_id, "tags"))
        current_tags.remove('parent_partial_selection') if 'parent_partial_selection' in current_tags else None
        
        if selected_count == 0:
            self.tree.set(parent_id, "status", "☐")
        elif selected_count == len(selectable_children):
            self.tree.set(parent_id, "status", "☑")
        else: # Partial selection
            self.tree.set(parent_id, "status", "◪")
            current_tags.append('parent_partial_selection')
        
        self.tree.item(parent_id, tags=tuple(current_tags))

    def _toggle_selection_with_space(self, event: tk.Event) -> str:
        selected_ids = self.tree.selection()
        if not selected_ids: return "break"
        
        item_id = selected_ids[0]
        tags = self.tree.item(item_id, "tags")
        
        if 'parent_item' in tags:
            self._toggle_group_selection(item_id)
        else: 
            self._toggle_selection_by_item_id(item_id)
            
        return "break"

    def _get_all_selectable_paths(self):
        paths = set()
        for item_id in self.item_to_path:
            tags = self.tree.item(item_id, "tags")
            if 'protected_item' not in tags:
                paths.add(self.item_to_path[item_id])
        return paths

    def _refresh_all_checkboxes(self):
        # Update standalone items (like QR)
        for item_id in self.tree.get_children(""):
             if 'qr_item' in self.tree.item(item_id, "tags"):
                 path = self.item_to_path.get(item_id)
                 self.tree.set(item_id, "status", "☑" if path in self.selected_files else "☐")
        # Update groups
        for parent_id in self.parent_to_children:
            self._update_group_checkbox(parent_id)

    def _select_all(self) -> None:
        self.selected_files.update(self._get_all_selectable_paths())
        self._refresh_all_checkboxes()

    def _select_suggested_for_deletion(self) -> None:
        paths_to_select = set()
        # Handle groups
        for parent_id, children in self.parent_to_children.items():
            # Skip the first child (the base image)
            for child_id in children[1:]:
                if 'protected_item' not in self.tree.item(child_id, "tags"):
                    paths_to_select.add(self.item_to_path.get(child_id))
        
        # Handle standalone QR items
        for item_id in self.tree.get_children(""):
            if 'qr_item' in self.tree.item(item_id, "tags"):
                paths_to_select.add(self.item_to_path.get(item_id))
                
        self.selected_files.update(paths_to_select)
        self._refresh_all_checkboxes()
        
    def _deselect_all(self) -> None:
        self.selected_files.clear()
        self._refresh_all_checkboxes()

    def _invert_selection(self) -> None:
        all_paths = self._get_all_selectable_paths()
        self.selected_files = all_paths.symmetric_difference(self.selected_files)
        self._refresh_all_checkboxes()
    
    def _mark_new_ads(self) -> None:
        paths_to_select = set()
        for item_id in self.tree.get_children(""):
            if 'qr_item' in self.tree.item(item_id, "tags"):
                path = self.item_to_path.get(item_id)
                paths_to_select.add(path)
        
        if not paths_to_select:
            messagebox.showinfo("提示", "目前沒有可自動標記的新廣告圖。", parent=self)
            return

        self.selected_files.update(paths_to_select)
        self._refresh_all_checkboxes()
        self.status_label.config(text=f"已自動選取所有 {len(paths_to_select)} 個 QR Code 項目。")
        
    def _get_unique_ad_path(self, source_path: str, ad_dir: str) -> str:
        base, extension = os.path.splitext(os.path.basename(source_path))
        new_base = f"ad_{base}"
        target_path = os.path.join(ad_dir, f"{new_base}{extension}")
        i = 1
        while os.path.exists(target_path):
            target_path = os.path.join(ad_dir, f"{new_base}({i}){extension}")
            i += 1
        return target_path

    def _move_selected_to_ad_library(self) -> None:
        selected_paths = list(self.selected_files)
        if not selected_paths:
            messagebox.showinfo("沒有選取", "請先勾選要移入廣告庫的圖片。", parent=self)
            return

        ad_folder_path = self.config.get('ad_folder_path')
        if not ad_folder_path or not os.path.isdir(ad_folder_path):
            messagebox.showerror("錯誤", "廣告圖片資料夾路徑無效，請在設定中指定一個有效的資料夾。", parent=self)
            return

        if not messagebox.askyesno("確認移動", f"確定要將選中的 {len(selected_paths)} 個檔案移動到廣告庫嗎？\n目的地：'{os.path.basename(ad_folder_path)}'\n\n檔案將從原位置移動。", parent=self):
            return

        moved_count, failed_moves = 0, 0
        items_to_remove_from_gui = []

        for path in selected_paths:
            try:
                dest_path = self._get_unique_ad_path(path, ad_folder_path)
                shutil.move(path, dest_path)
                log_info(f"已將檔案 '{path}' 移動到 '{dest_path}'")
                items_to_remove_from_gui.append(path)
                moved_count += 1
            except Exception as e:
                log_error(f"移動檔案 '{path}' 到廣告庫失敗: {e}", True)
                failed_moves += 1

        if moved_count > 0:
            self.all_found_items = [(p1, p2, v) for p1, p2, v in self.all_found_items if p2 not in items_to_remove_from_gui]
            self.selected_files.clear()
            self._process_scan_results([])
            messagebox.showinfo("移動完成", f"成功移動 {moved_count} 個檔案到廣告庫。", parent=self)

        if failed_moves > 0:
            messagebox.showerror("移動失败", f"有 {failed_moves} 個檔案移動失敗，詳情請見 error_log.txt。", parent=self)

#######
    def _delete_selected_from_disk(self) -> None:
        if not self.selected_files:
            messagebox.showinfo("沒有選取", "請先勾選要移至回收桶的圖片。", parent=self)
            return
            
        paths_to_delete = [p for p in self.selected_files if p not in self.protected_paths]
        
        if not paths_to_delete:
            messagebox.showinfo("無需操作", "所有選中的項目均為受保護的檔案，\n沒有可移至回收桶的檔案。", parent=self)
            return

        if not messagebox.askyesno("確認刪除", f"確定要將 {len(paths_to_delete)} 個圖片移至回收桶嗎？"):
            return

        root_folder = self.config.get('root_scan_folder')
        ad_folder = self.config.get('ad_folder_path')
        
        image_cache_manager = ScannedImageCacheManager(root_folder, ad_folder) if root_folder else None
        folder_cache_manager = FolderStateCacheManager(root_folder) if root_folder else None

        deleted_count, failed_count = 0, 0
        skipped_count = len(self.selected_files) - len(paths_to_delete)
        
        successfully_deleted_paths = []
        modified_folders = set()

        for path in paths_to_delete:
            if self._send2trash(path):
                deleted_count += 1
                successfully_deleted_paths.append(path)
                modified_folders.add(os.path.dirname(path))
                
                if image_cache_manager:
                    image_cache_manager.remove_data(path)
            else:
                failed_count += 1
        
        if image_cache_manager:
            image_cache_manager.save_cache()
            
        if folder_cache_manager and modified_folders:
            log_info(f"[快取清理] 因檔案刪除，正在從資料夾快取中移除 {len(modified_folders)} 個條目...")
            folder_cache_manager.remove_folders(list(modified_folders))
            folder_cache_manager.save_cache()

        # ... (訊息框顯示邏輯保持不變)
        title = ""
        message_parts = []
        message_box_func = messagebox.showinfo

        if deleted_count > 0:
            message_parts.append(f"✅ 成功將 {deleted_count} 個檔案移至回收桶。")
        if skipped_count > 0:
            message_parts.append(f"🔒 {skipped_count} 個檔案因受保護而未被刪除。")
        if failed_count > 0:
            message_parts.append(f"❌ {failed_count} 個檔案刪除失敗 (可能已被移動或不存在)，詳情請見 error_log.txt。")

        if not message_parts:
            title = "無需操作"
            final_message = "所有選中的項目均為受保護的檔案，沒有可刪除的項目。"
        else:
            if failed_count > 0 and deleted_count > 0:
                title = "部分完成"
                message_box_func = messagebox.showwarning
            elif failed_count > 0 and deleted_count == 0:
                title = "刪除失敗"
                message_box_func = messagebox.showerror
            else:
                title = "刪除完成"
                message_box_func = messagebox.showinfo
            final_message = "\n\n".join(message_parts)
        message_box_func(title, final_message, parent=self)
        # --- 訊息顯示結束 ---

        # [核心修復] 只要有任何刪除成功或失敗，都應該刷新UI
        if deleted_count > 0 or failed_count > 0:
            # 從核心資料列表中移除所有“嘗試過”刪除的項目（無論成敗）
            # 因為成功的已經沒了，失敗的也證明不存在，都不應再顯示
            self.all_found_items = [
                (p1, p2, v) for p1, p2, v in self.all_found_items 
                if p1 not in paths_to_delete and p2 not in paths_to_delete
            ]
            # 清理總選擇列表
            self.selected_files.clear()
            # 使用更新後的核心資料列表重繪整個UI
            self._process_scan_results([])

#######
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
            path = self.item_to_path.get(selected[0])
            if path and os.path.isfile(path): 
                self._open_folder(os.path.dirname(path))

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
        parent_id = self.child_to_parent.get(item_id) or item_id
        
        if 'parent_item' in self.tree.item(parent_id, "tags"):
            # Find the original group_key path from the first child
            first_child = self.parent_to_children[parent_id][0]
            base_path = self.item_to_path[first_child]
            
            original_group_key = next((gk for gk, _ in self.sorted_groups if gk == base_path), None)

            if original_group_key: 
                self.banned_groups.add(original_group_key)
                self._process_scan_results([])

    def _unban_all_groups(self) -> None: self.banned_groups.clear(); self._process_scan_results([])

    def _on_mouse_motion(self, event: tk.Event) -> None:
        item_id = self.tree.identify_row(event.y)
        if hasattr(self, 'tooltip_item_id') and self.tooltip_item_id == item_id: return
        if self.tooltip: self.tooltip.leave(); self.tooltip = None; self.tooltip_item_id = None
        if item_id and 'protected_item' in self.tree.item(item_id, "tags"):
            self.tooltip = Tooltip(self.tree, "廣告圖片 (受保護不會被刪除)"); self.tooltip.enter(event)
            self.tooltip_item_id = item_id

    def _on_closing(self) -> None:
        if self.scan_thread and self.scan_thread.is_alive():
            if messagebox.askokcancel("關閉程式", "掃描仍在進行中，確定要強制關閉程式嗎？"):
                self.cancel_event.set()
                if self.is_paused:
                    self.pause_event.set()
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.destroy()
        else:
            if messagebox.askokcancel("關閉程式", "確定要關閉程式嗎？"):
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.destroy()

def main() -> None:
    if sys.platform.startswith('win'):
        try: 
            set_start_method('spawn', force=True)
        except RuntimeError: 
            pass
    
    app = MainWindow()
    app.withdraw()
    
    try:
        check_and_install_packages()
        
        while app.tk.dooneevent(tk._tkinter.DONT_WAIT):
            pass
    except SystemExit:
        app.destroy()
        return
    except Exception as e:
        log_error(f"啟動時發生未預期錯誤: {e}", True)
        messagebox.showerror("啟動失敗", f"程式啟動失敗，請檢查 error_log.txt。\n錯誤: {e}")
        app.destroy()
        return
    
    app.deiconify()
    app.mainloop()

if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()
#版本14.2.1完結
