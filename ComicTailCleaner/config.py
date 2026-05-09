# ======================================================================
# 檔案名稱：config.py
# 模組目的：存放 ComicTailCleaner 的全域常數與預設設定
# 版本：1.7.2 (路徑優化：增強對打包環境的支援)
# ======================================================================

import os
import sys

# === 基本路徑 (增強版) ===
# 檢查是否在打包環境 (如 PyInstaller) 中運行
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    # 如果是打包的 .exe，則以執行檔所在目錄為基礎
    _BASE_DIR = os.path.dirname(sys.executable)
    # 打包後的資源釋放目錄
    ASSET_DIR = sys._MEIPASS
else:
    # 否則，以 config.py 檔案所在目錄為基礎
    _BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    ASSET_DIR = _BASE_DIR

# 所有資料（設定、日誌、快取、資料庫）都存放在與主程式平級的 'data' 資料夾中
DATA_DIR = os.path.join(_BASE_DIR, "data")

# --- 定義結構化子目錄 ---
CONFIG_DIR = os.path.join(DATA_DIR, "configs")
CACHE_DIR  = os.path.join(DATA_DIR, "caches")
LOG_DIR    = os.path.join(DATA_DIR, "logs")

# 內置資源目錄 (打包後位於 _MEIPASS 內)
# 注意：在打包命令中，Everything DLL 與翻譯檔被放在了 data/bin 與 data/eh_database_tools 內
BIN_DIR    = os.path.join(ASSET_DIR, "data", "bin")

# 確保所有目錄存在
for d in [DATA_DIR, CONFIG_DIR, CACHE_DIR, LOG_DIR, BIN_DIR]:
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass

# =============== 路徑常數 (含相容層邏輯) ===============
# 1. 設定檔 (優先從 configs/ 讀取，若無則從 data/ 讀取並自動遷移)
_CONFIG_FILE_OLD = os.path.join(DATA_DIR, "config.json")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

if not os.path.exists(CONFIG_FILE) and os.path.exists(_CONFIG_FILE_OLD):
    try:
        import shutil
        shutil.move(_CONFIG_FILE_OLD, CONFIG_FILE)
        # 建立一個簡單的標記說明已經搬走了 (選用)
        with open(_CONFIG_FILE_OLD + ".moved", "w") as f:
            f.write("Moved to configs/config.json")
    except Exception:
        CONFIG_FILE = _CONFIG_FILE_OLD # 搬移失敗則回退

# 2. 隔離區檔案
QUARANTINE_FILE_OLD = os.path.join(DATA_DIR, "quarantine.json")
QUARANTINE_FILE = os.path.join(CONFIG_DIR, "quarantine.json")
if not os.path.exists(QUARANTINE_FILE) and os.path.exists(QUARANTINE_FILE_OLD):
    try: os.rename(QUARANTINE_FILE_OLD, QUARANTINE_FILE)
    except Exception: QUARANTINE_FILE = QUARANTINE_FILE_OLD

# 3. 日誌檔
INFO_LOG_FILE = os.path.join(LOG_DIR, "info_log.txt")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error_log.txt")

# 補上日誌搬家邏輯
for old_log in [os.path.join(DATA_DIR, "info_log.txt"), os.path.join(DATA_DIR, "error_log.txt")]:
    new_log = os.path.join(LOG_DIR, os.path.basename(old_log))
    if not os.path.exists(new_log) and os.path.exists(old_log):
        try: os.rename(old_log, new_log)
        except Exception: pass

# 4. 二進位工具 (Everything64.dll)
EVERYTHING_DLL_OLD = os.path.join(DATA_DIR, "Everything64.dll")
EVERYTHING_DLL_PATH = os.path.join(BIN_DIR, "Everything64.dll")

if not os.path.exists(EVERYTHING_DLL_PATH) and os.path.exists(EVERYTHING_DLL_OLD):
    try:
        import shutil
        shutil.move(EVERYTHING_DLL_OLD, EVERYTHING_DLL_PATH)
    except Exception:
        EVERYTHING_DLL_PATH = EVERYTHING_DLL_OLD

# === 應用程式基本資訊 ===
APP_VERSION = "17.1.0"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"

# === 虛擬路徑系統常數 ===
VPATH_PREFIX = "zip://"
VPATH_SEPARATOR = "!"

# === 核心設定預設值 ===
default_config = {
    # --- 路徑設定 ---
    'root_scan_folder': '',
    'ad_folder_path': '',
    'enable_archive_scan': True,

    # --- 掃描與提取設定 ---
    'extract_count': 10,
    'enable_extract_count_limit': True,
    'excluded_folders': [],

    # --- 比對模式與閾值 ---
    'comparison_mode': 'ad_comparison',
    'similarity_threshold': 95,

    # --- 時間篩選設定 ---
    'enable_time_filter': False,
    'start_date_filter': '',
    'end_date_filter': '',

    # --- QR Code 相關設定 ---
    'enable_qr_hybrid_mode': True,
    'enable_qr_color_filter': False,
    'qr_resize_size': 1000,
    'qr_pages_per_archive': 10,
    'qr_global_cap': 20000,

    # --- 性能與進階設定 ---
    'worker_processes': 0,
    'ux_scan_start_delay': 0.1,
    'enable_inter_folder_only': True,
    'enable_ad_cross_comparison': True,
    'enable_color_filter': True,
    'cross_comparison_include_bw': False,
    'changed_container_cap': 500,
    'global_extract_cap': 100000,
    'enable_newest_first_pruning': True,
    'changed_container_depth_limit': 1,
    'folder_time_mode': 'mtime',

    # --- 進階快取與增量比對設定 ---
    'preserve_cache_across_time_windows': True,
    'prune_image_cache_on_missing_folder': False,
    'enable_missing_folder_cleanup': False,
    'container_empty_mark': True,
    'cache_flush_threshold': 10000,
    "first_scan_extract_count": 0,
    'enable_quarantine': True,
    'enable_quick_digest': True,

    # --- UI 顯示設定 ---
    'page_size': 'all',
}

# === 雙哈希 LSH 演算法相關常數 (保持與 core_engine 一致) ===
HASH_BITS = 64
PHASH_FAST_THRESH = 0.80
PHASH_STRICT_SKIP = 0.93
WHASH_TIER_1 = 0.90
WHASH_TIER_2 = 0.92
WHASH_TIER_3 = 0.95
WHASH_TIER_4 = 0.98
AD_GROUPING_THRESHOLD = 0.95
LSH_BANDS = 8
