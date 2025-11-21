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
else:
    # 否則，以 config.py 檔案所在目錄為基礎
    _BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# 所有資料（設定、日誌、快取）都存放在與主程式平級的 'data' 資料夾中
DATA_DIR = os.path.join(_BASE_DIR, "data")

# 確保 data/ 資料夾存在
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    # 如果建立失敗 (例如權限問題)，則退回到基礎目錄
    DATA_DIR = _BASE_DIR

# === 應用程式基本資訊 ===
APP_VERSION = "16.0.2"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"

# =============== 路徑常數 ===============
# 設定檔存放於 data/config.json
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

# 日誌檔
INFO_LOG_FILE = os.path.join(DATA_DIR, "info_log.txt")
ERROR_LOG_FILE = os.path.join(DATA_DIR, "error_log.txt")

# 隔離區檔案
QUARANTINE_FILE = os.path.join(DATA_DIR, "quarantine.json")

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
    'container_empty_mark': True,
    'cache_flush_threshold': 10000,
    "first_scan_extract_count": 64,
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
LSH_BANDS = 4

