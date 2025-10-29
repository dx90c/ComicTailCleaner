# ======================================================================
# 檔案名稱：config.py
# 模組目的：存放 ComicTailCleaner 的全域常數與預設設定
# 版本：1.7.1 (路徑微調：config/log 置於 data/)
# ======================================================================

import os

# === 基本路徑 ===
_BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(_BASE_DIR, "data")

# 確保 data/ 存在
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    # 若建立失敗，仍讓程式可用，但會回退到舊路徑
    DATA_DIR = _BASE_DIR

# === 應用程式基本資訊 ===
APP_VERSION = "16.0.1"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"

# =============== 路徑常數（改為 data/） ===============
# 設定檔存放於 data/config.json
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

# 日誌檔（若 utils 有使用這兩個常數，將會自動寫到 data/）
INFO_LOG_FILE = os.path.join(DATA_DIR, "info_log.txt")
ERROR_LOG_FILE = os.path.join(DATA_DIR, "error_log.txt")
# =====================================================
# === 檔案路徑集中 ===
def _in_data(name: str) -> str:
    return os.path.join(DATA_DIR, name)

# 既有的：
CONFIG_FILE     = _in_data("config.json")
INFO_LOG_FILE   = _in_data("info_log.txt")
ERROR_LOG_FILE  = _in_data("error_log.txt")

# 新增集中管理的檔名：
def FOLDER_STATE_CACHE(tag: str) -> str:
    # 例如 tag="E-Download" → data/folder_state_cache_E-Download.json
    return _in_data(f"folder_state_cache_{tag}.json")

def SCANNED_HASHES_CACHE(tag: str) -> str:
    # 例如 tag="E-Download" → data/scanned_hashes_cache_E-Download.json
    return _in_data(f"scanned_hashes_cache_{tag}.json")

QUARANTINE_FILE = _in_data("quarantine.json")

# === 虛擬路徑系統常數 ===
VPATH_PREFIX = "zip://"
VPATH_SEPARATOR = "!"

# === 核心設定預設值 ===
default_config = {
    # --- 路徑設定 ---
    'root_scan_folder': '',          # 漫畫掃描的根目錄
    'ad_folder_path': '',            # 廣告圖片庫的目錄
    'enable_archive_scan': True,     # 是否啟用壓縮檔 (zip, cbz, rar, cbr) 掃描功能

    # --- 掃描與提取設定 ---
    'extract_count': 10,             # 從每個資料夾或壓縮檔末尾提取的圖片數量
    'enable_extract_count_limit': True, # 是否啟用上述的數量限制
    'excluded_folders': [],          # 掃描時要排除的資料夾名稱列表

    # --- 比對模式與閾值 ---
    'comparison_mode': 'ad_comparison', # 預設模式: 'ad_comparison', 'mutual_comparison', 'qr_detection'
    'similarity_threshold': 95,      # 圖片相似度的百分比門檻 (UI上的拉桿)

    # --- 時間篩選設定 ---
    'enable_time_filter': False,     # 是否啟用基於檔案修改時間的篩選
    'start_date_filter': '',         # 篩選的開始日期 (格式: YYYY-MM-DD)
    'end_date_filter': '',           # 篩選的結束日期 (格式: YYYY-MM-DD)

    # --- QR Code 相關設定 ---
    'enable_qr_hybrid_mode': True,   # 在 QR 模式下，是否啟用與廣告庫的混合比對以加速
    'qr_resize_size': 1000,          # 進行 QR Code 檢測前，將圖片縮放到的尺寸 (像素)
    'qr_pages_per_archive': 10,      # 在 QR 模式下，每個壓縮檔末尾提取的圖片數量
    'qr_global_cap': 20000,          # 在 QR 模式下，全局最多處理的檔案總數上限

    # --- 性能與進階設定 ---
    'worker_processes': 0,           # 用於計算圖片雜湊的進程數 (0 代表自動設定)
    'ux_scan_start_delay': 0.1,      # 點擊開始後延遲多久開始計算 (秒)
    'enable_inter_folder_only': True,# 在互相比對模式下，是否只比對不同資料夾的圖片
    'enable_ad_cross_comparison': True, # 在互相比對模式下，是否啟用與廣告庫的交叉比對
    'enable_color_filter': True,     # 預設開啟顏色過濾
    'cross_comparison_include_bw': False, # 交叉比對時是否也比對純黑/純白

    'changed_container_cap': 500,
    'global_extract_cap': 100000,

    'enable_newest_first_pruning': True,
    'changed_container_depth_limit': 1,
    'folder_time_mode': 'mtime',

    # --- 【v1.6.0】 進階快取與增量比對設定 ---
    'preserve_cache_across_time_windows': True,
    'prune_image_cache_on_missing_folder': False,
    'container_empty_mark': True,
    'cache_flush_threshold': 10000,
    "first_scan_extract_count": 64,

    # --- 【v1.7.0】 ---
    'enable_quarantine': True,
    'enable_quick_digest': True,

    # --- 【v1.8.0】 EH 前置處理器設定 ---
    'enable_eh_preprocessor': False,
    'eh_data_directory': '',
    'eh_backup_directory': '',
    'eh_syringe_directory': '',
    'eh_mmd_json_path': '',  # --- 【v1.8.1 新增】 ---

    # --- UI 顯示設定 ---
    'page_size': 'all',
}

# === 雙哈希 LSH 演算法相關常數 ===
HASH_BITS = 64

# --- 相似度門檻 ---
PHASH_FAST_THRESH = 0.80
PHASH_STRICT_SKIP = 0.93

# --- wHash 複核分級門檻 ---
WHASH_TIER_1 = 0.90
WHASH_TIER_2 = 0.92
WHASH_TIER_3 = 0.95
WHASH_TIER_4 = 0.98

# --- LSH 相關設定 ---
AD_GROUPING_THRESHOLD = 0.95
LSH_BANDS = 4
