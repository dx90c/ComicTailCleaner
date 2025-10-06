# ======================================================================
# 檔案名稱：config.py
# 模組目的：存放 ComicTailCleaner 的全域常數與預設設定
# 版本：1.1.0 (更新至 v14.5.0 並新增 QR 模式專屬設定)
# ======================================================================

# === 應用程式基本資訊 ===
APP_VERSION = "15.0.0"
APP_NAME_EN = "ComicTailCleaner"
APP_NAME_TC = "漫畫尾頁廣告清理"
CONFIG_FILE = "config.json"  # 用於保存使用者設定的檔案名稱

# === 虛擬路徑系統常數 ===
# 這是為了讓程式能處理壓縮檔內的檔案所定義的特殊路徑格式
# 格式: zip://C:/path/to/archive.cbz!/inner/image.jpg
VPATH_PREFIX = "zip://"
VPATH_SEPARATOR = "!"

# === 核心設定預設值 ===
# 這裡定義了所有可在設定介面中調整的選項的初始值。
# 當 config.json 檔案不存在或損毀時，程式會使用這些預設值。
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
    'qr_pages_per_archive': 10,      # 【新增】在 QR 模式下，每個壓縮檔末尾提取的圖片數量
    'qr_global_cap': 20000,          # 【新增】在 QR 模式下，全局最多處理的檔案總數上限

    # --- 性能與進階設定 ---
    'worker_processes': 0,           # 用於計算圖片雜湊的進程數 (0 代表自動設定)
    'ux_scan_start_delay': 0.1,      # 點擊開始後延遲多久開始計算，以確保UI能即時更新 (秒)
    'enable_inter_folder_only': True,# 在互相比對模式下，是否只比對來自不同資料夾的圖片
    'enable_ad_cross_comparison': True, # 在互相比對模式下，是否啟用與廣告庫的交叉比對來標記相似羣組
    'enable_color_filter': True, # 【新增】預設開啟
    'cross_comparison_include_bw': False, # 進行交叉比對時，是否也比對純黑/純白圖片

    # --- UI 顯示設定 ---
    'page_size': 'all',              # 結果列表中每頁顯示的項目數量
}

# === 雙哈希 LSH 演算法相關常數 ===
# 這些是比對引擎內部的微調參數，通常不需要修改。
HASH_BITS = 64                   # 感知雜湊的位元數 (64位元 = 8x8)

# --- 相似度門檻 ---
PHASH_FAST_THRESH = 0.80         # pHash 快速篩選的最低相似度門檻 (80%)，低於此值直接拋棄
PHASH_STRICT_SKIP = 0.93         # pHash 相似度高到可以直接信任，無需 wHash 複核的門檻 (93%)

# --- wHash 複核分級門檻 (Tiers) ---
# 當 pHash 相似度落在特定區間時，需要 wHash 達到對應的更高標準才能通過。
# 這是為了過濾掉那些 pHash 看起來相似但實際上紋理細節差異很大的情況。
WHASH_TIER_1 = 0.90              # 對應 pHash 區間: 0.90 <= sim < 0.93
WHASH_TIER_2 = 0.92              # 對應 pHash 區間: 0.88 <= sim < 0.90
WHASH_TIER_3 = 0.95              # 對應 pHash 區間: 0.85 <= sim < 0.88
WHASH_TIER_4 = 0.98              # 對應 pHash 區間: 0.80 <= sim < 0.85

# --- LSH (局部敏感雜湊) 相關設定 ---
AD_GROUPING_THRESHOLD = 0.95     # 在預處理廣告庫時，用於將極度相似的廣告分組的內部固定門檻 (95%)
LSH_BANDS = 4                    # 將 64 位元的雜湊值分成 4 個 16 位元的 "band" 進行索引，以加速尋找候選圖片