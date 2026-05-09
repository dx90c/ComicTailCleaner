# ======================================================================
# 檔案：plugins/eh_database_tools/processor.py
# 功能：實作「自動化 EH 數據庫同步」的核心邏輯
# 版本：v1.9.15 (結構重構版：對齊新資料夾架構)
# ======================================================================
from __future__ import annotations
import os
import sqlite3
import hashlib
import datetime
import json
import time
from time import perf_counter
import re
import shutil
from tqdm import tqdm
from typing import Dict, Any, Tuple, List, Optional, Union, Sequence, Iterable
from pathlib import Path
import io
import csv
import tempfile
import subprocess
import threading
import queue
import difflib
import unicodedata

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error, log_warning
from config import DATA_DIR, CACHE_DIR, CONFIG_DIR, LOG_DIR

try:
    import pyautogui, keyboard, psutil, pyperclip, ctypes
    from ctypes import wintypes
    from PIL import Image
    AUTOMATION_LIBS_AVAILABLE = True
except ImportError:
    AUTOMATION_LIBS_AVAILABLE = False

# --- 全域變數 ---
GLOBAL_ARTIST_MAP = {} 
GLOBAL_GROUP_MAP = {}  
GLOBAL_ARTIST_KEYS = [] 
GLOBAL_GROUP_KEYS = []  

summary = None
PLUGIN_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))

PAGE_LOAD_DELAY = 2.0;
SEARCH_BOX_X_OFFSET = -100; TITLE_X_OFFSET = -100; TITLE_Y_OFFSET = -20
MAIN_SEARCH_ICON_IMG, BOOKMARK_ICON_IMG, BOOKMARK_ICON_READY_IMG, RESCAN_BUTTON_IMG, CLOSE_BUTTON_IMG, PAGE_END_IMG, CLEAR_SEARCH_BUTTON_IMG, NO_COVER_IMG = 'main_search_icon.png', 'bookmark_icon.png', 'bookmark_icon_ready.png', 'rescan_button.png', 'close_button.png', 'page_end.png', 'clear_search_button.png', 'no_cover.png'
# [自動化 UI] 某些按鈕圖片名稱在 run_automation_suite_headless 會被重複調用
NON_TAG_BTN_IMG = 'non_tag_button.png'

# --- EH-FEAT-06: 備份輪轉策略 ---
EH_BACKUP_MIN_KEEP = 10
EH_BACKUP_KEEP_DAYS = 7

# --- 狀態常數 ---
DELETED_STATUS = "file_deleted"
ALL_DELETED_STATUSES = ("file_deleted", "檔案已被刪除", "已刪除", "deleted")

# --- 核心輔助函式 ---
def _eh_log_elapsed(label: str, start: float):
    log_info(f"[EH timing] {label}: {perf_counter() - start:.2f}s")

def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """獲取 SQLite 表的欄位清單。"""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [info[1] for info in cursor.fetchall()]

def _status_in_clause(column_name: str, statuses: Sequence[str]) -> Tuple[str, List[str]]:
    """建立 SQL IN 子句與對應的參數。"""
    placeholders = ', '.join(['?'] * len(statuses))
    return f"{column_name} IN ({placeholders})", list(statuses)

def _status_not_in_clause(column_name: str, statuses: Sequence[str]) -> Tuple[str, List[str]]:
    """建立 SQL NOT IN 子句與對應的參數。"""
    placeholders = ', '.join(['?'] * len(statuses))
    return f"{column_name} NOT IN ({placeholders})", list(statuses)

METADATA_REPAIRED_STATUS = 'tagged'

def _sync_metadata_status_by_hash(metadata_path: str, hashes: List[str], target_status: str, 
                                 exclude_statuses: Optional[List[str]] = None,
                                 only_statuses: Optional[List[str]] = None) -> int:
    """批量同步 metadata.sqlite 中雜湊的狀態。
    可選：排除某些狀態 (exclude_statuses) 或 僅包含某些狀態 (only_statuses)。
    """
    if not hashes: return 0
    affected_total = 0
    conn = None
    try:
        conn = sqlite3.connect(metadata_path)
        where_clause = "hash = ?"
        params_base = [target_status]
        
        if exclude_statuses:
            clause, p = _status_not_in_clause("status", exclude_statuses)
            where_clause += f" AND {clause}"
            params_base.extend(p)
        elif only_statuses:
            clause, p = _status_in_clause("status", only_statuses)
            where_clause += f" AND {clause}"
            params_base.extend(p)

        sql = f"UPDATE Metadata SET status = ?, updatedAt = datetime('now') WHERE {where_clause}"
        
        # 建立參數清單：(target_status, hash, ...extra_params)
        batch_params = []
        for h in hashes:
            p = list(params_base)
            p.insert(1, h) # 插入在 target_status 之後
            batch_params.append(tuple(p))
        
        cursor = conn.cursor()
        cursor.executemany(sql, batch_params)
        affected_total = cursor.rowcount
        conn.commit()
    except Exception as e:
        log_error(f"[EH 備份] 同步 Metadata 狀態時發生錯誤: {e}")
    finally:
        if conn is not None:
            conn.close()
    return affected_total

SPEED_PRESETS = {
    "safe":   {"PAUSE": 0.35, "CLICK": 0.30, "PAGEDOWN": 0.15, "AFTER_SCROLL": 0.25},
    "normal": {"PAUSE": 0.20, "CLICK": 0.18, "PAGEDOWN": 0.10, "AFTER_SCROLL": 0.15},
    "fast":   {"PAUSE": 0.05, "CLICK": 0.08, "PAGEDOWN": 0.08, "AFTER_SCROLL": 0.10},
}

def _init_automation_speed_from_config(config: dict):
    speed = (config or {}).get("automation_speed", "fast").strip().lower()
    timing = SPEED_PRESETS.get(speed, SPEED_PRESETS["fast"])
    if AUTOMATION_LIBS_AVAILABLE:
        pyautogui.PAUSE = timing["PAUSE"]
    return timing

class ExecutionSummary:
    def __init__(self):
        self.start_time = time.time()
        self.end_time = None
        self.mode = "unknown"
        self.added = 0
        self.soft_deleted = 0
        self.restored = 0
        self.moved_empty = 0
        self.tasks_total = 0
        self.tasks_processed = 0
        self.detected_empty_list = []

    def finish(self):
        self.end_time = time.time()

    def report(self):
        if not self.end_time:
            self.finish()
        duration = self.end_time - self.start_time
        mins, secs = divmod(duration, 60)
        report_lines = [
            "\n",
            "=" * 70,
            "[EH plugin] execution summary",
            "=" * 70,
            f"mode: {self.mode}",
            f"duration: {int(mins)}m {int(secs)}s",
            "--- database sync ---",
            f"added: {self.added}",
            f"soft_deleted: {self.soft_deleted}",
            f"restored: {self.restored}",
            f"detected_empty_folders: {len(self.detected_empty_list)}",
            "--- UI automation ---",
            f"tasks_total: {self.tasks_total}",
            f"tasks_processed: {self.tasks_processed}",
            "=" * 70,
        ]
        for line in report_lines:
            log_info(line)
def normalize_path(path: str) -> str:
    if not path: return ""
    return os.path.normpath(path).replace('\\', '/')

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', '_', name); name = name.strip('. '); return name

def add_normalized_path_column_if_not_exists(db_path: str):
    with sqlite3.connect(db_path) as conn:
        if 'filepath_normalized' not in [info[1] for info in conn.execute("PRAGMA table_info(Mangas)")]:
            log_info("[EH plugin] Adding filepath_normalized column...")
            conn.execute("ALTER TABLE Mangas ADD COLUMN filepath_normalized TEXT")
            log_info("  -> Column added.")

def migrate_to_v20_structure(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.executemany("UPDATE Mangas SET filepath = ? WHERE id = ?", [(path.replace('/', '\\'), pid) for pid, path in conn.execute("SELECT id, filepath FROM Mangas WHERE filepath LIKE '%/%'")])
        records_to_migrate = list(conn.execute("SELECT id, filepath FROM Mangas WHERE filepath_normalized IS NULL OR filepath_normalized = '' OR filepath_normalized LIKE '%\\%'"))
        if records_to_migrate:
            log_info(f"[EH plugin] Migrating {len(records_to_migrate)} records to normalized paths...")
            conn.executemany("UPDATE Mangas SET filepath_normalized = ? WHERE id = ?", [(normalize_path(path), pid) for pid, path in records_to_migrate])
            log_info("  -> Path migration completed.")

def smart_normalize(text: str) -> str:
    if not text: return ""
    return unicodedata.normalize('NFKC', text).lower().strip()

def load_maps_from_ast_json(filepath: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    def extract_text_recursive(obj: Any) -> Union[str, None]:
        if isinstance(obj, str): return obj
        if isinstance(obj, list):
            for item in obj:
                res = extract_text_recursive(item)
                if res: return res
        if isinstance(obj, dict):
            if "name" in obj and isinstance(obj["name"], str): return obj["name"]
            for v in obj.values():
                res = extract_text_recursive(v)
                if res: return res
        return None

    artist_map, group_map = {}, {}
    try:
        with open(filepath, "r", encoding="utf-8") as f: root = json.load(f)
        is_ast = "data" in root and isinstance(root["data"], list)
        
        if is_ast:
            sections = root.get("data")
            for ns in ("artist", "group"):
                section = next((s for s in sections if isinstance(s, dict) and s.get("namespace") == ns), None)
                if not section: continue
                data_block = section.get("data")
                if not isinstance(data_block, dict): continue
                target_map = artist_map if ns == "artist" else group_map
                
                for raw_tag, entry_data in data_block.items():
                    value_romaji = raw_tag.replace('_', ' ').title()
                    key_japanese = extract_text_recursive(entry_data.get("name"))
                    if key_japanese and key_japanese.strip():
                        target_map[smart_normalize(key_japanese)] = value_romaji
    except Exception as e: 
        log_error(f"[EH 憭?] 閫?? EhTag 鞈?摨急??潛??航炊: {e}")
        return {}, {}
    return artist_map, group_map

def load_translation_maps(config: Dict):
    global GLOBAL_ARTIST_MAP, GLOBAL_GROUP_MAP, GLOBAL_ARTIST_KEYS, GLOBAL_GROUP_KEYS
    t_start = perf_counter()
    log_info("[EH 外掛] 正在載入 EhTag 雙軌翻譯資料庫...")
    ehtag_db_dir = config.get('eh_syringe_directory')
    if not ehtag_db_dir or not os.path.isdir(ehtag_db_dir):
        log_warning("[EH 外掛] 未設定有效的 EhTag DB 路徑，將跳過翻譯與 CSV 功能。"); return
    
    db_candidates = ['db.ast.json', 'db.json', 'db.text.json']
    db_path = None
    for cand in db_candidates:
        p = os.path.join(ehtag_db_dir, cand)
        if os.path.exists(p):
            db_path = p; break
            
    if not db_path: 
        log_error(f"[EH 外掛] 找不到資料庫檔案 (搜尋順序: {db_candidates})"); return
        
    log_info(f"  -> 讀取資料庫: {os.path.basename(db_path)}")
    GLOBAL_ARTIST_MAP, GLOBAL_GROUP_MAP = load_maps_from_ast_json(db_path)
    GLOBAL_ARTIST_KEYS = list(GLOBAL_ARTIST_MAP.keys())
    GLOBAL_GROUP_KEYS = list(GLOBAL_GROUP_MAP.keys())
    
    log_info(f"  -> Artist 資料庫載入完成: {len(GLOBAL_ARTIST_MAP)} 筆")
    log_info(f"  -> Group 資料庫載入完成: {len(GLOBAL_GROUP_MAP)} 筆")
    _eh_log_elapsed("EhTag DB Loading", t_start)

# === 隢?ㄐ? is_romaji_candidate ===
def is_romaji_candidate(text: str) -> bool:
    # 蝪∪?斗嚗??之?典?摮泵??ASCII嚗?閬蝢收?潮?
    return all(ord(c) < 128 for c in text.replace(' ', '').replace('_', '').replace('-', ''))
# ========================================
def fuzzy_lookup(query: str, mapping: Dict[str, str], keys: List[str], cutoff: float = 0.8) -> Optional[str]:
    norm_query = smart_normalize(query)
    if not norm_query: return None
    if norm_query in mapping: return mapping[norm_query]
    if len(norm_query) > 2:
        matches = difflib.get_close_matches(norm_query, keys, n=1, cutoff=cutoff)
        if matches: return mapping[matches[0]]
    return None

def analyze_title_tags(title: str) -> Tuple[str, str]:
    if not title:
        return "", ""
    artist_val, group_val = "", ""
    ignored_tags = {
        "chinese", "dl", "dl版", "中国翻訳", "翻訳",
        "uncensored", "eng", "english",
    }
    for raw_content in re.findall(r'\[([^\]]+)\]', title):
        content = raw_content.strip()
        content_lower = content.lower()
        if content_lower in ignored_tags:
            continue

        inner_match = re.search(r'[\(\uFF08]([^\)\uFF09]+)[\)\uFF09]', content)
        if inner_match:
            inner_artist = inner_match.group(1).strip()
            outer_group = re.split(r'[\(\uFF08]', content)[0].strip()
            if not artist_val:
                artist_val = fuzzy_lookup(inner_artist, GLOBAL_ARTIST_MAP, GLOBAL_ARTIST_KEYS)
                if not artist_val and is_romaji_candidate(inner_artist):
                    artist_val = inner_artist.title()
                if not artist_val:
                    artist_val = inner_artist
            if outer_group and not group_val:
                group_val = fuzzy_lookup(outer_group, GLOBAL_GROUP_MAP, GLOBAL_GROUP_KEYS)
                if not group_val and is_romaji_candidate(outer_group):
                    group_val = outer_group.title()
                if not group_val:
                    group_val = outer_group
            continue

        mapped_artist = fuzzy_lookup(content, GLOBAL_ARTIST_MAP, GLOBAL_ARTIST_KEYS)
        if mapped_artist and not artist_val:
            artist_val = mapped_artist
            continue
        mapped_group = fuzzy_lookup(content, GLOBAL_GROUP_MAP, GLOBAL_GROUP_KEYS)
        if mapped_group and not group_val:
            group_val = mapped_group
            continue
        if is_romaji_candidate(content):
            if not artist_val:
                artist_val = content.title()
            elif not group_val:
                group_val = content.title()
        elif not artist_val:
            artist_val = content
    return artist_val, group_val

_JUNK_FILENAMES = frozenset({'thumbs.db', '.ds_store', 'desktop.ini', '.picasa.ini'})

def is_folder_effectively_empty(folder_path: str) -> bool:
    """Return True when the folder only contains ignorable OS metadata files."""
    try:
        for entry in os.scandir(folder_path):
            if entry.name.lower() not in _JUNK_FILENAMES:
                return False
        return True
    except (PermissionError, FileNotFoundError):
        return False

def load_scan_cache(cache_path: str) -> dict:
    if not os.path.exists(cache_path): return {}
    try:
        with open(cache_path, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception: return {}

def save_scan_cache(cache_path: str, data: dict):
    try:
        with open(cache_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=None, separators=(',', ':'))
    except IOError: pass

def handle_empty_folders(root_dir: str, quarantine_path: str, cache_path: str, config: Optional[Dict] = None) -> set:
    """Detect real, existing empty folders for UI cleanup suggestions only.

    This must not be used to infer DB-only missing folders. The normal DB sync
    diff handles "SQL has it, filesystem does not" separately as a SQL-only
    soft-delete. Empty-folder cleanup is only for folders that still exist on
    disk and are effectively empty.
    """
    if not root_dir: return set()

    log_info("[EH plugin] Scanning empty folders for UI reporting only; no folders will be moved.")
    t_start = perf_counter()
    summary.cache_misses = summary.cache_hits = 0

    try:
        all_local_entries = list(os.scandir(root_dir))
        all_local_folders = {os.path.normpath(e.path) for e in all_local_entries if e.is_dir()}
    except Exception as e:
        log_error(f"[EH 憭?] ??鞈?憭曉仃?? {e}"); return set()

    # --- ?皜嚗?銝餌?撘?Setting UI ???嚗?--
    excluded_rules = config.get('excluded_folders', []) if config else []
    excluded_names = {
        name.strip().lower()
        for name in excluded_rules
        if name.strip() and os.path.sep not in name and (not os.path.altsep or os.path.altsep not in name)
    }
    if excluded_names:
        log_info(f"[EH plugin] Excluded folder names active: {excluded_names}")

    candidate_folders = {p for p in all_local_folders if os.path.basename(p).lower() not in excluded_names}

    if not root_dir: return set()

    log_info("[EH plugin] Scanning empty folders for UI reporting only; no folders will be moved.")
    t_start = perf_counter()
    summary.cache_misses = summary.cache_hits = 0

    try:
        all_local_entries = list(os.scandir(root_dir))
        all_local_folders = {os.path.normpath(e.path) for e in all_local_entries if e.is_dir()}
    except Exception as e:
        log_error(f"[EH 憭?] ??鞈?憭曉仃?? {e}"); return set()

    excluded_rules = config.get('excluded_folders', []) if config else []
    excluded_names = {
        name.strip().lower()
        for name in excluded_rules
        if name.strip() and os.path.sep not in name and (not os.path.altsep or os.path.altsep not in name)
    }
    if excluded_names:
        log_info(f"[EH plugin] Excluded folder names active: {excluded_names}")

    candidate_folders = {p for p in all_local_folders if os.path.basename(p).lower() not in excluded_names}

    # === 快速路徑：Everything SDK 提示 ===
    sdk_hints = config.get('eh_non_empty_folder_hints') if config else None
    if sdk_hints is not None:
        non_empty_norm = {os.path.normcase(p) for p in sdk_hints}
        detected_folders = {normalize_path(p) for p in candidate_folders if os.path.normcase(p) not in non_empty_norm}
        log_info(f"[EH plugin] Everything SDK 模式: candidates={len(candidate_folders)}, empty={len(detected_folders)}")
        if summary:
            summary.detected_empty_list = list(detected_folders)
        if detected_folders:
            log_info(f"  -> Detected {len(detected_folders)} empty folders for UI reporting.")
        return set()

    # === 銝€?祈楝敺?os.scandir + mtime 敹怠? ===
    cache = load_scan_cache(cache_path); new_cache = {}; detected_folders = set()
    mtime_map = {os.path.normpath(e.path): e.stat().st_mtime for e in all_local_entries if e.is_dir()}

    for folder_path in tqdm(candidate_folders, desc="[EH plugin] Detecting empty folders"):
        current_mtime = mtime_map.get(folder_path)
        if current_mtime is None: continue
        cache_entry = cache.get(folder_path)
        if cache_entry and cache_entry.get('mtime') == current_mtime:
            is_empty = cache_entry.get('is_empty', False); summary.cache_hits += 1
        else:
            is_empty = is_folder_effectively_empty(folder_path); summary.cache_misses += 1
        new_cache[folder_path] = {'mtime': current_mtime, 'is_empty': is_empty}
        if is_empty:
            detected_folders.add(normalize_path(folder_path))

    save_scan_cache(cache_path, new_cache)
    _eh_log_elapsed("Empty folder detection", t_start)
    if summary:
        summary.detected_empty_list = list(detected_folders)
    if detected_folders:
        log_info(f"  -> Detected {len(detected_folders)} empty folders for UI reporting.")
    return set()

def create_manga_record(folder_path, url_map):
    from nanoid import generate
    title = os.path.basename(folder_path)
    url = url_map.get(sanitize_filename(title), ""); normalized_fp = normalize_path(folder_path)
    sha1_hash = hashlib.sha1(normalized_fp.encode('utf-8')).hexdigest()
    mtime = os.path.getmtime(folder_path)
    return {"id": generate(), "title": title, "hash": sha1_hash, "filepath": os.path.normpath(folder_path), "filepath_normalized": normalized_fp, "type": "folder", "mtime": datetime.datetime.utcfromtimestamp(mtime).isoformat(timespec='milliseconds') + 'Z', "date": int(mtime * 1000), "status": "non-tag", "url": url, "tags": "{}", "rating": 0.0, "exist": 1, "hiddenBook": 0, "createdAt": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "updatedAt": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

def update_database_records(db_path, records_to_add=[], paths_to_soft_delete=[], paths_to_restore=[]):
    if not any([records_to_add, paths_to_soft_delete, paths_to_restore]): return
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_filepath_normalized ON Mangas(filepath_normalized)")
        
        # 動態檢查欄位
        manga_cols = _get_table_columns(conn, "Mangas")
        has_exist = "exist" in manga_cols
        has_hidden_book = "hiddenBook" in manga_cols

        if records_to_add:
            fields = "id, title, hash, filepath, filepath_normalized, type, mtime, date, status, url, tags, rating, exist, createdAt, updatedAt"
            placeholders = ":id, :title, :hash, :filepath, :filepath_normalized, :type, :mtime, :date, :status, :url, :tags, :rating, :exist, :createdAt, :updatedAt"
            if has_hidden_book:
                fields += ", hiddenBook"
                placeholders += ", :hiddenBook"
            cursor.executemany(f"INSERT OR IGNORE INTO Mangas ({fields}) VALUES ({placeholders})", records_to_add)

            summary.added += cursor.rowcount

        if paths_to_soft_delete:
            set_parts = ["status = ?", "updatedAt = datetime('now')"]
            if has_exist: set_parts.append("exist = 0")
            if has_hidden_book: set_parts.append("hiddenBook = 1")
            sql = f"UPDATE Mangas SET {', '.join(set_parts)} WHERE filepath_normalized = ?"
            cursor.executemany(sql, [(DELETED_STATUS, p) for p in paths_to_soft_delete])
            summary.soft_deleted += cursor.rowcount

        if paths_to_restore:
            set_parts = ["status = ?", "updatedAt = datetime('now')"]
            if has_exist: set_parts.append("exist = 1")
            if has_hidden_book: set_parts.append("hiddenBook = 0")
            sql = f"UPDATE Mangas SET {', '.join(set_parts)} WHERE filepath_normalized = ?"
            cursor.executemany(sql, [('non-tag', p) for p in paths_to_restore])
            summary.restored += cursor.rowcount
        conn.commit()
    finally:
        conn.close()
def export_tag_failed_to_csv(config: Dict):
    log_info("[EH plugin] Exporting tag-failed records to CSV...")
    
    db_path = os.path.join(config.get('eh_data_directory'), "database.sqlite")
    if not os.path.exists(db_path):
        log_warning("[EH plugin] database.sqlite not found; cannot export tag-failed records.")
        return

    output_csv_path = config.get('eh_tagfailed_path') or os.path.join(DATA_DIR, 'logs', 'tagfailed.csv')

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT title, filepath, url FROM Mangas WHERE status = 'tag-failed'")
            failed_records = cursor.fetchall()

        if not failed_records:
            log_info("[EH plugin] No tag-failed records found; CSV export skipped.")
            return

        csv_data = [['Title', 'Filepath', 'URL', 'Artist (Romaji)', 'Group (Romaji)']]
        for title, filepath, url in failed_records:
            artist_romaji, group_romaji = analyze_title_tags(title)
            csv_data.append([
                title or '',
                filepath or '',
                url or '',
                artist_romaji or '',
                group_romaji or '',
            ])
        
        if _atomic_write_csv_rows(csv_data, output_csv_path):
            log_info(f"[EH plugin] Exported {len(failed_records)} tag-failed records to {output_csv_path}")
        else:
            log_error(f"[EH plugin] Failed to write tag-failed CSV: {output_csv_path}")

    except sqlite3.Error as e:
        log_error(f"[EH plugin] Failed to read tag-failed records: {e}")
    except Exception as e:
        log_error(f"[EH plugin] Unexpected tag-failed CSV export error: {e}", include_traceback=True)

def run_full_sync_headless(config: Dict, progress_queue: Optional[any]):
    t_total = perf_counter()
    _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
    log_info("[EH 外掛] 開始執行資料庫同步 (Headless)...")
    
    root_dir = config.get('root_scan_folder')
    data_dir = config.get('eh_data_directory')
    db_path = os.path.join(data_dir, "database.sqlite")
    
    download_list_json_path = config.get('eh_mmd_json_path')
    url_map, json_data = {}, []
    if download_list_json_path and os.path.isfile(download_list_json_path):
        try:
            t_mmd = perf_counter()
            _update_progress("正在讀取 MMD JSON...", 5)
            with open(download_list_json_path, 'r', encoding='utf-8') as f: json_data = json.load(f)
            url_map = {sanitize_filename(item['Name']): item['Url'] for item in json_data if item.get('Command') == 'Completed' and 'exhentai.org/g/' in item.get('Url', '') and item.get('Name')}
            log_info(f"[EH 外掛] 成功從 MMD JSON 載入 {len(url_map)} 個 URL 映射。")
            update_csv_dashboard(json_data, config.get('eh_csv_path'))
            _eh_log_elapsed("MMD JSON & CSV Dashboard", t_mmd)
        except Exception as e: log_error(f"[EH 外掛] 讀取或解析 MMD JSON 時發生錯誤: {e}")
    else: log_warning("[EH 外掛] 未設定或找不到 MMD JSON 檔案，無法匹配 URL 或更新 CSV。")
        
    quarantine_path = config.get('eh_quarantine_path')

    cache_path = os.path.join(CACHE_DIR, 'scan_cache.json')
    old_cache_path = os.path.join(DATA_DIR, 'scan_cache.json')
    if not os.path.exists(cache_path) and os.path.exists(old_cache_path):
        try:
            shutil.move(old_cache_path, cache_path)
            log_info(f"[EH 外掛] 已自動遷移掃描快取至 caches/")
        except Exception: pass
    log_info(f"[EH 外掛] 掃描快取路徑: {cache_path}")

    # Side effect only: populates summary.detected_empty_list for UI cleanup.
    # Do not feed this into SQL soft-delete; missing folders are handled by
    # db_paths - local_paths below, and real empty folders should be deleted
    # only by the user's explicit UI cleanup action.
    handle_empty_folders(root_dir, quarantine_path, cache_path, config)

    _update_progress("正在掃描本地資料夾...", 25)
    t_local = perf_counter()
    try:
        local_paths = {normalize_path(entry.path) for entry in os.scandir(root_dir) if entry.is_dir()}
        _eh_log_elapsed("Local folder scan (os.scandir)", t_local)
    except FileNotFoundError:
        log_error(f"[EH 外掛] 錯誤：找不到指定的根目錄 '{root_dir}'"); return

    _update_progress("正在讀取資料庫記錄...", 30)
    try:
        with sqlite3.connect(db_path) as conn:
            # 使用我們新加的 helper
            manga_cols = _get_table_columns(conn, "Mangas")
            required_cols = {"filepath_normalized", "status", "hash"}
            missing_required = required_cols - set(manga_cols)
            if missing_required:
                log_warning(f"[EH 外掛] Mangas 表缺少必要欄位 {sorted(missing_required)}，同步中止。")
                return

            select_cols = ["filepath_normalized", "status", "hash"]
            has_exist = "exist" in manga_cols
            has_hidden_book = "hiddenBook" in manga_cols
            if has_exist: select_cols.append("exist")
            if has_hidden_book: select_cols.append("hiddenBook")

            db_records = {}
            t_db = perf_counter()
            _update_progress("正在讀取 EMM 記錄...", 30)
            rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM Mangas").fetchall()
            total_rows = len(rows)
            for idx, row in enumerate(rows, start=1):
                record = {"status": row[1], "hash": row[2], "exist": None, "hiddenBook": None}
                offset = 3
                if has_exist:
                    record["exist"] = row[offset]
                    offset += 1
                if has_hidden_book:
                    record["hiddenBook"] = row[offset]
                db_records[row[0]] = record
                
                if total_rows and (idx % 500 == 0 or idx == total_rows):
                    val = 30 + (idx / total_rows * 8)
                    _update_progress(f"[EH] 讀取 EMM 記錄 ({idx}/{total_rows})...", val)
            
            _eh_log_elapsed("EMM Mangas table read", t_db)
    except sqlite3.Error as e:
        log_error(f"[EH 外掛] 讀取資料庫時發生錯誤: {e}"); return

    _update_progress("正在計算差異...", 39)
    t_diff = perf_counter()
    db_paths = set(db_records.keys())
    paths_to_add = local_paths - db_paths
    
    paths_to_soft_delete = {
        p for p in (db_paths - local_paths)
        if db_records.get(p, {}).get('status') not in ALL_DELETED_STATUSES
        or (has_exist and db_records.get(p, {}).get('exist') != 0)
        or (has_hidden_book and db_records.get(p, {}).get('hiddenBook') != 1)
    }
    paths_to_restore = {
        p for p in (local_paths & db_paths)
        if db_records.get(p, {}).get('status') in ALL_DELETED_STATUSES
        or (has_exist and db_records.get(p, {}).get('exist') == 0)
        or (has_hidden_book and db_records.get(p, {}).get('hiddenBook') == 1)
    }

    _eh_log_elapsed("Difference calculation", t_diff)
    log_info(f"[EH 外掛] 比對完成：{len(paths_to_add)} 待新增, {len(paths_to_soft_delete)} 待軟刪除, {len(paths_to_restore)} 待還原。")
    
    t_records = perf_counter()
    new_records = []
    if paths_to_add:
        _update_progress(f"正在建立 {len(paths_to_add)} 筆新記錄...", 41)
        for idx, path in enumerate(paths_to_add, start=1):
            if rec := create_manga_record(path.replace('/', '\\'), url_map):
                new_records.append(rec)
            if idx % 100 == 0:
                _update_progress(f"[EH] 準備新記錄 ({idx}/{len(paths_to_add)})...", 41 + (idx/len(paths_to_add)*4))
        _eh_log_elapsed("New records creation", t_records)
        
    _update_progress("正在寫入資料庫...", 45)
    t_write = perf_counter()
    update_database_records(db_path, records_to_add=new_records, paths_to_soft_delete=list(paths_to_soft_delete), paths_to_restore=list(paths_to_restore))
    _eh_log_elapsed("Database sync (write)", t_write)
    
    # 同步 metadata.sqlite (如果有的話)
    metadata_path = os.path.join(data_dir, "metadata.sqlite")
    if os.path.exists(metadata_path):
        t_meta = perf_counter()
        _update_progress("正在同步 metadata.sqlite...", 48)
        try:
            with sqlite3.connect(db_path) as conn:
                status_in_sql, status_in_params = _status_in_clause("status", ALL_DELETED_STATUSES)
                status_not_in_sql, status_not_in_params = _status_not_in_clause("status", ALL_DELETED_STATUSES)
                
                if has_exist:
                    deleted_sql = f"SELECT hash FROM Mangas WHERE hash IS NOT NULL AND (exist = 0 OR {status_in_sql})"
                    active_sql = f"SELECT hash FROM Mangas WHERE hash IS NOT NULL AND exist = 1 AND {status_not_in_sql}"
                else:
                    deleted_sql = f"SELECT hash FROM Mangas WHERE hash IS NOT NULL AND {status_in_sql}"
                    active_sql = f"SELECT hash FROM Mangas WHERE hash IS NOT NULL AND {status_not_in_sql}"
                
                deleted_candidate_hashes = {row[0] for row in conn.execute(deleted_sql, status_in_params)}
                active_hashes = {row[0] for row in conn.execute(active_sql, status_not_in_params)}

            # 只有「完全沒有活著紀錄」的雜湊才執行刪除標記
            safe_deleted_hashes = list(deleted_candidate_hashes - active_hashes)
            if safe_deleted_hashes:
                affected = _sync_metadata_status_by_hash(metadata_path, safe_deleted_hashes, DELETED_STATUS, exclude_statuses=ALL_DELETED_STATUSES)
                if affected > 0:
                    log_info(f"[EH 外掛] Metadata 同步：標記 {affected} 個已刪除的雜湊。")

            # 救回被誤殺的紀錄：如果主庫活著但 metadata 被標記為刪除，則補正回來
            if active_hashes:
                affected_repair = _sync_metadata_status_by_hash(metadata_path, list(active_hashes), METADATA_REPAIRED_STATUS, only_statuses=ALL_DELETED_STATUSES)
                if affected_repair > 0:
                    log_info(f"[EH 外掛] Metadata 修復：將 {affected_repair} 個誤刪記錄恢復為 '{METADATA_REPAIRED_STATUS}'。")
        except Exception as e:
            log_error(f"[EH 外掛] Metadata 同步失敗: {e}")
        _eh_log_elapsed("metadata.sqlite sync", t_meta)

    _update_progress("資料庫同步完成。", 50)
    _eh_log_elapsed("Total run_full_sync_headless", t_total)
    log_info("[EH 外掛] 資料庫完整同步完成 (Headless)。")

_PENDING_FILENAME = "download_dashboard_pending.jsonl"
_MAX_WRITE_RETRIES = 5
_WRITE_BACKOFF = 0.6

def _plugin_dir() -> Path:
    return Path(os.path.dirname(__file__))

def _pending_path() -> Path:
    return _plugin_dir() / _PENDING_FILENAME

def _atomic_write_text_to_path(path: Path, text: str, max_retries: int = _MAX_WRITE_RETRIES, backoff: float = _WRITE_BACKOFF) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    attempt = 0
    while attempt < max_retries:
        temp_name = None
        try:
            with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8-sig", newline="") as tf:
                tf.write(text); temp_name = tf.name
            os.replace(temp_name, str(path))
            return True
        except PermissionError:
            if temp_name and os.path.exists(temp_name):
                try: os.remove(temp_name)
                except Exception: pass
            attempt += 1; time.sleep(backoff)
        except Exception:
            if temp_name and os.path.exists(temp_name):
                try: os.remove(temp_name)
                except Exception: pass
            raise
    return False

def _csv_rows_to_text(rows: List[List[Any]]) -> str:
    buf = io.StringIO(); writer = csv.writer(buf, lineterminator="\n")
    for r in rows: writer.writerow(r)
    return buf.getvalue()

def _append_pending_rows(rows: Iterable[Sequence[Any]]):
    p = _pending_path(); p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        for row in rows:
            obj = {"Name": row[0] if len(row)>0 else "", "Url": row[1] if len(row)>1 else "", "Status": row[2] if len(row)>2 else "", "Artist": row[3] if len(row)>3 else "", "Group": row[4] if len(row)>4 else ""}
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _read_pending_items() -> List[Dict[str, Any]]:
    p = _pending_path()
    if not p.exists(): return []
    items: List[Dict[str, Any]] = []
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try: items.append(json.loads(line))
                except Exception: continue
    except Exception: return []
    return items

def _clear_pending():
    p = _pending_path()
    try:
        if p.exists(): p.unlink()
    except Exception: pass

def _merge_pending_into_rows(all_rows: List[List[Any]], pending_items: Iterable[Dict[str, Any]]) -> bool:
    if not all_rows: all_rows.append(['Name','URL','Status','Artist (Romaji)','Group (Romaji)'])
    header = all_rows[0]; url_to_idx: Dict[str,int] = {}
    for i, row in enumerate(all_rows[1:], start=1):
        if len(row) > 1: url_to_idx[row[1]] = i
    changed = False
    for item in pending_items:
        url = item.get("Url") or item.get("URL") or item.get("url")
        if not url: continue
        name, status, artist, group = item.get("Name",""), item.get("Status",""), item.get("Artist",""), item.get("Group","")
        new_row = [name, url, status, artist, group]
        if url in url_to_idx:
            idx = url_to_idx[url]
            if all_rows[idx] != new_row:
                all_rows[idx] = new_row; changed = True
        else:
            all_rows.append(new_row); url_to_idx[url] = len(all_rows) - 1; changed = True
    return changed

def _atomic_write_csv_rows(all_rows: List[List[Any]], csv_path: str) -> bool:
    text = _csv_rows_to_text(all_rows)
    return _atomic_write_text_to_path(Path(csv_path), text)

def flush_pending_to_main(csv_path: str) -> bool:
    pending = _read_pending_items()
    if not pending: return True
    p = Path(csv_path)
    if p.exists():
        try:
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f); rows = list(reader)
                if not rows: rows = [['Name','URL','Status','Artist (Romaji)','Group (Romaji)']]
        except Exception: return False
    else: rows = [['Name','URL','Status','Artist (Romaji)','Group (Romaji)']]
    changed = _merge_pending_into_rows(rows, pending)
    if not changed: _clear_pending(); return True
    ok = _atomic_write_csv_rows(rows, csv_path)
    if ok: _clear_pending(); return True
    else: return False

def update_csv_dashboard(json_data: list, csv_path: str):
    if not csv_path:
        log_warning("[EH plugin] CSV dashboard path is not configured; update skipped.")
        return
    try:
        if flush_pending_to_main(csv_path): log_info("[EH plugin] Pending CSV rows flushed.")
    except Exception as e: log_warning(f"[EH plugin] Pending CSV flush failed: {e}")
    header = ['Name','URL','Status','Artist (Romaji)','Group (Romaji)']; rows: List[List[Any]] = []
    p = Path(csv_path)
    if p.exists():
        try:
            with p.open('r', encoding='utf-8-sig', newline='') as f:
                reader = csv.reader(f); rows = list(reader)
                if not rows or rows[0] != header: rows = [header]
        except Exception: rows = [header]
    else: rows = [header]
    url_to_idx: Dict[str,int] = {}
    for i, r in enumerate(rows[1:], start=1):
        if len(r) > 1: url_to_idx[r[1]] = i
    changed_rows: List[List[Any]] = []
    for it in json_data:
        url = it.get('Url')
        if not url: continue
        name, status = it.get('Name',''), it.get('Command','')
        artist_romaji, group_romaji = analyze_title_tags(name)
        new_row = [name, url, status, artist_romaji, group_romaji]
        if url in url_to_idx:
            idx = url_to_idx[url]
            if rows[idx] != new_row: rows[idx] = new_row; changed_rows.append(new_row)
        else: rows.append(new_row); url_to_idx[url] = len(rows) - 1; changed_rows.append(new_row)
    if not changed_rows:
        log_info("[EH plugin] CSV dashboard has no changes.")
        return
    if _atomic_write_csv_rows(rows, csv_path):
        log_info(f"[EH plugin] CSV dashboard updated: {csv_path}; rows changed={len(changed_rows)}")
    else:
        _append_pending_rows(changed_rows)
        log_warning(f"[EH plugin] CSV dashboard write failed; queued {len(changed_rows)} rows as pending.")

def get_image_path(image_name: str) -> str:
    plugin_assets = os.path.join(os.path.dirname(__file__), 'assets', image_name)
    if os.path.exists(plugin_assets): return plugin_assets
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(project_root, 'assets', image_name)

try:
    import numpy as np, cv2
    _HAS_CV2 = True
except Exception: _HAS_CV2 = False

_DEFAULT_CONFIDENCE = 0.85; _DEFAULT_TIMEOUT = 3.0; _SCALE_SET = [1.25, 1.10, 1.00, 0.90, 0.80]

def _pil_open_strict(path: str) -> Image.Image | None:
    try: return Image.open(path).convert('RGB')
    except Exception: return None

def _cv2_read_unicode(path: str):
    if not _HAS_CV2: return None
    try:
        data = np.fromfile(path, dtype=np.uint8); img = cv2.imdecode(data, cv2.IMREAD_COLOR)
        return img
    except Exception: return None

def _to_cv(bgr_or_pil):
    if not _HAS_CV2: return None
    if isinstance(bgr_or_pil, Image.Image):
        rgb = np.array(bgr_or_pil); return cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
    return bgr_or_pil

def _match_template_cv(screen_bgr, needle_bgr, confidence: float):
    if not _HAS_CV2: return None
    H, W = needle_bgr.shape[:2]; best = None
    for scale in _SCALE_SET:
        try: resized = cv2.resize(needle_bgr, (int(W*scale), int(H*scale)), interpolation=cv2.INTER_AREA)
        except Exception: continue
        for use_gray in (False, True):
            src = cv2.cvtColor(screen_bgr, cv2.COLOR_BGR2GRAY) if use_gray else screen_bgr
            tpl = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY) if use_gray else resized
            res = cv2.matchTemplate(src, tpl, cv2.TM_CCOEFF_NORMED)
            min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
            if max_val >= confidence:
                h, w = tpl.shape[:2]; return (max_loc[0] + w//2, max_loc[1] + h//2)
            if (best is None) or (max_val > best[0]): best = (max_val, max_loc, tpl.shape[1], tpl.shape[0])
    return None

def _pillow_exact_match(screen_img: Image.Image, needle_img: Image.Image):
    try:
        import pyscreeze; pyscreeze.useOpenCV = False
        loc = pyautogui.locateCenterOnScreen(needle_img)
        return (loc.x, loc.y) if loc else None
    except Exception: return None

class _ScreenFinder:
    def __init__(self):
        try:
            import pyscreeze; pyscreeze.useOpenCV = bool(_HAS_CV2)
        except Exception: pass
    def _screenshot_cv(self):
        if not _HAS_CV2: return None
        return _to_cv(pyautogui.screenshot())
    def locate(self, image_name: str, confidence: float = _DEFAULT_CONFIDENCE, timeout: float = _DEFAULT_TIMEOUT):
        path = get_image_path(image_name)
        if not os.path.exists(path): log_error(f"[EH ?芸?? ?曆??啣????? {path}"); return None
        needle_pil = _pil_open_strict(path)
        if needle_pil is None: log_error(f"[EH ?芸?? ???澆?銝?湔?撌脫?憯? {path}"); return None
        start = time.time()
        while time.time() - start < timeout:
            try:
                loc = pyautogui.locateCenterOnScreen(needle_pil, confidence=confidence) if _HAS_CV2 else pyautogui.locateCenterOnScreen(needle_pil)
                if loc: return (loc.x, loc.y)
            except Exception: break
            time.sleep(0.25)
            if _HAS_CV2:
                screen_bgr = self._screenshot_cv(); needle_bgr = _to_cv(needle_pil)
                if screen_bgr is not None and needle_bgr is not None:
                    if pt := _match_template_cv(screen_bgr, needle_bgr, confidence): return pt
            if pt := _pillow_exact_match(pyautogui.screenshot(), needle_pil): return pt
        return None
    def click(self, image_name: str, confidence: float = _DEFAULT_CONFIDENCE, timeout: float = _DEFAULT_TIMEOUT, delay: float = 0.4):
        if pt := self.locate(image_name, confidence=confidence, timeout=timeout):
            try: pyautogui.click(pt[0], pt[1]); time.sleep(delay); return True
            except Exception as e: log_warning(f"[EH ?芸?? click 憭望?: {e}")
        return False

SCREEN = _ScreenFinder()

def find_element(image_name: str, confidence: float = _DEFAULT_CONFIDENCE, timeout: float = _DEFAULT_TIMEOUT):
    if pt := SCREEN.locate(image_name, confidence=confidence, timeout=timeout):
        class _P: pass
        o = _P(); o.x, o.y = pt[0], pt[1]
        return o
    return None

def find_and_click(image_name: str, confidence: float = _DEFAULT_CONFIDENCE, timeout: float = _DEFAULT_TIMEOUT) -> bool:
    return SCREEN.click(image_name, confidence=confidence, timeout=timeout, delay=0.5)

def activate_window_by_pid(pid: int) -> bool:
    if not AUTOMATION_LIBS_AVAILABLE: return False
    found_hwnd = None
    def foreach_window(hwnd, lParam):
        nonlocal found_hwnd
        if ctypes.windll.user32.IsWindowVisible(hwnd):
            lpdwProcessId = ctypes.c_ulong()
            ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(lpdwProcessId))
            if lpdwProcessId.value == pid: found_hwnd = hwnd; return False
        return True
    try:
        EnumWindows = ctypes.windll.user32.EnumWindows
        WINFUNCTYPE = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int))
        EnumWindows(WINFUNCTYPE(foreach_window), 0)
        if found_hwnd:
            ctypes.windll.user32.ShowWindow(found_hwnd, 3)
            ctypes.windll.user32.SetForegroundWindow(found_hwnd)
            log_info(f"  -> Activated window for PID {pid}.")
            return True
        else:
            log_warning(f"  -> Could not find visible window for PID {pid}.")
            return False
    except Exception as e:
        log_error(f"  -> Failed to activate window: {e}")
        return False

def _get_current_hkl():
    if not AUTOMATION_LIBS_AVAILABLE: return None
    try: return ctypes.windll.user32.GetKeyboardLayout(ctypes.windll.user32.GetWindowThreadProcessId(ctypes.windll.user32.GetForegroundWindow(), None))
    except Exception: return None

def ensure_english_input():
    if not AUTOMATION_LIBS_AVAILABLE: return
    try: ctypes.windll.user32.ActivateKeyboardLayout(ctypes.windll.user32.LoadKeyboardLayoutA(b"00000409", 1), 256)
    except Exception as e: log_warning(f"強制切換英文輸入法失敗: {e}")

def restore_keyboard_layout(original_hkl):
    if original_hkl and AUTOMATION_LIBS_AVAILABLE:
        try: ctypes.windll.user32.ActivateKeyboardLayout(original_hkl, 256)
        except Exception as e: log_warning(f"還原輸入法失敗: {e}")

def _spawn_eh_manager(app_path: str, port: int):
    try:
        creation_flags = 0x00000008 | 0x00000200
        launch_args = [
            app_path,
            f'--remote-debugging-port={port}',
            '--remote-allow-origins=*',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
        ]
        # Preparation for spawning EMM

        emm_log_path = os.path.join(LOG_DIR, "emm_subprocess.log")
        log_info(f'[EH automation] Spawning EMM with args: {launch_args}, logging to: {emm_log_path}')
        
        # Check if binary exists
        if app_path and not os.path.exists(app_path):
            log_error(f'[EH automation] EMM binary not found at: {app_path}')
            return None

        # Open log file for the subprocess
        try:
            f_log = open(emm_log_path, 'a', encoding='utf-8', buffering=1)
            f_log.write(f"\n--- EMM Launch at {datetime.datetime.now()} ---\n")
            f_log.write(f"Args: {launch_args}\n")
            f_log.flush()
        except Exception as e:
            log_error(f'[EH automation] Failed to open EMM log file: {e}')
            f_log = open(os.devnull, 'w')

        env = os.environ.copy()
        proc = subprocess.Popen(
            launch_args,
            stdout=f_log,
            stderr=subprocess.STDOUT,
            close_fds=True,
            creationflags=creation_flags,
            env=env,
            cwd=os.path.dirname(app_path) if app_path else None
        )
        try:
            f_log.close()
        except Exception:
            pass
        return proc
    except Exception as exc:
        log_error(f'[EH automation] Failed to launch EMM: {exc}')
        return None

def _rotate_eh_backups(backup_base: str):
    """執行備份輪轉策略：保留最近 10 份或 7 天內的所有備份 (取其多)。
    特別保護 pre_restore_* 備份目錄。
    """
    if not os.path.isdir(backup_base):
        return

    log_info(f"[EH 備份] 執行輪轉策略 (保留至少 {EH_BACKUP_MIN_KEEP} 份 / {EH_BACKUP_KEEP_DAYS} 天)...")
    
    all_dirs = []
    for d in os.listdir(backup_base):
        d_path = os.path.join(backup_base, d)
        if not os.path.isdir(d_path):
            continue
        if len(d) != 17:
            continue
        try:
            backup_time = datetime.datetime.strptime(d, "%Y-%m-%d_%H%M%S")
        except ValueError:
            continue
        all_dirs.append({'name': d, 'path': d_path, 'time': backup_time})

    if not all_dirs:
        log_info("  -> 無可清理的自動化備份目錄。")
        return

    all_dirs.sort(key=lambda x: x['time'], reverse=True)
    
    cutoff_time = datetime.datetime.now() - datetime.timedelta(days=EH_BACKUP_KEEP_DAYS)
    
    to_keep = []
    to_delete = []
    
    for i, d_info in enumerate(all_dirs):
        if i < EH_BACKUP_MIN_KEEP or d_info['time'] >= cutoff_time:
            to_keep.append(d_info)
        else:
            to_delete.append(d_info)
            
    if to_delete:
        log_info(f"  -> 發現 {len(all_dirs)} 個備份，將保留 {len(to_keep)} 個，刪除 {len(to_delete)} 個過期備份。")
        for d_info in to_delete:
            try:
                shutil.rmtree(d_info['path'])
                log_info(f"    - 已刪除過期備份: {d_info['name']}")
            except Exception as e:
                log_error(f"    - 刪除備份失敗 {d_info['name']}: {e}")
    else:
        log_info(f"  -> 目前備份數 ({len(to_keep)}) 未觸發輪轉閥值。")

def create_database_backup(config: Dict):
    """在 EMM 自動化前備份 EH SQLite 資料庫（database.sqlite + metadata.sqlite）。
    每次備份建立一個時戳子目錄，並執行輪轉策略。
    """
    log_info("[EH 外掛] 正在檢查並執行資料庫備份...")
    backup_base = config.get('eh_backup_directory')
    if not backup_base:
        log_info("  -> 未設定備份資料夾，跳過備份程序。"); return
    data_dir = config.get('eh_data_directory', '')
    primary = os.path.join(data_dir, "database.sqlite")
    if not os.path.exists(primary):
        log_warning(f"  -> 找不到來源資料庫，無法備份: {primary}"); return
    try:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        backup_set_dir = os.path.join(backup_base, timestamp)
        os.makedirs(backup_set_dir, exist_ok=True)
        # 備份兩個目標檔案
        for fname in ('database.sqlite', 'metadata.sqlite'):
            src = os.path.join(data_dir, fname)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(backup_set_dir, fname))
                log_info(f"  -> 備份 {fname} → {backup_set_dir}")
            else:
                log_warning(f"  -> {fname} 不存在，略過。")
        
        # 執行輪轉清理
        _rotate_eh_backups(backup_base)
        
    except Exception as e:
        log_error(f"[EH 外掛] 資料庫備份失敗: {e}", include_traceback=True)


def restore_latest_backup(config: Dict) -> tuple:
    """還原最新一組備份（database.sqlite + metadata.sqlite）。
    還原前會自動關閉 EMM。
    回傳 (success: bool, message: str)。
    """
    backup_base = config.get('eh_backup_directory', '')
    if not backup_base or not os.path.isdir(backup_base):
        return False, "未設定備份資料夾或資料夾不存在。"
    all_sets = []
    for d in os.listdir(backup_base):
        backup_set_dir = os.path.join(backup_base, d)
        if not os.path.isdir(backup_set_dir):
            continue
        if len(d) != 17:
            continue
        try:
            datetime.datetime.strptime(d, "%Y-%m-%d_%H%M%S")
        except ValueError:
            continue
        if os.path.exists(os.path.join(backup_set_dir, 'database.sqlite')):
            all_sets.append(d)
    all_sets.sort()
    if not all_sets:
        return False, "找不到可還原的標準備份組。"
    latest_set = all_sets[-1]
    backup_set_dir = os.path.join(backup_base, latest_set)
    data_dir = config.get('eh_data_directory', '')
    if not data_dir or not os.path.isdir(data_dir):
        return False, f"目標資料夾不存在: {data_dir}"
    # 先關閉 EMM
    try:
        close_manga_app_if_running(config)
    except Exception:
        pass
    
    # 建立「還原前」的緊急備份 (Rollback)
    rollback_set_dir = os.path.join(
        backup_base,
        'pre_restore_' + datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
    )
    try:
        os.makedirs(rollback_set_dir, exist_ok=False)
        rollback_files = []
        for fname in ('database.sqlite', 'metadata.sqlite'):
            current_path = os.path.join(data_dir, fname)
            if os.path.exists(current_path):
                shutil.copy2(current_path, os.path.join(rollback_set_dir, fname))
                rollback_files.append(fname)
        if rollback_files:
            log_info(f"[EH 備份] 已建立還原前備份 (Rollback): {rollback_set_dir}")
        else:
            shutil.rmtree(rollback_set_dir, ignore_errors=True)
            return False, "目前資料庫不存在，取消還原。"
    except Exception as e:
        log_error(f"[EH 備份] 建立還原前備份失敗: {e}", include_traceback=True)
        return False, f"建立還原前備份失敗: {e}"

    restored = []
    try:
        for fname in ('database.sqlite', 'metadata.sqlite'):
            src = os.path.join(backup_set_dir, fname)
            dst = os.path.join(data_dir, fname)
            if os.path.exists(src):
                shutil.copy2(src, dst)
                restored.append(fname)
                log_info(f"  -> 已還原 {fname} ← {src}")
            else:
                log_warning(f"  -> 備份組 [{latest_set}] 中找不到 {fname}，略過。")
    except Exception as e:
        log_error(f"[EH plugin] Backup restore failed: {e}", include_traceback=True)
        return False, f"Restore failed: {e}"
    if restored:
        msg = f"Restored latest backup set [{latest_set}]:\n" + "\n".join(restored)
        return True, msg
    return False, "Backup set did not contain any restorable files."


def _legacy_close_manga_app_if_running(config: Dict):
    """Deprecated legacy GUI close hook. Kept only to avoid stale references."""
    log_warning("[EH legacy] _legacy_close_manga_app_if_running is deprecated; active close_manga_app_if_running is defined below.")


def fix_pagediff(config: Dict) -> tuple:
    """Fix EMM pagediff warnings and log the affected folders to a CSV file."""
    data_dir = config.get('eh_data_directory', '')
    if not data_dir or not os.path.isdir(data_dir):
        return False, f"Invalid EMM data directory: {data_dir}"
    
    db_path = os.path.join(data_dir, 'database.sqlite')
    if not os.path.exists(db_path):
        return False, f"database.sqlite not found: {db_path}"
        
    try:
        import csv
        create_database_backup(config)
        close_manga_app_if_running(config)
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Fetch affected rows with detailed columns
            cursor.execute("SELECT title, title_jpn, url, pageCount, filecount, filepath FROM Mangas WHERE pageCount != filecount AND status != 'non-tag'")
            affected_rows = cursor.fetchall()
            
            if not affected_rows:
                return True, "沒有發現需要修復的 Pagediff 項目。"
                
            # Log the affected folders to a CSV
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file_path = os.path.join(os.path.dirname(db_path), f'pagediff_fixed_log_{timestamp}.csv')
            
            with open(log_file_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['English Title', 'Japanese Title', 'URL', 'Local Page Count (Old)', 'Metadata Page Count (New)', 'File Path'])
                for row in affected_rows:
                    writer.writerow(row)
            
            # Perform the fix
            cursor.execute("UPDATE Mangas SET pageCount = filecount WHERE pageCount != filecount AND status != 'non-tag'")
            conn.commit()
            
            return True, f"成功修復了 {len(affected_rows)} 筆 Pagediff 項目！\n\n已將被修改的詳細清單輸出為 CSV：\n{log_file_path}\n\n重新啟動 EMM 後，紅字警告將會消失。"
    except Exception as e:
        log_error(f"[EH plugin] Fix pagediff failed: {e}", include_traceback=True)
        return False, f"修復失敗: {e}"


def count_untagged_manga(db_path: str) -> int:
    if not os.path.exists(db_path): return 0
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA query_only = ON")
            return conn.execute("SELECT COUNT(*) FROM Mangas WHERE status = 'non-tag'").fetchone()[0]
    except sqlite3.Error: return 0


def _legacy_run_automation_suite_headless(config: Dict, progress_queue: Optional[any], control_events: Dict):
    """Deprecated legacy EMM automation. Active implementation is run_automation_suite_headless below."""
    raise RuntimeError("Deprecated legacy EMM automation path must not be used; use run_automation_suite_headless instead.")


# --- Clean EH plugin overrides: CDP-only runtime path ---
try:
    import psutil as _eh_psutil
    _EH_PROCESS_LIBS_AVAILABLE = True
except ImportError:
    _eh_psutil = None
    _EH_PROCESS_LIBS_AVAILABLE = False


def _eh_build_progress_callback(progress_queue: Optional[any]):
    def _update_progress(text: str, value: Optional[int] = None):
        if not progress_queue:
            return
        payload = {'type': 'progress' if value is not None else 'text', 'text': text}
        if value is not None:
            payload['value'] = value
        progress_queue.put(payload)
    return _update_progress


def _eh_is_cancelled(control_events: Optional[Dict]) -> bool:
    if not control_events:
        return False
    cancel_event = control_events.get('cancel')
    return bool(cancel_event and cancel_event.is_set())


def _eh_run_checkpoint(db_path: str):
    try:
        log_info('[EH plugin] Running WAL checkpoint...')
        with sqlite3.connect(db_path) as conn:
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE);')
        log_info('  -> WAL checkpoint completed.')
    except sqlite3.OperationalError as exc:
        if 'database is locked' not in str(exc):
            log_error(f'[EH plugin] WAL checkpoint failed: {exc}')
            return
        log_warning('[EH plugin] Database is locked; retrying WAL checkpoint once...')
        time.sleep(1)
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute('PRAGMA wal_checkpoint(TRUNCATE);')
            log_info('  -> WAL checkpoint completed after retry.')
        except Exception as retry_exc:
            log_error(f'[EH plugin] WAL checkpoint retry failed: {retry_exc}')
    except Exception as exc:
        log_error(f'[EH plugin] WAL checkpoint unexpected error: {exc}')

def _eh_validate_runtime_paths(config: Dict[str, Any]) -> Optional[str]:
    required_paths = ['eh_data_directory', 'root_scan_folder']
    if config.get('automation_enabled', False):
        required_paths.append('eh_manga_manager_path')
    for key in required_paths:
        value = config.get(key)
        if not value:
            return f'Missing required setting: {key}'
        if not os.path.exists(value):
            return f'Configured path does not exist: {value}'
    db_path = os.path.join(config.get('eh_data_directory', ''), 'database.sqlite')
    if not os.path.isfile(db_path):
        return f'database.sqlite not found: {db_path}'
    return None

def close_manga_app_if_running(config: Dict):
    if not _EH_PROCESS_LIBS_AVAILABLE:
        log_warning('[EH automation] psutil is unavailable; cannot close EMM automatically.')
        return
    manga_app_path = config.get('eh_manga_manager_path', '')
    if not manga_app_path:
        log_warning('[EH automation] eh_manga_manager_path is not configured; close skipped.')
        return
    target_app_name = os.path.basename(manga_app_path)
    log_info(f'[EH automation] Closing running EMM process if present: {target_app_name}')
    try:
        found_count = 0
        for proc in _eh_psutil.process_iter(['name', 'pid']):
            try:
                if proc.info['name'] and proc.info['name'].lower() == target_app_name.lower():
                    log_info(f'  -> Terminating PID {proc.pid}...')
                    try:
                        proc.terminate()
                        proc.wait(timeout=3)
                    except (_eh_psutil.NoSuchProcess, _eh_psutil.TimeoutExpired):
                        proc.kill()
                        proc.wait(timeout=3)
                    found_count += 1
            except (_eh_psutil.NoSuchProcess, _eh_psutil.AccessDenied):
                continue
        if found_count > 0:
            log_info(f'  -> Closed {found_count} EMM process(es).')
        else:
            log_info('  -> No running EMM process found.')
    except Exception as exc:
        log_warning(f'[EH automation] Failed while closing EMM: {exc}')

def run_automation_suite_headless(config: Dict, progress_queue: Optional[any], control_events: Dict):
    update_progress = _eh_build_progress_callback(progress_queue)
    try:
        import urllib.request
        import json as _json
        import socket
        from websocket import create_connection
    except ImportError as e:
        log_error(f'[EH automation] Dependency missing: {e}')
        update_progress(f'Error: missing dependency {e}', 100)
        return

    db_path = os.path.join(config.get('eh_data_directory'), 'database.sqlite')
    task_limit = count_untagged_manga(db_path)
    if 'summary' in globals() and summary is not None:
        summary.tasks_total = task_limit

    if task_limit == 0:
        log_info('[EH automation] No non-tag records found; skipping EMM automation.')
        update_progress('No non-tag records to process.', 100)
        return

    log_info(f'[EH automation] Processing {task_limit} non-tag record(s) via EMM/CDP detail view.')
    app_path = config.get('eh_manga_manager_path')
    if not app_path or not os.path.exists(app_path):
        log_error('[EH automation] EMM application path is missing or invalid.')
        update_progress('Error: invalid EMM application path.', 100)
        return

    close_manga_app_if_running(config)
    create_database_backup(config)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        cdp_port = sock.getsockname()[1]

    proc = _spawn_eh_manager(app_path, cdp_port)
    if not proc:
        update_progress('Error: failed to launch EMM.', 100)
        return

    log_info(f'[EH automation] EMM launched (PID: {proc.pid}, CDP port: {cdp_port})')
    update_progress(f'Waiting for EMM CDP port {cdp_port}...', 60)

    ws_url = None
    json_url = f'http://127.0.0.1:{cdp_port}/json'
    started_time = time.time()
    while time.time() - started_time < 300:
        if _eh_is_cancelled(control_events):
            log_info('[EH automation] Cancelled while waiting for EMM CDP.')
            return
        try:
            with urllib.request.urlopen(urllib.request.Request(json_url), timeout=2) as response:
                pages = _json.loads(response.read().decode('utf-8'))
            for page in pages:
                if page.get('type') == 'page' and page.get('webSocketDebuggerUrl'):
                    ws_url = page['webSocketDebuggerUrl']
                    break
            if ws_url:
                break
        except Exception:
            pass
        time.sleep(1)

    if not ws_url:
        log_error(f'[EH CDP] Could not connect to EMM CDP port {cdp_port}.')
        update_progress('Error: could not connect to EMM CDP.', 100)
        return

    class CDPClient:
        def __init__(self, url: str):
            self.ws = create_connection(url)
            self.msg_id = 0

        def evaluate(self, expression: str):
            self.msg_id += 1
            payload = {'id': self.msg_id, 'method': 'Runtime.evaluate', 'params': {'expression': expression, 'returnByValue': True, 'awaitPromise': True}}
            self.ws.send(_json.dumps(payload))
            while True:
                resp = _json.loads(self.ws.recv())
                if resp.get('id') == self.msg_id:
                    if 'exceptionDetails' in resp.get('result', {}):
                        raise Exception(f"CDP Evaluate Error: {resp['result']['exceptionDetails']}")
                    return resp.get('result', {}).get('result', {}).get('value')

        def execute(self, method: str, params=None):
            self.msg_id += 1
            payload = {'id': self.msg_id, 'method': method, 'params': params or {}}
            self.ws.send(_json.dumps(payload))
            while True:
                resp = _json.loads(self.ws.recv())
                if resp.get('id') == self.msg_id:
                    if 'error' in resp:
                        raise Exception(f"CDP Execute Error: {resp['error']}")
                    return resp.get('result', {})

        def close(self):
            self.ws.close()

    speed = config.get('automation_speed', 'fast').strip().lower()
    timing = {
        'fast': {'poll_ms': 500, 'loading_settle_ms': 2500, 'ui_settle_ms': 2500, 'input_confirm_ms': 1500, 'post_search_ms': 500, 'open_book_ms': 1800, 'page_turn_ms': 100, 'escape_ms': 500, 'wait_ready_timeout_ms': 30000, 'batch_poll_attempts': 100},
        'normal': {'poll_ms': 500, 'loading_settle_ms': 3000, 'ui_settle_ms': 3000, 'input_confirm_ms': 1800, 'post_search_ms': 500, 'open_book_ms': 2200, 'page_turn_ms': 120, 'escape_ms': 600, 'wait_ready_timeout_ms': 45000, 'batch_poll_attempts': 120},
        'safe': {'poll_ms': 500, 'loading_settle_ms': 3800, 'ui_settle_ms': 3600, 'input_confirm_ms': 2200, 'post_search_ms': 700, 'open_book_ms': 2600, 'page_turn_ms': 150, 'escape_ms': 800, 'wait_ready_timeout_ms': 60000, 'batch_poll_attempts': 150},
    }.get(speed, {'poll_ms': 500, 'loading_settle_ms': 2500, 'ui_settle_ms': 2500, 'input_confirm_ms': 1500, 'post_search_ms': 500, 'open_book_ms': 1800, 'page_turn_ms': 100, 'escape_ms': 500, 'wait_ready_timeout_ms': 30000, 'batch_poll_attempts': 100})

    client = None
    try:
        client = CDPClient(ws_url)
        log_info('[EH CDP] Waiting for search input focus...')
        js_focus = f"""
        (async () => {{
            const sleep = (ms) => new Promise(r => setTimeout(r, ms));
            let retries = 0;
            let searchInput = null;
            while (retries < 60) {{
                const loadingMask = document.querySelector('.el-loading-mask');
                if (loadingMask && loadingMask.offsetParent !== null) {{
                    await sleep({timing['poll_ms']});
                    retries++;
                    continue;
                }}
                searchInput = document.querySelector('.search-input input') || document.querySelector('input[type="text"]');
                if (searchInput && !searchInput.disabled) break;
                await sleep({timing['poll_ms']});
                retries++;
            }}
            if (!searchInput) return 'Timeout waiting for EMM UI search input';
            await sleep({timing['loading_settle_ms']});
            await sleep({timing['ui_settle_ms']});
            searchInput.focus();
            searchInput.value = '';
            searchInput.select();
            return 'OK';
        }})();
        """
        focus_res = client.evaluate(js_focus)
        if focus_res != 'OK':
            log_error(f'[EH CDP] Search input focus failed: {focus_res}')
            update_progress(f'Error: {focus_res}', 100)
            return

        log_info('[EH CDP] Searching for "non-tag" records...')
        time.sleep(1.0)
        client.execute('Input.insertText', {'text': '"non-tag"$'})
        time.sleep(timing['input_confirm_ms'] / 1000.0)
        client.execute('Input.dispatchKeyEvent', {'type': 'keyDown', 'windowsVirtualKeyCode': 13, 'key': 'Enter', 'code': 'Enter', 'text': '\r'})
        client.execute('Input.dispatchKeyEvent', {'type': 'keyUp', 'windowsVirtualKeyCode': 13, 'key': 'Enter', 'code': 'Enter'})

        log_info('[EH CDP] Running detail-view PageDown/PageUp rescan flow...')
        js_action = f"""
        (async () => {{
            const sleep = (ms) => new Promise(r => setTimeout(r, ms));
            const waitReady = async (timeoutMs = {timing['wait_ready_timeout_ms']}) => {{
                const started = Date.now();
                while (Date.now() - started < timeoutMs) {{
                    const masks = Array.from(document.querySelectorAll('.el-loading-mask'));
                    const visibleMasks = masks.filter(mask => mask && mask.offsetParent !== null);
                    if (visibleMasks.length === 0) return true;
                    await sleep({timing['poll_ms']});
                }}
                return false;
            }};
            const findButtonByText = (keywords) => {{
                const activeKeywords = keywords.includes('Rescan')
                    ? Array.from(new Set([...keywords, '\\u91cd\\u6383', '\\u91cd\\u626b']))
                    : keywords;
                return Array.from(document.querySelectorAll('button.el-button')).find(b =>
                    activeKeywords.some(keyword =>
                        (b.textContent && b.textContent.includes(keyword)) ||
                        (b.innerText && b.innerText.includes(keyword)) ||
                        (b.title && b.title.includes(keyword)) ||
                        (b.getAttribute('aria-label') && b.getAttribute('aria-label').includes(keyword))
                    )
                );
            }};
            const waitForButton = async (keywords, attempts = 20) => {{
                for (let i = 0; i < attempts; i++) {{
                    const btn = findButtonByText(keywords);
                    if (btn && !btn.disabled) return btn;
                    await sleep({timing['poll_ms']});
                }}
                return null;
            }};
            const clickElement = (el) => {{
                el.click();
                el.dispatchEvent(new MouseEvent('click', {{ bubbles: true, cancelable: true, view: window }}));
            }};
            const getTitles = () => Array.from(document.querySelectorAll('.book-title, [class*="book-title"]'))
                .filter(el => (el.innerText || el.textContent || '').trim().length > 0);
            const triggerSearch = async () => {{
                const searchBtns = Array.from(document.querySelectorAll('.book-search-bar button, button.el-button'));
                const searchBtn = searchBtns.find(b => /search|搜尋|搜索|查詢|查询/i.test(b.innerText || b.textContent || b.title || b.getAttribute('aria-label') || ''))
                    || searchBtns[1]
                    || searchBtns[0];
                if (searchBtn) clickElement(searchBtn);
                document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true }}));
                document.dispatchEvent(new KeyboardEvent('keyup', {{ key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true }}));
                await waitReady();
            }};
            const waitForTitles = async () => {{
                for (let round = 0; round < 3; round++) {{
                    await triggerSearch();
                    for (let attempts = 0; attempts < 30; attempts++) {{
                        const titles = getTitles();
                        if (titles.length > 0) return titles;
                        await sleep({timing['post_search_ms']});
                    }}
                }}
                return [];
            }};

            let titles = await waitForTitles();
            if (titles.length === 0) return 'No non-tag comics found on screen';

            const taskLimit = {task_limit};
            const processDetailView = async () => {{
                if (taskLimit <= 0) return 0;
                const firstTitle = getTitles()[0];
                if (!firstTitle) return 'No non-tag comics found on screen';
                const openTargets = [
                    firstTitle,
                    firstTitle.closest('.book-item, .book-card, .el-card, li, tr, [class*="book"]'),
                ].filter(Boolean);
                let opened = false;
                for (const target of openTargets) {{
                    clickElement(target);
                    target.dispatchEvent(new MouseEvent('dblclick', {{ bubbles: true, cancelable: true, view: window }}));
                    await sleep({timing['open_book_ms']});
                    if (await waitForButton(['\\u91cd\\u6383', '\\u91cd\\u626b', 'Rescan'], 8)) {{
                        opened = true;
                        break;
                    }}
                }}
                if (!opened) {{
                    firstTitle.focus && firstTitle.focus();
                    document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true }}));
                    document.dispatchEvent(new KeyboardEvent('keyup', {{ key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true }}));
                    await sleep({timing['open_book_ms']});
                }}

                const getDetailSignature = () => {{
                    const dialog = document.querySelector('.el-dialog, .el-modal-dialog') || document.body;
                    const img = dialog.querySelector('img');
                    const text = (dialog.innerText || '').replace(/\\s+/g, ' ').slice(0, 800);
                    return text + '|' + (img ? img.src : '');
                }};
                const getTransientMessage = () => Array.from(document.querySelectorAll('.el-message, .el-notification, [role="alert"]'))
                    .map(el => (el.innerText || el.textContent || '').trim())
                    .filter(Boolean)
                    .join(' ');

                let forwardProcessed = 0;
                for (let i = 0; i < taskLimit; i++) {{
                    const rescanBtn = await waitForButton(['\\u91cd\\u6383', '\\u91cd\\u626b', 'Rescan'], 40);
                    if (!rescanBtn) return 'Error: Could not find Rescan button';
                    clickElement(rescanBtn);
                    await sleep(300); // 確保 Loading Mask 有機會出現
                    await waitReady();
                    forwardProcessed++;

                    if (i < taskLimit - 1) {{
                        document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'PageDown', code: 'PageDown', keyCode: 34, bubbles: true }}));
                        await sleep({timing['page_turn_ms']});
                        
                        const transientMessage = getTransientMessage();
                        if (/bottom|range|limit|out of/i.test(transientMessage)) break;
                    }}
                }}

                for (let j = 0; j < forwardProcessed - 1; j++) {{
                    document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'PageUp', code: 'PageUp', keyCode: 33, bubbles: true }}));
                    await sleep({timing['page_turn_ms']});
                    const rescanBtnBack = await waitForButton(['\\u91cd\\u6383', '\\u91cd\\u626b', 'Rescan'], 40);
                    if (rescanBtnBack) {{
                        clickElement(rescanBtnBack);
                        await sleep(300);
                        await waitReady();
                    }}
                }}

                document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Escape', code: 'Escape', keyCode: 27, bubbles: true }}));
                await waitReady();
                await sleep({timing['escape_ms']});
                return forwardProcessed;
            }};

            const processed = await processDetailView();
            if (typeof processed === 'string') return processed;
            if (processed <= 0) return 'No non-tag comics processed in detail view';

            const batchBtn = await waitForButton(['\\u624b\\u52d5\\u7372\\u53d6\\u5143\\u6578\\u64da', '\\u624b\\u52a8\\u83b7\\u53d6\\u5143\\u6570\\u636e', '\\u6279\\u91cf\\u7372\\u53d6\\u5143\\u6578\\u64da', '\\u6279\\u91cf\\u83b7\\u53d6\\u5143\\u6570\\u636e', 'Metadata'], {timing['batch_poll_attempts']});
            if (!batchBtn) return 'Error: Could not find Batch Fetch Metadata button';
            clickElement(batchBtn);
            await sleep({timing['escape_ms']});
            return processed;
        }})();
        """
        result = client.evaluate(js_action)
        if isinstance(result, (int, float)):
            log_info(f'[EH CDP] Detail-view automation completed. Processed: {result}')
            if summary: summary.tasks_processed = int(result)
            update_progress('EH automation completed.', 100)
        elif result is True or result == 'true':
            log_info('[EH CDP] Detail-view automation completed (success flag only).')
            update_progress('EH automation completed.', 100)
        else:
            log_error(f'[EH CDP] Detail-view automation returned: {result}')
            update_progress(f'Error: {result}', 100)
    except Exception as exc:
        import traceback
        log_error(f'[EH CDP] Automation failed: {exc}\n{traceback.format_exc()}')
        update_progress(f'Error: {exc}', 100)
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        log_info('[EH automation] EMM automation flow ended.')

def _eh_run_preprocessor_pipeline(config: Dict[str, Any], progress_queue: Optional[any], control_events: Optional[Dict]):
    update_progress = _eh_build_progress_callback(progress_queue)
    update_progress('[EH preprocessor] Starting...', 0)
    try:
        flush_pending_to_main(config.get('eh_csv_path', 'download_dashboard.csv'))
    except Exception:
        pass

    error_message = _eh_validate_runtime_paths(config)
    if error_message:
        log_error(f'[EH plugin] {error_message}')
        update_progress(f'Error: {error_message}')
        return

    if _eh_is_cancelled(control_events):
        return

    db_path = os.path.join(config.get('eh_data_directory'), 'database.sqlite')
    update_progress('Preparing EH database...', 10)
    add_normalized_path_column_if_not_exists(db_path)
    migrate_to_v20_structure(db_path)
    if _eh_is_cancelled(control_events):
        return

    load_translation_maps(config)
    if _eh_is_cancelled(control_events):
        return

    if config.get('eh_sync_enabled', False):
        run_full_sync_headless(config, progress_queue)
    else:
        log_info('[EH plugin] Database sync is disabled; skipping full sync.')

    if _eh_is_cancelled(control_events):
        return

    _eh_run_checkpoint(db_path)
    if _eh_is_cancelled(control_events):
        return

    if config.get('automation_enabled', False):
        run_automation_suite_headless(config, progress_queue, control_events or {})
    else:
        log_info('[EH plugin] UI automation is disabled; skipping EMM/CDP automation.')
    update_progress('[EH preprocessor] Complete.', 100)

class EhDatabaseToolsPlugin(BasePlugin):
    def get_id(self) -> str:
        return 'eh_database_tools'

    def get_name(self) -> str:
        return 'exhentai-manga-manager database tools'

    def get_description(self) -> str:
        return 'Syncs EH database records and runs optional EMM CDP automation.'

    def get_plugin_type(self) -> str:
        return 'preprocessor'

    def get_default_config(self):
        return {
            'enable_eh_preprocessor': False,
            'eh_sync_enabled': False, # 預設關閉同步與空資料夾掃描
            'eh_data_directory': '',
            'eh_backup_directory': '',
            'eh_syringe_directory': '',
            'eh_mmd_json_path': '',
        }

    def get_slot_order(self) -> int:
        return 10

    def plugin_prefers_inner_enable(self) -> bool:
        return True

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any], ui_vars: Dict) -> Optional['ttk.Frame']:
        from . import plugin_gui
        return plugin_gui.create_settings_frame(parent_frame, config, ui_vars)

    def save_settings(self, config: Dict[str, Any], ui_vars: Dict) -> Dict[str, Any]:
        from . import plugin_gui
        return plugin_gui.save_settings(config, ui_vars)

    def run(self, config: Dict, progress_queue: Optional[any], control_events: Optional[Dict], app_update_callback=None):
        global summary
        summary = ExecutionSummary()
        summary.mode = 'EH preprocessor'
        update_progress = _eh_build_progress_callback(progress_queue)

        try:
            from nanoid import generate  # noqa: F401
        except ImportError:
            message = 'Missing dependency: nanoid. Please install it before running the EH plugin.'
            log_error(f'[EH plugin] {message}')
            update_progress(f'Error: {message}')
            return None

        try:
            _eh_run_preprocessor_pipeline(config, progress_queue, control_events)
        except Exception as exc:
            log_error(f'[EH plugin] Preprocessor failed: {exc}', include_traceback=True)
            update_progress(f'Error: {exc}')
        finally:
            try:
                flush_pending_to_main(config.get('eh_csv_path', 'download_dashboard.csv'))
            except Exception:
                pass

        try:
            export_tag_failed_to_csv(config)
        except Exception as exc:
            log_error(f'[EH plugin] tag-failed export failed: {exc}')

        if summary:
            summary.report()
        return summary
