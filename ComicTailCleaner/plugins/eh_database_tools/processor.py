# ======================================================================
# 檔案：plugins/eh_database_tools/processor.py
# 目的：實現一個「前置處理器」，在主任務前同步 EH 資料庫
# 版本：1.9.10 (穩定性修正：確保進程檢查絕不中斷主流程)
# ======================================================================
# v1.9.10 更新日誌:
#   1. 【防呆機制】: 將 close_manga_app_if_running 函式內部邏輯完全包裹在
#      try...except 區塊中，確保即使 psutil 發生錯誤或權限問題，
#      程式也只會記錄警告並「強制繼續」執行下一步，解決卡住問題。
#   2. 【狀態回饋】: 在檢查結束後立即更新 UI 進度，給予使用者明確的繼續訊號。
# ======================================================================

from __future__ import annotations
import os
import sqlite3
import hashlib
import datetime
import json
import time
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

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error, log_warning
from config import DATA_DIR

try:
    import pyautogui, keyboard, psutil, pyperclip, ctypes
    from ctypes import wintypes
    from PIL import Image
    AUTOMATION_LIBS_AVAILABLE = True
except ImportError:
    AUTOMATION_LIBS_AVAILABLE = False

GLOBAL_ARTIST_MAP = {}
GLOBAL_GROUP_MAP = {}
summary = None
PLUGIN_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))

PAGE_LOAD_DELAY = 2.0;
SEARCH_BOX_X_OFFSET = -100; TITLE_X_OFFSET = -100; TITLE_Y_OFFSET = -20
MAIN_SEARCH_ICON_IMG, BOOKMARK_ICON_IMG, BOOKMARK_ICON_READY_IMG, RESCAN_BUTTON_IMG, CLOSE_BUTTON_IMG, PAGE_END_IMG, CLEAR_SEARCH_BUTTON_IMG, NO_COVER_IMG = 'main_search_icon.png', 'bookmark_icon.png', 'bookmark_icon_ready.png', 'rescan_button.png', 'close_button.png', 'page_end.png', 'clear_search_button.png', 'no_cover.png'

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
        self.start_time = time.time(); self.end_time = None; self.mode = "未知"
        self.added = 0; self.soft_deleted = 0; self.restored = 0
        self.moved_empty = 0; self.tasks_total = 0; self.tasks_processed = 0
    def finish(self): self.end_time = time.time()
    def report(self):
        if not self.end_time: self.finish()
        duration = self.end_time - self.start_time; mins, secs = divmod(duration, 60)
        report_lines = ["\n", "="*70, f"[EH 外掛] 執行摘要報告 (v29.1 核心)", "="*70, f"執行模式: {self.mode}", f"歷時 {int(mins)}分 {int(secs)}秒", "--- 資料庫同步成果 ---", f"    [+] 新增記錄: {self.added} 筆", f"    [-] 軟刪除記錄: {self.soft_deleted} 筆", f"    [*] 還原記錄: {self.restored} 筆", f"    [+] 移動空資料夾: {self.moved_empty} 個", "--- UI 自動化成果 ---", f"    [*] 待處理任務總數: {self.tasks_total} 個", f"    [√] 成功處理任務: {self.tasks_processed} 個", "="*70]
        for line in report_lines: log_info(line)

def normalize_path(path: str) -> str:
    if not path: return ""
    return os.path.normpath(path).replace('\\', '/')

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', '_', name); name = name.strip('. '); return name

def add_normalized_path_column_if_not_exists(db_path: str):
    with sqlite3.connect(db_path) as conn:
        if 'filepath_normalized' not in [info[1] for info in conn.execute("PRAGMA table_info(Mangas)")]:
            log_info("[EH 外掛] 偵測到舊版資料庫，正在新增 'filepath_normalized' 欄位...")
            conn.execute("ALTER TABLE Mangas ADD COLUMN filepath_normalized TEXT")
            log_info("  -> 欄位新增完成。")

def migrate_to_v20_structure(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.executemany("UPDATE Mangas SET filepath = ? WHERE id = ?", [(path.replace('/', '\\'), pid) for pid, path in conn.execute("SELECT id, filepath FROM Mangas WHERE filepath LIKE '%/%'")])
        records_to_migrate = list(conn.execute("SELECT id, filepath FROM Mangas WHERE filepath_normalized IS NULL OR filepath_normalized = '' OR filepath_normalized LIKE '%\\%'"))
        if records_to_migrate:
            log_info(f"[EH 外掛] 正在遷移 {len(records_to_migrate)} 筆記錄到新的路徑標準...")
            conn.executemany("UPDATE Mangas SET filepath_normalized = ? WHERE id = ?", [(normalize_path(path), pid) for pid, path in records_to_migrate])
            log_info("  -> 路徑遷移完成。")

def load_maps_from_ast_json(filepath: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    def first_text_in(obj: Any) -> Union[str, None]:
        if isinstance(obj, dict):
            if obj.get("type") == "text" and isinstance(obj.get("text"), str): return obj["text"]
            for v in obj.values():
                found = first_text_in(v)
                if isinstance(found, str) and found.strip(): return found
        elif isinstance(obj, list):
            for item in obj:
                found = first_text_in(item)
                if isinstance(found, str) and found.strip(): return found
        return None
    artist_map, group_map = {}, {}
    try:
        with open(filepath, "r", encoding="utf-8") as f: root = json.load(f)
        sections = root.get("data")
        if not isinstance(sections, list): return {}, {}
        for ns in ("artist", "group"):
            section = next((s for s in sections if isinstance(s, dict) and s.get("namespace") == ns), None)
            if not section: continue
            data_block = section.get("data")
            if not isinstance(data_block, dict): continue
            target_map = artist_map if ns == "artist" else group_map
            for raw_tag, entry_data in data_block.items():
                if not isinstance(entry_data, dict): continue
                key_japanese = first_text_in(entry_data.get("name"))
                value_romaji = raw_tag.replace('_', ' ').title()
                if isinstance(key_japanese, str) and key_japanese.strip() and value_romaji:
                    target_map[key_japanese.strip().lower()] = value_romaji
    except Exception as e: log_error(f"[EH 外掛] 解析 EhTag 資料庫時發生錯誤: {e}"); return {}, {}
    return artist_map, group_map

def load_translation_maps(config: Dict):
    global GLOBAL_ARTIST_MAP, GLOBAL_GROUP_MAP
    log_info("[EH 外掛] 正在載入 EhTag 雙軌翻譯資料庫...")
    ehtag_db_dir = config.get('eh_syringe_directory')
    if not ehtag_db_dir or not os.path.isdir(ehtag_db_dir):
        log_warning("[EH 外掛] 未設定有效的 EhTag DB 路徑，將跳過翻譯與 CSV 功能。"); return
    db_path = os.path.join(ehtag_db_dir, 'db.ast.json')
    if not os.path.exists(db_path): log_error(f"[EH 外掛] 找不到資料庫檔案: {db_path}"); return
    GLOBAL_ARTIST_MAP, GLOBAL_GROUP_MAP = load_maps_from_ast_json(db_path)
    log_info(f"  -> Artist 資料庫載入完成: {len(GLOBAL_ARTIST_MAP)} 筆")
    log_info(f"  -> Group 資料庫載入完成: {len(GLOBAL_GROUP_MAP)} 筆")

def is_romaji_candidate(text: str) -> bool:
    return all(ord(c) < 128 for c in text.replace(' ', '').replace('_', '').replace('-', ''))

def analyze_title_tags(title: str) -> Tuple[str, str]:
    if not title: return "", ""
    artist_val, group_val = "", ""
    matches = re.findall(r'\[([^\]]+)\]', title)
    for content in matches:
        content = content.strip(); content_lower = content.lower()
        if content_lower in ['chinese', 'dl版', '中国翻訳', '翻訳', '無修正', 'uncensored']: continue
        inner_match = re.search(r'[(（]([^)）]+)[)）]', content)
        if inner_match:
            inner_artist = inner_match.group(1).strip()
            outer_group = re.split(r'[(（]', content)[0].strip()
            if not artist_val:
                artist_val = GLOBAL_ARTIST_MAP.get(inner_artist.lower())
                if not artist_val and is_romaji_candidate(inner_artist): artist_val = inner_artist
            if not group_val:
                 group_val = GLOBAL_GROUP_MAP.get(outer_group.lower())
                 if not group_val and is_romaji_candidate(outer_group) and outer_group: group_val = outer_group
        else:
            if not artist_val and content_lower in GLOBAL_ARTIST_MAP:
                artist_val = GLOBAL_ARTIST_MAP[content_lower]; continue
            if not group_val and content_lower in GLOBAL_GROUP_MAP:
                group_val = GLOBAL_GROUP_MAP[content_lower]; continue
            if is_romaji_candidate(content):
                if not artist_val: artist_val = content
                elif not group_val: group_val = content
    return artist_val, group_val

def is_folder_effectively_empty(folder_path: str) -> bool:
    try:
        return not any(entry.is_file() and entry.name.lower().endswith(('.zip', '.cbz', '.rar', '.cbr', '.7z', '.cb7', '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')) for entry in os.scandir(folder_path))
    except (PermissionError, FileNotFoundError): return False

def load_scan_cache(cache_path: str) -> dict:
    if not os.path.exists(cache_path): return {}
    try:
        with open(cache_path, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception: return {}

def save_scan_cache(cache_path: str, data: dict):
    try:
        with open(cache_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=None, separators=(',', ':'))
    except IOError: pass

def handle_empty_folders(root_dir: str, quarantine_path: str, cache_path: str) -> set:
    if not quarantine_path: log_warning("[EH 外掛] 未設定隔離區路徑，將跳過空資料夾處理。"); return set()
    
    log_info("[EH 外掛] 開始執行空資料夾過濾...")
    cache = load_scan_cache(cache_path); new_cache = {}; moved_folders = set(); summary.cache_misses = summary.cache_hits = 0
    try: all_local_folders = {entry.path: entry.stat().st_mtime for entry in os.scandir(root_dir) if entry.is_dir()}
    except FileNotFoundError: return moved_folders
    
    if not os.path.exists(quarantine_path): os.makedirs(quarantine_path)
    
    for folder_path, current_mtime in tqdm(all_local_folders.items(), desc="[EH 外掛] 過濾空資料夾"):
        cache_entry = cache.get(folder_path)
        if cache_entry and cache_entry.get('mtime') == current_mtime: 
            is_empty = cache_entry.get('is_empty', False); summary.cache_hits += 1
        else: 
            is_empty = is_folder_effectively_empty(folder_path); summary.cache_misses += 1
        new_cache[folder_path] = {'mtime': current_mtime, 'is_empty': is_empty}
        
        if is_empty:
            try: 
                shutil.move(folder_path, os.path.join(quarantine_path, os.path.basename(folder_path)))
                moved_folders.add(normalize_path(folder_path))
            except Exception as e: log_warning(f"  -> 移動空資料夾失敗: {folder_path} ({e})")

    save_scan_cache(cache_path, new_cache)
    summary.moved_empty = len(moved_folders)
    if moved_folders: log_info(f"  -> {len(moved_folders)} 個空資料夾已被移動至隔離區。")
    return moved_folders

def create_manga_record(folder_path, url_map):
    from nanoid import generate
    title = os.path.basename(folder_path)
    url = url_map.get(sanitize_filename(title), ""); normalized_fp = normalize_path(folder_path)
    sha1_hash = hashlib.sha1(normalized_fp.encode('utf-8')).hexdigest()
    mtime = os.path.getmtime(folder_path)
    return {"id": generate(), "title": title, "hash": sha1_hash, "filepath": os.path.normpath(folder_path), "filepath_normalized": normalized_fp, "type": "folder", "mtime": datetime.datetime.utcfromtimestamp(mtime).isoformat(timespec='milliseconds') + 'Z', "date": int(mtime * 1000), "status": "non-tag", "url": url, "tags": "{}", "rating": 0.0, "exist": 1, "createdAt": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "updatedAt": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

def update_database_records(db_path, records_to_add=[], paths_to_soft_delete=[], paths_to_restore=[]):
    if not any([records_to_add, paths_to_soft_delete, paths_to_restore]): return
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_filepath_normalized ON Mangas(filepath_normalized)")
        if records_to_add:
            cursor.executemany("INSERT OR IGNORE INTO Mangas (id, title, hash, filepath, filepath_normalized, type, mtime, date, status, url, tags, rating, exist, createdAt, updatedAt) VALUES (:id, :title, :hash, :filepath, :filepath_normalized, :type, :mtime, :date, :status, :url, :tags, :rating, :exist, :createdAt, :updatedAt)", records_to_add)
            summary.added += cursor.rowcount
        if paths_to_soft_delete:
            cursor.executemany("UPDATE Mangas SET status = ?, updatedAt = datetime('now') WHERE filepath_normalized = ?", [('檔案已被刪除', path) for path in paths_to_soft_delete])
            summary.soft_deleted += cursor.rowcount
        if paths_to_restore:
            cursor.executemany("UPDATE Mangas SET status = ?, updatedAt = datetime('now') WHERE filepath_normalized = ?", [('non-tag', path) for path in paths_to_restore])
            summary.restored += cursor.rowcount

def export_tag_failed_to_csv(config: Dict):
    log_info("[EH 外掛] 開始匯出 'tag-failed' 項目至 CSV...")
    
    db_path = os.path.join(config.get('eh_data_directory'), "database.sqlite")
    if not os.path.exists(db_path):
        log_warning("[EH 外掛] 找不到資料庫，無法匯出 'tag-failed' 項目。"); return

    # === v-MOD START: 優先使用設定中的路徑 ===
    output_csv_path = config.get('eh_csv_path')
    if not output_csv_path:
        # 保底：如果設定檔沒值，才用 data/tagfailed.csv (理論上 plugin_gui 會填入預設值)
        output_csv_path = os.path.join(DATA_DIR, 'tagfailed.csv')
    # === v-MOD END ===

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT title, filepath, url FROM Mangas WHERE status = 'tag-failed'")
            failed_records = cursor.fetchall()

        if not failed_records:
            log_info("[EH 外掛] 資料庫中沒有 'tag-failed' 的項目，無需生成 CSV。"); return

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
            log_info(f"[EH 外掛] 成功將 {len(failed_records)} 筆 'tag-failed' 記錄匯出至: {output_csv_path}")
        else:
            log_error(f"[EH 外掛] 無法寫入 'tag-failed' CSV 檔案，可能檔案被鎖定: {output_csv_path}")

    except sqlite3.Error as e:
        log_error(f"[EH 外掛] 讀取資料庫以匯出 'tag-failed' 項目時發生錯誤: {e}")
    except Exception as e:
        log_error(f"[EH 外掛] 匯出 'tag-failed' CSV 時發生未知錯誤: {e}", include_traceback=True)
        
        
def run_full_sync_headless(config: Dict, progress_queue: Optional[any]):
    _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
    log_info("[EH 外掛] 開始執行資料庫完整同步...")
    
    root_dir = config.get('root_scan_folder')
    data_dir = config.get('eh_data_directory')
    db_path = os.path.join(data_dir, "database.sqlite")
    
    download_list_json_path = config.get('eh_mmd_json_path')
    url_map, json_data = {}, []
    if download_list_json_path and os.path.isfile(download_list_json_path):
        try:
            with open(download_list_json_path, 'r', encoding='utf-8') as f: json_data = json.load(f)
            url_map = {sanitize_filename(item['Name']): item['Url'] for item in json_data if item.get('Command') == 'Completed' and 'exhentai.org/g/' in item.get('Url', '') and item.get('Name')}
            log_info(f"[EH 外掛] 成功從 MMD JSON 載入 {len(url_map)} 個 URL 映射。")
            update_csv_dashboard(json_data, config.get('eh_csv_path'))
        except Exception as e: log_error(f"[EH 外掛] 讀取或解析 MMD JSON 時發生錯誤: {e}")
    else: log_warning("[EH 外掛] 未設定或找不到 MMD JSON 檔案，無法匹配 URL 或更新 CSV。")
        
    quarantine_path = config.get('eh_quarantine_path')

    cache_path = os.path.join(DATA_DIR, 'scan_cache.json')
    log_info(f"[EH 外掛] 掃描快取路徑已定位至: {cache_path}")

    moved_empty_folders = handle_empty_folders(root_dir, quarantine_path, cache_path)

    _update_progress("正在掃描本地資料夾...", 20)
    try:
        local_paths = {normalize_path(entry.path) for entry in os.scandir(root_dir) if entry.is_dir()}
    except FileNotFoundError:
        log_error(f"[EH 外掛] 錯誤：找不到指定的根目錄 '{root_dir}'"); return

    _update_progress("正在讀取資料庫記錄...", 30)
    try:
        with sqlite3.connect(db_path) as conn: db_records = {row[0]: row[1] for row in conn.execute("SELECT filepath_normalized, status FROM Mangas")}
    except sqlite3.Error as e:
        log_error(f"[EH 外掛] 讀取資料庫時發生嚴重錯誤: {e}"); return

    db_paths = set(db_records.keys())
    paths_to_add = local_paths - db_paths
    paths_to_soft_delete = {p for p in (db_paths - local_paths) if db_records.get(p) != '檔案已被刪除'}.union(moved_empty_folders)
    paths_to_restore = {p for p in (local_paths & db_paths) if db_records.get(p) == '檔案已被刪除'}

    log_info(f"[EH 外掛] 比對完成：{len(paths_to_add)} 待新增, {len(paths_to_soft_delete)} 待軟刪除, {len(paths_to_restore)} 待還原。")
    
    new_records = [rec for path in tqdm(paths_to_add, desc="[EH 外掛] 處理新資料夾") if (rec := create_manga_record(path.replace('/', '\\'), url_map))] if paths_to_add else []
        
    update_database_records(db_path, records_to_add=new_records, paths_to_soft_delete=list(paths_to_soft_delete), paths_to_restore=list(paths_to_restore))
    _update_progress("資料庫同步完成。", 50)
    log_info("[EH 外掛] 資料庫完整同步完成。")

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
    if not csv_path: log_warning("[EH 外掛] 未設定 CSV 儀表板路徑，跳過更新。"); return
    try:
        if flush_pending_to_main(csv_path): log_info("[EH 外掛] 已嘗試合併先前的 pending 至主 CSV。")
    except Exception as e: log_warning(f"[EH 外掛] 合併 pending 發生例外：{e}")
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
    if not changed_rows: log_info("[EH 外掛] CSV 儀表板無變更。"); return
    if _atomic_write_csv_rows(rows, csv_path): log_info(f"[EH 外掛] CSV 儀表板更新完成：{csv_path}（寫入 {len(changed_rows)} 筆變更）")
    else: _append_pending_rows(changed_rows); log_warning(f"[EH 外掛] CSV 被鎖定，已將 {len(changed_rows)} 筆變更寫入 pending，待下次自動合併。")

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
        if not os.path.exists(path): log_error(f"[EH 自動化] 找不到圖片資產: {path}"); return None
        needle_pil = _pil_open_strict(path)
        if needle_pil is None: log_error(f"[EH 自動化] 圖片格式不支援或已損壞: {path}"); return None
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
            except Exception as e: log_warning(f"[EH 自動化] click 失敗: {e}")
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
            log_info(f"  -> 已成功最大化並激活 PID 為 {pid} 的視窗。"); return True
        else: log_warning(f"  -> 找不到 PID 為 {pid} 的可見視窗。"); return False
    except Exception as e: log_error(f"  -> 激活視窗時發生錯誤: {e}"); return False

def _get_current_hkl():
    if not AUTOMATION_LIBS_AVAILABLE: return None
    try: return ctypes.windll.user32.GetKeyboardLayout(ctypes.windll.user32.GetWindowThreadProcessId(ctypes.windll.user32.GetForegroundWindow(), None))
    except Exception: return None

def ensure_english_input():
    if not AUTOMATION_LIBS_AVAILABLE: return
    try: ctypes.windll.user32.ActivateKeyboardLayout(ctypes.windll.user32.LoadKeyboardLayoutA(b"00000409", 1), 256)
    except Exception as e: log_warning(f"切換至英文輸入法失敗: {e}")

def restore_keyboard_layout(original_hkl):
    if original_hkl and AUTOMATION_LIBS_AVAILABLE:
        try: ctypes.windll.user32.ActivateKeyboardLayout(original_hkl, 256)
        except Exception as e: log_warning(f"還原輸入法失敗: {e}")

_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs"); os.makedirs(_LOG_DIR, exist_ok=True)
_CHILD_LOG = os.path.join(_LOG_DIR, "eh_manager_child.log"); _FILTER_TAGS = ("EBUSY", "Saved", "unlink", "Error", "WARN", "scanned", "Digest")

def _spawn_eh_manager(app_path: str):
    try:
        p = subprocess.Popen([app_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", creationflags=subprocess.CREATE_NO_WINDOW)
    except Exception as e: log_error(f"[EH 自動化] 無法啟動應用程式：{e}"); return None
    qlines = queue.Queue()
    def _reader():
        with open(_CHILD_LOG, "a", encoding="utf-8") as fout:
            for line in iter(p.stdout.readline, ""):
                fout.write(line); fout.flush()
                if any(tag in line for tag in _FILTER_TAGS): qlines.put(line.rstrip("\n"))
    t = threading.Thread(target=_reader, daemon=True); t.start()
    def _drain():
        while True:
            try: log_info(f"[EHM] {qlines.get(timeout=0.2)}")
            except queue.Empty:
                if p.poll() is not None and qlines.empty(): break
    threading.Thread(target=_drain, daemon=True).start()
    return p

def close_manga_app_if_running(config: Dict):
    """
    檢查並關閉目標應用程式。
    v1.9.10: 使用 try...except 包裹，確保即使出錯也不會中斷主程式。
    """
    if not AUTOMATION_LIBS_AVAILABLE: return
    manga_app_path = config.get('eh_manga_manager_path', '')
    if not manga_app_path:
        log_warning("[EH 自動化] 設定中未提供 manga_manager_path，跳過關閉程序。"); return
    
    target_app_name = os.path.basename(manga_app_path)
    log_info(f"[EH 自動化] 檢查 '{target_app_name}' 執行狀態...") 
    
    try:
        found_count = 0
        for proc in psutil.process_iter(['name', 'pid']):
            try:
                if proc.info['name'] and proc.info['name'].lower() == target_app_name.lower():
                    log_info(f"  -> 發現進程 (PID: {proc.pid})，正在關閉...")
                    try:
                        proc.terminate()
                        proc.wait(timeout=3)
                    except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                        proc.kill()
                        proc.wait(timeout=3)
                    found_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        if found_count > 0:
            log_info(f"  -> 已關閉 {found_count} 個實例。")
        else:
            log_info(f"  -> 未發現運行中的應用程式，無需操作。")
            
    except Exception as e:
        log_warning(f"[EH 自動化] 檢查進程時發生異常 (已忽略): {e}")
        # 這裡不拋出異常，確保主程式可以繼續執行

def count_untagged_manga(db_path: str) -> int:
    if not os.path.exists(db_path): return 0
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA query_only = ON")
            return conn.execute("SELECT COUNT(*) FROM Mangas WHERE status = 'non-tag'").fetchone()[0]
    except sqlite3.Error: return 0

def run_automation_suite_headless(config: Dict, progress_queue: Optional[any], control_events: Dict):
    if not AUTOMATION_LIBS_AVAILABLE: log_error("[EH 外掛] 缺少 UI 自動化函式庫，無法執行元數據更新。"); return
    _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
    timing = _init_automation_speed_from_config(config)
    CLICK_DELAY, PAGEDOWN_DELAY, AFTER_SCROLL = timing["CLICK"], timing["PAGEDOWN"], timing["AFTER_SCROLL"]

    log_info("[EH 外掛] UI 自動化流程開始...")
    db_path = os.path.join(config.get('eh_data_directory'), "database.sqlite")
    task_limit = count_untagged_manga(db_path)
    summary.tasks_total = task_limit
    
    if task_limit == 0:
        log_info("[EH 外掛] 資料庫中沒有 non-tag 項目，無需執行 UI 自動化。"); _update_progress("資料庫無需更新。", 100); return

    _update_progress(f"檢測到 {task_limit} 個項目需要更新元數據...", 55)
    
    app_path = config.get("eh_manga_manager_path")
    proc = _spawn_eh_manager(app_path)
    if not proc: _update_progress("❌ 程式啟動失敗。", 100); return
    app_pid = proc.pid
    time.sleep(float(config.get('automation_page_load_delay', 2.0)) * 3)

    if not activate_window_by_pid(app_pid):
        log_error("[EH 自動化] 視窗激活失敗，自動化中止。"); _update_progress("❌ 錯誤: 程式視窗激活失敗。", 100); return

    original_hkl = None
    try:
        original_hkl = _get_current_hkl()
        _update_progress("正在定位 UI 元素...", 60)
        search_icon = find_element(MAIN_SEARCH_ICON_IMG, timeout=10)
        if not search_icon: log_error("[EH 自動化] 找不到主搜尋框錨點！"); _update_progress("❌ 錯誤: 找不到主搜尋框。"); return
        
        pyautogui.click(search_icon.x + SEARCH_BOX_X_OFFSET, search_icon.y)
        ensure_english_input(); pyperclip.copy('"non-tag"$'); pyautogui.hotkey('ctrl', 'v'); pyautogui.press('enter')

        log_info("[EH 自動化] 已執行搜尋，正在主動輪詢等待 UI 結果出現...")
        first_target = None; wait_start_time = time.time()
        while time.time() - wait_start_time < 15:
            first_target = find_element(BOOKMARK_ICON_IMG, timeout=0.5) or find_element(BOOKMARK_ICON_READY_IMG, timeout=0.5)
            if first_target: log_info(f"[EH 自動化] 目標已出現！(耗時 {time.time() - wait_start_time:.2f} 秒)"); break
            time.sleep(0.5)

        if not first_target: log_warning("[EH 自動化] 等待超時 (15秒)，仍未在螢幕上找到任何 non-tag 項目，流程結束。"); find_and_click(CLEAR_SEARCH_BUTTON_IMG); return

        _update_progress("正在開始自動化迴圈...", 65)
        pyautogui.click(first_target.x + TITLE_X_OFFSET, first_target.y + TITLE_Y_OFFSET)
        time.sleep(PAGE_LOAD_DELAY)
        
        for i in range(task_limit):
            if control_events['cancel'].is_set(): log_info("[EH 自動化] 收到取消訊號，流程終止。"); break
            while control_events['pause'].is_set(): time.sleep(0.2)
            
            summary.tasks_processed = i + 1
            progress_val = 65 + int(30 * (summary.tasks_processed / task_limit))
            _update_progress(f"正在處理第 {summary.tasks_processed}/{task_limit} 本...", progress_val)
            
            if find_and_click(RESCAN_BUTTON_IMG, timeout=5): time.sleep(CLICK_DELAY)
            if summary.tasks_processed >= task_limit: break
            
            pyautogui.press('pagedown'); time.sleep(PAGEDOWN_DELAY); time.sleep(AFTER_SCROLL)
            
            if find_element(PAGE_END_IMG, timeout=1): log_info("[EH 自動化] 偵測到頁面末端，提前結束。"); break
                
        if find_element(CLOSE_BUTTON_IMG, timeout=1): find_and_click(CLOSE_BUTTON_IMG, timeout=2)
        find_and_click(CLEAR_SEARCH_BUTTON_IMG, timeout=5)

    except Exception as e: log_error(f"[EH 自動化] 自動化過程中發生錯誤: {e}", include_traceback=True); _update_progress(f"❌ 自動化錯誤: {e}")
    finally:
        if original_hkl: restore_keyboard_layout(original_hkl)

def create_database_backup(config: Dict):
    BACKUPS_TO_KEEP = 3; log_info("[EH 外掛] 正在檢查並執行資料庫備份...")
    backup_dir = config.get('eh_backup_directory')
    if not backup_dir: log_info("  -> 未設定備份資料夾，跳過備份程序。"); return
    data_dir = config.get('eh_data_directory')
    source_db_path = os.path.join(data_dir, "database.sqlite")
    if not os.path.exists(source_db_path): log_warning(f"  -> 找不到來源資料庫檔案，無法備份: {source_db_path}"); return
    try:
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        backup_filename = f"database_{timestamp}.sqlite"
        destination_path = os.path.join(backup_dir, backup_filename)
        shutil.copy2(source_db_path, destination_path)
        log_info(f"  -> 資料庫成功備份至: {destination_path}")

        log_info(f"  -> 正在清理舊備份，僅保留最新的 {BACKUPS_TO_KEEP} 個...")
        all_backups = sorted([f for f in os.listdir(backup_dir) if f.startswith('database_') and f.endswith('.sqlite')])
        if len(all_backups) > BACKUPS_TO_KEEP:
            to_delete = all_backups[:-BACKUPS_TO_KEEP]
            log_info(f"  -> 發現 {len(all_backups)} 個備份，將刪除 {len(to_delete)} 個最舊的備份。")
            for old_backup in to_delete:
                try:
                    os.remove(os.path.join(backup_dir, old_backup))
                    log_info(f"    - 已刪除舊備份: {old_backup}")
                except OSError as e: log_error(f"    - 刪除舊備份 {old_backup} 失敗: {e}")
        else: log_info(f"  -> 當前備份數量 ({len(all_backups)}) 未超過限制，無需清理。")
    except Exception as e: log_error(f"[EH 外掛] 建立或清理資料庫備份時發生錯誤: {e}", include_traceback=True)

class EhDatabaseToolsPlugin(BasePlugin):
    def get_id(self) -> str: return "eh_database_tools"
    def get_name(self) -> str: return "exhentai-manga-manager 資料庫更新工具"
    def get_description(self) -> str: return "在每次掃描前，自動同步 EH 資料庫、更新 CSV 並透過 UI 自動化更新元數據。"
    def get_plugin_type(self) -> str: return 'preprocessor'
    def get_default_config(self):
        return {"enable_eh_preprocessor": False, "eh_data_directory": "", "eh_backup_directory": "", "eh_syringe_directory": "", "eh_mmd_json_path": ""}
    def get_slot_order(self) -> int: return 10
    def plugin_prefers_inner_enable(self) -> bool: return True
    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any], ui_vars: Dict) -> Optional['ttk.Frame']:
        from . import plugin_gui
        return plugin_gui.create_settings_frame(parent_frame, config, ui_vars)
    def save_settings(self, config: Dict[str, Any], ui_vars: Dict) -> Dict[str, Any]:
        from . import plugin_gui
        return plugin_gui.save_settings(config, ui_vars)

    def run(self, config: Dict, progress_queue: Optional[any], control_events: Optional[Dict], app_update_callback=None):
        global summary
        summary = ExecutionSummary(); summary.mode = "前置處理"
        try: from nanoid import generate
        except ImportError:
            log_error("[EH 外掛] 缺少必要的函式庫 'nanoid'。請執行 'pip install nanoid'。")
            if progress_queue: progress_queue.put({'type':'text', 'text': "❌ [EH 外掛] 錯誤: 缺少 nanoid 函式庫。"})
            return
        _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
        create_database_backup(config)
        try:
            _update_progress("🚀 [EH 前置處理] 開始執行...", 0)
            try: flush_pending_to_main(config.get("eh_csv_path", "download_dashboard.csv"))
            except Exception: pass
            required_paths = ['eh_data_directory', 'root_scan_folder']
            if config.get('automation_enabled', False): required_paths.append('eh_manga_manager_path')
            if not all(config.get(p) and os.path.exists(config.get(p)) for p in required_paths):
                log_error("[EH 外掛] 設定中的一個或多個必要路徑無效或不存在。"); _update_progress("❌ 錯誤: 外掛路徑設定不完整或無效。"); return
            
            # --- 強制繼續邏輯 ---
            if config.get('automation_enabled', False):
                close_manga_app_if_running(config)
            # 強制給予回饋，表明流程已推進
            _update_progress("正在連接資料庫...", 10) 
            # -------------------

            if control_events and control_events['cancel'].is_set(): return
            data_dir = config.get('eh_data_directory')
            db_path = os.path.join(data_dir, "database.sqlite")
            if not os.path.isfile(db_path): _update_progress("❌ [EH 外掛] 錯誤: 找不到 database.sqlite。"); return
            add_normalized_path_column_if_not_exists(db_path)
            migrate_to_v20_structure(db_path)
            if control_events and control_events['cancel'].is_set(): return
            load_translation_maps(config)
            if control_events and control_events['cancel'].is_set(): return
            run_full_sync_headless(config, progress_queue)
            if control_events and control_events['cancel'].is_set(): return
            db_path = os.path.join(config.get('eh_data_directory'), "database.sqlite")
            try:
                log_info("[EH 外掛] 正在強制同步資料庫日誌 (WAL Checkpoint)...")
                with sqlite3.connect(db_path) as conn: conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                log_info("  -> 資料庫日誌同步完成。")
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    log_warning("[EH 外掛] 資料庫被鎖定，等待 1 秒後重試 Checkpoint...")
                    time.sleep(1)
                    try:
                        with sqlite3.connect(db_path) as conn: conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                        log_info("  -> 重試成功！")
                    except Exception as retry_e: log_error(f"[EH 外掛] Checkpoint 重試失敗: {retry_e}")
                else: log_error(f"[EH 外掛] WAL Checkpoint 執行失敗: {e}")
            except Exception as e: log_error(f"[EH 外掛] WAL Checkpoint 發生未知錯誤: {e}")
            if config.get('automation_enabled', False):
                if AUTOMATION_LIBS_AVAILABLE: run_automation_suite_headless(config, progress_queue, control_events)
                else: log_warning("[EH 外掛] 跳過 UI 自動化，缺少必要函式庫(pyautogui/psutil 等)。")
            else: log_info("[EH 外掛] UI 自動化功能已在設定中被禁用，跳過此步驟。")
            _update_progress("✅ [EH 前置處理] 完成！", 100)
        except Exception as e:
            log_error(f"[EH 外掛] 執行期間發生嚴重錯誤: {e}", include_traceback=True)
            if progress_queue: progress_queue.put({'type':'text', 'text': f"❌ [EH 外掛] 錯誤: {e}"})
        finally:
            try: flush_pending_to_main(config.get("eh_csv_path", "download_dashboard.csv"))
            except Exception: pass
        try: export_tag_failed_to_csv(config)
        except Exception as e: log_error(f"[EH 外掛] 執行 tag-failed 匯出時發生例外: {e}")
        if summary: summary.report()