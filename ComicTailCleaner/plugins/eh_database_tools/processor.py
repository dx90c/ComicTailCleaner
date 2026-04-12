# ======================================================================
# 檔案：plugins/eh_database_tools/processor.py
# 目的：實現一個「前置處理器」，在主任務前同步 EH 資料庫
# 版本：1.9.15 (資源管理：自動化完成後強制關閉 EMM 子進程)
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
import difflib
import unicodedata

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error, log_warning
from config import DATA_DIR

try:
    import keyboard, psutil, pyperclip, ctypes
    from ctypes import wintypes
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
        log_error(f"[EH 外掛] 解析 EhTag 資料庫時發生錯誤: {e}")
        return {}, {}
    return artist_map, group_map

def load_translation_maps(config: Dict):
    global GLOBAL_ARTIST_MAP, GLOBAL_GROUP_MAP, GLOBAL_ARTIST_KEYS, GLOBAL_GROUP_KEYS
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
# === 請在這裡插入 is_romaji_candidate ===
def is_romaji_candidate(text: str) -> bool:
    # 簡單判斷：如果大部分字符是 ASCII，則視為羅馬拼音候選
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
    if not title: return "", ""
    artist_val, group_val = "", ""
    matches = re.findall(r'\[([^\]]+)\]', title)
    for content in matches:
        content = content.strip(); content_lower = content.lower()
        if content_lower in ['chinese', 'dl版', '中国翻訳', '翻訳', '無修正', 'uncensored', 'eng', 'english']: continue
        inner_match = re.search(r'[(（]([^)）]+)[)）]', content)
        if inner_match:
            inner_artist = inner_match.group(1).strip()
            outer_group = re.split(r'[(（]', content)[0].strip()
            if not artist_val:
                artist_val = fuzzy_lookup(inner_artist, GLOBAL_ARTIST_MAP, GLOBAL_ARTIST_KEYS)
                if not artist_val and is_romaji_candidate(inner_artist): artist_val = inner_artist.title()
                if not artist_val: artist_val = inner_artist
            if not group_val:
                 group_val = fuzzy_lookup(outer_group, GLOBAL_GROUP_MAP, GLOBAL_GROUP_KEYS)
                 if not group_val and is_romaji_candidate(outer_group): group_val = outer_group.title()
                 if not group_val: group_val = outer_group
        else:
            mapped_artist = fuzzy_lookup(content, GLOBAL_ARTIST_MAP, GLOBAL_ARTIST_KEYS)
            if mapped_artist:
                if not artist_val: artist_val = mapped_artist; continue 
            mapped_group = fuzzy_lookup(content, GLOBAL_GROUP_MAP, GLOBAL_GROUP_KEYS)
            if mapped_group:
                if not group_val: group_val = mapped_group; continue
            if is_romaji_candidate(content):
                if not artist_val: artist_val = content.title()
                elif not group_val: group_val = content.title()
            elif not artist_val: artist_val = content
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
            # v-MOD: 根據您的方案 B，直接執行 DELETE 讓 EMM 總計數下降
            # 優先嘗試以正規化路徑刪除
            cursor.executemany(
                "DELETE FROM Mangas WHERE filepath_normalized = ?",
                [(path,) for path in paths_to_soft_delete]
            )
            affected = cursor.rowcount
            
            # 備援：若沒刪到，嘗試用原始路徑（相容斜線差異）
            if affected < len(paths_to_soft_delete):
                cursor.executemany(
                    "DELETE FROM Mangas WHERE REPLACE(REPLACE(filepath, '/', '\\'), '\\\\', '\\') = ?",
                    [(path.replace('/', '\\'),) for path in paths_to_soft_delete]
                )
            summary.soft_deleted += cursor.rowcount
        if paths_to_restore:
            # v-MOD: 同時還原 exist=1
            cursor.executemany(
                "UPDATE Mangas SET exist = 1, status = ?, updatedAt = datetime('now') WHERE filepath_normalized = ?",
                [('non-tag', path) for path in paths_to_restore]
            )
            summary.restored += cursor.rowcount

def export_tag_failed_to_csv(config: Dict):
    log_info("[EH 外掛] 開始匯出 'tag-failed' 項目至 CSV...")
    
    db_path = os.path.join(config.get('eh_data_directory'), "database.sqlite")
    if not os.path.exists(db_path):
        log_warning("[EH 外掛] 找不到資料庫，無法匯出 'tag-failed' 項目。"); return

    # 使用 config 中的路徑（由 plugin_gui 寫死為 PLUGIN_BASE_DIR/tagfailed.csv）
    output_csv_path = config.get('eh_csv_path', os.path.join(DATA_DIR, 'tagfailed.csv'))

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

_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs"); os.makedirs(_LOG_DIR, exist_ok=True)
_CHILD_LOG = os.path.join(_LOG_DIR, "eh_manager_child.log"); _FILTER_TAGS = ("EBUSY", "Saved", "unlink", "Error", "WARN", "scanned", "Digest")


def _spawn_eh_manager(app_path: str, port: int):
    try:
        # DETACHED_PROCESS = 0x00000008
        # CREATE_NEW_PROCESS_GROUP = 0x00000200
        # 這些 flag 確保 EMM 成為獨立進程
        creation_flags = 0x00000008 | 0x00000200
        
        # 注意：為了脫鉤，我們必須放棄 stdout=subprocess.PIPE
        # 將輸出導向 devnull (空)，避免 CTC 關閉時 EMM 因為寫入 log 失敗而崩潰
        with open(os.devnull, 'w') as devnull:
            p = subprocess.Popen(
                [app_path, f'--remote-debugging-port={port}', '--remote-allow-origins=*'],
                stdout=devnull, 
                stderr=devnull,
                close_fds=True, # 關閉繼承的檔案描述符
                creationflags=creation_flags
            )
            
        log_info(f"[EH 自動化] 已獨立啟動應用程式 (PID: {p.pid}，CDP 連接埠: {port})")
        return p
        
    except Exception as e:
        log_error(f"[EH 自動化] 無法啟動應用程式：{e}")
        return None

def close_manga_app_if_running(config: Dict):
    if not AUTOMATION_LIBS_AVAILABLE: return
    manga_app_path = config.get('eh_manga_manager_path', '')
    if not manga_app_path: log_warning("[EH 自動化] 設定中未提供 manga_manager_path，跳過關閉程序。"); return
    target_app_name = os.path.basename(manga_app_path)
    
    log_info(f"[EH 自動化] 檢查 '{target_app_name}' 執行狀態...") 
    try:
        found_count = 0
        for proc in psutil.process_iter(['name', 'pid']):
            try:
                if proc.info['name'] and proc.info['name'].lower() == target_app_name.lower():
                    log_info(f"  -> 發現進程 (PID: {proc.pid})，正在關閉...")
                    try: proc.terminate(); proc.wait(timeout=3)
                    except (psutil.NoSuchProcess, psutil.TimeoutExpired): proc.kill(); proc.wait(timeout=3)
                    found_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied): continue
        if found_count > 0: log_info(f"  -> 已關閉 {found_count} 個實例。")
        else: log_info(f"  -> 未發現運行中的應用程式，無需操作。")
    except Exception as e:
        log_warning(f"[EH 自動化] 檢查進程時發生異常 (已忽略): {e}")

def count_untagged_manga(db_path: str) -> int:
    if not os.path.exists(db_path): return 0
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA query_only = ON")
            return conn.execute("SELECT COUNT(*) FROM Mangas WHERE status = 'non-tag'").fetchone()[0]
    except sqlite3.Error: return 0

def run_automation_suite_headless(config: Dict, progress_queue: Optional[any], control_events: Dict):
    import urllib.request, urllib.error, json, time
    _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
    
    log_info("[EH 自動化] CDP 無頭自動化流程開始...")
    db_path = os.path.join(config.get('eh_data_directory'), "database.sqlite")
    task_limit = count_untagged_manga(db_path)
    if 'summary' in globals(): summary.tasks_total = task_limit
    
    if task_limit == 0:
        log_info("[EH 外掛] 資料庫中沒有 non-tag 項目，無需執行 UI 自動化。")
        _update_progress("資料庫無需更新。", 100)
        return

    _update_progress(f"檢測到 {task_limit} 個項目需要更新元數據...", 55)
    
    app_path = config.get("eh_manga_manager_path")
    if not app_path or not os.path.exists(app_path):
        log_error("[EH 自動化] EMM 執行檔路徑設定錯誤！")
        _update_progress("❌ 自動化失敗: 找不到 EMM 執行檔", 100)
        return

    # Ensure previous instances are closed to avoid debugging port conflicts and stuck state
    close_manga_app_if_running(config)

    import socket
    cdp_port = 9222
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        cdp_port = s.getsockname()[1]

    proc = _spawn_eh_manager(app_path, cdp_port)
    if not proc: 
        _update_progress("❌ 程式啟動失敗。", 100)
        return

    _update_progress(f"等待 EMM 服務啟動並開放 CDP 連接埠 ({cdp_port})...", 60)
    
    ws_url = None
    url = f"http://127.0.0.1:{cdp_port}/json"
    started_time = time.time()
    
    # EMM 可以載入很久，給予 120 秒等待 CDP 開放
    while time.time() - started_time < 120:
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
             log_info("[EH 自動化] 收到取消訊號。"); return
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=2) as response:
                pages = json.loads(response.read().decode('utf-8'))
                for page in pages:
                    if page.get("type") == "page" and "webSocketDebuggerUrl" in page:
                        ws_url = page["webSocketDebuggerUrl"]
                        break
            if ws_url: break
        except Exception: pass
        time.sleep(2)

    if not ws_url:
        log_error(f"[EH CDP] 無法連接到 EMM 的偵錯連接埠 ({cdp_port})。")
        _update_progress("❌ 連接 EMM 失敗 (無法讀取 CDP)", 100)
        return

    _update_progress("成功連接 EMM！準備注入操作腳本...", 65)
    
    try:
        from websocket import create_connection
        class CDPClient:
            def __init__(self, url):
                self.ws = create_connection(url)
                self.msg_id = 0
            
            def evaluate(self, expression):
                self.msg_id += 1
                payload = {"id": self.msg_id, "method": "Runtime.evaluate", "params": {"expression": expression, "returnByValue": True, "awaitPromise": True}}
                self.ws.send(json.dumps(payload))
                while True:
                    resp = json.loads(self.ws.recv())
                    if "id" in resp and resp["id"] == self.msg_id:
                        if "exceptionDetails" in resp.get("result", {}):
                            raise Exception(f"CDP Evaluate Error: {resp['result']['exceptionDetails']}")
                        return resp["result"].get("result", {}).get("value")
            
            def execute(self, method, params=None):
                self.msg_id += 1
                payload = {"id": self.msg_id, "method": method, "params": params or {}}
                self.ws.send(json.dumps(payload))
                while True:
                    resp = json.loads(self.ws.recv())
                    if "id" in resp and resp["id"] == self.msg_id:
                        if "error" in resp:
                            raise Exception(f"CDP Execute Error: {resp['error']}")
                        return resp.get("result", {})
                        
            def close(self): self.ws.close()
        
        client = CDPClient(ws_url)
        
        log_info("[EH CDP] 正在確保介面載入並 Focus 搜尋框 (階段 1)...")
        js_focus = """
        (async () => {
            let retries = 0;
            let isReady = false;
            let searchInput = null;
            
            while (retries < 60) { // Wait up to 30 seconds for initial DB load
                searchInput = document.querySelector('.search-input input') || document.querySelector('input[type="text"]');
                let masks = document.querySelectorAll('.el-loading-mask');
                let isMaskVis = Array.from(masks).some(m => m.style.display !== 'none' && getComputedStyle(m).display !== 'none');
                let books = document.querySelectorAll('.book-title, .el-card');
                
                if (searchInput && !searchInput.disabled && !isMaskVis && books.length > 0) {
                    isReady = true;
                    break;
                }
                await new Promise(r => setTimeout(r, 500));
                retries++;
            }
            if (!isReady) return 'Timeout waiting for EMM to load books or clear loading mask';
            
            // Brief extra buffer after loading mask clears - increased to 2500ms as per request 
            await new Promise(r => setTimeout(r, 2500));
            
            searchInput.focus();
            searchInput.value = '';
            searchInput.dispatchEvent(new Event('input', { bubbles: true }));
            searchInput.select();
            return 'OK';
        })();
        """
        focus_res = client.evaluate(js_focus)
        if focus_res != 'OK':
            log_error(f"[EH CDP] 選擇搜尋框失敗，返回值: {focus_res}")
            return
            
        log_info("[EH CDP] 執行原生 CDP 鍵盤輸入與觸發 (階段 2)...")
        time.sleep(1.0)  # Brief micro-pause for Vue state - increased to 1.0s
        
        # --- Native CDP Typing (Bypasses Vue's event shadowing perfectly) ---
        client.execute("Input.insertText", {"text": '"non-tag"$'})
        time.sleep(1.5)  # Ensure text is populated - increased to 1.5s
        
        # Type the Enter key natively with full sequence
        client.execute("Input.dispatchKeyEvent", {"type": "rawKeyDown", "windowsVirtualKeyCode": 13, "key": "Enter", "code": "Enter"})
        client.execute("Input.dispatchKeyEvent", {"type": "char", "windowsVirtualKeyCode": 13, "key": "Enter", "code": "Enter", "text": "\r"})
        client.execute("Input.dispatchKeyEvent", {"type": "keyUp", "windowsVirtualKeyCode": 13, "key": "Enter", "code": "Enter"})
        
        log_info("[EH CDP] 等待 Vue 響應搜尋操作並點擊 (階段 3)...")
        
        js_action = f"""
        (async () => {{
            // --- 工具函數：DOM 就緒輪詢 ---
            const waitReady = async (maxMs = 10000) => {{
                const t0 = Date.now();
                while (Date.now() - t0 < maxMs) {{
                    let masks = document.querySelectorAll('.el-loading-mask');
                    let busy = Array.from(masks).some(m => m.style.display !== 'none' && getComputedStyle(m).display !== 'none');
                    if (!busy) return true;
                    await new Promise(r => setTimeout(r, 50));
                }}
                return false;
            }};

            // 1. 等待搜尋結果載入 (Enter 鍵已在階段 2 觸發搜尋)
            await waitReady(15000);
            await new Promise(r => setTimeout(r, 500));

            let titles = document.querySelectorAll('.book-title');
            if (titles.length === 0) return 'No non-tag comics found on screen';

            // 4. 取得真實總數
            let totalElem = document.querySelector('span.el-pagination__total.is-first') || document.querySelector('.el-pagination__total');
            let totalMatch = totalElem ? totalElem.textContent.match(/\\d+/) : null;
            let trueCount = totalMatch ? parseInt(totalMatch[0]) : {task_limit};
            let count = Math.min({task_limit}, trueCount);
            if (count === 0) return 'No non-tag comics to process';

            // 5. 點進第一本詳情頁
            titles[0].click();
            await waitReady();
            await new Promise(r => setTimeout(r, 300));

            // 6. 逐本循環：重掃 + PageDown，全部用 DOM polling
            for (let i = 0; i < count; i++) {{
                // 找「重掃」按鈕
                let rescanBtn = Array.from(document.querySelectorAll('button.el-button')).find(b => 
                    (b.textContent && b.textContent.includes('重掃')) || 
                    (b.innerText && b.innerText.includes('重掃')) || 
                    (b.title && b.title.includes('重掃')) ||
                    (b.getAttribute('aria-label') && b.getAttribute('aria-label').includes('重掃'))
                );

                if (rescanBtn) {{
                    rescanBtn.click();
                }} else {{
                    return 'Error: Could not find Rescan button on comic ' + i;
                }}

                // 等待重掃完成（loading mask 消失）
                await waitReady();

                // 下一本
                if (i < count - 1) {{
                    document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'PageDown', code: 'PageDown', keyCode: 34, bubbles: true }}));
                    await waitReady();
                    await new Promise(r => setTimeout(r, 100)); // 微小緩衝確保 DOM 更新
                }}
            }}

            // 7. Escape 回到列表
            document.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Escape', code: 'Escape', keyCode: 27, bubbles: true }}));
            await waitReady();
            await new Promise(r => setTimeout(r, 500));

            // 8. 點「批量獲取元數據」
            let batchBtn = Array.from(document.querySelectorAll('button.el-button')).find(b => 
                (b.textContent && b.textContent.includes('批量獲取元數據')) ||
                (b.innerText && b.innerText.includes('批量獲取元數據')) ||
                (b.title && b.title.includes('批量獲取元數據')) ||
                (b.getAttribute('aria-label') && b.getAttribute('aria-label').includes('批量獲取元數據'))
            );

            if (batchBtn) {{
                batchBtn.click();
            }} else {{
                return 'Error: Could not find Batch Fetch Metadata button';
            }}

            return true;
        }})();
        """
        
        result = client.evaluate(js_action)
        
        if result is True or result == True or result == "true":
            _update_progress("✅ 自動化更新完成", 100)
            log_info("[EH CDP] JS 自動化流程執行成功。")
        else:
            log_error(f"[EH CDP] JS 執行未成功，返回值: {result}")
            _update_progress(f"❌ 自動化中斷: {result}", 100)
    except Exception as e:
        import traceback
        log_error(f"[EH CDP] 自動化執行失敗: {e}\n{traceback.format_exc()}")
        _update_progress(f"❌ 自動化失敗: {e}", 100)
    finally:
        if 'client' in locals() and client: client.close()
        log_info("[EH 自動化] 自動化完成，EMM 將保持開啟。")

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
            if config.get('automation_enabled', False): close_manga_app_if_running(config)
            _update_progress("正在連接資料庫...", 10) 
            
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
        # --- v-MOD: 回傳執行摘要物件，讓其他外掛可以讀取數據 ---
        return summary 
        # --- v-MOD END ---
        
    def sync_deleted_files(self, config: Dict[str, Any], deleted_paths: List[str]):
        # v-MOD: 只要配置了 eh_data_directory 就能同步，不再需要 enable_eh_preprocessor
        if not deleted_paths: return
        db_dir = config.get('eh_data_directory')
        if not db_dir: return
        db_file = os.path.join(db_dir, "database.sqlite")
        if not os.path.exists(db_file): return
        
        log_info(f"[EH 外掛] 接收主程式刪除訊號。準備同步軟刪除 {len(deleted_paths)} 筆檔案記錄...")
        create_database_backup(config)
        update_database_records(db_file, paths_to_soft_delete=deleted_paths)
        log_info("[EH 外掛] 同步軟刪除完成。")

    def sync_restored_files(self, config: Dict[str, Any], restored_paths: List[str]):
        # v-MOD: 只要配置了 eh_data_directory 就能同步
        if not restored_paths: return
        db_dir = config.get('eh_data_directory')
        if not db_dir: return
        db_file = os.path.join(db_dir, "database.sqlite")
        if not os.path.exists(db_file): return
        
        log_info(f"[EH 外掛] 接收主程式復原訊號。準備同步還原 {len(restored_paths)} 筆檔案記錄...")
        create_database_backup(config)
        update_database_records(db_file, paths_to_restore=restored_paths)
        log_info("[EH 外掛] 同步還原完成。")