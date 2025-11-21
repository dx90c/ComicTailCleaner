# ======================================================================
# 檔案名稱：processors/scanner.py
# 模組目的：提供統一的文件掃描、SQLite 緩存管理及多進程 workers
# 版本：3.0.1 (Hotfix: 修正 remove_folders 中的 CACHE_LOCK 死結問題)
# ======================================================================

import os
import datetime
import json
import time
import sqlite3
import re
from collections import deque, defaultdict
from queue import Queue
from typing import Union, Tuple, Dict, List, Set, Optional, Generator, Any

# --- 第三方庫 ---
try:
    import send2trash
except ImportError:
    send2trash = None
try:
    import imagehash
except ImportError:
    imagehash = None
try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = None
    np = None

# --- 本地模組 ---
import config
from config import VPATH_PREFIX, VPATH_SEPARATOR
from utils import (log_info, log_error, _is_virtual_path, _parse_virtual_path, 
                   CACHE_LOCK, _sanitize_path_for_filename, _open_image_from_any_path, 
                   _get_file_stat, _norm_key)

try:
    from utils import log_warning
except ImportError:
    def log_warning(msg: str): print(f"[WARN] {msg}")

try:
    import archive_handler
    ARCHIVE_SUPPORT_ENABLED = True
except ImportError:
    archive_handler = None
    ARCHIVE_SUPPORT_ENABLED = False
    
# === 版本常數 ===
SCANNER_ENGINE_VERSION = "3.0.1"

# --- 全域設定讀取 ---
DEFAULT_IMG_FLUSH_THRESHOLD = 1000
DEFAULT_FOLDER_FLUSH_THRESHOLD = 200

# --- 掃描輔助函式 ---
def _natural_sort_key(s: str) -> list:
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', s)]

def _folder_time(st: os.stat_result, mode: str) -> float:
    if mode == 'ctime':  return st.st_ctime
    if mode == 'hybrid': return max(st.st_mtime, st.st_ctime)
    return st.st_mtime

def _iter_scandir_recursively(root_path: str, excluded_paths: set, excluded_names: set, control_events: Optional[dict]) -> Generator[os.DirEntry, None, None]:
    queue = deque([root_path])
    while queue:
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
            return
        
        current_dir = queue.popleft()
        try:
            with os.scandir(current_dir) as it:
                for entry in it:
                    norm_path = _norm_key(entry.path)
                    base_name = os.path.basename(norm_path).lower()
                    
                    if any(norm_path == ex or norm_path.startswith(ex + os.sep) for ex in excluded_paths) or base_name in excluded_names:
                        continue

                    if entry.is_dir(follow_symlinks=False):
                        queue.append(entry.path)
                    elif entry.is_file():
                        yield entry
        except OSError:
            continue

def _iter_scandir_time_pruned(root_path: str, excluded_paths: set, excluded_names: set,
                              control_events: Optional[dict], max_depth: int,
                              start: Optional[datetime.datetime], end: Optional[datetime.datetime],
                              time_mode: str) -> Generator[os.DirEntry, None, None]:
    root_norm = _norm_key(root_path)
    base_depth = root_norm.count(os.sep)
    queue = deque([root_path])
    while queue:
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
            return
        cur = queue.popleft()
        norm_cur = _norm_key(cur)
        if (
            any(norm_cur == ex or norm_cur.startswith(ex + os.sep) for ex in excluded_paths)
            or os.path.basename(norm_cur).lower() in excluded_names
        ):
            continue
        cur_depth = norm_cur.count(os.sep) - base_depth
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    if entry.is_file():
                        yield entry
                    elif entry.is_dir(follow_symlinks=False):
                        try:
                            st = entry.stat(follow_symlinks=False)
                            ts = _folder_time(st, time_mode)
                            if start and datetime.datetime.fromtimestamp(ts) < start:
                                continue
                            if end and datetime.datetime.fromtimestamp(ts) > end:
                                continue
                        except OSError:
                            continue
                        if cur_depth < max_depth:
                            queue.append(entry.path)
        except OSError:
            continue


# === 多進程 Worker 函式 ===
def _detect_qr_on_image(img) -> Union[list, None]:
    if cv2 is None or np is None: return None
    try:
        arr = np.array(img.convert("RGB"))
        q = cv2.QRCodeDetector()
        ok, infos, pts, _ = q.detectAndDecodeMulti(arr)
        if ok and pts is not None and len(pts) > 0:
            return pts.tolist()
        ok, pts = q.detectMulti(arr)
        if ok and pts is not None and len(pts) > 0:
            return pts.tolist()
        return None
    except Exception:
        return None

def _pool_worker_detect_qr_code(payload: Tuple[str, int]):
    image_path, resize_size = payload
    try:
        from PIL import Image, ImageOps
        with _open_image_from_any_path(image_path) as pil_img:
            if not pil_img or pil_img.width == 0 or pil_img.height == 0:
                return (image_path, {'error': '圖片尺寸異常或無法讀取'})
            pil_img = ImageOps.exif_transpose(pil_img)
            tmp = pil_img.copy()
            tmp.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            pts = _detect_qr_on_image(tmp)
            if not pts:
                pts = _detect_qr_on_image(pil_img)
            return (image_path, {'qr_points': pts, 'width': pil_img.width, 'height': pil_img.height})
    except Exception as e:
        return (image_path, {'error': f'QR檢測失敗: {e}'})

def _pool_worker_process_image_phash_only(payload: Union[str, tuple]) -> tuple[str, dict | None]:
    image_path = ""
    try:
        from PIL import Image, ImageOps
        if isinstance(payload, tuple) and len(payload) > 0:
            image_path = payload[0]
        elif isinstance(payload, str):
            image_path = payload
        else:
            return ("invalid_payload", {'error': f"收到了無效的 payload 格式: {type(payload)}"})

        with _open_image_from_any_path(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                return (image_path, {'error': f"圖片尺寸異常或無法讀取: {image_path}"})
            
            img = ImageOps.exif_transpose(img)
            ph = imagehash.phash(img)
            st = _get_file_stat(image_path)
            
            return (image_path, {
                'phash': str(ph), 
                'size': st.st_size if st else 0, 
                'ctime': st.st_ctime if st else 0, 
                'mtime': st.st_mtime if st else 0
            })
    except Exception as e:
        error_path = image_path if image_path else str(payload)
        return (error_path, {'error': f"處理 pHash 失敗: {e}"})


# === SQLite 快取基類 ===
class SQLiteCacheBase:
    """提供通用的 SQLite 操作基礎"""
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self.conn = self._init_db()
        self._pending_updates = {}
        self.flush_threshold = 1000
        
    def _init_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # 啟用 WAL 模式以提升並發性能
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} (path TEXT PRIMARY KEY, data TEXT)")
        conn.commit()
        return conn

    def _serialize(self, data: dict) -> str:
        serializable = data.copy()
        if imagehash:
            for k in ['phash', 'whash']:
                if k in serializable and isinstance(serializable[k], imagehash.ImageHash):
                    serializable[k] = str(serializable[k])
        if 'avg_hsv' in serializable and isinstance(serializable['avg_hsv'], tuple):
            serializable['avg_hsv'] = list(serializable['avg_hsv'])
        return json.dumps(serializable)

    def _deserialize(self, json_str: str) -> dict:
        try:
            data = json.loads(json_str)
            if imagehash:
                for k in ['phash', 'whash']:
                    if k in data and data[k] and isinstance(data[k], str):
                        try: data[k] = imagehash.hex_to_hash(data[k])
                        except ValueError: data[k] = None
            if 'avg_hsv' in data and isinstance(data['avg_hsv'], list):
                try: data['avg_hsv'] = tuple(float(x) for x in data['avg_hsv'])
                except ValueError: data[k] = None
            return data
        except (json.JSONDecodeError, TypeError):
            return {}

    def get_data(self, path: str) -> Union[dict, None]:
        key = _norm_key(path)
        if key in self._pending_updates:
            return self._pending_updates[key]
        
        try:
            cursor = self.conn.execute(f"SELECT data FROM {self.table_name} WHERE path=?", (key,))
            row = cursor.fetchone()
            if row:
                return self._deserialize(row[0])
        except sqlite3.Error as e:
            log_error(f"SQLite 讀取錯誤: {e}")
        return None

    def update_data(self, path: str, data: dict):
        if not data or 'error' in data: return
        key = _norm_key(path)
        
        current = self.get_data(key) or {}
        current.update(data)
        
        self._pending_updates[key] = current
        
        if len(self._pending_updates) >= self.flush_threshold:
            self.save_cache()

    def save_cache(self):
        if not self._pending_updates: return
        with CACHE_LOCK:
            try:
                items = [(k, self._serialize(v)) for k, v in self._pending_updates.items()]
                self.conn.executemany(f"INSERT OR REPLACE INTO {self.table_name} (path, data) VALUES (?, ?)", items)
                self.conn.commit()
                self._pending_updates.clear()
            except sqlite3.Error as e:
                log_error(f"SQLite 寫入錯誤: {e}")

    def remove_data(self, path: str) -> bool:
        key = _norm_key(path)
        if key in self._pending_updates:
            del self._pending_updates[key]
        
        try:
            with CACHE_LOCK:
                self.conn.execute(f"DELETE FROM {self.table_name} WHERE path=?", (key,))
                self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def remove_prefix(self, prefix: str):
        prefix_norm = _norm_key(prefix)
        keys_to_del = [k for k in self._pending_updates if k.startswith(prefix_norm)]
        for k in keys_to_del:
            del self._pending_updates[k]
            
        try:
            with CACHE_LOCK:
                pattern = prefix_norm + "%"
                self.conn.execute(f"DELETE FROM {self.table_name} WHERE path LIKE ?", (pattern,))
                self.conn.commit()
        except sqlite3.Error as e:
            log_error(f"SQLite 批量刪除錯誤: {e}")

    def close(self):
        self.save_cache()
        self.conn.close()

# === 具體快取管理類 (SQLite 版) ===

class ScannedImageCacheManager(SQLiteCacheBase):
    def __init__(self, root_scan_folder: str):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        base_name = f"scanned_hashes_cache_{sanitized_root}"
        from config import DATA_DIR
        
        db_path = os.path.join(DATA_DIR, f"{base_name}.db")
        json_path_legacy = os.path.join(DATA_DIR, f"{base_name}.json")
        
        super().__init__(db_path, "images")
        self.flush_threshold = DEFAULT_IMG_FLUSH_THRESHOLD
        self.cache_file_path = db_path

        if not os.path.exists(db_path) or self._is_db_empty():
            if os.path.exists(json_path_legacy):
                self._migrate_from_json(json_path_legacy)
        
        log_info(f"[快取] SQLite 圖片快取已就緒: '{self.cache_file_path}'")

    def _is_db_empty(self) -> bool:
        try:
            return self.conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 0
        except sqlite3.Error:
            return True

    def _migrate_from_json(self, json_path: str):
        log_info(f"[遷移] 偵測到舊版 JSON 快取 '{json_path}'，正在遷移至 SQLite...")
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                images = data.get('images', data)
                
            if images:
                items = []
                for path, meta in images.items():
                    norm_p = _norm_key(path)
                    items.append((norm_p, json.dumps(meta)))
                
                with CACHE_LOCK:
                    self.conn.executemany("INSERT OR REPLACE INTO images (path, data) VALUES (?, ?)", items)
                    self.conn.commit()
                log_info(f"[遷移] 成功遷移 {len(items)} 筆資料。")
                try: os.rename(json_path, json_path + ".bak")
                except OSError: pass
                
        except Exception as e:
            log_error(f"[遷移] 遷移失敗: {e}", True)

    def remove_entries_from_folder(self, folder_path: str) -> int:
        self.remove_prefix(folder_path)
        return 0 

    def invalidate_cache(self) -> None:
        if send2trash is None: return
        self.close()
        if os.path.exists(self.db_path):
            try: send2trash.send2trash(self.db_path)
            except Exception: pass
        self.conn = self._init_db()


class FolderStateCacheManager(SQLiteCacheBase):
    def __init__(self, root_scan_folder: str):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        base_name = f"folder_state_cache_{sanitized_root}"
        from config import DATA_DIR
        
        db_path = os.path.join(DATA_DIR, f"{base_name}.db")
        json_path_legacy = os.path.join(DATA_DIR, f"{base_name}.json")

        super().__init__(db_path, "folders")
        self.flush_threshold = DEFAULT_FOLDER_FLUSH_THRESHOLD
        self.cache_file_path = db_path

        if not os.path.exists(db_path) or self._is_db_empty():
            if os.path.exists(json_path_legacy):
                self._migrate_from_json(json_path_legacy)

        log_info(f"[快取] SQLite 資料夾快取已就緒: '{self.cache_file_path}'")
        
    def _is_db_empty(self) -> bool:
        try: return self.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0] == 0
        except: return True

    def _migrate_from_json(self, json_path: str):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if data:
                items = [(k, json.dumps(v)) for k, v in data.items()]
                with CACHE_LOCK:
                    self.conn.executemany("INSERT OR REPLACE INTO folders (path, data) VALUES (?, ?)", items)
                    self.conn.commit()
        except Exception: pass

    def get_folder_state(self, folder_path: str) -> Union[dict, None]:
        return self.get_data(folder_path)

    def update_folder_state(self, folder_path: str, mtime: float, ctime: Union[float, None], extra: Optional[Dict] = None):
        state = {'mtime': mtime, 'ctime': ctime}
        if extra: state.update(extra)
        self.update_data(folder_path, state)

    def remove_folders(self, folder_paths: list[str]):
        """
        修正: 使用 executemany 直接刪除，避免呼叫 self.remove_data() 造成 CACHE_LOCK 重入死鎖。
        """
        keys_to_del = [_norm_key(p) for p in folder_paths]
        # 清理 buffer
        for k in keys_to_del:
             if k in self._pending_updates:
                 del self._pending_updates[k]
        
        if not keys_to_del: return

        try:
            with CACHE_LOCK:
                # 直接操作 SQL，不調用會再次加鎖的 self.remove_data
                items = [(k,) for k in keys_to_del]
                self.conn.executemany(f"DELETE FROM {self.table_name} WHERE path=?", items)
                self.conn.commit()
        except sqlite3.Error as e:
            log_error(f"SQLite 批量移除資料夾失敗: {e}")

    @property
    def cache(self) -> dict:
        # 先將 pending 寫入
        self.save_cache()
        try:
            cursor = self.conn.execute("SELECT path, data FROM folders")
            return {row[0]: self._deserialize(row[1]) for row in cursor.fetchall()}
        except sqlite3.Error:
            return {}

    def invalidate_cache(self) -> None:
        if send2trash is None: return
        self.close()
        if os.path.exists(self.db_path):
            try: send2trash.send2trash(self.db_path)
            except: pass
        self.conn = self._init_db()

# ======================================================================
# Section: 高效檔案列舉
# ======================================================================

def _unified_scan_traversal(root_folder: str, excluded_paths: set, excluded_names: set, time_filter: dict, folder_cache: 'FolderStateCacheManager', progress_queue: Optional[Queue], control_events: Optional[dict], use_pruning: bool, time_mode: str) -> Tuple[Dict[str, Any], Set[str], Set[str]]:
    log_info(f"啟動 v{SCANNER_ENGINE_VERSION} 統一掃描引擎...")
    
    def _scan_newest_first_recursive(path: str, time_filter: dict, excluded_paths: set, excluded_names: set, control_events: Optional[dict], stats: Dict[str, int], time_mode: str, is_root: bool = False) -> Generator[str, None, None]:
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
            return

        norm_path = _norm_key(path)
        base_name = os.path.basename(norm_path).lower()
        if any(norm_path == ex or norm_path.startswith(ex + os.sep) for ex in excluded_paths) or base_name in excluded_names:
            return

        try:
            stats['visited_dirs'] += 1
            st = os.stat(path)
            cur_ts = _folder_time(st, time_mode)
            mtime_dt = datetime.datetime.fromtimestamp(cur_ts)
            start = time_filter.get('start')
            end   = time_filter.get('end')

            if not is_root:
                if start and mtime_dt < start:
                    stats['pruned_by_start'] += 1
                    return

            in_range = (not start or mtime_dt >= start) and (not end or mtime_dt <= end)
            if in_range:
                yield path
            elif end and mtime_dt > end and not is_root:
                stats['skipped_by_end'] += 1

            subdirs = []
            with os.scandir(path) as it:
                for entry in it:
                    if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return
                    if entry.is_dir(follow_symlinks=False):
                        try:
                            st_sub = entry.stat(follow_symlinks=False)
                            subdirs.append((entry.path, _folder_time(st_sub, time_mode)))
                        except OSError:
                            continue
            
            subdirs.sort(key=lambda x: x[1], reverse=True)

            processed_count = 0
            for subdir_path, mt in subdirs:
                if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return
                if start and datetime.datetime.fromtimestamp(mt) < start:
                    stats['pruned_by_start'] += (len(subdirs) - processed_count)
                    break
                processed_count += 1
                yield from _scan_newest_first_recursive(subdir_path, time_filter, excluded_paths, excluded_names, control_events, stats, time_mode, is_root=False)
        except OSError:
            return

    if not use_pruning or not time_filter.get('enabled') or not time_filter.get('start'):
        log_info("使用標準 BFS 掃描 (未啟用剪枝或時間篩選)。")
        live_folders, changed_or_new_folders = {}, set()
        queue = deque([root_folder])
        scanned_count = 0
        cached_states = folder_cache.cache.copy()
        
        while queue:
            if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
                return {}, set(), set()
            current_dir = queue.popleft()
            norm_current_dir = _norm_key(current_dir)
            if any(norm_current_dir == ex or norm_current_dir.startswith(ex + os.sep) for ex in excluded_paths) or os.path.basename(norm_current_dir).lower() in excluded_names:
                continue
            try:
                stat_info = os.stat(current_dir)
                cached_states.pop(norm_current_dir, None)
                scanned_count += 1
                if scanned_count % 100 == 0:
                    time.sleep(0.001)
                    if progress_queue: progress_queue.put({'type': 'text', 'text': f"📁 正在檢查資料夾結構... ({scanned_count})"})

                live_folders[norm_current_dir] = {'mtime': stat_info.st_mtime, 'ctime': stat_info.st_ctime}
                cached_entry = folder_cache.get_folder_state(norm_current_dir)
                if not cached_entry or abs(_folder_time(stat_info, time_mode) - cached_entry.get('mtime', 0)) > 1e-6:
                    changed_or_new_folders.add(norm_current_dir)

                with os.scandir(current_dir) as it:
                    for entry in it:
                        if entry.is_dir(follow_symlinks=False):
                            queue.append(entry.path)
            except OSError: continue
        
        ghost_folders = set(cached_states.keys())
        log_info(f"BFS 掃描完成。即時資料夾: {len(live_folders)}, 新/變更: {len(changed_or_new_folders)}, 幽靈資料夾: {len(ghost_folders)}")
        return live_folders, changed_or_new_folders, ghost_folders

    log_info("啟用時間篩選，使用智慧型遞迴剪枝 (DFS) 掃描...")
    stats = defaultdict(int)
    all_scanned_paths = list(_scan_newest_first_recursive(root_folder, time_filter, excluded_paths, excluded_names, control_events, stats, time_mode, is_root=True))
    
    live_folders, changed_or_new_folders = {}, set()
    cached_states = folder_cache.cache.copy()

    for path in all_scanned_paths:
        norm_path = _norm_key(path)
        cached_states.pop(norm_path, None)
        try:
            stat_info = os.stat(path)
            live_folders[norm_path] = {'mtime': stat_info.st_mtime, 'ctime': stat_info.st_ctime}
            cached_entry = folder_cache.get_folder_state(norm_path)
            if not cached_entry or abs(_folder_time(stat_info, time_mode) - cached_entry.get('mtime', 0)) > 1e-6:
                changed_or_new_folders.add(norm_path)
        except OSError:
            continue
            
    ghost_folders = set(cached_states.keys())
    log_info(f"DFS 掃描完成。訪問: {stats['visited_dirs']}, 起始日剪枝: {stats['pruned_by_start']}, 結束日跳過: {stats['skipped_by_end']}")
    log_info(f"符合時間的資料夾: {len(live_folders)}, 新/變更: {len(changed_or_new_folders)}, 幽靈資料夾: {len(ghost_folders)}")
    return live_folders, changed_or_new_folders, ghost_folders
    
def get_files_to_process(config_dict: Dict, 
                         image_cache_manager: 'ScannedImageCacheManager', 
                         progress_queue: Optional[Queue] = None, 
                         control_events: Optional[Dict] = None,
                         quarantine_list: Optional[Set[str]] = None) -> Tuple[List[str], Dict[str, int]]:
    root_folder = config_dict['root_scan_folder']
    if not os.path.isdir(root_folder): return [], {}
    
    enable_archive_scan = config_dict.get('enable_archive_scan', False) and ARCHIVE_SUPPORT_ENABLED
    
    fmts = []
    if enable_archive_scan and archive_handler:
        for e in archive_handler.get_supported_formats():
            e = e.lower().strip()
            if not e.startswith('.'): e = '.' + e
            fmts.append(e)
    supported_archive_exts = tuple(fmts)

    folder_cache = FolderStateCacheManager(root_folder)
    
    excluded_folders_config = config_dict.get('excluded_folders', [])
    excluded_paths = {_norm_key(p) for p in excluded_folders_config if os.path.sep in p or (os.path.altsep and os.path.altsep in p)}
    excluded_names = {name.lower() for name in excluded_folders_config if (os.path.sep not in name) and (not os.path.altsep or os.path.altsep not in name)}

    time_filter = {'enabled': config_dict.get('enable_time_filter', False)}
    if time_filter['enabled']:
        try:
            start_str, end_str = config_dict.get('start_date_filter'), config_dict.get('end_date_filter')
            time_filter['start'] = datetime.datetime.strptime(start_str, "%Y-%m-%d") if start_str else None
            time_filter['end'] = datetime.datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if end_str else None
        except (ValueError, TypeError):
            log_error("時間篩選日期格式錯誤，將被忽略。"); time_filter['enabled'] = False

    use_pruning = config_dict.get('enable_newest_first_pruning', True)
    time_mode = str(config_dict.get('folder_time_mode', 'mtime'))
    live_folders, folders_to_scan_content, ghost_folders = _unified_scan_traversal(root_folder, excluded_paths, excluded_names, time_filter, folder_cache, progress_queue, control_events, use_pruning, time_mode)

    new_folders = {f for f in live_folders if folder_cache.get_folder_state(f) is None}
    if new_folders:
        if time_filter['enabled']:
            log_info(f"[新容器] 偵測到 {len(new_folders)} 個首次出現的資料夾，將根據時間窗口決定是否加入掃描。")
            
            start, end = time_filter.get('start'), time_filter.get('end')
            folders_to_add = set()
            for f in new_folders:
                try:
                    st = os.stat(f)
                    ts = _folder_time(st, time_mode)
                    dt = datetime.datetime.fromtimestamp(ts)
                    if start and dt < start: continue
                    if end and dt > end: continue
                    folders_to_add.add(f)
                except OSError:
                    continue
            folders_to_scan_content.update(folders_to_add)
        else:
            log_info(f"[新容器] 偵測到 {len(new_folders)} 個首次出現的資料夾，將無條件加入本輪掃描。")
            folders_to_scan_content.update(new_folders)

    root_norm = _norm_key(config_dict['root_scan_folder'])
    if root_norm in folders_to_scan_content:
        log_warning("[保護] 根資料夾被標記為『變更』— 將改用保底模式（僅補快取缺口，不全面遞迴）。")
        folders_to_scan_content.discard(root_norm)

    def _prune_to_leaf_changed(changed_set: set[str]) -> set[str]:
        changed = sorted(changed_set)
        if not changed: return set()
        keep = set(changed)
        for i, p in enumerate(changed):
            prefix = p + os.sep
            for j in range(i + 1, len(changed)):
                q = changed[j]
                if q.startswith(prefix):
                    keep.discard(p)
                    break
        return keep

    orig_cnt = len(folders_to_scan_content)
    folders_to_scan_content = _prune_to_leaf_changed(folders_to_scan_content)
    if len(folders_to_scan_content) < orig_cnt:
        log_info(f"[變更集縮減] 由 {orig_cnt} 夾縮至 {len(folders_to_scan_content)} 個最深變更夾，避免整棵樹重掃。")
    
    if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return [], {}

    use_time_window = bool(time_filter.get('enabled') and (time_filter.get('start') or time_filter.get('end')))
    preserve = bool(config_dict.get('preserve_cache_across_time_windows', True))
    strict_img_prune = bool(config_dict.get('prune_image_cache_on_missing_folder', False))

    if ghost_folders:
        if use_time_window and preserve:
            truly_missing = [f for f in ghost_folders if not os.path.exists(f)]
            if truly_missing:
                log_info(f"正在從狀態快取中移除 {len(truly_missing)} 個已不存在的資料夾...")
                folder_cache.remove_folders(truly_missing)
                if strict_img_prune:
                    log_info(f"正在同步移除對應的圖片快取...")
                    for folder in truly_missing:
                        image_cache_manager.remove_entries_from_folder(folder)
        else:
            log_info(f"正在清理 {len(ghost_folders)} 個幽靈資料夾的快取...")
            folder_cache.remove_folders(list(ghost_folders))
            for folder in ghost_folders:
                image_cache_manager.remove_entries_from_folder(folder)

    unchanged_folders = set(live_folders.keys()) - folders_to_scan_content
    
    folders_needing_scan_due_to_empty_cache = set()
    if image_cache_manager._is_db_empty() and unchanged_folders:
         folders_needing_scan_due_to_empty_cache = unchanged_folders.copy()

    if root_norm in folders_needing_scan_due_to_empty_cache:
        folders_needing_scan_due_to_empty_cache.discard(root_norm)
        log_warning("[保護] 根夾缺快取但已跳過保底補掃；如需掃描請改選子資料夾或取消根夾保護。")

    if folders_needing_scan_due_to_empty_cache:
        def _prune_to_leaf_only(cands: Set[str]) -> Set[str]:
            items = sorted(cands)
            keep = set(items)
            for i, p in enumerate(items):
                pref = p + os.sep
                for j in range(i + 1, len(items)):
                    if items[j].startswith(pref):
                        keep.discard(p)
                        break
            return keep

        pruned = _prune_to_leaf_only(folders_needing_scan_due_to_empty_cache)
        if pruned != folders_needing_scan_due_to_empty_cache:
            log_info(f"[保底裁剪] 從 {len(folders_needing_scan_due_to_empty_cache)} 夾裁成 {len(pruned)} 個葉節點夾以避免整樹補掃。")
        folders_needing_scan_due_to_empty_cache = pruned

        log_info(f"[保底] {len(folders_needing_scan_due_to_empty_cache)} 個未變更資料夾因在圖片快取中無記錄，已加入掃描。")
        folders_to_scan_content.update(folders_needing_scan_due_to_empty_cache)
        unchanged_folders -= folders_needing_scan_due_to_empty_cache

    image_exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    
    count = int(config_dict.get('extract_count', 8))
    first_scan_extract = int(config_dict.get('first_scan_extract_count', 64))
    enable_limit = config_dict.get('enable_extract_count_limit', True)
    
    qr_mode = config_dict.get('comparison_mode') == 'qr_detection'
    if qr_mode and enable_limit:
        count = int(config_dict.get('qr_pages_per_archive', 10))
    qr_global_cap = int(config_dict.get('qr_global_cap', 20000))

    scanned_files = []
    vpath_size_map = {}
    
    changed_container_cap = int(config_dict.get('changed_container_cap', 0) or 0)
    depth_limit = int(config_dict.get('changed_container_depth_limit', 1))
    start, end = time_filter.get('start'), time_filter.get('end')
    container_empty_mark = config_dict.get('container_empty_mark', True)

    def _container_mtime(p: str) -> float:
        try: return os.path.getmtime(p)
        except OSError: return 0.0

    for folder in sorted(list(folders_to_scan_content)):
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): break
        
        before_len = len(scanned_files)
        temp_files_in_container = defaultdict(list)
        
        for entry in _iter_scandir_recursively(folder, excluded_paths, excluded_names, control_events):
            try:
                if use_time_window:
                    st = entry.stat(follow_symlinks=False)
                    file_ts = st.st_ctime if time_mode == 'ctime' else st.st_mtime
                    file_dt = datetime.datetime.fromtimestamp(file_ts)
                    if start and file_dt < start: continue
                    if end and file_dt > end: continue

                f_lower = entry.name.lower()
                if enable_archive_scan and f_lower.endswith(supported_archive_exts):
                    temp_files_in_container[entry.path] = []
                elif f_lower.endswith(image_exts):
                    temp_files_in_container[os.path.dirname(entry.path)].append(_norm_key(entry.path))
            except OSError:
                continue
        
        if changed_container_cap > 0 and len(temp_files_in_container) > changed_container_cap:
            keep = sorted(temp_files_in_container.keys(), key=_container_mtime, reverse=True)[:changed_container_cap]
            dropped = len(temp_files_in_container) - len(keep)
            temp_files_in_container = {k: temp_files_in_container[k] for k in keep}
            log_info(f"[變更夾容器上限] {folder} 僅保留 {len(keep)} 個近期容器，捨棄 {dropped} 個")

        for container_path, files in temp_files_in_container.items():
            ext = os.path.splitext(container_path)[1].lower()
            if ext in supported_archive_exts:
                try:
                    all_vpaths = []
                    if archive_handler:
                        for arc_entry in archive_handler.iter_archive_images(container_path):
                            vpath = f"{VPATH_PREFIX}{arc_entry.archive_path}{VPATH_SEPARATOR}{arc_entry.inner_path}"
                            all_vpaths.append(vpath)
                    all_vpaths.sort(key=_natural_sort_key)
                    files.extend(all_vpaths)
                except Exception as e:
                    log_error(f"讀取壓縮檔失敗: {container_path}: {e}", True)
                    continue
            files.sort(key=_natural_sort_key)
            
            container_dir = _norm_key(os.path.dirname(container_path))
            is_new = container_dir in new_folders
            
            if enable_limit:
                take_n = first_scan_extract if is_new else count
                if qr_mode:
                    take_n = int(config_dict.get('qr_pages_per_archive', 10))
                scanned_files.extend(files[-take_n:])
            else:
                scanned_files.extend(files)

        norm_folder = _norm_key(folder)
        if norm_folder in live_folders: 
            added_for_this_folder = len(scanned_files) - before_len
            is_empty = (added_for_this_folder == 0)
            if container_empty_mark:
                folder_cache.update_folder_state(
                    norm_folder,
                    live_folders[norm_folder]['mtime'],
                    live_folders[norm_folder]['ctime'],
                    extra={'is_empty': is_empty}
                )
            else:
                 folder_cache.update_folder_state(
                    norm_folder,
                    live_folders[norm_folder]['mtime'],
                    live_folders[norm_folder]['ctime']
                 )

    if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return [], {}
    
    cached_files = []
    if unchanged_folders:
        try:
            conn = image_cache_manager.conn
            cursor = conn.execute("SELECT path FROM images")
            for row in cursor:
                p = row[0]
                try:
                    if _is_virtual_path(p):
                         arch_path, _ = _parse_virtual_path(p)
                         parent = _norm_key(os.path.dirname(arch_path))
                    else:
                        parent = _norm_key(os.path.dirname(p))
                    
                    if parent in unchanged_folders:
                         cached_files.append(p)
                except: continue
        except sqlite3.Error as e:
            log_error(f"讀取 SQLite 以恢復快取時出錯: {e}")
            
    final_file_list = scanned_files + cached_files
    folder_cache.save_cache()
    image_cache_manager.save_cache()
    
    unique_files = sorted(list(set(final_file_list)))

    if quarantine_list:
        unique_files = [f for f in unique_files if _norm_key(f) not in quarantine_list]
    
    def _path_mtime_for_cap(p: str) -> float:
        try:
            if _is_virtual_path(p):
                arch_path, _ = _parse_virtual_path(p)
                return os.path.getmtime(arch_path) if arch_path and os.path.exists(arch_path) else 0.0
            else:
                return os.path.getmtime(p) if os.path.exists(p) else 0.0
        except Exception: return 0.0

    mode = str(config_dict.get('comparison_mode', '')).lower()
    global_cap = int(config_dict.get('global_extract_cap', 0) or 0)

    if mode != 'qr_detection' and global_cap > 0 and len(unique_files) > global_cap:
        unique_files.sort(key=_path_mtime_for_cap, reverse=True)
        unique_files = unique_files[:global_cap]
    elif qr_mode and not enable_limit and qr_global_cap > 0 and len(unique_files) > qr_global_cap:
        unique_files.sort(key=_path_mtime_for_cap, reverse=True)
        unique_files = unique_files[:qr_global_cap]
        
    log_info(f"檔案提取完成。掃描 {len(scanned_files)} 筆, 恢復 {len(cached_files)} 筆。總計: {len(unique_files)}")
    return unique_files, vpath_size_map