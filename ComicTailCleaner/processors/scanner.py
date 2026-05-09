# ======================================================================
# 檔案名稱：processors/scanner.py
# 模組目的：提供統一的文件掃描、SQLite 緩存管理及多進程 workers
# 版本：3.1.3 (合併版：整合 v3.1.2 智慧掃描與 v3.0.1 死結修復/剪枝優化)
# ======================================================================

import os
import datetime
import json
import time
import sqlite3
import re
import threading
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
from .everything_ipc import EverythingIPCManager

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
SCANNER_ENGINE_VERSION = "3.1.3"

# --- 全域設定讀取 ---
DEFAULT_IMG_FLUSH_THRESHOLD = 1000
DEFAULT_FOLDER_FLUSH_THRESHOLD = 200
RESTORE_FOLDER_BATCH_SIZE = 500
AD_INDEX_HASH_KIND = "phash_64"
AD_INDEX_VERSION = "ad_lsh_v1_bands8_bits64"
AD_INDEX_BITS = 64
AD_INDEX_BANDS = 8

# --- 掃描輔助函式 ---
def _natural_sort_key(s: str) -> list:
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', s)]

def _folder_time(st: os.stat_result, mode: str) -> float:
    if mode == 'ctime':  return st.st_ctime
    if mode == 'hybrid': return max(st.st_mtime, st.st_ctime)
    return st.st_mtime

def _cache_folder_key(path: str) -> str:
    if _is_virtual_path(path):
        archive_path, _ = _parse_virtual_path(path)
        return _norm_key(os.path.dirname(archive_path)) if archive_path else ""
    return _norm_key(os.path.dirname(path))


def _compute_lsh_buckets_from_hash_obj(phash_obj, bands: int = AD_INDEX_BANDS, bits: int = AD_INDEX_BITS) -> list[int]:
    if not phash_obj:
        return []
    try:
        value = int(str(phash_obj), 16)
    except (TypeError, ValueError):
        return []
    seg_bits = bits // bands
    mask = (1 << seg_bits) - 1
    return [((value >> (band * seg_bits)) & mask) for band in range(bands)]

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


def _build_time_filter(config_dict: Dict) -> dict:
    time_filter = {'enabled': config_dict.get('enable_time_filter', False)}
    if not time_filter['enabled']:
        return time_filter
    try:
        start_str = config_dict.get('start_date_filter')
        end_str = config_dict.get('end_date_filter')
        time_filter['start'] = datetime.datetime.strptime(start_str, "%Y-%m-%d") if start_str else None
        time_filter['end'] = datetime.datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if end_str else None
    except (ValueError, TypeError):
        time_filter['enabled'] = False
    return time_filter


def _build_scan_requirements(config_dict: Dict) -> tuple[bool, int, int]:
    limit_enabled = config_dict.get('enable_extract_count_limit', True)
    target_count = int(config_dict.get('extract_count', 8))
    if config_dict.get('comparison_mode') == 'qr_detection' and limit_enabled:
        target_count = int(config_dict.get('qr_pages_per_archive', 10))
    first_scan_extract = int(config_dict.get('first_scan_extract_count', 0))
    required_count = max(target_count, first_scan_extract)
    if not limit_enabled:
        required_count = 999999
    return limit_enabled, target_count, required_count


def _build_scan_context(config_dict: Dict, root_folder: str) -> dict:
    enable_archive_scan = config_dict.get('enable_archive_scan', False) and ARCHIVE_SUPPORT_ENABLED
    supported_archive_exts = ()
    if enable_archive_scan and archive_handler:
        fmts = []
        for ext in archive_handler.get_supported_formats():
            ext = ext.lower().strip()
            if not ext.startswith('.'):
                ext = '.' + ext
            fmts.append(ext)
        supported_archive_exts = tuple(fmts)

    image_exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    folder_cache = FolderStateCacheManager(root_folder)
    time_filter = _build_time_filter(config_dict)
    limit_enabled, target_count, required_count = _build_scan_requirements(config_dict)

    return {
        'enable_archive_scan': enable_archive_scan,
        'supported_archive_exts': supported_archive_exts,
        'image_exts': image_exts,
        'folder_cache': folder_cache,
        'time_filter': time_filter,
        'limit_enabled': limit_enabled,
        'target_count': target_count,
        'required_count': required_count,
        'first_scan_extract': int(config_dict.get('first_scan_extract_count', 0)),
    }


def _resolve_scan_runtime_options(config_dict: Dict) -> dict:
    use_pruning = config_dict.get('enable_newest_first_pruning', True)
    time_mode = str(config_dict.get('folder_time_mode', 'mtime'))
    if config_dict.get('_everything_force_dm') and time_mode == 'ctime':
        time_mode = 'mtime'
        log_info("[SDK] dc: 索引未就緒，本次以修改日期 (dm:) 取代建立日期過濾。")

    use_everything_setting = config_dict.get('enable_everything_mft_scan', True)
    log_info(f"[診斷] Everything SDK 設定狀態: {use_everything_setting}")
    return {
        'use_pruning': use_pruning,
        'time_mode': time_mode,
        'use_everything_setting': use_everything_setting,
    }


def _resolve_excluded_folder_rules(config_dict: Dict) -> tuple[Set[str], Set[str]]:
    excluded_folders_config = config_dict.get('excluded_folders', [])
    excluded_paths = {
        _norm_key(p)
        for p in excluded_folders_config
        if os.path.sep in p or (os.path.altsep and os.path.altsep in p)
    }
    excluded_names = {
        name.lower()
        for name in excluded_folders_config
        if (os.path.sep not in name) and (not os.path.altsep or os.path.altsep not in name)
    }
    ad_folder = config_dict.get('ad_folder_path')
    root_folder = config_dict.get('root_scan_folder')
    if ad_folder and root_folder and os.path.isdir(ad_folder):
        try:
            norm_root = _norm_key(root_folder)
            norm_ad = _norm_key(ad_folder)
            if norm_ad == norm_root or norm_ad.startswith(norm_root + os.sep):
                excluded_paths.add(norm_ad)
                log_info(f"[掃描排除] 廣告資料夾位於根目錄內，已排除於根掃描之外: {norm_ad}")
        except Exception:
            pass
    return excluded_paths, excluded_names


def _expand_new_folders_into_scan_set(
    new_folders: Set[str],
    folders_to_scan_content: Set[str],
    time_filter: dict,
    time_mode: str,
) -> None:
    if not new_folders:
        return
    if not time_filter['enabled']:
        folders_to_scan_content.update(new_folders)
        return

    start = time_filter.get('start')
    end = time_filter.get('end')
    folders_to_add = set()
    for folder in new_folders:
        try:
            st = os.stat(folder)
            ts = _folder_time(st, time_mode)
            dt = datetime.datetime.fromtimestamp(ts)
            if start and dt < start:
                continue
            if end and dt > end:
                continue
            folders_to_add.add(folder)
        except OSError:
            continue
    folders_to_scan_content.update(folders_to_add)


def _prune_to_leaf_only(cands: Set[str]) -> Set[str]:
    items = sorted(cands)
    keep = set(items)
    for i, path in enumerate(items):
        prefix = path + os.sep
        for j in range(i + 1, len(items)):
            if items[j].startswith(prefix):
                keep.discard(path)
                break
    return keep


def _cleanup_ghost_folders(
    config_dict: Dict,
    ghost_folders: Set[str],
    time_filter: dict,
    folder_cache: 'FolderStateCacheManager',
    image_cache_manager: 'ScannedImageCacheManager',
):
    if not ghost_folders:
        return

    use_time_window = bool(time_filter.get('enabled') and (time_filter.get('start') or time_filter.get('end')))
    preserve = bool(config_dict.get('preserve_cache_across_time_windows', True))
    strict_img_prune = bool(config_dict.get('prune_image_cache_on_missing_folder', False))

    if use_time_window and preserve:
        if config_dict.get('enable_missing_folder_cleanup', False):
            truly_missing = [folder for folder in ghost_folders if not os.path.exists(folder)]
            if truly_missing:
                log_info(f"正在從狀態快取中移除 {len(truly_missing)} 個已不存在的資料夾...")
                folder_cache.remove_folders(truly_missing)
                if strict_img_prune:
                    log_info(f"正在同步移除對應的圖片快取...")
                    for folder in truly_missing:
                        image_cache_manager.remove_entries_from_folder(folder)
        else:
            log_info(f"[保留模式] 略過 {len(ghost_folders)} 個幽靈資料夾的存在檢查，以避免 HDD 掃描卡頓。")
        return

    log_info(f"正在清理 {len(ghost_folders)} 個幽靈資料夾的快取...")
    folder_cache.remove_folders(list(ghost_folders))
    for folder in ghost_folders:
        image_cache_manager.remove_entries_from_folder(folder)


def _restore_cached_files(
    unchanged_folders: Set[str],
    image_cache_manager: 'ScannedImageCacheManager',
    progress_queue: Optional[Queue] = None,
) -> List[str]:
    if not unchanged_folders:
        return []
    log_info(f"開始從 {len(unchanged_folders)} 個未變更資料夾恢復圖片快取...")
    missing_folder_path_count = image_cache_manager.count_missing_folder_paths()
    if missing_folder_path_count:
        log_info(f"[快取索引] 偵測到 {missing_folder_path_count} 筆舊資料缺少 folder_path，先執行一次性回填...")
        image_cache_manager.backfill_folder_paths(progress_queue=progress_queue)
    return image_cache_manager.iter_paths_for_folders(unchanged_folders, progress_queue=progress_queue)


def _apply_root_folder_protection(root_folder: str, folders_to_scan_content: Set[str], image_exts: tuple[str, ...]) -> None:
    root_norm = _norm_key(root_folder)
    if root_norm not in folders_to_scan_content:
        return

    has_loose_images = False
    try:
        with os.scandir(root_folder) as it:
            for entry in it:
                if entry.is_file() and entry.name.lower().endswith(image_exts):
                    has_loose_images = True
                    break
    except OSError:
        pass

    if not has_loose_images:
        log_info("[保護] 根資料夾變更，但未偵測到鬆散圖片，跳過根目錄本身的內容掃描。")
        folders_to_scan_content.discard(root_norm)
    else:
        log_info("[智慧掃描] 根資料夾變更且包含圖片，執行掃描。")


def _expand_empty_cache_fallback(
    root_folder: str,
    image_cache_manager: 'ScannedImageCacheManager',
    unchanged_folders: Set[str],
    folders_to_scan_content: Set[str],
) -> None:
    if not image_cache_manager._is_db_empty() or not unchanged_folders:
        return

    folders_needing_scan_due_to_empty_cache = unchanged_folders.copy()
    folders_needing_scan_due_to_empty_cache.discard(_norm_key(root_folder))
    if not folders_needing_scan_due_to_empty_cache:
        return

    pruned = _prune_to_leaf_only(folders_needing_scan_due_to_empty_cache)
    log_info(f"[保底] {len(pruned)} 個未變更資料夾因圖片快取缺失加入掃描。")
    folders_to_scan_content.update(pruned)


def _log_extraction_plan(live_folders: Dict[str, Any], folders_to_scan_content: Set[str]) -> None:
    if folders_to_scan_content:
        log_info(f"開始萃取 {len(folders_to_scan_content)} 個資料夾的檔案清單...")
    elif live_folders:
        log_info("本輪沒有需要重掃的資料夾，準備從圖片快取恢復既有檔案清單...")


def _extract_files_from_folders(
    folders_to_scan_content: Set[str],
    live_folders: Dict[str, Dict[str, float]],
    everything_files_by_dir,
    new_folders: Set[str],
    enable_archive_scan: bool,
    supported_archive_exts: tuple[str, ...],
    image_exts: tuple[str, ...],
    time_filter: dict,
    time_mode: str,
    limit_enabled: bool,
    target_count: int,
    first_scan_extract: int,
    required_count: int,
    folder_cache: 'FolderStateCacheManager',
    progress_queue: Optional[Queue] = None,
    control_events: Optional[Dict] = None,
    changed_container_cap: int = 0,
    container_empty_mark: bool = True,
) -> tuple[List[str], Dict[str, int]]:
    scanned_files = []
    vpath_size_map = {}
    folders_to_scan_list = sorted(list(folders_to_scan_content))
    total_folders_to_scan = len(folders_to_scan_list)

    for idx, folder in enumerate(folders_to_scan_list):
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
            break

        if progress_queue and idx % 200 == 0:
            progress_queue.put({'type': 'status_update', 'text': f"📦 萃取檔案清單中... ({idx}/{total_folders_to_scan})"})

        before_len = len(scanned_files)
        temp_files_in_container = defaultdict(list)
        norm_folder = _norm_key(folder)

        if everything_files_by_dir and norm_folder in everything_files_by_dir:
            for p in everything_files_by_dir[norm_folder]:
                p_lower = p.lower()
                if enable_archive_scan and p_lower.endswith(supported_archive_exts):
                    temp_files_in_container[p] = []
                elif p_lower.endswith(image_exts):
                    temp_files_in_container[os.path.dirname(p)].append(_norm_key(p))
        else:
            try:
                for entry in os.scandir(folder):
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            continue
                        if time_filter['enabled'] and (time_filter.get('start') or time_filter.get('end')):
                            st = entry.stat(follow_symlinks=False)
                            ts = st.st_ctime if time_mode == 'ctime' else st.st_mtime
                            dt = datetime.datetime.fromtimestamp(ts)
                            if time_filter['start'] and dt < time_filter['start']:
                                continue
                            if time_filter['end'] and dt > time_filter['end']:
                                continue

                        f_lower = entry.name.lower()
                        if enable_archive_scan and f_lower.endswith(supported_archive_exts):
                            temp_files_in_container[entry.path] = []
                        elif f_lower.endswith(image_exts):
                            temp_files_in_container[os.path.dirname(entry.path)].append(_norm_key(entry.path))
                    except OSError:
                        continue
            except OSError:
                pass

        if changed_container_cap > 0 and len(temp_files_in_container) > changed_container_cap:
            pass

        for container_path, files in temp_files_in_container.items():
            container_ext = os.path.splitext(container_path)[1].lower()
            if container_ext in supported_archive_exts:
                deferred_vpath = f"{VPATH_PREFIX}{container_path}{VPATH_SEPARATOR}__DEFERRED_SCAN__"
                scanned_files.append(deferred_vpath)
                continue

            files.sort(key=_natural_sort_key)
            container_dir = _norm_key(os.path.dirname(container_path))
            is_new = container_dir in new_folders
            current_limit = max(target_count, first_scan_extract if is_new else 0)

            if limit_enabled:
                scanned_files.extend(files[-current_limit:])
            else:
                scanned_files.extend(files)

        if norm_folder in live_folders:
            added_for_this_folder = len(scanned_files) - before_len
            is_empty = added_for_this_folder == 0
            extra_state = {'scanned_count': required_count}
            if container_empty_mark:
                extra_state['is_empty'] = is_empty
            folder_cache.update_folder_state(
                norm_folder,
                live_folders[norm_folder]['mtime'],
                live_folders[norm_folder]['ctime'],
                extra=extra_state
            )

    return scanned_files, vpath_size_map


def _finalize_scan_results(
    scanned_files: List[str],
    cached_files: List[str],
    folder_cache: 'FolderStateCacheManager',
    image_cache_manager: 'ScannedImageCacheManager',
    quarantine_list: Optional[Set[str]] = None,
) -> List[str]:
    final_file_list = scanned_files + cached_files
    folder_cache.save_cache()
    image_cache_manager.save_cache()

    unique_files = sorted(list(set(final_file_list)))
    if quarantine_list:
        unique_files = [f for f in unique_files if _norm_key(f) not in quarantine_list]
    return unique_files

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
    """
    Legacy compatibility wrapper.

    The maintained implementation lives in processors.qr_engine so the worker
    contract, metadata shape, and hash options stay consistent across callers.
    """
    from .qr_engine import _pool_worker_process_image_phash_only as qr_worker

    if isinstance(payload, tuple):
        return qr_worker(*payload)
    return qr_worker(payload)


# === SQLite 快取基類 ===
class SQLiteCacheBase:
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self._pending_lock = threading.RLock()
        self._pending_updates = {}
        self._known_columns = set()
        self.flush_threshold = 1000
        self.conn = self._init_db()

    def _init_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                path TEXT PRIMARY KEY,
                folder_path TEXT,
                data TEXT,
                phash_32 TEXT,
                phash_128 BLOB,
                phash_512 BLOB,
                mtime REAL
            )
        """)
        self._ensure_columns(conn)
        conn.commit()
        self._refresh_known_columns(conn)
        return conn

    def _refresh_known_columns(self, conn: Optional[sqlite3.Connection] = None) -> set:
        target_conn = conn or self.conn
        try:
            cursor = target_conn.execute(f"PRAGMA table_info({self.table_name})")
            self._known_columns = {row[1] for row in cursor.fetchall()}
        except sqlite3.Error:
            self._known_columns = set()
        return self._known_columns

    def _ensure_columns(self, conn: Optional[sqlite3.Connection] = None):
        target_conn = conn or self.conn
        try:
            cursor = target_conn.execute(f"PRAGMA table_info({self.table_name})")
            columns = {row[1]: (row[2] or "").upper() for row in cursor.fetchall()}
        except sqlite3.Error as e:
            log_error(f"SQLite schema inspect failed: {e}")
            return

        if columns.get("phash_32") == "INTEGER":
            log_info(f"[Schema] migrating {self.table_name}.phash_32 from INTEGER to TEXT")
            try:
                target_conn.execute(f"ALTER TABLE {self.table_name} RENAME COLUMN phash_32 TO phash_32_old")
                target_conn.execute(f"ALTER TABLE {self.table_name} ADD COLUMN phash_32 TEXT")
                target_conn.execute(f"UPDATE {self.table_name} SET phash_32 = CAST(phash_32_old AS TEXT)")
            except sqlite3.Error as e:
                log_error(f"SQLite phash_32 migration failed: {e}")

        for col, ctype in (("folder_path", "TEXT"), ("phash_32", "TEXT"), ("phash_128", "BLOB"), ("phash_512", "BLOB"), ("mtime", "REAL")):
            if col not in columns:
                try:
                    target_conn.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {col} {ctype}")
                except sqlite3.Error as e:
                    log_error(f"SQLite add column failed ({col}): {e}")
        try:
            target_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_folder_path ON {self.table_name}(folder_path)")
        except sqlite3.Error as e:
            log_error(f"SQLite add index failed ({self.table_name}.folder_path): {e}")

    def _row_count(self) -> int:
        try:
            return int(self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0])
        except sqlite3.Error:
            return 0

    def _is_db_empty(self) -> bool:
        return self._row_count() == 0

    def _migrate_from_json_file(
        self,
        json_path: str,
        *,
        payload_key: Optional[str] = None,
        normalize_keys: bool = True,
        log_prefix: str = "[遷移]",
    ) -> int:
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            log_error(f"{log_prefix} 讀取舊快取失敗: {e}", True)
            return 0

        payload = data.get(payload_key, data) if payload_key and isinstance(data, dict) else data
        if not isinstance(payload, dict) or not payload:
            return 0

        items = []
        for path, meta in payload.items():
            key = _norm_key(path) if normalize_keys else path
            items.append((key, json.dumps(meta)))

        try:
            with CACHE_LOCK:
                self.conn.executemany(
                    f"INSERT OR REPLACE INTO {self.table_name} (path, data) VALUES (?, ?)",
                    items,
                )
                self.conn.commit()
            try:
                os.rename(json_path, json_path + ".bak")
            except OSError:
                pass
            return len(items)
        except sqlite3.Error as e:
            log_error(f"{log_prefix} 寫入 SQLite 失敗: {e}", True)
            return 0

    def _serialize(self, data: dict) -> str:
        serializable = data.copy()
        if imagehash:
            for key in ["phash", "whash"]:
                if key in serializable and isinstance(serializable[key], imagehash.ImageHash):
                    serializable[key] = str(serializable[key])
        if 'avg_hsv' in serializable and isinstance(serializable['avg_hsv'], tuple):
            serializable['avg_hsv'] = list(serializable['avg_hsv'])
        for key, value in list(serializable.items()):
            if isinstance(value, bytes):
                serializable[key] = f"__hex__{value.hex()}"
        return json.dumps(serializable)

    def _deserialize(self, json_str: str) -> dict:
        try:
            data = json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return {}

        if imagehash:
            for key in ["phash", "whash"]:
                if key in data and data[key] and isinstance(data[key], str):
                    try:
                        data[key] = imagehash.hex_to_hash(data[key])
                    except ValueError:
                        data[key] = None
        if 'avg_hsv' in data and isinstance(data['avg_hsv'], list):
            try:
                data['avg_hsv'] = tuple(float(x) for x in data['avg_hsv'])
            except ValueError:
                pass
        for key, value in list(data.items()):
            if isinstance(value, str) and value.startswith("__hex__"):
                try:
                    data[key] = bytes.fromhex(value[7:])
                except ValueError:
                    pass
        return data

    @staticmethod
    def _coerce_blob(value, minimum_len: int) -> Optional[bytes]:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, str):
            text = value[7:] if value.startswith("__hex__") else value.strip()
            try:
                return bytes.fromhex(text)
            except ValueError:
                return text.encode("utf-8", errors="ignore")
        if isinstance(value, int):
            if value < 0:
                return None
            size = max(minimum_len, 1, (value.bit_length() + 7) // 8)
            try:
                return value.to_bytes(size, byteorder="big", signed=False)
            except OverflowError:
                return None
        return None

    @staticmethod
    def _coerce_mtime(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0

    def _select_row(self, key: str) -> Union[tuple, None]:
        columns = self._known_columns or self._refresh_known_columns()
        select_cols = ["data"]
        for col in ("phash_32", "phash_128", "phash_512", "mtime"):
            if col in columns:
                select_cols.append(col)
        try:
            cursor = self.conn.execute(f"SELECT {', '.join(select_cols)} FROM {self.table_name} WHERE path=?", (key,))
            row = cursor.fetchone()
        except sqlite3.Error as e:
            log_error(f"SQLite read failed: {e}")
            return None
        if not row:
            return None
        row_map = dict(zip(select_cols, row))
        return (row_map.get("data"), row_map.get("phash_32"), row_map.get("phash_128"), row_map.get("phash_512"), row_map.get("mtime"))

    def get_data(self, path: str) -> Union[dict, None]:
        key = _norm_key(path)
        with self._pending_lock:
            if key in self._pending_updates:
                return self._pending_updates[key]
            return self.get_data_inner(key)

    def get_data_inner(self, key: str) -> Union[dict, None]:
        row = self._select_row(key)
        if not row:
            return None
        base_data = self._deserialize(row[0])
        if row[1] is not None:
            base_data["phash_32"] = str(row[1])
        if row[2] is not None:
            base_data["phash_128"] = self._coerce_blob(row[2], 32)
        if row[3] is not None:
            base_data["phash_512"] = self._coerce_blob(row[3], 128)
        if row[4] is not None:
            base_data["mtime"] = self._coerce_mtime(row[4])
        return base_data

    def update_data(self, path: str, data: dict):
        if not data or "error" in data:
            return
        key = _norm_key(path)
        with self._pending_lock:
            current = self.get_data_inner(key) or {}
            current.update(data)
            self._pending_updates[key] = current
            if len(self._pending_updates) >= self.flush_threshold:
                self.save_cache_inner()

    def save_cache(self):
        with self._pending_lock:
            self.save_cache_inner()

    def save_cache_inner(self):
        with self._pending_lock:
            if not self._pending_updates:
                return
            pending_snapshot = self._pending_updates
            self._pending_updates = {}

        try:
            self._ensure_columns()
            self._refresh_known_columns()
            sql = f"INSERT OR REPLACE INTO {self.table_name} (path, folder_path, data, phash_32, phash_128, phash_512, mtime) VALUES (?, ?, ?, ?, ?, ?, ?)"
            items = []
            for key, value in pending_snapshot.items():
                p32 = value.get("phash_32")
                if p32 is not None and not isinstance(p32, str):
                    p32 = str(p32)
                items.append((
                    key,
                    _cache_folder_key(key),
                    self._serialize(value),
                    p32,
                    self._coerce_blob(value.get("phash_128"), 32),
                    self._coerce_blob(value.get("phash_512"), 128),
                    self._coerce_mtime(value.get("mtime", 0)),
                ))
            self.conn.executemany(sql, items)
            self.conn.commit()
        except (sqlite3.Error, OverflowError) as e:
            with self._pending_lock:
                pending_snapshot.update(self._pending_updates)
                self._pending_updates = pending_snapshot
            log_error(f"SQLite write failed: {e}")

    def remove_data(self, path: str) -> bool:
        key = _norm_key(path)
        with self._pending_lock:
            if key in self._pending_updates:
                del self._pending_updates[key]
            try:
                self.conn.execute(f"DELETE FROM {self.table_name} WHERE path=?", (key,))
                self.conn.commit()
                return True
            except sqlite3.Error:
                return False

    def remove_prefix(self, prefix: str):
        prefix_norm = _norm_key(prefix)
        with self._pending_lock:
            keys_to_del = [key for key in self._pending_updates if key.startswith(prefix_norm)]
            for key in keys_to_del:
                del self._pending_updates[key]
            try:
                self.conn.execute(f"DELETE FROM {self.table_name} WHERE path LIKE ?", (prefix_norm + "%",))
                self.conn.commit()
            except sqlite3.Error as e:
                log_error(f"SQLite remove_prefix failed: {e}")

    def close(self):
        self.save_cache()
        self.conn.close()

    def invalidate_cache(self) -> None:
        if send2trash is None:
            return
        self.close()
        if os.path.exists(self.db_path):
            try:
                send2trash.send2trash(self.db_path)
            except Exception:
                pass
        self.conn = self._init_db()


class MasterAdCacheManager(SQLiteCacheBase):
    """廣告庫快取。與圖片快取共用同一份 schema，並額外維護廣告 LSH 索引。"""

    def __init__(self, ad_folder_path: str):
        from config import DATA_DIR, CACHE_DIR
        import shutil

        self.ad_folder_path = ad_folder_path
        sanitized_root = _sanitize_path_for_filename(ad_folder_path)
        base_filename = f"scanned_hashes_cache_{sanitized_root}.db"
        
        # 相容層：檢查舊路徑是否存在，若存在則搬移至新 CACHE_DIR
        old_db_path = os.path.join(DATA_DIR, base_filename)
        db_path = os.path.join(CACHE_DIR, base_filename)
        
        if not os.path.exists(db_path) and os.path.exists(old_db_path):
            try:
                shutil.move(old_db_path, db_path)
                log_info(f"[遷移] 已將廣告庫資料庫從 data/ 搬移至 caches/")
            except Exception:
                db_path = old_db_path

        self._legacy_db_path = os.path.join(DATA_DIR, "ad_master_v17.db")
        super().__init__(db_path, "images")
        self.flush_threshold = 100
        self._ensure_aux_tables()
        if (not os.path.exists(db_path) or self._is_db_empty()) and os.path.exists(self._legacy_db_path):
            self._migrate_from_legacy_ad_db()
        log_info(f"[Hybrid Storage] Ad Master DB Ready: {db_path}")

    def _ensure_aux_tables(self):
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ad_hash_index (
                    hash_kind TEXT NOT NULL,
                    band INTEGER NOT NULL,
                    bucket INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    variant TEXT NOT NULL DEFAULT 'base',
                    PRIMARY KEY (hash_kind, band, bucket, path, variant)
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ad_hash_index_lookup
                ON ad_hash_index (hash_kind, band, bucket)
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ad_index_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            self.conn.commit()
        except sqlite3.Error as e:
            log_error(f"Ad index schema ensure failed: {e}")

    def _migrate_from_legacy_ad_db(self):
        if os.path.abspath(self._legacy_db_path) == os.path.abspath(self.db_path):
            return
        try:
            legacy_conn = sqlite3.connect(self._legacy_db_path)
        except sqlite3.Error as e:
            log_error(f"[遷移][廣告快取] 無法開啟舊 ad_master DB: {e}")
            return

        try:
            row = legacy_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ad_master', 'images') ORDER BY CASE name WHEN 'ad_master' THEN 0 ELSE 1 END LIMIT 1"
            ).fetchone()
            if not row:
                return
            source_table = row[0]
            cursor = legacy_conn.execute(f"PRAGMA table_info({source_table})")
            columns = {info[1] for info in cursor.fetchall()}
            select_cols = ["path"]
            for col in ("data", "phash_32", "phash_128", "phash_512", "mtime"):
                if col in columns:
                    select_cols.append(col)
            query = f"SELECT {', '.join(select_cols)} FROM {source_table}"
            items = []
            for row in legacy_conn.execute(query):
                row_map = dict(zip(select_cols, row))
                path = row_map.get("path")
                if not path:
                    continue
                key = _norm_key(path)
                items.append((
                    key,
                    _cache_folder_key(key),
                    row_map.get("data"),
                    str(row_map["phash_32"]) if row_map.get("phash_32") is not None else None,
                    self._coerce_blob(row_map.get("phash_128"), 32),
                    self._coerce_blob(row_map.get("phash_512"), 128),
                    self._coerce_mtime(row_map.get("mtime", 0)),
                ))
            if items:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO images (path, folder_path, data, phash_32, phash_128, phash_512, mtime) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    items,
                )
                self.conn.commit()
                log_info(f"[遷移][廣告快取] 已從 ad_master_v17.db 匯入 {len(items)} 筆資料。")
        except sqlite3.Error as e:
            log_error(f"[遷移][廣告快取] 舊 ad_master 匯入失敗: {e}")
        finally:
            legacy_conn.close()

    def index_is_current(self, digest: str, index_version: str = AD_INDEX_VERSION) -> bool:
        if not digest:
            return False
        try:
            rows = dict(self.conn.execute("SELECT key, value FROM ad_index_meta").fetchall())
            return rows.get("catalog_digest") == digest and rows.get("index_version") == index_version
        except sqlite3.Error:
            return False

    def rebuild_hash_index(
        self,
        ad_data: dict,
        *,
        digest: str,
        hash_kind: str = AD_INDEX_HASH_KIND,
        bands: int = AD_INDEX_BANDS,
        index_version: str = AD_INDEX_VERSION,
    ) -> None:
        self._ensure_aux_tables()
        rows = []
        for path, data in ad_data.items():
            if not data:
                continue
            variants = [("base", data.get("phash"))]
            rotations = data.get("phash_rotations", {}) or {}
            for rot_key in ("90", "180", "270"):
                if rotations.get(rot_key):
                    variants.append((f"rot{rot_key}", rotations.get(rot_key)))
            for variant, phash_obj in variants:
                buckets = _compute_lsh_buckets_from_hash_obj(phash_obj, bands=bands, bits=AD_INDEX_BITS)
                for band, bucket in enumerate(buckets):
                    rows.append((hash_kind, band, bucket, _norm_key(path), variant))

        try:
            self.conn.execute("DELETE FROM ad_hash_index")
            self.conn.execute("DELETE FROM ad_index_meta")
            if rows:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO ad_hash_index (hash_kind, band, bucket, path, variant) VALUES (?, ?, ?, ?, ?)",
                    rows,
                )
            self.conn.executemany(
                "INSERT OR REPLACE INTO ad_index_meta (key, value) VALUES (?, ?)",
                [
                    ("catalog_digest", digest or ""),
                    ("index_version", index_version),
                    ("row_count", str(len(rows))),
                ],
            )
            self.conn.commit()
            log_info(f"[AdIndex] rebuilt: {len(rows)} rows, version={index_version}")
        except sqlite3.Error as e:
            log_error(f"[AdIndex] rebuild failed: {e}")

    def query_hash_index(
        self,
        phash_obj,
        *,
        hash_kind: str = AD_INDEX_HASH_KIND,
        bands: int = AD_INDEX_BANDS,
    ) -> Set[str]:
        buckets = _compute_lsh_buckets_from_hash_obj(phash_obj, bands=bands, bits=AD_INDEX_BITS)
        if not buckets:
            return set()
        paths = set()
        try:
            for band, bucket in enumerate(buckets):
                cursor = self.conn.execute(
                    "SELECT path FROM ad_hash_index WHERE hash_kind=? AND band=? AND bucket=?",
                    (hash_kind, band, bucket),
                )
                paths.update(_norm_key(row[0]) for row in cursor.fetchall() if row and row[0])
        except sqlite3.Error as e:
            log_error(f"[AdIndex] query failed: {e}")
        return paths

    def invalidate_cache(self) -> None:
        if send2trash is None:
            return
        log_info(f"[清理] 廣告快取資料庫已移至回收桶: {self.db_path}")
        super().invalidate_cache()
        self._ensure_aux_tables()

# === 具體快取管理類 (SQLite 版) ===

class ScannedImageCacheManager(SQLiteCacheBase):
    def __init__(self, root_scan_folder: str):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        base_name = f"scanned_hashes_cache_{sanitized_root}"
        from config import DATA_DIR, CACHE_DIR
        
        import shutil
        old_db_path = os.path.join(DATA_DIR, f"{base_name}.db")
        db_path = os.path.join(CACHE_DIR, f"{base_name}.db")
        json_path_legacy = os.path.join(DATA_DIR, f"{base_name}.json")
        
        if not os.path.exists(db_path) and os.path.exists(old_db_path):
            try:
                shutil.move(old_db_path, db_path)
                log_info(f"[遷移] 已將圖片快取 {base_name}.db 搬移至 caches/")
            except Exception:
                db_path = old_db_path
                
        super().__init__(db_path, "images")
        self.flush_threshold = DEFAULT_IMG_FLUSH_THRESHOLD
        self.cache_file_path = db_path

        if not os.path.exists(db_path) or self._is_db_empty():
            if os.path.exists(json_path_legacy):
                migrated = self._migrate_from_json_file(
                    json_path_legacy,
                    payload_key="images",
                    normalize_keys=True,
                    log_prefix="[遷移][圖片快取]",
                )
                if migrated:
                    log_info(f"[遷移][圖片快取] 成功遷移 {migrated} 筆資料。")
        
        log_info(f"[快取] SQLite 圖片快取已就緒: '{self.cache_file_path}'")

    def count_missing_folder_paths(self) -> int:
        try:
            return self.conn.execute("SELECT COUNT(*) FROM images WHERE folder_path IS NULL OR folder_path = ''").fetchone()[0]
        except sqlite3.Error:
            return 0

    def backfill_folder_paths(self, progress_queue: Optional[Queue] = None, commit_every: int = 5000) -> int:
        columns = self._known_columns or self._refresh_known_columns()
        if "folder_path" not in columns:
            return 0

        updated = 0
        pending = []
        try:
            cursor = self.conn.execute("SELECT path FROM images WHERE folder_path IS NULL OR folder_path = ''")
            for row in cursor:
                path = row[0]
                try:
                    folder_key = _cache_folder_key(path)
                except Exception:
                    continue
                if not folder_key:
                    continue
                pending.append((folder_key, path))
                if len(pending) >= commit_every:
                    self.conn.executemany("UPDATE images SET folder_path=? WHERE path=?", pending)
                    self.conn.commit()
                    updated += len(pending)
                    if progress_queue:
                        progress_queue.put({'type': 'status_update', 'text': f"🧱 回填快取資料夾索引中... ({updated} 筆)"})
                    log_info(f"[快取索引] 已回填 {updated} 筆 folder_path...")
                    pending.clear()

            if pending:
                self.conn.executemany("UPDATE images SET folder_path=? WHERE path=?", pending)
                self.conn.commit()
                updated += len(pending)
                if progress_queue:
                    progress_queue.put({'type': 'status_update', 'text': f"🧱 回填快取資料夾索引中... ({updated} 筆)"})
                log_info(f"[快取索引] 已回填 {updated} 筆 folder_path...")
        except sqlite3.Error as e:
            log_error(f"SQLite folder_path backfill failed: {e}")
        return updated

    def remove_entries_from_folder(self, folder_path: str) -> int:
        self.remove_prefix(folder_path)
        return 0

    def iter_paths_for_folders(self, folder_paths: Set[str], progress_queue: Optional[Queue] = None) -> List[str]:
        if not folder_paths:
            return []

        normalized_folder_set = {_norm_key(p) for p in folder_paths if p}
        normalized_folders = sorted(normalized_folder_set)
        restored_paths = []
        checked_rows = 0
        columns = self._known_columns or self._refresh_known_columns()
        has_folder_column = "folder_path" in columns
        missing_folder_paths = self.count_missing_folder_paths() if has_folder_column else -1

        try:
            if has_folder_column:
                for start in range(0, len(normalized_folders), RESTORE_FOLDER_BATCH_SIZE):
                    batch = normalized_folders[start:start + RESTORE_FOLDER_BATCH_SIZE]
                    placeholders = ",".join("?" for _ in batch)
                    cursor = self.conn.execute(
                        f"SELECT path FROM images WHERE folder_path IN ({placeholders})",
                        tuple(batch),
                    )
                    rows = cursor.fetchall()
                    checked_rows += len(rows)
                    restored_paths.extend(row[0] for row in rows)
                    if checked_rows and checked_rows % 5000 == 0:
                        if progress_queue:
                            progress_queue.put({'type': 'status_update', 'text': f"♻️ 恢復圖片快取中... ({checked_rows} 筆)"})
                        log_info(f"  [快取恢復] 已恢復 {checked_rows} 筆圖片快取...")
                if missing_folder_paths <= 0:
                    return restored_paths
                missing_cursor = self.conn.execute("SELECT path FROM images WHERE folder_path IS NULL OR folder_path = ''")
            else:
                missing_cursor = self.conn.execute("SELECT path FROM images")

            backfill_updates = {}
            for row in missing_cursor:
                path = row[0]
                checked_rows += 1
                if checked_rows % 5000 == 0:
                    if progress_queue:
                        progress_queue.put({'type': 'status_update', 'text': f"♻️ 恢復圖片快取中... ({checked_rows} 筆)"})
                    log_info(f"  [快取恢復] 已檢查 {checked_rows} 筆圖片快取...")
                try:
                    folder_key = _cache_folder_key(path)
                    if folder_key and folder_key in normalized_folder_set:
                        restored_paths.append(path)
                    if has_folder_column and folder_key:
                        backfill_updates[path] = folder_key
                except Exception:
                    continue

            if backfill_updates and has_folder_column:
                try:
                    self.conn.executemany(
                        "UPDATE images SET folder_path=? WHERE path=?",
                        [(folder, path) for path, folder in backfill_updates.items()],
                    )
                    self.conn.commit()
                except sqlite3.Error as e:
                    log_warning(f"[快取恢復] folder_path 回填失敗: {e}")
        except sqlite3.Error as e:
            log_error(f"SQLite restore lookup failed: {e}")

        return restored_paths

class FolderStateCacheManager(SQLiteCacheBase):
    def __init__(self, root_scan_folder: str):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        base_name = f"folder_state_cache_{sanitized_root}"
        from config import DATA_DIR, CACHE_DIR
        
        import shutil
        old_db_path = os.path.join(DATA_DIR, f"{base_name}.db")
        db_path = os.path.join(CACHE_DIR, f"{base_name}.db")
        json_path_legacy = os.path.join(DATA_DIR, f"{base_name}.json")
        
        if not os.path.exists(db_path) and os.path.exists(old_db_path):
            try:
                shutil.move(old_db_path, db_path)
                log_info(f"[遷移] 已將資料夾快取 {base_name}.db 搬移至 caches/")
            except Exception:
                db_path = old_db_path

        super().__init__(db_path, "folders")
        self.flush_threshold = DEFAULT_FOLDER_FLUSH_THRESHOLD
        self.cache_file_path = db_path

        if not os.path.exists(db_path) or self._is_db_empty():
            if os.path.exists(json_path_legacy):
                migrated = self._migrate_from_json_file(
                    json_path_legacy,
                    normalize_keys=True,
                    log_prefix="[遷移][資料夾快取]",
                )
                if migrated:
                    log_info(f"[遷移][資料夾快取] 成功遷移 {migrated} 筆資料。")

        log_info(f"[快取] SQLite 資料夾快取已就緒: '{self.cache_file_path}'")

    def get_folder_state(self, folder_path: str) -> Union[dict, None]:
        return self.get_data(folder_path)

    def update_folder_state(self, folder_path: str, mtime: float, ctime: Union[float, None], extra: Optional[Dict] = None):
        state = {'mtime': mtime, 'ctime': ctime}
        if extra:
            state.update(extra)
        self.update_data(folder_path, state)

    def remove_folders(self, folder_paths: list[str]):
        # [v3.0.1 Fix] 使用 executemany 避免 CACHE_LOCK 死結
        keys_to_del = [_norm_key(p) for p in folder_paths]
        with self._pending_lock:
            for k in keys_to_del:
                 if k in self._pending_updates:
                     del self._pending_updates[k]
        if not keys_to_del: return
        try:
            with CACHE_LOCK:
                items = [(k,) for k in keys_to_del]
                self.conn.executemany(f"DELETE FROM {self.table_name} WHERE path=?", items)
                self.conn.commit()
        except sqlite3.Error as e:
            log_error(f"SQLite 批量移除資料夾失敗: {e}")
            
    @property
    def cache(self) -> dict:
        self.save_cache()
        try:
            cursor = self.conn.execute("SELECT path, data FROM folders")
            return {row[0]: self._deserialize(row[1]) for row in cursor.fetchall()}
        except sqlite3.Error:
            return {}

# ======================================================================
# Section: 高效檔案列舉 (修正版：智慧根目錄保護 + 剪枝優化 + 完整清理)
# ======================================================================

def _unified_scan_traversal(root_folder: str, excluded_paths: set, excluded_names: set, time_filter: dict, folder_cache: 'FolderStateCacheManager', progress_queue: Optional[Queue], control_events: Optional[dict], use_pruning: bool, time_mode: str, required_count: int, use_everything: bool = False, everything_exts: list = None) -> Tuple[Dict[str, Any], Set[str], Set[str], Optional[Dict[str, List[str]]]]:
    
    def _scan_newest_first_recursive(path: str, stats: Dict[str, int], is_root: bool = False) -> Generator[Tuple[str, float, float], None, None]:
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return

        norm_path = _norm_key(path)
        base_name = os.path.basename(norm_path).lower()
        if any(norm_path == ex or norm_path.startswith(ex + os.sep) for ex in excluded_paths) or base_name in excluded_names:
            return

        try:
            stats['visited_dirs'] += 1
            if progress_queue and stats['visited_dirs'] % 500 == 0:
                progress_queue.put({'type': 'status_update', 'text': f"🔍 智慧搜索中... 已發現 {stats['visited_dirs']} 個資料夾"})
                
            st = os.stat(path)
            cur_ts = _folder_time(st, time_mode)
            mtime_dt = datetime.datetime.fromtimestamp(cur_ts)
            start, end = time_filter.get('start'), time_filter.get('end')

            if not is_root and start and mtime_dt < start:
                stats['pruned_by_start'] += 1
                return

            in_range = (not start or mtime_dt >= start) and (not end or mtime_dt <= end)
            if in_range:
                yield (path, st.st_mtime, st.st_ctime)
            elif end and mtime_dt > end and not is_root:
                stats['skipped_by_end'] += 1

            subdirs = []
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        try:
                            st_sub = entry.stat(follow_symlinks=False)
                            subdirs.append((entry.path, _folder_time(st_sub, time_mode)))
                        except OSError: continue
            
            subdirs.sort(key=lambda x: x[1], reverse=True)

            processed_count = 0
            for subdir_path, mt in subdirs:
                if start and datetime.datetime.fromtimestamp(mt) < start:
                    stats['pruned_by_start'] += (len(subdirs) - processed_count)
                    break
                processed_count += 1
                yield from _scan_newest_first_recursive(subdir_path, stats, is_root=False)
        except OSError:
            return

    target_folders_iter = []
    everything_files_by_dir = None
    scan_start_time = time.perf_counter()
    
    # --- [Everything SDK Integration] ---
    if use_everything and everything_exts:
        ev = EverythingIPCManager()
        if ev.is_everything_running():
            log_info(f"⚡ [Everything SDK] 正在使用 MFT 秒搜下達指令...")
            if progress_queue: progress_queue.put({'type': 'status_update', 'text': f"⚡ Everything SDK 引擎啟動中..."})
            
            # Everything will return ALL matching files. We need to collect their parents.
            all_files = ev.search(root_folder, everything_exts, list(excluded_paths), list(excluded_names),
                                 min_mtime=time_filter.get('start').timestamp() if time_filter.get('start') else None,
                                 max_mtime=time_filter.get('end').timestamp() if time_filter.get('end') else None,
                                 time_mode=time_mode)
            
            log_info(f"⚡ [Everything SDK] 瞬間發現 {len(all_files)} 個匹配檔案。")
            
            # Reconstruct folder list from files for compatibility with the rest of existing engine
            unique_dirs = set()
            everything_files_by_dir = defaultdict(list)
            for f in all_files:
                d = _norm_key(os.path.dirname(f))
                unique_dirs.add(d)
                everything_files_by_dir[d].append(f)
            
            log_info(f"⚡ [Everything SDK] 記憶體分組完成：{len(all_files)} 個檔案 → {len(unique_dirs)} 個唯一資料夾，正在讀取資料夾時間戳記...")
            if progress_queue: progress_queue.put({'type': 'status_update', 'text': f"⚡ SDK 分組完成，正在核對 {len(unique_dirs)} 個資料夾時間戳記..."})
            
            sorted_dirs = sorted(list(unique_dirs))
            for idx, d in enumerate(sorted_dirs):
                try:
                    st = os.stat(d)
                    target_folders_iter.append((d, st.st_mtime, st.st_ctime))
                except OSError: continue
                if idx > 0 and idx % 1000 == 0:
                    log_info(f"  [SDK 戳記] 資料夾時間核對中... {idx}/{len(sorted_dirs)}")
                    if progress_queue: progress_queue.put({'type': 'status_update', 'text': f"⚡ 核對資料夾時間戳記中... ({idx}/{len(sorted_dirs)})"})
            
            log_info(f"⚡ [Everything SDK] 戳記核對完成，共 {len(target_folders_iter)} 個有效資料夾進入快取比對。")
        else:
            log_info("Everything 服務未運行或 SDK 無法使用，退回標準掃描模式。")
            use_everything = False

    if not use_everything:
        if use_pruning and time_filter.get('enabled') and time_filter.get('start'):
            log_info("啟用時間篩選，使用智慧型遞迴剪枝 (DFS) 掃描...")
            stats = defaultdict(int)
            target_folders_iter = list(_scan_newest_first_recursive(root_folder, stats, is_root=True))
            log_info(f"DFS 掃描完成。訪問: {stats['visited_dirs']}")
        else:
            log_info("使用標準 BFS 掃描。")
            target_folders_iter = []
            try:
                 root_st = os.stat(root_folder)
                 target_folders_iter.append((root_folder, root_st.st_mtime, root_st.st_ctime))
            except OSError: pass
            
            queue = deque([root_folder])
            while queue:
                curr = queue.popleft()
                visited_count = len(target_folders_iter)
                if progress_queue and visited_count % 500 == 0:
                    progress_queue.put({'type': 'status_update', 'text': f"🔍 地毯搜索中... 已發現 {visited_count} 個資料夾"})
                    
                try:
                    with os.scandir(curr) as it:
                        for entry in it:
                            if entry.is_dir(follow_symlinks=False):
                                norm = _norm_key(entry.path)
                                base = os.path.basename(norm).lower()
                                if not (any(norm == ex or norm.startswith(ex + os.sep) for ex in excluded_paths) or base in excluded_names):
                                    queue.append(entry.path)
                                    try:
                                        st_entry = entry.stat(follow_symlinks=False)
                                        target_folders_iter.append((entry.path, st_entry.st_mtime, st_entry.st_ctime))
                                    except OSError: pass
                except OSError: pass

    live_folders, changed_or_new_folders = {}, set()
    cached_states = folder_cache.cache.copy()

    for path_data in target_folders_iter:
        path_str, mtime, ctime = path_data
        norm_path = _norm_key(path_str)
        cached_states.pop(norm_path, None)
        try:
            live_folders[norm_path] = {'mtime': mtime, 'ctime': ctime}
            
            cached_entry = folder_cache.get_folder_state(norm_path)
            
            is_changed = False
            if not cached_entry:
                is_changed = True
            else:
                cur_ts = ctime if time_mode == 'ctime' else (max(mtime, ctime) if time_mode == 'hybrid' else mtime)
                time_diff = abs(cur_ts - cached_entry.get('mtime', 0))
                if time_diff > 1e-6:
                    is_changed = True
                
                # [v3.1.x feature] 檢查數量要求 (例如設定從 8 張改為 10 張)
                cached_count = cached_entry.get('scanned_count', 0)
                if not is_changed and required_count > 0 and cached_count < required_count:
                    is_changed = True

            if is_changed:
                changed_or_new_folders.add(norm_path)
                
        except OSError: continue
            
    ghost_folders = set(cached_states.keys())
    
    scan_duration = time.perf_counter() - scan_start_time
    log_info(f"掃描判斷完成 (耗時 {scan_duration:.2f} 秒)。即時: {len(live_folders)}, 變更(含增量): {len(changed_or_new_folders)}, 幽靈: {len(ghost_folders)}")
    return live_folders, changed_or_new_folders, ghost_folders, everything_files_by_dir
    
def get_files_to_process(config_dict: Dict, 
                         image_cache_manager: 'ScannedImageCacheManager', 
                         progress_queue: Optional[Queue] = None, 
                         control_events: Optional[Dict] = None,
                         quarantine_list: Optional[Set[str]] = None) -> Tuple[List[str], Dict[str, int]]:
    root_folder = config_dict['root_scan_folder']
    if not os.path.isdir(root_folder): return [], {}
    scan_context = _build_scan_context(config_dict, root_folder)
    runtime_options = _resolve_scan_runtime_options(config_dict)
    enable_archive_scan = scan_context['enable_archive_scan']
    supported_archive_exts = scan_context['supported_archive_exts']
    image_exts = scan_context['image_exts']
    folder_cache = scan_context['folder_cache']
    time_filter = scan_context['time_filter']
    limit_enabled = scan_context['limit_enabled']
    target_count = scan_context['target_count']
    required_count = scan_context['required_count']
    first_scan_extract = scan_context['first_scan_extract']
    excluded_paths, excluded_names = _resolve_excluded_folder_rules(config_dict)
    
    live_folders, folders_to_scan_content, ghost_folders, everything_files_by_dir = _unified_scan_traversal(
        root_folder, excluded_paths, excluded_names, time_filter, folder_cache, 
        progress_queue, control_events, runtime_options['use_pruning'], runtime_options['time_mode'], 
        required_count,
        use_everything=runtime_options['use_everything_setting'],
        everything_exts=list(image_exts) + list(supported_archive_exts)
    )

    new_folders = {f for f in live_folders if folder_cache.get_folder_state(f) is None}
    _expand_new_folders_into_scan_set(new_folders, folders_to_scan_content, time_filter, runtime_options['time_mode'])

    _apply_root_folder_protection(root_folder, folders_to_scan_content, image_exts)
    _cleanup_ghost_folders(config_dict, ghost_folders, time_filter, folder_cache, image_cache_manager)
    _log_extraction_plan(live_folders, folders_to_scan_content)

    unchanged_folders = set(live_folders.keys()) - folders_to_scan_content
    _expand_empty_cache_fallback(root_folder, image_cache_manager, unchanged_folders, folders_to_scan_content)

    changed_container_cap = int(config_dict.get('changed_container_cap', 0) or 0)
    container_empty_mark = config_dict.get('container_empty_mark', True)
    scanned_files, vpath_size_map = _extract_files_from_folders(
        folders_to_scan_content,
        live_folders,
        everything_files_by_dir,
        new_folders,
        enable_archive_scan,
        supported_archive_exts,
        image_exts,
        time_filter,
        runtime_options['time_mode'],
        limit_enabled,
        target_count,
        first_scan_extract,
        required_count,
        folder_cache,
        progress_queue=progress_queue,
        control_events=control_events,
        changed_container_cap=changed_container_cap,
        container_empty_mark=container_empty_mark,
    )

    if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return [], {}
    
    cached_files = _restore_cached_files(unchanged_folders, image_cache_manager, progress_queue=progress_queue)
    unique_files = _finalize_scan_results(
        scanned_files,
        cached_files,
        folder_cache,
        image_cache_manager,
        quarantine_list=quarantine_list,
    )
        
    log_info(f"檔案提取完成。掃描 {len(scanned_files)} 筆, 恢復 {len(cached_files)} 筆。總計: {len(unique_files)}")
    return unique_files, vpath_size_map
