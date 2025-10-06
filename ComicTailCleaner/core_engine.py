# ======================================================================
# 檔案名稱：core_engine.py
# 模組目的：包含核心的比對引擎、檔案掃描與快取管理邏輯
# 版本：2.0.1 (最終穩定版：整合所有健壯性修正與性能優化)
# ======================================================================

import os
import re
import json
import time
import datetime
import sys
import platform
from collections import deque, defaultdict
from multiprocessing import Pool, cpu_count, Event, set_start_method
from queue import Queue
from typing import Union, Tuple, Dict, List, Set, Optional, Generator

# --- 第三方庫 ---
try:
    import imagehash
except ImportError:
    imagehash = None
try:
    import send2trash
except ImportError:
    send2trash = None
try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = None
    np = None

# --- 本地模組 ---
try:
    import archive_handler
    ARCHIVE_SUPPORT_ENABLED = True
except ImportError:
    archive_handler = None
    ARCHIVE_SUPPORT_ENABLED = False

import config
import utils
from utils import (log_info, log_error, log_performance, _is_virtual_path,
                   _parse_virtual_path, _open_image_from_any_path,
                   _get_file_stat, sim_from_hamming, hamming_from_sim,
                   _avg_hsv, _color_gate, CACHE_LOCK, _norm_key)

try:
    from utils import log_warning
except ImportError:
    def log_warning(msg: str): print(f"[WARN] {msg}")

from processors.scanner import ScannedImageCacheManager, FolderStateCacheManager
try:
    from processors.qr_engine import (_pool_worker_detect_qr_code, 
                                     _pool_worker_process_image_full, 
                                     _pool_worker_process_image_phash_only)
    QR_ENGINE_ENABLED = True
except ImportError:
    utils.log_warning("[警告] 無法從 processors.qr_engine 導入 QR worker，QR 相關功能將不可用。")
    _pool_worker_detect_qr_code = None
    _pool_worker_process_image_full = None
    _pool_worker_process_image_phash_only = None
    QR_ENGINE_ENABLED = False

# ======================================================================
# Section: 全局常量與輔助函式
# ======================================================================
HASH_BITS = 64
PHASH_FAST_THRESH   = 0.80
PHASH_STRICT_SKIP   = 0.93
WHASH_TIER_1        = 0.90
WHASH_TIER_2        = 0.92
WHASH_TIER_3        = 0.95
WHASH_TIER_4        = 0.98
AD_GROUPING_THRESHOLD = 0.95
LSH_BANDS = 4

def _natural_sort_key(s: str) -> list:
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', s)]

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
                    base_name = os.path.basename(norm_path)
                    
                    if any(norm_path == ex or norm_path.startswith(ex + os.sep) for ex in excluded_paths) or base_name in excluded_names:
                        continue

                    if entry.is_dir():
                        queue.append(entry.path)
                    elif entry.is_file():
                        yield entry
        except OSError:
            continue

# ======================================================================
# Section: 高效檔案列舉
# ======================================================================

def _unified_scan_traversal(root_folder: str, excluded_paths: set, excluded_names: set, time_filter: dict, folder_cache: 'FolderStateCacheManager', progress_queue: Optional[Queue], control_events: Optional[dict]) -> tuple[dict, set, set]:
    log_info("啟動 v2.0.1 統一掃描引擎...")
    live_folders, changed_or_new_folders = {}, set()
    queue = deque([root_folder])
    scanned_count = 0
    cached_states = folder_cache.cache.copy()
    root_norm_path = _norm_key(root_folder)

    while queue:
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
            return {}, set(), set()
        
        current_dir = queue.popleft()
        norm_current_dir = _norm_key(current_dir)
        
        if any(norm_current_dir == ex or norm_current_dir.startswith(ex + os.sep) for ex in excluded_paths) or os.path.basename(norm_current_dir) in excluded_names:
            continue
        
        try:
            stat_info = os.stat(current_dir)
            cached_states.pop(norm_current_dir, None)

            if norm_current_dir != root_norm_path and time_filter.get('enabled'):
                ctime_dt = datetime.datetime.fromtimestamp(stat_info.st_ctime)
                if (time_filter.get('start') and ctime_dt < time_filter['start']) or \
                   (time_filter.get('end') and ctime_dt > time_filter['end']):
                    continue
            
            scanned_count += 1
            if scanned_count % 100 == 0 and progress_queue:
                progress_queue.put({'type': 'text', 'text': f"📁 正在檢查資料夾結構... ({scanned_count})"})

            live_folders[norm_current_dir] = {'mtime': stat_info.st_mtime, 'ctime': stat_info.st_ctime}
            
            cached_entry = folder_cache.get_folder_state(norm_current_dir)
            if not cached_entry or abs(stat_info.st_mtime - cached_entry.get('mtime', 0)) > 1e-6:
                changed_or_new_folders.add(norm_current_dir)

            with os.scandir(current_dir) as it:
                for entry in it:
                    if entry.is_dir():
                        queue.append(entry.path)
        except OSError:
            continue
    
    ghost_folders = set(cached_states.keys())
    log_info(f"統一掃描完成。即時(且符合時間)的資料夾: {len(live_folders)}, 新/變更: {len(changed_or_new_folders)}, 真正的幽靈資料夾: {len(ghost_folders)}")
    return live_folders, changed_or_new_folders, ghost_folders

def get_files_to_process(config_dict: Dict, image_cache_manager: ScannedImageCacheManager, progress_queue: Optional[Queue] = None, control_events: Optional[Dict] = None) -> Tuple[List[str], Dict[str, int]]:
    root_folder = config_dict['root_scan_folder']
    if not os.path.isdir(root_folder): return [], {}
    
    enable_archive_scan = config_dict.get('enable_archive_scan', False) and ARCHIVE_SUPPORT_ENABLED
    
    fmts = []
    if enable_archive_scan:
        for e in archive_handler.get_supported_formats():
            e = e.lower().strip()
            if not e.startswith('.'):
                e = '.' + e
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

    live_folders, folders_to_scan_content, ghost_folders = _unified_scan_traversal(root_folder, excluded_paths, excluded_names, time_filter, folder_cache, progress_queue, control_events)

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

    if ghost_folders:
        folder_cache.remove_folders(list(ghost_folders))
        for folder in ghost_folders: image_cache_manager.remove_entries_from_folder(folder)

    unchanged_folders = set(live_folders.keys()) - folders_to_scan_content
    
    folders_in_cache = set()
    for p in image_cache_manager.cache.keys():
        try:
            if _is_virtual_path(p):
                arch_path, _ = _parse_virtual_path(p)
                if arch_path: folders_in_cache.add(_norm_key(os.path.dirname(arch_path)))
            else:
                folders_in_cache.add(_norm_key(os.path.dirname(p)))
        except Exception: continue
    
    augmented_cache_folders = set(folders_in_cache)
    for f in list(folders_in_cache):
        cur = f
        while True:
            parent = _norm_key(os.path.dirname(cur))
            if not parent or parent == cur: break
            if parent in augmented_cache_folders: break
            augmented_cache_folders.add(parent)
            cur = parent

    folders_needing_scan_due_to_empty_cache = unchanged_folders - augmented_cache_folders

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
    count, enable_limit = config_dict.get('extract_count', 8), config_dict.get('enable_extract_count_limit', True)
    
    qr_mode = config_dict.get('comparison_mode') == 'qr_detection'
    if qr_mode and enable_limit:
        count = config_dict.get('qr_pages_per_archive', 10)
    qr_global_cap = config_dict.get('qr_global_cap', 20000)

    scanned_files = []
    
    changed_container_cap = int(config_dict.get('changed_container_cap', 0) or 0)
    def _container_mtime(p: str) -> float:
        try: return os.path.getmtime(p)
        except OSError: return 0.0

    for folder in sorted(list(folders_to_scan_content)):
        if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): break
        
        temp_files_in_container = defaultdict(list)
        
        try:
            with os.scandir(folder) as it:
                for entry in it:
                    if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): break
                    if entry.is_file():
                        f_lower = entry.name.lower()
                        if enable_archive_scan and f_lower.endswith(supported_archive_exts):
                            temp_files_in_container[entry.path] = []
                        elif f_lower.endswith(image_exts):
                            temp_files_in_container[folder].append(_norm_key(entry.path))
            
            if changed_container_cap > 0:
                archive_keys = [k for k in temp_files_in_container.keys() if os.path.splitext(k)[1].lower() in supported_archive_exts]
                if len(archive_keys) > changed_container_cap:
                    keep_archives = sorted(archive_keys, key=_container_mtime, reverse=True)[:changed_container_cap]
                    dropped_count = len(archive_keys) - len(keep_archives)
                    temp_files_in_container = {
                        k: temp_files_in_container[k] for k in temp_files_in_container.keys()
                        if (os.path.splitext(k)[1].lower() not in supported_archive_exts) or (k in keep_archives)
                    }
                    log_info(f"[變更夾容器上限] {folder} 僅保留近期壓縮檔 {len(keep_archives)} 個，捨棄 {dropped_count} 個")

            for container_path, files in temp_files_in_container.items():
                ext = os.path.splitext(container_path)[1].lower()
                if ext in supported_archive_exts:
                    try:
                        all_vpaths = []
                        for arc_entry in archive_handler.iter_archive_images(container_path):
                            vpath = f"{config.VPATH_PREFIX}{arc_entry.archive_path}{config.VPATH_SEPARATOR}{arc_entry.inner_path}"
                            all_vpaths.append(vpath)
                        all_vpaths.sort(key=_natural_sort_key)
                        files.extend(all_vpaths)
                    except Exception as e:
                        log_error(f"讀取壓縮檔失敗: {container_path}: {e}", True)
                        continue
                files.sort(key=_natural_sort_key)
                if enable_limit:
                    scanned_files.extend(files[-count:])
                else:
                    scanned_files.extend(files)
        except OSError as e:
            log_error(f"掃描資料夾 '{folder}' 時發生錯誤: {e}"); continue

        norm_folder = _norm_key(folder)
        if norm_folder in live_folders: 
            folder_cache.update_folder_state(norm_folder, live_folders[norm_folder]['mtime'], live_folders[norm_folder]['ctime'])

    if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): return [], {}
    
    cached_files = []
    if unchanged_folders:
        by_container = defaultdict(list)
        for p, meta in image_cache_manager.cache.items():
            try:
                container_key = ""
                parent_dir = ""
                if _is_virtual_path(p):
                    archive_path, _ = _parse_virtual_path(p)
                    if archive_path:
                        container_key = archive_path
                        parent_dir = _norm_key(os.path.dirname(archive_path))
                else:
                    if _get_file_stat(p)[2] is None: continue
                    parent_dir = _norm_key(os.path.dirname(p))
                    container_key = parent_dir
                
                if parent_dir in unchanged_folders:
                    by_container[container_key].append(p)
            except Exception:
                continue
        
        for container, lst in by_container.items():
            lst.sort(key=_natural_sort_key)
            take = lst[-count:] if enable_limit else lst
            cached_files.extend(take)
            
    final_file_list = scanned_files + cached_files
    folder_cache.save_cache()
    unique_files = sorted(list(set(final_file_list)))
    
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
        kept = unique_files[:global_cap]
        log_warning(f"[全域上限] 提取 {len(unique_files)} → {global_cap}（依 mtime 篩選最新）")
        unique_files = kept
    elif qr_mode and not enable_limit and qr_global_cap > 0 and len(unique_files) > qr_global_cap:
        log_error(f"[防爆量] 提取總數 {len(unique_files)} 超過全域上限 {qr_global_cap}，將只處理最新 {qr_global_cap} 筆。")
        unique_files.sort(key=_path_mtime_for_cap, reverse=True)
        unique_files = unique_files[:qr_global_cap]
        
    log_info(f"檔案提取完成。從 {len(folders_to_scan_content)} 個新/變更夾掃描 {len(scanned_files)} 筆, 從 {len(unchanged_folders)} 個未變更夾恢復 {len(cached_files)} 筆。總計: {len(unique_files)}")
    return unique_files, {}

# ======================================================================
# Section: 核心比對引擎
# ======================================================================


class ImageComparisonEngine:
    def __init__(self, config_dict: dict, progress_queue: Union[Queue, None] = None, control_events: Union[dict, None] = None):
        self.config = config_dict; self.progress_queue = progress_queue; self.control_events = control_events
        self.system_qr_scan_capability = QR_ENGINE_ENABLED
        self.pool = None; self.file_data = {}; self.tasks_to_process = []
        self.total_task_count = 0; self.completed_task_count = 0; self.failed_tasks = []
        self.vpath_size_map = {}
        log_performance("[初始化] 掃描引擎實例")
        
    def _check_control(self) -> str:
        if self.control_events:
            if self.control_events.get('cancel') and self.control_events['cancel'].is_set(): return 'cancel'
            if self.control_events.get('pause') and self.control_events['pause'].is_set(): return 'pause'
        return 'continue'
        
    def _update_progress(self, p_type: str = 'text', value: Union[int, None] = None, text: Union[str, None] = None) -> None:
        if self.progress_queue: self.progress_queue.put({'type': p_type, 'value': value, 'text': text})
        
    def _cleanup_pool(self):
        if self.pool:
            log_info("正在終結現有進程池...");
            if self.progress_queue: self.progress_queue.put({'type': 'status_update', 'text': "正在終止背景任務..."})
            self.pool.terminate(); self.pool.join()
            log_info("進程池已成功終結。"); self.pool = None
            
    def find_duplicates(self) -> Union[tuple[list, dict, list], None]:
        try:
            self._update_progress(text="任務開始..."); log_performance("[開始] 掃描任務")
            root_scan_folder = self.config.get('root_scan_folder')
            ad_folder_path = self.config.get('ad_folder_path')
            scan_cache_manager = ScannedImageCacheManager(root_scan_folder, ad_folder_path, self.config.get('comparison_mode'))
            
            try:
                mode = str(self.config.get("comparison_mode", "mutual_comparison")).lower()
                mode_map = { "ad_comparison": "廣告比對", "mutual_comparison": "互相比對", "qr_detection": "QR Code 檢測" }
                mode_str = mode_map.get(mode, "未知")
                log_info("=" * 50)
                log_info(f"[引擎版本] 核心引擎 v1.9.8")
                log_info(f"[模式檢查] 當前模式: {mode_str}")
                log_info(f"[模式檢查] - 時間篩選: {'啓用' if self.config.get('enable_time_filter', False) else '關閉'}")
                enable_limit = bool(self.config.get('enable_extract_count_limit', False))
                lim_n = int(self.config.get('extract_count', 0))
                if mode == 'qr_detection' and enable_limit: lim_n = int(self.config.get('qr_pages_per_archive', 10))
                log_info(f"[模式檢查] - 提取數量限制: {'啓用 ('+str(lim_n)+'張)' if enable_limit else '關閉'}")
                log_info(f"[模式檢查] 實際使用的圖片快取: {scan_cache_manager.cache_file_path}")
                log_info("=" * 50)
            except Exception as e: log_error(f"[模式檢查] 模式橫幅日誌生成失敗: {e}")

            initial_files, self.vpath_size_map = get_files_to_process(self.config, scan_cache_manager, self.progress_queue, self.control_events)
            
            if self._check_control() == 'cancel': return None

            self.tasks_to_process = initial_files
            self.total_task_count = len(self.tasks_to_process)
            
            if not self.tasks_to_process:
                self.progress_queue.put({'type': 'text', 'text':"在指定路徑下未找到任何符合條件的圖片檔案。"})
                return [], {}, []
            
            mode = self.config.get('comparison_mode', 'mutual_comparison').lower()
            if mode == "qr_detection":
                if not QR_ENGINE_ENABLED:
                    log_error("QR 引擎不可用，無法執行 QR Code 檢測。")
                    return [], {}, [("系統錯誤", "QR 引擎未載入")]
                result = self._detect_qr_codes(scan_cache_manager)
            else:
                result = self._find_similar_images(scan_cache_manager)
                
            if result is None: return None
            found, data = result
            return found, data, self.failed_tasks
        finally:
            self._cleanup_pool()
    
    def _process_images_with_cache(self, current_task_list: list[str], cache_manager: ScannedImageCacheManager, description: str, worker_function: callable, data_key: str, progress_scope: str = 'global') -> tuple[bool, dict]:
        if not current_task_list: return True, {}
        local_file_data = {}
        time.sleep(self.config.get('ux_scan_start_delay', 0.1))
        self._update_progress(text=f"📂 正在檢查 {len(current_task_list)} 個{description}的快取...")
        
        local_total = len(current_task_list)
        local_completed = 0
        
        paths_to_recalc, cache_hits = [], 0
        folders_to_rescan = set()
        paths_to_purge = set()

        for path in list(current_task_list):
            cached_data = cache_manager.get_data(path)
            
            _, _, mt = _get_file_stat(path)
            
            if mt is None:
                if cached_data:
                    paths_to_purge.add(path)
                
                parent_folder = os.path.dirname(path if not _is_virtual_path(path) else _parse_virtual_path(path)[0])
                if os.path.isdir(parent_folder):
                    folders_to_rescan.add(parent_folder)
                
                if progress_scope == 'global':
                    self.total_task_count = max(0, self.total_task_count - 1)
                else:
                    local_total = max(0, local_total - 1)
                continue

            if cached_data:
                for hash_key in ['phash', 'whash']:
                    if hash_key in cached_data and cached_data[hash_key] and not isinstance(cached_data[hash_key], imagehash.ImageHash):
                        try: cached_data[hash_key] = imagehash.hex_to_hash(str(cached_data[hash_key]))
                        except (TypeError, ValueError): cached_data[hash_key] = None
            
            if cached_data and data_key in cached_data and cached_data.get(data_key) is not None and \
               abs(mt - float(cached_data.get('mtime', 0))) < 1e-6:
                local_file_data[path] = cached_data
                cache_hits += 1
                if progress_scope == 'global':
                    self.completed_task_count += 1
                else:
                    local_completed += 1
            else:
                paths_to_recalc.append(path)
                if cached_data: local_file_data[path] = cached_data

        if folders_to_rescan:
            log_info(f"檢測到 {len(folders_to_rescan)} 個資料夾快取失效，正在重新掃描...")
            self._update_progress(text=f"♻️ 偵測到快取失效，重新掃描 {len(folders_to_rescan)} 個資料夾...")
            
            count = self.config.get('extract_count', 8)
            enable_limit = self.config.get('enable_extract_count_limit', True)
            
            for folder in sorted(list(folders_to_rescan)):
                try:
                    container_map = defaultdict(list)
                    for f in os.listdir(folder):
                        full_path = os.path.join(folder, f)
                        f_lower = f.lower()

                        if self.config.get("enable_archive_scan", False) and \
                           os.path.isfile(full_path) and \
                           os.path.splitext(f_lower)[1] in ('.zip', '.cbz', '.rar', '.cbr', '.7z'):
                            try:
                                all_vpaths = []
                                for ent in archive_handler.iter_archive_images(full_path):
                                    vpath = f"{config.VPATH_PREFIX}{ent.archive_path}{config.VPATH_SEPARATOR}{ent.inner_path}"
                                    all_vpaths.append(vpath)
                                all_vpaths.sort(key=_natural_sort_key)
                                take = all_vpaths[-count:] if enable_limit else all_vpaths
                                container_map[full_path].extend(take)
                            except Exception as e:
                                log_error(f"保底展開壓縮檔失敗: {full_path}: {e}", True)

                        elif f_lower.endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')):
                            container_map[folder].append(_norm_key(full_path))

                    for container, lst in container_map.items():
                        lst.sort(key=_natural_sort_key)
                        take = lst[-count:] if enable_limit else lst
                        for new_path in take:
                            if new_path not in paths_to_recalc and new_path not in local_file_data:
                                paths_to_recalc.append(new_path)
                                if progress_scope == 'global':
                                    self.total_task_count += 1
                                else:
                                    local_total += 1
                                log_info(f"  -> 新增檔案進行哈希計算: {new_path}")
                except OSError as e:
                    log_error(f"重新掃描資料夾 '{folder}' 失敗: {e}")

        if progress_scope == 'global':
            if self.total_task_count > 0:
                log_info(f"快取檢查 - 命中: {cache_hits}/{self.total_task_count} | 總進度: {self.completed_task_count}/{self.total_task_count}")
        else:
            if local_total > 0:
                log_info(f"[局部] 快取檢查 - 命中: {cache_hits}/{local_total} | 進度: {local_completed}/{local_total}")
        
        if paths_to_purge:
            log_info(f"正在從快取中批次移除 {len(paths_to_purge)} 個無效條目...")
            for path in paths_to_purge:
                cache_manager.remove_data(path)
        
        if not paths_to_recalc:
            cache_manager.save_cache()
            return True, local_file_data
        
        user_proc_setting = self.config.get('worker_processes', 0)
        pool_size = max(1, min(user_proc_setting, cpu_count())) if user_proc_setting > 0 else max(1, min(cpu_count() // 2, 8))
        if not self.pool:
            if sys.platform.startswith('win'):
                try: set_start_method('spawn', force=True)
                except RuntimeError: pass
            self.pool = Pool(processes=pool_size)
        self._update_progress(text=f"⚙️ 使用 {pool_size} 進程計算 {len(paths_to_recalc)} 個新檔案...")
        async_results, path_map = [], {}
        worker_args = {}
        worker_name = worker_function.__name__
        if 'full' in worker_name or 'qr_code' in worker_name:
            worker_args['resize_size'] = self.config.get('qr_resize_size', 800)
        for path in paths_to_recalc:
            res = self.pool.apply_async(worker_function, args=(path,), kwds=worker_args)
            async_results.append(res)
            path_map[res] = path
        while async_results:
            if self._check_control() == 'cancel':
                self._cleanup_pool(); return False, {}
            remaining_results = []
            for res in async_results:
                if res.ready():
                    try:
                        path, data = res.get()
                        if data.get('error'): 
                            self.failed_tasks.append((path, data['error']))
                            if "不存在" in data['error']:
                                cache_manager.remove_data(path)
                        else:
                            local_file_data.setdefault(path, {}).update(data)
                            cache_manager.update_data(path, data)
                        
                        if progress_scope == 'global':
                            self.completed_task_count += 1
                        else:
                            local_completed += 1
                    except Exception as e:
                        path = path_map.get(res, "未知路徑")
                        error_msg = f"從子進程獲取結果失敗: {e}"
                        log_error(error_msg, True); self.failed_tasks.append((path, error_msg))
                        if progress_scope == 'global':
                            self.completed_task_count += 1
                        else:
                            local_completed += 1
                else: remaining_results.append(res)
            async_results = remaining_results

            if progress_scope == 'global':
                if self.total_task_count > 0:
                    current_progress = int(self.completed_task_count / self.total_task_count * 100)
                    self._update_progress(p_type='progress', value=current_progress, text=f"⚙️ 計算{description}中... ({self.completed_task_count}/{self.total_task_count})")
            else:
                if local_total > 0:
                    current_progress = int(local_completed / local_total * 100)
                    self._update_progress(p_type='progress', value=current_progress, text=f"⚙️ [局部] 計算{description}中... ({local_completed}/{local_total})")

            time.sleep(0.05)
        cache_manager.save_cache()
        return True, local_file_data

    # (後續所有函式 _build_phash_band_index, _find_similar_images, _detect_qr_codes 等都保持不變)

    def _build_phash_band_index(self, gallery_file_data: dict, bands=LSH_BANDS):
        seg_bits = HASH_BITS // bands
        mask = (1 << seg_bits) - 1
        index = [defaultdict(list) for _ in range(bands)]
        for path, ent in gallery_file_data.items():
            phash_obj = self._coerce_hash_obj(ent.get('phash'))
            if not phash_obj: continue
            try: v = int(str(phash_obj), 16)
            except (ValueError, TypeError): continue
            for b in range(bands):
                key = (v >> (b * seg_bits)) & mask
                index[b][key].append(_norm_key(path))
        return index

    def _lsh_candidates_for(self, ad_path: str, ad_hash_obj: imagehash.ImageHash, index: list, bands=LSH_BANDS):
        seg_bits = HASH_BITS // bands; mask = (1 << seg_bits) - 1
        v = int(str(ad_hash_obj), 16)
        cand = set()
        for b in range(bands):
            key = (v >> (b * seg_bits)) & mask
            cand.update(index[b].get(key, []))
        if _norm_key(ad_path) in cand: cand.remove(_norm_key(ad_path))
        return cand
        
    def _ensure_features(self, path: str, cache_mgr: ScannedImageCacheManager, need_hsv: bool = False, need_whash: bool = False) -> bool:
        norm_path = _norm_key(path)
        ent = self.file_data.get(norm_path)
        if not ent:
            ent = cache_mgr.get_data(norm_path) or {}
            self.file_data[norm_path] = ent
        
        if 'phash' in ent: ent['phash'] = self._coerce_hash_obj(ent['phash'])
        if 'whash' in ent: ent['whash'] = self._coerce_hash_obj(ent['whash'])
        if 'avg_hsv' in ent and isinstance(ent['avg_hsv'], list):
            try: ent['avg_hsv'] = tuple(float(x) for x in ent['avg_hsv'])
            except (ValueError, TypeError): ent['avg_hsv'] = None

        has_hsv = 'avg_hsv' in ent and ent['avg_hsv'] is not None
        has_whash = 'whash' in ent and ent['whash'] is not None
        
        if (not need_hsv or has_hsv) and (not need_whash or has_whash): return True

        img = None
        try:
            from PIL import Image, ImageOps
            img = _open_image_from_any_path(path)
            if not img: raise IOError("無法開啟圖片")
            
            img = ImageOps.exif_transpose(img)
            if need_hsv and not has_hsv: ent['avg_hsv'] = _avg_hsv(img)
            if need_whash and not has_whash and imagehash: ent['whash'] = imagehash.whash(img, hash_size=8, mode='haar', remove_max_haar_ll=True)
            
            _, _, mtime = _get_file_stat(path)
            update_payload = {'mtime': mtime}
            if 'avg_hsv' in ent and ent['avg_hsv'] is not None: update_payload['avg_hsv'] = list(ent['avg_hsv'])
            if 'whash' in ent and ent['whash'] is not None: update_payload['whash'] = str(ent['whash'])
            
            cache_mgr.update_data(norm_path, update_payload)
            return True
        except Exception as e:
            log_error(f"懶加載特徵失敗: {path}: {e}")
            return False
        finally:
            if img:
                try:
                    img.close()
                except Exception:
                    pass

    def _coerce_hash_obj(self, h):
        if h is None: return None
        if isinstance(h, imagehash.ImageHash): return h
        try: return imagehash.hex_to_hash(str(h))
        except (TypeError, ValueError): return None

    def _accept_pair_with_dual_hash(self, ad_hash_obj, g_hash_obj, ad_w_hash, g_w_hash) -> tuple[bool, float]:
        h1, h2 = self._coerce_hash_obj(ad_hash_obj), self._coerce_hash_obj(g_hash_obj)
        w1, w2 = self._coerce_hash_obj(ad_w_hash), self._coerce_hash_obj(g_w_hash)
        if not h1 or not h2: return False, 0.0
        
        sim_p = sim_from_hamming(h1 - h2, HASH_BITS)
        if sim_p < PHASH_FAST_THRESH: return False, sim_p
        if sim_p >= PHASH_STRICT_SKIP: return True, sim_p
        
        if not w1 or not w2: return False, sim_p
        
        sim_w = sim_from_hamming(w1 - w2, HASH_BITS)
        if sim_p >= 0.90:   ok = sim_w >= WHASH_TIER_1
        elif sim_p >= 0.88: ok = sim_w >= WHASH_TIER_2
        elif sim_p >= 0.85: ok = sim_w >= WHASH_TIER_3
        else:               ok = sim_w >= WHASH_TIER_4
        return (ok, min(sim_p, sim_w) if ok else sim_p)

    def _find_similar_images(self, scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        continue_processing, self.file_data = self._process_images_with_cache(self.tasks_to_process, scan_cache_manager, "目標雜湊", _pool_worker_process_image_phash_only, 'phash', progress_scope='global')
        if not continue_processing: return None
        
        gallery_data = self.file_data
        
        user_thresh_percent = self.config.get('similarity_threshold', 95.0)
        is_mutual_mode = self.config.get('comparison_mode') == 'mutual_comparison'
        ad_folder_path = self.config.get('ad_folder_path'); ad_phash_set = set()
        
        if is_mutual_mode and self.config.get('enable_ad_cross_comparison', True):
            if ad_folder_path and os.path.isdir(ad_folder_path):
                self._update_progress(text="📦 正在預處理廣告庫...（此階段為局部進度）")
                ad_paths = []
                for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
                    if ent.is_file() and ent.name.lower().endswith(('.png','.jpg','.jpeg','.webp')):
                        ad_paths.append(_norm_key(ent.path))

                ad_cache = ScannedImageCacheManager(ad_folder_path, ad_folder_path, 'ad_comparison')
                _, ad_data_for_marking = self._process_images_with_cache(ad_paths, ad_cache, "預載入廣告庫", _pool_worker_process_image_phash_only, 'phash', progress_scope='local')
                if ad_data_for_marking:
                    for data in ad_data_for_marking.values():
                        phash = self._coerce_hash_obj(data.get('phash'))
                        if phash: ad_phash_set.add(phash)
                    log_info(f"[交叉比對] 成功從檔案預載入 {len(ad_phash_set)} 個廣告哈希。")
            if self.config.get('cross_comparison_include_bw', False) and imagehash:
                log_info("[交叉比對] 已啟用對純黑/純白圖片的智慧標記。")
                ad_phash_set.add(imagehash.hex_to_hash('0000000000000000')); ad_phash_set.add(imagehash.hex_to_hash('ffffffffffffffff'))
                
                try:
                    ad_int_set = {int(str(h), 16) for h in ad_phash_set}
                except Exception:
                    ad_int_set = set()
                _ad_cross_sim = 0.98
                _ad_dmax = hamming_from_sim(_ad_cross_sim, HASH_BITS)
                _ONEBIT_MASKS = [1 << i for i in range(HASH_BITS)]
                
        def lerp(p, s, l): return s + ((100.0 - max(80.0, min(100.0, float(p)))) / 20.0) * (l - s)
        color_gate_params = { 'hue_deg_tol': lerp(user_thresh_percent, 18.0, 25.0), 'sat_tol': lerp(user_thresh_percent, 0.18, 0.25),
            'low_sat_value_tol': lerp(user_thresh_percent, 0.10, 0.15), 'low_sat_achroma_tol': lerp(user_thresh_percent, 0.12, 0.18),
            'low_sat_thresh': 0.18 }
        
        ad_data, ad_cache_manager, leader_to_ad_group = {}, None, {}
        is_ad_mode = self.config.get('comparison_mode') == 'ad_comparison'
        
        if is_ad_mode:
            self._update_progress(text="📦 正在預處理廣告庫...（此階段為局部進度）")
            ad_paths = []
            for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
                if ent.is_file() and ent.name.lower().endswith(('.png','.jpg','.jpeg','.webp')):
                    ad_paths.append(_norm_key(ent.path))
            ad_cache_manager = ScannedImageCacheManager(ad_folder_path, ad_folder_path, 'ad_comparison')
            continue_proc_ad, ad_data = self._process_images_with_cache(ad_paths, ad_cache_manager, "廣告圖片雜湊", _pool_worker_process_image_phash_only, 'phash', progress_scope='local')
            if not continue_proc_ad: return None
            self.file_data.update(ad_data)
            self._update_progress(text="🔍 正在使用 LSH 高效預處理廣告庫...")
            ad_lsh_index = self._build_phash_band_index(ad_data)
            ad_path_to_leader = {p: p for p in ad_data}; ad_paths_sorted = sorted(list(ad_data.keys()))
            grouping_dist = hamming_from_sim(AD_GROUPING_THRESHOLD, HASH_BITS)
            for p1 in ad_paths_sorted:
                if ad_path_to_leader[p1] != p1: continue
                h1 = self._coerce_hash_obj(ad_data.get(p1, {}).get('phash'))
                if not h1: continue
                for p2 in self._lsh_candidates_for(p1, h1, ad_lsh_index):
                    if p2 <= p1 or ad_path_to_leader[p2] != p2: continue
                    h2 = self._coerce_hash_obj(ad_data.get(p2, {}).get('phash'))
                    if h2 and (h1 - h2) <= grouping_dist: ad_path_to_leader[p2] = ad_path_to_leader[p1]
            for path, leader in ad_path_to_leader.items(): leader_to_ad_group.setdefault(leader, []).append(path)
            ad_data_representatives = {p: d for p, d in ad_data.items() if p in leader_to_ad_group}
            self._update_progress(text=f"🔍 廣告庫預處理完成，找到 {len(ad_data_representatives)} 個獨立廣告組。")
        else: 
            ad_data_representatives = gallery_data.copy()
            
        phash_index = self._build_phash_band_index(gallery_data)
        temp_found_pairs, user_thresh = [], user_thresh_percent / 100.0
        inter_folder_only = self.config.get('enable_inter_folder_only', False) and is_mutual_mode
        stats = {'comparisons': 0, 'passed_phash': 0, 'passed_color': 0, 'entered_whash': 0, 'filtered_inter': 0}
        
        for i, (p1_path, p1_ent) in enumerate(ad_data_representatives.items()):
            if self._check_control() != 'continue': return None
            p1_p_hash = self._coerce_hash_obj(p1_ent.get('phash'))
            if not p1_p_hash: continue
            
            for p2_path in self._lsh_candidates_for(p1_path, p1_p_hash, phash_index):
                if is_mutual_mode:
                    if p2_path <= p1_path: continue
                    if inter_folder_only:
                        p1_parent_base = p1_path if not _is_virtual_path(p1_path) else _parse_virtual_path(p1_path)[0]
                        p2_parent_base = p2_path if not _is_virtual_path(p2_path) else _parse_virtual_path(p2_path)[0]
                        if os.path.dirname(p1_parent_base) == os.path.dirname(p2_parent_base): 
                            stats['filtered_inter'] += 1; continue
                if is_ad_mode and p2_path in ad_data: continue
                
                is_match, best_sim = False, 0.0
                ad_group = leader_to_ad_group.get(p1_path, [p1_path])
                
                for member_path in ad_group:
                    stats['comparisons'] += 1
                    
                    h1_raw = (gallery_data if is_mutual_mode else ad_data).get(member_path, {}).get('phash')
                    h2_raw = gallery_data.get(p2_path, {}).get('phash')
                    
                    h1 = self._coerce_hash_obj(h1_raw)
                    h2 = self._coerce_hash_obj(h2_raw)
                    if not h1 or not h2: continue
                    
                    sim_p = sim_from_hamming(h1 - h2, HASH_BITS)
                    if sim_p < PHASH_FAST_THRESH: continue
                    stats['passed_phash'] += 1
                    
                    if self.config.get('enable_color_filter', True):
                        cache1 = scan_cache_manager if is_mutual_mode else ad_cache_manager
                        if not self._ensure_features(member_path, cache1, need_hsv=True) or \
                           not self._ensure_features(p2_path, scan_cache_manager, need_hsv=True): continue
                        
                        hsv1 = self.file_data[_norm_key(member_path)].get('avg_hsv')
                        hsv2 = self.file_data[_norm_key(p2_path)].get('avg_hsv')
                        if not _color_gate(hsv1, hsv2, **color_gate_params): continue
                    
                    stats['passed_color'] += 1

                    accepted, final_sim = True, sim_p
                    if sim_p < PHASH_STRICT_SKIP:
                        stats['entered_whash'] += 1
                        cache1 = scan_cache_manager if is_mutual_mode else ad_cache_manager
                        if not self._ensure_features(member_path, cache1, need_whash=True) or \
                           not self._ensure_features(p2_path, scan_cache_manager, need_whash=True): continue
                        
                        w1 = self.file_data[_norm_key(member_path)].get('whash')
                        w2 = self.file_data[_norm_key(p2_path)].get('whash')
                        accepted, final_sim = self._accept_pair_with_dual_hash(h1, h2, w1, w2)
                    
                    if accepted and final_sim >= user_thresh:
                        is_match, best_sim = True, max(best_sim, final_sim)

                if is_match:
                    temp_found_pairs.append((p1_path, p2_path, f"{best_sim * 100:.1f}%"))
        
        found_items = []
        if is_mutual_mode:
            self._update_progress(text="🔄 正在合併相似羣組...")
            path_to_leader = {}
            sorted_pairs = sorted([(_norm_key(p1), _norm_key(p2), sim) for p1, p2, sim in temp_found_pairs], key=lambda x: (min(x[0], x[1]), max(x[0], x[1])))
            for p1, p2, _ in sorted_pairs:
                l1, l2 = path_to_leader.get(p1, p1), path_to_leader.get(p2, p2)
                if l1 != l2:
                    final_l = min(l1, l2)
                    path_to_leader[l1] = path_to_leader[l2] = final_l
                    path_to_leader[p1] = path_to_leader[p2] = final_l
            final_groups = defaultdict(list)
            all_paths_in_pairs = {_norm_key(p) for pair in temp_found_pairs for p in pair[:2]}
            for path in all_paths_in_pairs:
                leader = path
                while leader in path_to_leader and path_to_leader[leader] != leader: leader = path_to_leader[leader]
                final_groups[leader].append(path)
            
            if ad_phash_set:
                self._update_progress(text="🔄 正在與廣告庫進行交叉比對...")
                if '_ad_dmax' in locals() and '_ONEBIT_MASKS' in locals() and '_ad_int_set' not in locals():
                    ad_int_set = locals().get('ad_int_set', set())
                if '_ad_dmax' in locals() and _ad_dmax <= 1 and 'ad_int_set' in locals() and ad_int_set:
                    for leader, children in final_groups.items():
                        is_ad_like = False
                        for p in [leader] + children:
                            h_obj = self._coerce_hash_obj(self.file_data.get(_norm_key(p), {}).get('phash'))
                            if not h_obj: continue
                            v = int(str(h_obj), 16)
                            if v in ad_int_set:
                                is_ad_like = True; break
                            hit = False
                            for m in _ONEBIT_MASKS:
                                if (v ^ m) in ad_int_set:
                                    hit = True; break
                            if hit:
                                is_ad_like = True; break
                        for child in sorted([p for p in children if p != leader]):
                            h1, h2 = self._coerce_hash_obj(self.file_data.get(_norm_key(leader), {}).get('phash')), \
                                     self._coerce_hash_obj(self.file_data.get(_norm_key(child), {}).get('phash'))
                            if h1 and h2:
                                sim = sim_from_hamming(h1 - h2, HASH_BITS) * 100
                                value_str = f"{sim:.1f}%" + (" (似廣告)" if is_ad_like else "")
                                found_items.append((leader, child, value_str))
                else:
                    ad_match_thresh = hamming_from_sim(0.98, HASH_BITS)
                    for leader, children in final_groups.items():
                        is_ad_like = False
                        group_hashes = {self._coerce_hash_obj(self.file_data.get(_norm_key(p), {}).get('phash')) for p in [leader] + children}
                        group_hashes.discard(None)
                        if any(h and any((h - ah) <= ad_match_thresh for ah in ad_phash_set) for h in group_hashes):
                            is_ad_like = True
                        for child in sorted([p for p in children if p != leader]):
                            h1, h2 = self._coerce_hash_obj(self.file_data.get(_norm_key(leader), {}).get('phash')), \
                                     self._coerce_hash_obj(self.file_data.get(_norm_key(child), {}).get('phash'))
                            if h1 and h2:
                                sim = sim_from_hamming(h1 - h2, HASH_BITS) * 100
                                value_str = f"{sim:.1f}%" + (" (似廣告)" if is_ad_like else "")
                                found_items.append((leader, child, value_str))

            else:
                for leader, children in final_groups.items():
                    for child in sorted([p for p in children if p != leader]):
                        h1, h2 = self.file_data.get(_norm_key(leader), {}).get('phash'), self.file_data.get(_norm_key(child), {}).get('phash')
                        if h1 and h2:
                            sim = sim_from_hamming(self._coerce_hash_obj(h1) - self._coerce_hash_obj(h2), HASH_BITS) * 100
                            found_items.append((leader, child, f"{sim:.1f}%"))
        else:
            results_by_leader = defaultdict(list)
            for leader, target, sim_str in temp_found_pairs: results_by_leader[leader].append((target, float(sim_str.replace('%','')), sim_str))
            for leader, targets in results_by_leader.items():
                for target, _, sim_str in sorted(targets, key=lambda x: x[1], reverse=True): found_items.append((leader, target, sim_str))
        
        log_performance("[完成] LSH 雙哈希比對階段")
        log_info("--- 比對引擎漏斗統計 ---")
        if stats['filtered_inter'] > 0: 
            log_info(f"因\"僅比對不同資料夾\"而跳過: {stats['filtered_inter']:,} 次")
        log_info(f"廣告組展開後總比對次數: {stats['comparisons']:,}")
        passed_phash, passed_color, entered_whash = stats['passed_phash'], stats['passed_color'], stats['entered_whash']
        if stats['comparisons'] > 0: 
            log_info(f"通過 pHash 快篩: {passed_phash:,} ({(passed_phash/stats['comparisons']*100 if stats['comparisons'] > 0 else 0):.1f}%)")
        if passed_phash > 0: 
            log_info(f" └─ 通過顏色過濾閘: {passed_color:,} ({(passed_color/passed_phash*100 if passed_phash > 0 else 0):.1f}%)")
        if passed_color > 0: 
            log_info(f"    └─ 進入 wHash 複核: {entered_whash:,} ({(entered_whash/passed_color*100 if passed_color > 0 else 0):.1f}%)")
        final_matches = len({(_norm_key(p1), _norm_key(p2)) for p1, p2, _ in temp_found_pairs})
        if passed_color > 0: 
            log_info(f"       └─ 最終有效匹配: {final_matches:,} ({(final_matches/passed_color*100 if passed_color > 0 else 0):.1f}%)")
        log_info("--------------------------")
        
        self.file_data = {**gallery_data, **ad_data}
        return found_items, self.file_data

    def _detect_qr_codes(self, scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        if self.config.get('enable_qr_hybrid_mode'):
            return self._detect_qr_codes_hybrid(self.tasks_to_process, scan_cache_manager)
        else:
            return self._detect_qr_codes_pure(self.tasks_to_process, scan_cache_manager)

    def _detect_qr_codes_pure(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        log_info("[QR] 正在執行純粹掃描模式...")
        continue_processing, file_data = self._process_images_with_cache(
            files_to_process, scan_cache_manager, "QR Code 檢測", 
            _pool_worker_detect_qr_code, 'qr_points'
        )
        if not continue_processing: return None
        found_qr_images = [(path, path, "QR Code 檢出") for path, data in file_data.items() if data and data.get('qr_points')]
        self.file_data = file_data
        return found_qr_images, self.file_data

    def _detect_qr_codes_hybrid(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        log_info("[QR] 正在執行混合掃描模式...")
        ad_folder_path = self.config.get('ad_folder_path')
        if not ad_folder_path or not os.path.isdir(ad_folder_path):
            log_info("[QR混合模式] 廣告資料夾無效，退回純粹掃描模式。")
            return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        
        self._update_progress(text="📦 正在預處理廣告庫...（此階段為局部進度）")
        ad_paths = []
        for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
            if ent.is_file() and ent.name.lower().endswith(('.png','.jpg','.jpeg','.webp')):
                ad_paths.append(_norm_key(ent.path))

        ad_cache_manager = ScannedImageCacheManager(ad_folder_path, ad_folder_path, 'ad_comparison')
        continue_proc_ad, ad_data = self._process_images_with_cache(
            ad_paths, ad_cache_manager, "廣告圖片屬性", 
            _pool_worker_process_image_full, 'qr_points', progress_scope='local'
        )
        if not continue_proc_ad: return None
        self.file_data.update(ad_data)
        ad_with_phash = {path: data for path, data in ad_data.items() if data and data.get('phash')}
        if not ad_with_phash:
            log_info("[QR混合模式] 廣告資料夾無有效哈希，退回純粹掃描模式。")
            return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        continue_proc_gallery, gallery_data = self._process_images_with_cache(
            files_to_process, scan_cache_manager, "目標雜湊", 
            _pool_worker_process_image_phash_only, 'phash', progress_scope='global'
        )
        if not continue_proc_gallery: return None
        self.file_data.update(gallery_data)
        self._update_progress(text="🔍 [混合模式] 正在使用 LSH 快速匹配廣告...")
        phash_index = self._build_phash_band_index(gallery_data)
        found_ad_matches = []
        user_thresh = self.config.get('similarity_threshold', 95.0) / 100.0
        for ad_path, ad_ent in ad_with_phash.items():
            if self._check_control() != 'continue': return None
            ad_p_hash = self._coerce_hash_obj(ad_ent.get('phash'))
            if not ad_p_hash: continue
            candidate_paths = self._lsh_candidates_for(ad_path, ad_p_hash, phash_index)
            for g_path in candidate_paths:
                g_p_hash = self._coerce_hash_obj(gallery_data.get(g_path, {}).get('phash'))
                if not g_p_hash: continue

                sim_p = sim_from_hamming(ad_p_hash - g_p_hash, HASH_BITS)
                if sim_p < PHASH_FAST_THRESH: continue
                
                is_accepted, final_sim_val = True, sim_p
                if sim_p < PHASH_STRICT_SKIP:
                    if not self._ensure_features(ad_path, ad_cache_manager, need_whash=True) or \
                       not self._ensure_features(g_path, scan_cache_manager, need_whash=True): continue
                    ad_w_hash = self.file_data.get(_norm_key(ad_path),{}).get('whash')
                    g_w_hash = self.file_data.get(_norm_key(g_path),{}).get('whash')
                    is_accepted, final_sim_val = self._accept_pair_with_dual_hash(ad_p_hash, g_p_hash, ad_w_hash, g_w_hash)
                
                if is_accepted and final_sim_val >= user_thresh and ad_ent.get('qr_points'):
                    found_ad_matches.append((ad_path, g_path, "廣告匹配(快速)"))
                    gallery_data.setdefault(g_path, {})['qr_points'] = ad_ent['qr_points']
                        
        matched_gallery_paths = {_norm_key(pair[1]) for pair in found_ad_matches}
        remaining_files_for_qr = [path for path in self.tasks_to_process if _norm_key(path) not in matched_gallery_paths]
        
        self._update_progress(text=f"🔍 對剩餘 {len(remaining_files_for_qr)} 個檔案進行 QR 掃描（局部進度）")
        if remaining_files_for_qr:
            if self._check_control() != 'continue': return None
            continue_proc_qr, qr_data = self._process_images_with_cache(
                remaining_files_for_qr, scan_cache_manager, "QR Code 檢測", 
                _pool_worker_detect_qr_code, 'qr_points', progress_scope='local'
            )
            if not continue_proc_qr: return None
            qr_results = [(path, path, "QR Code 檢出") for path, data in qr_data.items() if data and data.get('qr_points')]
            found_ad_matches.extend(qr_results)
            self.file_data.update(qr_data)
        scan_cache_manager.save_cache()
        ad_cache_manager.save_cache()
        return found_ad_matches, self.file_data