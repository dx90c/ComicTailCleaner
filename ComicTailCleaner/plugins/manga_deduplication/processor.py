# ======================================================================
# 檔案：plugins/manga_deduplication/processor.py
# 目的：實現一個擁有 v1.9.6 級別獨立掃描引擎的智慧型卷宗查找器
# 版本：10.1 (修復 NameError，並整合多項優化建議)
# ======================================================================

from __future__ import annotations
import os
import re
import datetime
import imagehash
from collections import defaultdict, deque
from typing import Dict, Any, Tuple, List, Optional, Generator
from queue import Queue
from PIL import Image, ImageOps
from multiprocessing import Pool, cpu_count, set_start_method
import sys

# 修復 NameError: name 'ttk' is not defined
try:
    from tkinter import ttk
except ImportError:
    ttk = None

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error, _norm_key
from processors.scanner import ScannedImageCacheManager, FolderStateCacheManager, _pool_worker_process_image_phash_only

from . import plugin_gui

# --- 輔助函式 (與 core_engine v1.9.6 同步) ---

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

# --- 核心掃描邏輯 (與 core_engine v1.9.6 同步) ---

def _plugin_scan_traversal(root_folder: str, excluded_paths: set, excluded_names: set, time_filter: dict, folder_cache: FolderStateCacheManager, progress_queue: Optional[Queue], control_events: Optional[dict]) -> tuple[dict, set, set]:
    log_info("[外掛引擎] 啟動 v1.9.6 統一掃描引擎...")
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
                progress_queue.put({'type': 'text', 'text': f"📁 [外掛] 已檢查 {scanned_count} 個資料夾..."})

            live_folders[norm_current_dir] = {'mtime': stat_info.st_mtime}
            cached_entry = folder_cache.get_folder_state(norm_current_dir)
            if not cached_entry or abs(stat_info.st_mtime - cached_entry.get('mtime', 0)) > 1e-6:
                changed_or_new_folders.add(norm_current_dir)

            with os.scandir(current_dir) as it:
                for entry in it:
                    if entry.is_dir():
                        queue.append(entry.path)
        except OSError: continue
    
    ghost_folders = set(cached_states.keys())
    log_info(f"[外掛引擎] 掃描完成。符合條件: {len(live_folders)}, 新/變更: {len(changed_or_new_folders)}, 幽靈: {len(ghost_folders)}")
    return live_folders, changed_or_new_folders, ghost_folders

class MangaDeduplicationPlugin(BasePlugin):
    def __init__(self):
        self.ui_vars = {}; self.pool = None

    def get_id(self) -> str: return "manga_volume_deduplication_smart"
    def get_name(self) -> str: return "相似卷宗查找 (獨立引擎 v1.9.6+)"
    def get_description(self) -> str: return "使用與主程式同步的 v1.9.6 掃描引擎，比對末尾圖片指紋，找出相似的漫畫卷宗。"

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any]) -> Optional['ttk.Frame']:
        if ttk is None: return None
        frame = plugin_gui.create_settings_frame(parent_frame, config, self.ui_vars)
        plugin_gui.load_settings(config, self.ui_vars)
        return frame

    def save_settings(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return plugin_gui.save_settings(config, self.ui_vars)

    def _process_images_with_cache(self, tasks, cache_manager, progress_queue, control_events):
        local_data, to_recalc, hits = {}, [], 0
        for path in tasks:
            norm_path = _norm_key(path)
            cached = cache_manager.get_data(norm_path)
            try:
                if cached and cached.get('phash') and abs(os.path.getmtime(path) - cached.get('mtime', 0)) < 1e-6:
                    if 'phash' in cached and isinstance(cached['phash'], str):
                       cached['phash'] = imagehash.hex_to_hash(cached['phash'])
                    local_data[norm_path] = cached
                    hits += 1
                else:
                    to_recalc.append(path)
            except OSError:
                continue
        log_info(f"[外掛引擎] 快取命中: {hits}/{len(tasks)}")
        
        if to_recalc:
            if not self.pool:
                if sys.platform.startswith('win'):
                    try: set_start_method('spawn', force=True)
                    except RuntimeError: pass
                pool_size = max(1, cpu_count() // 2)
                self.pool = Pool(processes=pool_size)

            results = self.pool.imap_unordered(_pool_worker_process_image_phash_only, to_recalc)
            
            for i, (path, data) in enumerate(results):
                if control_events['cancel'].is_set():
                    self.pool.terminate()
                    if progress_queue: progress_queue.put({'type':'status_update', 'text':'外掛任務已中止'})
                    return None
                if progress_queue:
                    progress_queue.put({'type':'progress', 'value': int(((i+1)/len(to_recalc))*50), 'text': f"⚙️ [外掛] 計算哈希: {i+1}/{len(to_recalc)}"})
                
                norm_path = _norm_key(path)
                if data and not data.get('error'):
                    data['phash'] = imagehash.hex_to_hash(data['phash'])
                    local_data[norm_path] = data
                    cache_manager.update_data(norm_path, data)

        cache_manager.save_cache()
        return local_data

    def run(self, config: Dict, progress_queue: Optional[Queue] = None, control_events: Optional[Dict] = None) -> Optional[Tuple[List, Dict, List]]:
        _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
        _is_cancelled = lambda: bool(control_events and control_events.get('cancel') and control_events['cancel'].is_set())
        
        try:
            log_info("[相似卷宗] 準備執行獨立掃描 (引擎版本 v1.9.6+)...")
            _update_progress("🚀 [相似卷宗] 啓動獨立掃描引擎...", 0)
            
            main_cache = ScannedImageCacheManager(config['root_scan_folder'], comparison_mode="plugin_manga_dedup")
            folder_cache = FolderStateCacheManager(config['root_scan_folder'])
            
            excluded_folders_config = config.get('excluded_folders', [])
            excluded_paths = {_norm_key(p) for p in excluded_folders_config if os.path.sep in p or (os.path.altsep and os.path.altsep in p)}
            excluded_names = {name.lower() for name in excluded_folders_config if (os.path.sep not in name) and (not os.path.altsep or os.path.altsep not in name)}
            
            time_filter = {'enabled': config.get('enable_time_filter', False)}
            if time_filter['enabled']:
                try:
                    s, e = config.get('start_date_filter'), config.get('end_date_filter')
                    time_filter['start'] = datetime.datetime.strptime(s, "%Y-%m-%d") if s else None
                    time_filter['end'] = datetime.datetime.strptime(e, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if e else None
                except: time_filter['enabled'] = False
            
            live_folders, _, _ = _plugin_scan_traversal(config['root_scan_folder'], excluded_paths, excluded_names, time_filter, folder_cache, progress_queue, control_events)
            if _is_cancelled():
                if progress_queue: progress_queue.put({'type':'status_update', 'text':'外掛任務已中止'})
                return None
            
            exts = ('.jpg', '.jpeg', '.png', '.webp')

            # --- 修正「只掃葉夾」的邊界情況 ---
            leaf_folders = {f for f in live_folders if not any(other.startswith(f + os.sep) for other in live_folders if other != f)}
            candidate_folders = set(leaf_folders)
            for f in live_folders:
                if f in leaf_folders: continue
                try:
                    # 如果一個父資料夾自身包含圖片，也將其視為一個獨立的取樣對象
                    if any(entry.is_file() and entry.name.lower().endswith(exts) for entry in os.scandir(f)):
                        candidate_folders.add(f)
                except OSError:
                    continue
            log_info(f"[外掛引擎] 從 {len(live_folders)} 個資料夾中篩選出 {len(candidate_folders)} 個候選取樣資料夾。")

            all_files = []
            count = int(config.get('plugin_sample_count', 12))
            limit = config.get('plugin_enable_sample_limit', True)

            for folder in sorted(list(candidate_folders)):
                if _is_cancelled(): break
                try:
                    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(exts)]
                    if files:
                        files.sort(key=_natural_sort_key)
                        all_files.extend(files[-count:] if limit else files)
                except OSError: continue
            
            file_data = self._process_images_with_cache(sorted(list(set(all_files))), main_cache, progress_queue, control_events)
            if file_data is None: return None
            
            files_by_folder = defaultdict(list)
            for f_path, data in file_data.items():
                files_by_folder[_norm_key(os.path.dirname(f_path))].append(f_path)
            
            fingerprints = {folder: {file_data.get(_norm_key(p), {}).get('phash') for p in paths} - {None} for folder, paths in files_by_folder.items()}

            _update_progress("🔄 比對指紋...", 55)
            MATCH_THRESHOLD = int(config.get("plugin_match_threshold", 8))
            folder_list = sorted(list(fingerprints.keys()))
            found_items = []
            
            if len(folder_list) < 2:
                _update_progress("✅ [相似卷宗] 資料夾數量不足，無需比對。"); return [], {}, []

            total_comps = len(folder_list) * (len(folder_list) - 1) // 2
            comps_done = 0
            for i in range(len(folder_list)):
                if _is_cancelled():
                    if progress_queue: progress_queue.put({'type':'status_update', 'text':'外掛任務已中止'})
                    return None
                if i > 0 and i % 50 == 0 and total_comps > 0:
                    progress_percent = 55 + int((comps_done / total_comps) * 40)
                    _update_progress(f"🔄 [相似卷宗] 正在比對... ({i+1}/{len(folder_list)})", value=progress_percent)
                
                for j in range(i + 1, len(folder_list)):
                    comps_done += 1
                    path1, path2 = folder_list[i], folder_list[j]
                    fp1, fp2 = fingerprints[path1], fingerprints[path2]
                    
                    if not fp1 or not fp2: continue
                    
                    intersection_size = len(fp1.intersection(fp2))
                    min_len = min(len(fp1), len(fp2))
                    
                    if min_len == 0: continue
                    
                    current_threshold = MATCH_THRESHOLD
                    if limit and count > 0 and min_len < count:
                        current_threshold = min(MATCH_THRESHOLD, max(1, int(MATCH_THRESHOLD * (min_len / count))))
                    
                    if intersection_size >= current_threshold:
                        found_items.append((min(path1, path2), max(path1, path2), f"{intersection_size}/{min_len} 頁內容重合"))
            
            _update_progress("✅ 整理結果...", 95)
            gui_file_data = {}
            involved_paths = {p for item in found_items for p in item[:2]}
            for path in involved_paths:
                image_files_for_preview = files_by_folder.get(_norm_key(path))
                if image_files_for_preview:
                    gui_file_data[path] = {'display_path': sorted(image_files_for_preview, key=_natural_sort_key)[0]}
                else: 
                    gui_file_data[path] = {}

            log_info(f"[相似卷宗] 掃描完成，找到 {len(found_items)} 組相似項目。")
            _update_progress("✅ [相似卷宗] 完成！", value=100)
            return found_items, gui_file_data, []
        except Exception as e:
            log_error(f"[外掛] 執行期間發生嚴重錯誤: {e}", include_traceback=True)
            if progress_queue: progress_queue.put({'type':'text', 'text': f"❌ 錯誤: {e}"})
            return [], {}, [("外掛錯誤", str(e))]
        finally:
            if self.pool: 
                self.pool.close()
                self.pool.join()
                self.pool = None
                log_info("[外掛引擎] 多進程池已關閉。")