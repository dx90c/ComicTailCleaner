# ======================================================================
# 檔案名稱：processors/scanner.py
# 模組目的：提供統一的文件掃描、緩存管理及多進程 workers
# 版本：1.1.0 (整合所有底層掃描與緩存邏輯)
# ======================================================================

import os
import datetime
from collections import deque, defaultdict
from queue import Queue
from typing import Union, Tuple, Dict, List, Optional
import re
import json

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
from config import VPATH_PREFIX, VPATH_SEPARATOR
from utils import (log_info, log_error, _is_virtual_path, _parse_virtual_path, 
                   CACHE_LOCK, _sanitize_path_for_filename, _open_image_from_any_path, _get_file_stat)

try:
    import archive_handler
    ARCHIVE_SUPPORT_ENABLED = True
except ImportError:
    archive_handler = None
    ARCHIVE_SUPPORT_ENABLED = False

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
    【v1.8.3 兼容版】
    能夠同時處理兩種格式的 payload:
    1. 核心引擎傳入的元組: (image_path, size_hint)
    2. 外掛傳入的字串: image_path
    """
    image_path = ""
    try:
        # --- 【核心修正】智慧型參數解析 ---
        if isinstance(payload, tuple) and len(payload) > 0:
            image_path = payload[0]
        elif isinstance(payload, str):
            image_path = payload
        else:
            return ("invalid_payload", {'error': f"收到了無效的 payload 格式: {type(payload)}"})

        if not os.path.exists(image_path):
            return (image_path, {'error': f"圖片檔案不存在: {image_path}"})

        with Image.open(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                return (image_path, {'error': f"圖片尺寸異常或無法讀取: {image_path}"})
            
            img = ImageOps.exif_transpose(img)
            ph = imagehash.phash(img)
            st = os.stat(image_path)
            
            return (image_path, {
                'phash': ph, 
                'size': st.st_size, 
                'ctime': st.st_ctime, 
                'mtime': st.st_mtime
            })
    except Exception as e:
        # 確保即使在解析參數階段出錯，也能返回一個包含原始路徑（如果可能）的錯誤
        error_path = image_path if image_path else str(payload)
        return (error_path, {'error': f"處理 pHash 失敗: {e}"})

# === 快取管理類 ===
class ScannedImageCacheManager:
    def __init__(self, root_scan_folder: str, ad_folder_path: Union[str, None] = None, comparison_mode: str = 'mutual_comparison'):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        cache_suffix = "_ad_comparison" if comparison_mode == 'ad_comparison' else ""
        base_name = f"scanned_hashes_cache_{sanitized_root}{cache_suffix}"
        self.cache_file_path = f"{base_name}.json"
        counter = 1
        norm_root = os.path.normpath(root_scan_folder).lower()
        while os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f: data = json.load(f)
                first_key = next(iter(data), None)
                if not first_key or os.path.normpath(first_key).lower().startswith(norm_root): break
            except (json.JSONDecodeError, StopIteration, TypeError, AttributeError): break
            self.cache_file_path = f"{base_name}_{counter}.json"; counter += 1
            if counter > 10: log_error("圖片快取檔名衝突過多。"); break
        self.cache = self._load_cache()
        log_info(f"[快取] 圖片快取已初始化: '{self.cache_file_path}'")
    def _normalize_loaded_data(self, data: dict) -> dict:
        converted_data = data.copy()
        if imagehash:
            for key in ['phash', 'whash']:
                if key in converted_data and converted_data[key] and not isinstance(converted_data[key], imagehash.ImageHash):
                    try: converted_data[key] = imagehash.hex_to_hash(str(converted_data[key]))
                    except (TypeError, ValueError): converted_data[key] = None
        if 'avg_hsv' in converted_data and isinstance(converted_data['avg_hsv'], list):
            try: converted_data['avg_hsv'] = tuple(float(x) for x in converted_data['avg_hsv'])
            except (ValueError, TypeError): converted_data['avg_hsv'] = None
        return converted_data
    def _load_cache(self) -> dict:
        if not os.path.exists(self.cache_file_path): return {}
        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f: loaded_data = json.load(f)
            if not isinstance(loaded_data, dict): return {}
            converted_cache = {}
            for path, data in loaded_data.items():
                norm_path = os.path.normpath(path).lower()
                if isinstance(data, dict): converted_cache[norm_path] = self._normalize_loaded_data(data)
            log_info(f"圖片快取 '{self.cache_file_path}' 已成功載入 {len(converted_cache)} 筆。")
            return converted_cache
        except (json.JSONDecodeError, Exception) as e:
            log_info(f"圖片快取檔案 '{self.cache_file_path}' 格式不正確或讀取失敗 ({e})，將重建。")
            return {}
    def save_cache(self) -> None:
        with CACHE_LOCK:
            serializable_cache = {}
            for path, data in self.cache.items():
                if data:
                    serializable_data = {k: str(v) if imagehash and isinstance(v, imagehash.ImageHash) else v for k, v in data.items()}
                    if 'avg_hsv' in serializable_data and isinstance(serializable_data['avg_hsv'], tuple):
                        serializable_data['avg_hsv'] = list(serializable_data['avg_hsv'])
                    serializable_cache[path] = serializable_data
            try:
                temp_file_path = self.cache_file_path + f".tmp{os.getpid()}"
                with open(temp_file_path, 'w', encoding='utf-8') as f: json.dump(serializable_cache, f, indent=2)
                os.replace(temp_file_path, self.cache_file_path)
            except (IOError, OSError) as e: log_error(f"保存圖片快取失敗: {e}", True)
    def get_data(self, file_path: str) -> Union[dict, None]: return self.cache.get(os.path.normpath(file_path).lower())
    def update_data(self, file_path: str, data: dict) -> None:
        if data and 'error' not in data:
            norm_path = os.path.normpath(file_path).lower()
            if self.cache.get(norm_path): self.cache[norm_path].update(data)
            else: self.cache[norm_path] = data
    def remove_data(self, file_path: str) -> bool:
        with CACHE_LOCK:
            normalized_path = os.path.normpath(file_path).lower()
            if normalized_path in self.cache: del self.cache[normalized_path]; return True
            return False
    def remove_entries_from_folder(self, folder_path: str) -> int:
        with CACHE_LOCK:
            count = 0
            norm_folder_path = os.path.normpath(folder_path).lower() + os.sep
            keys_to_delete = [key for key in self.cache if key.startswith(norm_folder_path)]
            for key in keys_to_delete: del self.cache[key]; count += 1
            if count > 0: log_info(f"[快取清理] 已從圖片快取中移除 '{folder_path}' 的 {count} 個條目。")
            return count
    def invalidate_cache(self) -> None:
        if send2trash is None: log_error("無法清理快取，因為 'send2trash' 模組未安裝。"); return
        with CACHE_LOCK:
            self.cache = {}
            if os.path.exists(self.cache_file_path):
                try: log_info(f"[快取清理] 準備將圖片快取檔案 '{self.cache_file_path}' 移至回收桶。"); send2trash.send2trash(self.cache_file_path)
                except Exception as e: log_error(f"刪除圖片快取檔案時發生錯誤: {e}", True)

class FolderStateCacheManager:
    def __init__(self, root_scan_folder: str):
        sanitized_root = _sanitize_path_for_filename(root_scan_folder)
        base_name = f"folder_state_cache_{sanitized_root}"
        self.cache_file_path = f"{base_name}.json"
        norm_root = os.path.normpath(root_scan_folder).lower()
        counter = 1
        while os.path.exists(self.cache_file_path):
            try:
                with open(self.cache_file_path, 'r', encoding='utf-8') as f: data = json.load(f)
                first_key = next(iter(data), None)
                if not first_key or os.path.normpath(first_key).lower().startswith(norm_root): break
            except (json.JSONDecodeError, StopIteration, TypeError, AttributeError): break
            self.cache_file_path = f"{base_name}_{counter}.json"; counter += 1
            if counter > 10: log_error("資料夾快取檔名衝突過多。"); break
        self.cache = self._load_cache()
        log_info(f"[快取] 資料夾快取已初始化: '{self.cache_file_path}'")
    def _load_cache(self) -> dict:
        if not os.path.exists(self.cache_file_path): return {}
        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f: loaded_cache = json.load(f)
            if not isinstance(loaded_cache, dict): return {}
            converted_cache = {}
            for path, state in loaded_cache.items():
                norm_path = os.path.normpath(path).lower()
                if isinstance(state, dict) and 'mtime' in state: converted_cache[norm_path] = state
            log_info(f"資料夾狀態快取 '{self.cache_file_path}' 已成功載入 {len(converted_cache)} 筆。")
            return converted_cache
        except Exception as e:
            log_error(f"載入資料夾狀態快取時發生錯誤: {e}", True); return {}
    def save_cache(self) -> None:
        with CACHE_LOCK:
            try:
                temp_file_path = self.cache_file_path + f".tmp{os.getpid()}"
                with open(temp_file_path, 'w', encoding='utf-8') as f: json.dump(self.cache, f, indent=2)
                os.replace(temp_file_path, self.cache_file_path)
            except (IOError, OSError) as e: log_error(f"保存資料夾快取失敗: {e}", True)
    def get_folder_state(self, folder_path: str) -> Union[dict, None]: return self.cache.get(os.path.normpath(folder_path).lower())
    def update_folder_state(self, folder_path: str, mtime: float, ctime: Union[float, None]):
        norm_path = os.path.normpath(folder_path).lower(); self.cache[norm_path] = {'mtime': mtime, 'ctime': ctime}
    def remove_folders(self, folder_paths: list[str]):
        for path in folder_paths:
            norm_path = os.path.normpath(path).lower()
            if norm_path in self.cache: del self.cache[norm_path]
    def invalidate_cache(self) -> None:
        if send2trash is None: log_error("無法清理快取，因為 'send2trash' 模組未安裝。"); return
        with CACHE_LOCK:
            self.cache = {};
            if os.path.exists(self.cache_file_path):
                try: log_info(f"[快取清理] 準備將資料夾快取檔案 '{self.cache_file_path}' 移至回收桶。"); send2trash.send2trash(self.cache_file_path)
                except Exception as e: log_error(f"刪除資料夾快取檔案時發生錯誤: {e}", True)

def _unified_scan_traversal(root_folder: str, excluded_paths: set, time_filter: dict, folder_cache: 'FolderStateCacheManager', progress_queue: Queue, control_events: dict) -> tuple[dict, set, set]:
    log_info("啟動統一掃描引擎...")
    live_folders, changed_or_new_folders = {}, set()
    queue = deque([root_folder]); scanned_count = 0; cached_states = folder_cache.cache.copy()
    root_norm_path = os.path.normpath(root_folder).lower()
    while queue:
        if control_events['cancel'].is_set(): return {}, set(), set()
        current_dir = queue.popleft(); norm_current_dir = os.path.normpath(current_dir).lower()
        if any(norm_current_dir.startswith(ex) for ex in excluded_paths): continue
        try:
            stat_info = os.stat(norm_current_dir)
            
            is_in_time_range = True
            if norm_current_dir != root_norm_path and time_filter.get('enabled'):
                ctime_dt = datetime.datetime.fromtimestamp(stat_info.st_ctime)
                if (time_filter.get('start') and ctime_dt < time_filter['start']) or \
                   (time_filter.get('end') and ctime_dt > time_filter['end']):
                    is_in_time_range = False
            
            live_folders[norm_current_dir] = {'mtime': stat_info.st_mtime, 'ctime': stat_info.st_ctime}
            cached_states.pop(norm_current_dir, None)

            if not is_in_time_range:
                continue

            scanned_count += 1
            if scanned_count % 100 == 0 and progress_queue:
                progress_queue.put({'type': 'text', 'text': f"📁 正在檢查資料夾結構... ({scanned_count})"})
            
            cached_entry = folder_cache.get_folder_state(norm_current_dir)
            if not cached_entry or abs(stat_info.st_mtime - cached_entry.get('mtime', 0)) > 1e-6:
                changed_or_new_folders.add(norm_current_dir)
                
            with os.scandir(norm_current_dir) as it:
                for entry in it:
                    if control_events['cancel'].is_set(): return {}, set(), set()
                    if entry.is_dir(): queue.append(entry.path)
        except OSError: continue
    ghost_folders = set(cached_states.keys())
    log_info(f"統一掃描完成。即時資料夾: {len(live_folders)}, 新/變更: {len(changed_or_new_folders)}, 幽靈資料夾: {len(ghost_folders)}")
    return live_folders, changed_or_new_folders, ghost_folders

def get_files_to_process(config: dict, image_cache: ScannedImageCacheManager, progress_queue: Union[Queue, None] = None, control_events: Union[dict, None] = None) -> Tuple[List[str], Dict[str, int]]:
    root_folder = config['root_scan_folder']
    if not os.path.isdir(root_folder): return [], {}
    enable_archive_scan = config.get('enable_archive_scan', False) and ARCHIVE_SUPPORT_ENABLED
    supported_archive_exts = tuple(archive_handler.get_supported_formats()) if enable_archive_scan else ()
    folder_cache = FolderStateCacheManager(root_folder)
    excluded_paths = {os.path.normpath(f).lower() for f in config.get('excluded_folders', [])}
    time_filter = {'enabled': config.get('enable_time_filter', False)}
    if time_filter['enabled']:
        try:
            start_str, end_str = config.get('start_date_filter'), config.get('end_date_filter')
            time_filter['start'] = datetime.datetime.strptime(start_str, "%Y-%m-%d") if start_str else None
            time_filter['end'] = datetime.datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if end_str else None
        except ValueError: log_error("時間篩選日期格式錯誤，將被忽略。"); time_filter['enabled'] = False
        
    live_folders, folders_to_scan_content, ghost_folders = _unified_scan_traversal(root_folder, excluded_paths, time_filter, folder_cache, progress_queue, control_events)
    
    if control_events and control_events['cancel'].is_set(): return [], {}
    if ghost_folders:
        folder_cache.remove_folders(list(ghost_folders))
        for folder in ghost_folders: image_cache.remove_entries_from_folder(folder)
        
    unchanged_folders = set(live_folders.keys()) - folders_to_scan_content
    
    if unchanged_folders:
        folders_in_cache = set()
        for p in image_cache.cache.keys():
            if _is_virtual_path(p):
                archive_path, _ = _parse_virtual_path(p)
                if archive_path: folders_in_cache.add(os.path.dirname(archive_path).lower())
            else: folders_in_cache.add(os.path.dirname(p).lower())
        
        folders_needing_scan_due_to_empty_cache = unchanged_folders - folders_in_cache
        
        if folders_needing_scan_due_to_empty_cache:
            if time_filter['enabled']:
                final_failsafe_folders = set()
                for folder in folders_needing_scan_due_to_empty_cache:
                    folder_ctime = live_folders.get(folder, {}).get('ctime')
                    if folder_ctime:
                        ctime_dt = datetime.datetime.fromtimestamp(folder_ctime)
                        if not ((time_filter.get('start') and ctime_dt < time_filter['start']) or \
                                (time_filter.get('end') and ctime_dt > time_filter['end'])):
                            final_failsafe_folders.add(folder)
                
                log_info(f"[保底] {len(final_failsafe_folders)} 個未變更資料夾因在圖片快取中無紀錄且符合時間範圍，已加入掃描。")
                folders_to_scan_content.update(final_failsafe_folders)
            else:
                log_info(f"[保底] {len(folders_needing_scan_due_to_empty_cache)} 個未變更資料夾因在圖片快取中無紀錄，已加入掃描。")
                folders_to_scan_content.update(folders_needing_scan_due_to_empty_cache)

    unchanged_folders = set(live_folders.keys()) - folders_to_scan_content

    image_exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    count, enable_limit = config.get('extract_count', 8), config.get('enable_extract_count_limit', True)
    scanned_files, cached_files = [], []
    vpath_size_map: Dict[str, int] = {}
    
    qr_mode = config.get('comparison_mode') == 'qr_detection'
    qr_pages_per_archive = int(config.get('qr_pages_per_archive', 10))
    qr_global_cap = int(config.get('qr_global_cap', 20000))
    
    def _in_time_range(path_real: str) -> bool:
        if not time_filter.get('enabled'): return True
        try:
            st = os.stat(path_real)
            f_dt = datetime.datetime.fromtimestamp(st.st_mtime)
            if time_filter.get('start') and f_dt < time_filter['start']: return False
            if time_filter.get('end') and f_dt > time_filter['end']: return False
            return True
        except OSError: return False

    for folder in sorted(list(folders_to_scan_content)):
        if control_events and control_events['cancel'].is_set(): break
        temp_files = []
        try:
            for dirpath, dirnames, filenames in os.walk(folder, topdown=True):
                norm_dirpath = os.path.normpath(dirpath).lower()
                if any(norm_dirpath.startswith(ex) for ex in excluded_paths):
                    dirnames[:] = []
                    continue
                for f in filenames:
                    full_path_real = os.path.join(dirpath, f)
                    full_path = os.path.normpath(os.path.join(norm_dirpath, f))
                    if not _in_time_range(full_path_real): continue
                    if enable_archive_scan and f.lower().endswith(supported_archive_exts):
                        try:
                            log_info(f"掃描壓縮檔: {full_path}")
                            archive_entries = []
                            for entry in archive_handler.iter_archive_images(full_path_real):
                                vpath = f"{VPATH_PREFIX}{entry.archive_path}{VPATH_SEPARATOR}{entry.inner_path}"
                                archive_entries.append((vpath, entry.inner_path))
                            archive_entries.sort(key=lambda x: x[1])
                            selected = []
                            if enable_limit:
                                selected = archive_entries[-count:]
                            elif qr_mode:
                                selected = archive_entries[-qr_pages_per_archive:]
                            else:
                                selected = archive_entries
                            temp_files.extend([item[0] for item in selected])
                        except Exception as e:
                            log_error(f"讀取壓縮檔失敗: {full_path_real}: {e}", True)
                            continue
                    elif f.lower().endswith(image_exts):
                        temp_files.append(full_path.lower())
        except OSError as e: log_error(f"掃描資料夾 '{folder}' 時發生錯誤: {e}"); continue
        if qr_mode and not enable_limit and qr_global_cap > 0 and len(scanned_files) + len(temp_files) > qr_global_cap:
            keep = max(0, qr_global_cap - len(scanned_files))
            if keep < len(temp_files):
                log_error(f"[QR防爆量] 本輪檔案數將超過 {qr_global_cap}，只保留本資料夾末尾 {keep} 張。")
                temp_files = temp_files[-keep:]
        scanned_files.extend(temp_files)
        norm_folder = os.path.normpath(folder).lower()
        if norm_folder in live_folders: folder_cache.update_folder_state(norm_folder, live_folders[norm_folder]['mtime'], live_folders[norm_folder]['ctime'])

    if control_events and control_events['cancel'].is_set(): return [], {}
    if unchanged_folders:
        folders_to_check = unchanged_folders
        if time_filter['enabled']:
            folders_to_check = {
                folder for folder in unchanged_folders
                if live_folders.get(folder) and live_folders[folder].get('ctime') and \
                not ((time_filter.get('start') and datetime.datetime.fromtimestamp(live_folders[folder]['ctime']) < time_filter['start']) or \
                     (time_filter.get('end') and datetime.datetime.fromtimestamp(live_folders[folder]['ctime']) > time_filter['end']))
            }
        by_parent = defaultdict(list)
        for p, meta in image_cache.cache.items():
            parent = ""
            if _is_virtual_path(p):
                archive_path, _ = _parse_virtual_path(p)
                if archive_path: parent = os.path.dirname(archive_path).lower()
            else: parent = os.path.dirname(p).lower()
            if parent in folders_to_check:
                by_parent[parent].append((p, float(meta.get('mtime', 0.0)), os.path.basename(p)))
        for parent, lst in by_parent.items():
            lst.sort(key=lambda x: (x[1], x[2]))
            take = lst[-count:] if enable_limit else lst
            cached_files.extend([path for (path, _, _) in take])
            
    final_file_list = scanned_files + cached_files
    folder_cache.save_cache()
    unique_files = sorted(list(set(final_file_list)))
    
    MAX_TOTAL = qr_global_cap if qr_mode and not enable_limit and qr_global_cap > 0 else 50000
    if len(unique_files) > MAX_TOTAL:
        log_error(f"[防爆量] 本輪提取數 {len(unique_files)} 超過上限 {MAX_TOTAL}，請檢查設定。將只處理前 {MAX_TOTAL} 個檔案。")
        unique_files = unique_files[:MAX_TOTAL]
        
    log_info(f"檔案提取完成。從 {len(folders_to_scan_content)} 個新/變更夾掃描 {len(scanned_files)} 筆, 從 {len(unchanged_folders)} 個未變更夾恢復 {len(cached_files)} 筆。總計: {len(unique_files)}")
    return unique_files, vpath_size_map