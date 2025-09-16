# ======================================================================
# 檔案名稱：core_engine.py
# 模組目的：包含核心的比對引擎、檔案掃描與快取管理邏輯
# 版本：1.0.2 (修正 Python 版本相容性)
# ======================================================================

import os
import re
import json
import time
import datetime
from collections import deque, defaultdict
from multiprocessing import Pool, cpu_count
from queue import Queue
from typing import Union # 【修正】導入 Union 類型

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
    
from config import *
from utils import (log_info, log_error, log_performance, _is_virtual_path,
                   _parse_virtual_path, _open_image_from_any_path,
                   _get_file_stat, sim_from_hamming, hamming_from_sim,
                   _avg_hsv, _color_gate, CACHE_LOCK)

# === 多進程工作函式 (必須保持在模組頂層以供 Pool 使用) ===

def _detect_qr_on_image(img) -> Union[list, None]:
    # 【修正】 list | None -> Union[list, None]
    """使用 OpenCV 檢測圖片中的 QR Code"""
    if cv2 is None or np is None: return None
    try:
        img_cv = np.array(img.convert('RGB'))
        if img_cv.shape[0] == 0 or img_cv.shape[1] == 0:
            return None
        qr_detector = cv2.QRCodeDetector()
        retval, decoded_info, points, _ = qr_detector.detectAndDecodeMulti(img_cv)
        if retval and decoded_info and any(info for info in decoded_info if info):
            return points.tolist()
    except (cv2.error, ValueError):
        pass # 靜默處理 OpenCV 錯誤
    return None

def _pool_worker_process_image_phash_only(image_path: str):
    """(子進程) 計算單一圖片的 pHash 和基本檔案資訊"""
    if imagehash is None: return (image_path, {'error': "imagehash 模組未安裝"})
    try:
        with _open_image_from_any_path(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                return (image_path, {'error': "圖片尺寸異常或無法讀取"})
            
            from PIL import ImageOps
            img = ImageOps.exif_transpose(img)
            ph = imagehash.phash(img, hash_size=8) # 64-bit
            
            _, ctime, mtime = _get_file_stat(image_path)
            
            return (image_path, {'phash': ph, 'ctime': ctime, 'mtime': mtime})
    except Exception as e:
        return (image_path, {'error': f"處理 pHash 失敗: {e}"})

def _pool_worker_detect_qr_code(payload: tuple[str, int]):
    """(子進程) 對單一圖片進行 QR Code 檢測"""
    image_path, resize_size = payload
    try:
        with _open_image_from_any_path(image_path) as pil_img:
            if not pil_img or pil_img.width == 0 or pil_img.height == 0:
                return (image_path, {'error': "圖片尺寸異常或無法讀取"})
            
            from PIL import ImageOps
            pil_img = ImageOps.exif_transpose(pil_img)
            
            resized_img = pil_img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            points = _detect_qr_on_image(resized_img)
            
            if not points:
                points = _detect_qr_on_image(pil_img)
            
            return (image_path, {'qr_points': points})
    except Exception as e:
        return (image_path, {'error': f"QR檢測失敗: {e}"})

# === 快取管理類 ===

def _sanitize_path_for_filename(path: str) -> str:
    """清理路徑字串，使其可用於檔名，避免特殊字元問題"""
    if not path: return ""
    basename = os.path.basename(os.path.normpath(path))
    sanitized = re.sub(r'[\\/*?:\"<>|]', '_', basename)
    return sanitized

class ScannedImageCacheManager:
    """管理圖片雜湊和元資料的快取，所有路徑鍵強制使用小寫"""
    def __init__(self, root_scan_folder: str, ad_folder_path: Union[str, None] = None, comparison_mode: str = 'mutual_comparison'):
        # 【修正】 str | None -> Union[str, None]
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
                if not first_key or os.path.normpath(first_key).lower().startswith(norm_root):
                    break
            except (json.JSONDecodeError, StopIteration, TypeError, AttributeError):
                break
            self.cache_file_path = f"{base_name}_{counter}.json"
            counter += 1
            if counter > 10: log_error("圖片快取檔名衝突過多。"); break

        self.cache = self._load_cache()
        log_info(f"[快取] 圖片快取已初始化: '{self.cache_file_path}'")

    def _normalize_loaded_data(self, data: dict) -> dict:
        """確保從 JSON 載入的資料格式正確 (例如將字串轉回 ImageHash 物件)"""
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
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            if not isinstance(loaded_data, dict): return {}
            
            converted_cache = {}
            for path, data in loaded_data.items():
                norm_path = os.path.normpath(path).lower()
                if isinstance(data, dict):
                    converted_cache[norm_path] = self._normalize_loaded_data(data)
            
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
                with open(temp_file_path, 'w', encoding='utf-8') as f:
                    json.dump(serializable_cache, f, indent=2)
                os.replace(temp_file_path, self.cache_file_path)
            except (IOError, OSError) as e:
                log_error(f"保存圖片快取失敗: {e}", True)

    def get_data(self, file_path: str) -> Union[dict, None]:
        # 【修正】 dict | None -> Union[dict, None]
        return self.cache.get(os.path.normpath(file_path).lower())
        
    def update_data(self, file_path: str, data: dict) -> None:
        if data and 'error' not in data:
            norm_path = os.path.normpath(file_path).lower()
            if self.cache.get(norm_path):
                self.cache[norm_path].update(data)
            else:
                self.cache[norm_path] = data

    def remove_data(self, file_path: str) -> bool:
        with CACHE_LOCK:
            normalized_path = os.path.normpath(file_path).lower()
            if normalized_path in self.cache:
                del self.cache[normalized_path]
                return True
            return False

    def remove_entries_from_folder(self, folder_path: str) -> int:
        with CACHE_LOCK:
            count = 0
            norm_folder_path = os.path.normpath(folder_path).lower() + os.sep
            keys_to_delete = [key for key in self.cache if key.startswith(norm_folder_path)]
            for key in keys_to_delete:
                del self.cache[key]
                count += 1
            if count > 0:
                log_info(f"[快取清理] 已從圖片快取中移除 '{folder_path}' 的 {count} 個條目。")
            return count

    def invalidate_cache(self) -> None:
        if send2trash is None:
            log_error("無法清理快取，因為 'send2trash' 模組未安裝。")
            return
        with CACHE_LOCK:
            self.cache = {}
            if os.path.exists(self.cache_file_path):
                try: 
                    log_info(f"[快取清理] 準備將圖片快取檔案 '{self.cache_file_path}' 移至回收桶。")
                    send2trash.send2trash(self.cache_file_path)
                except Exception as e: 
                    log_error(f"刪除圖片快取檔案時發生錯誤: {e}", True)

class FolderStateCacheManager:
    """管理資料夾狀態快取，以判斷資料夾內容是否發生變更"""
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
                if not first_key or os.path.normpath(first_key).lower().startswith(norm_root):
                    break
            except (json.JSONDecodeError, StopIteration, TypeError, AttributeError):
                break
            self.cache_file_path = f"{base_name}_{counter}.json"
            counter += 1
            if counter > 10: log_error("資料夾快取檔名衝突過多。"); break
                
        self.cache = self._load_cache()
        log_info(f"[快取] 資料夾快取已初始化: '{self.cache_file_path}'")

    def _load_cache(self) -> dict:
        if not os.path.exists(self.cache_file_path): return {}
        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                loaded_cache = json.load(f)
            if not isinstance(loaded_cache, dict): return {}
            
            converted_cache = {}
            for path, state in loaded_cache.items():
                norm_path = os.path.normpath(path).lower()
                if isinstance(state, dict) and 'mtime' in state:
                    converted_cache[norm_path] = state
            
            log_info(f"資料夾狀態快取 '{self.cache_file_path}' 已成功載入 {len(converted_cache)} 筆。")
            return converted_cache
        except Exception as e:
            log_error(f"載入資料夾狀態快取時發生錯誤: {e}", True)
            return {}

    def save_cache(self) -> None:
        with CACHE_LOCK:
            try:
                temp_file_path = self.cache_file_path + f".tmp{os.getpid()}"
                with open(temp_file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, indent=2)
                os.replace(temp_file_path, self.cache_file_path)
            except (IOError, OSError) as e:
                log_error(f"保存資料夾快取失敗: {e}", True)
    
    def get_folder_state(self, folder_path: str) -> Union[dict, None]:
        # 【修正】 dict | None -> Union[dict, None]
        return self.cache.get(os.path.normpath(folder_path).lower())

    def update_folder_state(self, folder_path: str, mtime: float, ctime: Union[float, None]):
        # 【修正】 float | None -> Union[float, None]
        norm_path = os.path.normpath(folder_path).lower()
        self.cache[norm_path] = {'mtime': mtime, 'ctime': ctime}

    def remove_folders(self, folder_paths: list[str]):
        for path in folder_paths:
            norm_path = os.path.normpath(path).lower()
            if norm_path in self.cache:
                del self.cache[norm_path]

    def invalidate_cache(self) -> None:
        if send2trash is None:
            log_error("無法清理快取，因為 'send2trash' 模組未安裝。")
            return
        with CACHE_LOCK:
            self.cache = {};
            if os.path.exists(self.cache_file_path):
                try: 
                    log_info(f"[快取清理] 準備將資料夾快取檔案 '{self.cache_file_path}' 移至回收桶。")
                    send2trash.send2trash(self.cache_file_path)
                except Exception as e: 
                    log_error(f"刪除資料夾快取檔案時發生錯誤: {e}", True)

# === 檔案掃描與處理 ===

def _update_progress(queue: Queue, **kwargs):
    """向 GUI 執行緒發送進度更新訊息"""
    if queue:
        queue.put({'type': 'text', **kwargs})

def _unified_scan_traversal(root_folder: str, excluded_paths: set, time_filter: dict, folder_cache: 'FolderStateCacheManager', progress_queue: Queue, control_events: dict) -> tuple[dict, set, set]:
    """統一掃描引擎，遍歷資料夾結構，找出即時、變更和已不存在的資料夾"""
    log_info("啟動統一掃描引擎...")
    live_folders, changed_or_new_folders = {}, set()
    queue = deque([root_folder])
    scanned_count = 0
    cached_states = folder_cache.cache.copy()
    root_norm_path = os.path.normpath(root_folder).lower()

    while queue:
        if control_events['cancel'].is_set(): return {}, set(), set()
        current_dir = queue.popleft()
        norm_current_dir = os.path.normpath(current_dir).lower()

        if any(norm_current_dir.startswith(ex) for ex in excluded_paths):
            continue
        
        try:
            stat_info = os.stat(norm_current_dir)
            if norm_current_dir != root_norm_path and time_filter.get('enabled'):
                ctime_dt = datetime.datetime.fromtimestamp(stat_info.st_ctime)
                if (time_filter.get('start') and ctime_dt < time_filter['start']) or \
                   (time_filter.get('end') and ctime_dt > time_filter['end']):
                    continue
            
            scanned_count += 1
            if scanned_count % 100 == 0:
                _update_progress(progress_queue, text=f"📁 正在檢查資料夾結構... ({scanned_count})")

            live_folders[norm_current_dir] = {'mtime': stat_info.st_mtime, 'ctime': stat_info.st_ctime}
            cached_states.pop(norm_current_dir, None)
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

def get_files_to_process(config: dict, image_cache: ScannedImageCacheManager, progress_queue: Union[Queue, None] = None, control_events: Union[dict, None] = None) -> list[str]:
    # 【修正】 Queue | None -> Union[Queue, None]
    """整合了所有修正的檔案獲取與處理函式，包括壓縮檔掃描"""
    root_folder = config['root_scan_folder']
    if not os.path.isdir(root_folder): return []
    
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
        except ValueError:
            log_error("時間篩選日期格式錯誤，將被忽略。"); time_filter['enabled'] = False

    live_folders, folders_to_scan_content, ghost_folders = _unified_scan_traversal(root_folder, excluded_paths, time_filter, folder_cache, progress_queue, control_events)

    if control_events and control_events['cancel'].is_set(): return []

    if ghost_folders:
        folder_cache.remove_folders(list(ghost_folders))
        for folder in ghost_folders: image_cache.remove_entries_from_folder(folder)

    unchanged_folders = set(live_folders.keys()) - folders_to_scan_content
    
    folders_in_cache = set()
    for p in image_cache.cache.keys():
        if _is_virtual_path(p):
            archive_path, _ = _parse_virtual_path(p)
            if archive_path: folders_in_cache.add(os.path.dirname(archive_path).lower())
        else:
            folders_in_cache.add(os.path.dirname(p).lower())

    folders_needing_scan_due_to_empty_cache = unchanged_folders - folders_in_cache
    if folders_needing_scan_due_to_empty_cache:
        log_info(f"[保底] {len(folders_needing_scan_due_to_empty_cache)} 個未變更資料夾因在圖片快取中無紀錄，已加入掃描。")
        folders_to_scan_content.update(folders_needing_scan_due_to_empty_cache)
        unchanged_folders -= folders_needing_scan_due_to_empty_cache

    image_exts = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff')
    count, enable_limit = config['extract_count'], config['enable_extract_count_limit']
    scanned_files, cached_files = [], []

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
                    full_path = os.path.normpath(os.path.join(norm_dirpath, f))
                    
                    if enable_archive_scan and f.lower().endswith(supported_archive_exts):
                        log_info(f"掃描壓縮檔: {full_path}")
                        archive_entries = []
                        for entry in archive_handler.iter_archive_images(full_path):
                            vpath = f"{VPATH_PREFIX}{entry.archive_path}{VPATH_SEPARATOR}{entry.inner_path}"
                            archive_entries.append((vpath, entry.inner_path))
                        
                        archive_entries.sort(key=lambda x: x[1])
                        temp_files.extend([item[0] for item in archive_entries])
                            
                    elif f.lower().endswith(image_exts):
                        temp_files.append(full_path.lower())
        except OSError as e:
            log_error(f"掃描資料夾 '{folder}' 時發生錯誤: {e}")
            continue

        temp_files.sort()
        if enable_limit:
            scanned_files.extend(temp_files[-count:])
        else:
            scanned_files.extend(temp_files)
        
        norm_folder = os.path.normpath(folder).lower()
        if norm_folder in live_folders:
            folder_cache.update_folder_state(norm_folder, live_folders[norm_folder]['mtime'], live_folders[norm_folder]['ctime'])

    if control_events and control_events['cancel'].is_set(): return []

    if unchanged_folders:
        by_parent = defaultdict(list)
        for p, meta in image_cache.cache.items():
            parent = ""
            if _is_virtual_path(p):
                archive_path, _ = _parse_virtual_path(p)
                if archive_path: parent = os.path.dirname(archive_path).lower()
            else:
                parent = os.path.dirname(p).lower()
            
            if parent in unchanged_folders:
                by_parent[parent].append((p, float(meta.get('mtime', 0.0)), os.path.basename(p)))

        for parent, lst in by_parent.items():
            lst.sort(key=lambda x: (x[1], x[2]))
            take = lst[-count:] if enable_limit else lst
            cached_files.extend([path for (path, _, _) in take])

    final_file_list = scanned_files + cached_files
    folder_cache.save_cache()
    
    unique_files = sorted(list(set(final_file_list)))
    MAX_TOTAL = 50000 
    if len(unique_files) > MAX_TOTAL:
        log_error(f"[防爆量] 本輪提取數 {len(unique_files)} 超過上限 {MAX_TOTAL}，請檢查設定。將只處理前 {MAX_TOTAL} 個檔案。")
        unique_files = unique_files[:MAX_TOTAL]
    
    log_info(f"檔案提取完成。從 {len(folders_to_scan_content)} 個新/變更夾掃描 {len(scanned_files)} 筆, 從 {len(unchanged_folders)} 個未變更夾恢復 {len(cached_files)} 筆。總計: {len(unique_files)}")
    return unique_files

# === 核心比對引擎 ===
class ImageComparisonEngine:
    """封裝了所有比對邏輯的核心類"""
    def __init__(self, config: dict, progress_queue: Union[Queue, None] = None, control_events: Union[dict, None] = None):
        # 【修正】 ... | None -> Union[..., None]
        self.config = config
        self.progress_queue = progress_queue
        self.control_events = control_events
        from utils import QR_SCAN_ENABLED
        self.system_qr_scan_capability = QR_SCAN_ENABLED
        self.pool = None
        
        self.file_data = {}
        self.tasks_to_process = []
        self.total_task_count = 0
        self.completed_task_count = 0
        self.failed_tasks = []
        
        log_performance("[初始化] 掃描引擎實例")

    def _check_control(self) -> str:
        """檢查是否有取消或暫停信號"""
        if self.control_events:
            if self.control_events['cancel'].is_set(): return 'cancel'
            if self.control_events['pause'].is_set(): return 'pause'
        return 'continue'

    def _update_progress(self, p_type: str = 'text', value: Union[int, None] = None, text: Union[str, None] = None) -> None:
        # 【修正】 ... | None -> Union[..., None]
        """向 GUI 安全地發送進度更新"""
        if self.progress_queue:
            self.progress_queue.put({'type': p_type, 'value': value, 'text': text})

    def _cleanup_pool(self):
        """安全地關閉多進程池"""
        if self.pool:
            log_info("正在終結現有進程池...")
            if self.progress_queue:
                self.progress_queue.put({'type': 'status_update', 'text': "正在終止背景任務..."})
            self.pool.terminate()
            self.pool.join()
            log_info("進程池已成功終結。")
            self.pool = None

    def find_duplicates(self) -> Union[tuple[list, dict, list], None]:
        # 【修正】 ... | None -> Union[..., None]
        """主執行函式，啟動整個掃描和比對流程"""
        try:
            self._update_progress(text="任務開始...")
            log_performance("[開始] 掃描任務")
            
            root_scan_folder = self.config['root_scan_folder']
            ad_folder_path = self.config.get('ad_folder_path')
            scan_cache_manager = ScannedImageCacheManager(root_scan_folder, ad_folder_path, self.config.get('comparison_mode'))
            
            try:
                mode = str(self.config.get('comparison_mode', 'mutual_comparison')).lower()
                inter_only = bool(self.config.get('enable_inter_folder_only', False))
                time_on = bool(self.config.get('enable_time_filter', False))
                limit_on = bool(self.config.get('enable_extract_count_limit', True))
                limit_n = int(self.config.get('extract_count', 8))
                mode_str = "廣告比對" if 'ad' in mode else "互相比對"
                log_info("="*50)
                log_info(f"[模式檢查] 當前模式: {mode_str}")
                log_info(f"[模式檢查] - 僅比對不同資料夾: {'啓用' if inter_only else '關閉'}")
                log_info(f"[模式檢查] - 時間篩選: {'啓用' if time_on else '關閉'}")
                log_info(f"[模式檢查] - 提取數量限制: {'啓用 (' + str(limit_n) + '張)' if limit_on else '關閉'}")
                log_info(f"[模式檢查] 實際使用的圖片快取: {scan_cache_manager.cache_file_path}")
                log_info("="*50)
            except Exception as e:
                log_error(f"[模式檢查] 模式橫幅日誌生成失敗: {e}")
            
            if not self.tasks_to_process:
                initial_files = get_files_to_process(self.config, scan_cache_manager, self.progress_queue, self.control_events)
                if self.control_events and self.control_events['cancel'].is_set(): return None
                self.tasks_to_process = sorted(list(set(initial_files)))
                self.total_task_count = len(self.tasks_to_process)
            else:
                log_info(f"從上次暫停點恢復，剩餘 {len(self.tasks_to_process)} 個檔案待處理。")
            
            if not self.tasks_to_process:
                self._update_progress(text="在指定路徑下未找到任何圖片檔案。")
                return [], {}, []
            
            if self.config['comparison_mode'] == "qr_detection":
                result = self._detect_qr_codes(scan_cache_manager)
            else:
                result = self._find_similar_images(scan_cache_manager)

            if result is None: return None
            
            found, data = result
            return found, data, self.failed_tasks
        
        finally:
            self._cleanup_pool()
            
    def _process_images_with_cache(self, current_task_list: list[str], cache_manager: ScannedImageCacheManager, description: str, worker_function: callable, data_key: str) -> tuple[bool, dict]:
        """通用函式，處理一批圖片的計算，並利用快取"""
        if not current_task_list: return True, {}
        local_file_data = {}
        
        time.sleep(self.config.get('ux_scan_start_delay', 0.1))
        
        self._update_progress(text=f"📂 正在檢查 {len(current_task_list)} 個{description}的快取...")
        paths_to_recalc, cache_hits = [], 0
        for path in current_task_list:
            try:
                cached_data = cache_manager.get_data(path)
                
                if cached_data and imagehash:
                    for hash_key in ['phash', 'whash']:
                        if hash_key in cached_data and cached_data[hash_key] and not isinstance(cached_data[hash_key], imagehash.ImageHash):
                            cached_data[hash_key] = imagehash.hex_to_hash(str(cached_data[hash_key]))

                _, _, real_mtime = _get_file_stat(path)

                if cached_data and data_key in cached_data and cached_data.get(data_key) is not None and \
                   real_mtime is not None and abs(real_mtime - cached_data.get('mtime', 0)) < 1e-6:
                    local_file_data[path] = cached_data
                    cache_hits += 1
                    self.completed_task_count += 1
                else:
                    paths_to_recalc.append(path)
                    if cached_data: local_file_data[path] = cached_data
            except FileNotFoundError:
                log_info(f"檔案在處理過程中被移除: {path}")
                cache_manager.remove_data(path)
                self.total_task_count = max(0, self.total_task_count - 1)
                continue

        if self.total_task_count > 0:
            log_info(f"快取檢查 - 命中: {cache_hits}/{len(current_task_list)} | 總進度: {self.completed_task_count}/{self.total_task_count}")
        
        if not paths_to_recalc:
            cache_manager.save_cache()
            return True, local_file_data

        user_proc_setting = self.config.get('worker_processes', 0)
        pool_size = max(1, min(user_proc_setting, cpu_count())) if user_proc_setting > 0 else max(1, min(cpu_count() // 2, 8))
        
        if not self.pool:
            self.pool = Pool(processes=pool_size)

        self._update_progress(text=f"⚙️ 使用 {pool_size} 進程計算 {len(paths_to_recalc)} 個新檔案...")
        
        worker_payloads = []
        is_qr_mode = worker_function.__name__ == '_pool_worker_detect_qr_code'
        if is_qr_mode:
            resize_size = self.config.get('qr_resize_size', 800)
            worker_payloads = [(path, resize_size) for path in paths_to_recalc]
            results_iterator = self.pool.imap_unordered(worker_function, worker_payloads)
        else:
            worker_payloads = paths_to_recalc
            results_iterator = self.pool.imap_unordered(worker_function, worker_payloads)

        for path, data in results_iterator:
            if self._check_control() in ['cancel', 'pause']:
                uncompleted_paths_count = len(paths_to_recalc) - self.completed_task_count
                log_info(f"檢測到控制信號。剩餘 {uncompleted_paths_count} 個任務未完成。")
                if self._check_control() == 'pause':
                    # imap_unordered makes it hard to know exactly which are left.
                    # A robust pause would require more complex logic.
                    # For now, we terminate and the user can resume the whole remaining set.
                    self.tasks_to_process = paths_to_recalc[self.completed_task_count:]

                self._cleanup_pool()
                return False, {}
            
            if data.get('error'):
                self.failed_tasks.append((path, data['error']))
            else:
                local_file_data.setdefault(path, {}).update(data)
                cache_manager.update_data(path, local_file_data[path])
            self.completed_task_count += 1

            if self.total_task_count > 0:
                current_progress = int(self.completed_task_count / self.total_task_count * 100)
                self._update_progress(p_type='progress', value=current_progress, text=f"⚙️ 計算{description}中... ({self.completed_task_count}/{self.total_task_count})")
        
        cache_manager.save_cache()
        return True, local_file_data

    def _build_phash_band_index(self, gallery_file_data: dict, bands=LSH_BANDS):
        """為 LSH 建立雜湊索引"""
        seg_bits = HASH_BITS // bands
        mask = (1 << seg_bits) - 1
        index = [defaultdict(list) for _ in range(bands)]
        for path, ent in gallery_file_data.items():
            phash_obj = ent.get('phash')
            if not phash_obj: continue
            try: v = int(str(phash_obj), 16)
            except (ValueError, TypeError): continue
            for b in range(bands):
                key = (v >> (b * seg_bits)) & mask
                index[b][key].append(path)
        return index

    def _lsh_candidates_for(self, ad_path: str, ad_hash_obj, index: list, bands=LSH_BANDS):
        """使用 LSH 索引為給定圖片尋找可能的匹配候選"""
        seg_bits = HASH_BITS // bands
        mask = (1 << seg_bits) - 1
        v = int(str(ad_hash_obj), 16)
        cand = set()
        for b in range(bands):
            key = (v >> (b * seg_bits)) & mask
            cand.update(index[b].get(key, []))
        if ad_path in cand:
            cand.remove(ad_path)
        return cand

    def _ensure_features(self, path: str, cache_mgr: 'ScannedImageCacheManager', need_hsv: bool = False, need_whash: bool = False) -> bool:
        """懶加載函式，只在需要時才計算圖片的 hsv 或 whash"""
        ent = self.file_data.get(path)
        if not ent:
            ent = cache_mgr.get_data(path) or {}
            self.file_data[path] = ent
        
        if 'avg_hsv' in ent and ent['avg_hsv'] is not None and isinstance(ent['avg_hsv'], list):
            try: ent['avg_hsv'] = tuple(ent['avg_hsv'])
            except (ValueError, TypeError): ent['avg_hsv'] = None

        has_hsv = 'avg_hsv' in ent and ent['avg_hsv'] is not None
        has_whash = 'whash' in ent and ent['whash'] is not None
        
        if (not need_hsv or has_hsv) and (not need_whash or has_whash):
            return True

        try:
            with _open_image_from_any_path(path) as img:
                if not img: raise IOError("無法開啟圖片")
                from PIL import ImageOps
                img = ImageOps.exif_transpose(img)
                if need_hsv and not has_hsv:
                    ent['avg_hsv'] = _avg_hsv(img)
                
                if need_whash and not has_whash and imagehash:
                    ent['whash'] = imagehash.whash(img, hash_size=8, mode='haar', remove_max_haar_ll=True)
            
            _, _, mtime = _get_file_stat(path)
            update_payload = {'mtime': mtime}
            if 'avg_hsv' in ent and ent.get('avg_hsv') is not None:
                update_payload['avg_hsv'] = list(ent['avg_hsv'])
            if 'whash' in ent and ent.get('whash') is not None:
                update_payload['whash'] = ent['whash']
            cache_mgr.update_data(path, update_payload)
            return True
        except Exception as e:
            log_error(f"懶加載特徵失敗: {path}: {e}")
            return False

    def _accept_pair_with_dual_hash(self, ad_hash_obj, g_hash_obj, ad_w_hash, g_w_hash) -> tuple[bool, float]:
        """雙重雜湊驗證，結合 pHash 和 wHash 進行更精確的比對"""
        sim_p = sim_from_hamming(ad_hash_obj - g_hash_obj, HASH_BITS)

        if sim_p < PHASH_FAST_THRESH: return False, sim_p
        if sim_p >= PHASH_STRICT_SKIP: return True, sim_p
        
        if not ad_w_hash or not g_w_hash: return False, sim_p
        
        sim_w = sim_from_hamming(ad_w_hash - g_w_hash, HASH_BITS)

        if sim_p >= 0.90:   ok = sim_w >= WHASH_TIER_1
        elif sim_p >= 0.88: ok = sim_w >= WHASH_TIER_2
        elif sim_p >= 0.85: ok = sim_w >= WHASH_TIER_3
        else:               ok = sim_w >= WHASH_TIER_4
        
        return (ok, min(sim_p, sim_w) if ok else sim_p)

    def _find_similar_images(self, scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        # 【修正】 ... | None -> Union[..., None]
        """相似圖片查找的主邏輯"""
        continue_processing, self.file_data = self._process_images_with_cache(self.tasks_to_process, scan_cache_manager, "目標雜湊", _pool_worker_process_image_phash_only, 'phash')
        if not continue_processing: return None
        
        gallery_data = self.file_data

        user_thresh_percent = self.config.get('similarity_threshold', 95.0)
        is_mutual_mode = self.config.get('comparison_mode') == 'mutual_comparison'
        ad_folder_path = self.config.get('ad_folder_path')

        ad_phash_set = set()
        if is_mutual_mode and self.config.get('enable_ad_cross_comparison', True):
            if ad_folder_path and os.path.isdir(ad_folder_path):
                log_info("[交叉比對] 功能已啟用，正在掃描並載入廣告庫...")
                ad_paths = [os.path.normpath(os.path.join(r, f)).lower() for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg','.webp'))]
                ad_cache = ScannedImageCacheManager(ad_folder_path, ad_folder_path, 'ad_comparison')
                
                _, ad_data_for_marking = self._process_images_with_cache(ad_paths, ad_cache, "預載入廣告庫", _pool_worker_process_image_phash_only, 'phash')

                if ad_data_for_marking:
                    for data in ad_data_for_marking.values():
                        if data and data.get('phash'): ad_phash_set.add(data['phash'])
                    log_info(f"[交叉比對] 成功從檔案預載入 {len(ad_phash_set)} 個廣告哈希。")
            
            if self.config.get('cross_comparison_include_bw', False) and imagehash:
                log_info("[交叉比對] 已啟用對純黑/純白圖片的智慧標記。")
                ad_phash_set.add(imagehash.hex_to_hash('0000000000000000')) # 純黑
                ad_phash_set.add(imagehash.hex_to_hash('ffffffffffffffff')) # 純白
        
        def lerp_strict_loose(p, s, l): return s + ((100.0 - max(80.0, min(100.0, float(p)))) / 20.0) * (l - s)
        color_gate_params = {
            'hue_deg_tol': lerp_strict_loose(user_thresh_percent, 18.0, 25.0), 'sat_tol': lerp_strict_loose(user_thresh_percent, 0.18, 0.25),
            'low_sat_value_tol': lerp_strict_loose(user_thresh_percent, 0.10, 0.15), 'low_sat_achroma_tol': lerp_strict_loose(user_thresh_percent, 0.12, 0.18),
            'low_sat_thresh': 0.18
        }
        log_info(f"[動態參數] 根據 UI 閥值 {user_thresh_percent:.1f}%，顏色閘門參數已動態設定。")
        
        ad_data, ad_cache_manager, leader_to_ad_group = {}, None, {}
        is_ad_mode = self.config['comparison_mode'] == 'ad_comparison'

        if is_ad_mode:
            ad_paths = [os.path.normpath(os.path.join(r, f)).lower() for r, _, fs in os.walk(ad_folder_path) for f in fs if f.lower().endswith(('.png','.jpg','.jpeg','.webp'))]
            ad_cache_manager = ScannedImageCacheManager(ad_folder_path, ad_folder_path, 'ad_comparison')
            continue_processing_ad, ad_data = self._process_images_with_cache(ad_paths, ad_cache_manager, "廣告圖片雜湊", _pool_worker_process_image_phash_only, 'phash')
            if not continue_processing_ad: return None
            
            self._update_progress(text="🔍 正在使用 LSH 高效預處理廣告庫...")
            ad_lsh_index = self._build_phash_band_index(ad_data)
            ad_path_to_leader = {p: p for p in ad_data}; ad_paths_sorted = sorted(list(ad_data.keys()))
            grouping_dist = hamming_from_sim(AD_GROUPING_THRESHOLD, HASH_BITS)
            for p1 in ad_paths_sorted:
                if ad_path_to_leader[p1] != p1: continue
                h1 = ad_data.get(p1, {}).get('phash')
                if not h1: continue
                for p2 in self._lsh_candidates_for(p1, h1, ad_lsh_index):
                    if p2 <= p1 or ad_path_to_leader[p2] != p2: continue
                    h2 = ad_data.get(p2, {}).get('phash')
                    if h2 and (h1 - h2) <= grouping_dist: ad_path_to_leader[p2] = ad_path_to_leader[p1]
            for path, leader in ad_path_to_leader.items(): leader_to_ad_group.setdefault(leader, []).append(path)
            ad_data_representatives = {p: d for p, d in ad_data.items() if p in leader_to_ad_group}
            self._update_progress(text=f"🔍 廣告庫預處理完成，找到 {len(ad_data_representatives)} 個獨立廣告組。")
        else: # is_mutual_mode
            ad_data_representatives = gallery_data.copy()

        phash_index = self._build_phash_band_index(gallery_data)
        temp_found_pairs, user_thresh = [], user_thresh_percent / 100.0
        inter_folder_only = self.config.get('enable_inter_folder_only', False) and is_mutual_mode
        stats = {'comparisons': 0, 'passed_phash': 0, 'passed_color': 0, 'entered_whash': 0, 'filtered_inter': 0}

        for i, (p1_path, p1_ent) in enumerate(ad_data_representatives.items()):
            if self._check_control() != 'continue': return None
            p1_p_hash = p1_ent.get('phash');
            if not p1_p_hash: continue
            
            candidate_paths = self._lsh_candidates_for(p1_path, p1_p_hash, phash_index)
            
            for p2_path in candidate_paths:
                if is_mutual_mode:
                    if p2_path <= p1_path: continue
                    p1_parent = os.path.dirname(p1_path) if not _is_virtual_path(p1_path) else os.path.dirname(_parse_virtual_path(p1_path)[0])
                    p2_parent = os.path.dirname(p2_path) if not _is_virtual_path(p2_path) else os.path.dirname(_parse_virtual_path(p2_path)[0])
                    if inter_folder_only and p1_parent == p2_parent: 
                        stats['filtered_inter'] += 1
                        continue

                if is_ad_mode and p2_path in ad_data: continue
                
                is_match, best_sim = False, 0.0
                ad_group = leader_to_ad_group.get(p1_path, [p1_path])
                
                for member_path in ad_group:
                    stats['comparisons'] += 1
                    h1 = (gallery_data if is_mutual_mode else ad_data).get(member_path, {}).get('phash')
                    h2 = gallery_data.get(p2_path, {}).get('phash')
                    if not h1 or not h2: continue
                    
                    sim_p = sim_from_hamming(h1 - h2, HASH_BITS)
                    if sim_p < PHASH_FAST_THRESH: continue
                    stats['passed_phash'] += 1
                    
                    cache1 = scan_cache_manager if is_mutual_mode else ad_cache_manager
                    if not self._ensure_features(member_path, cache1, need_hsv=True) or not self._ensure_features(p2_path, scan_cache_manager, need_hsv=True): continue
                    
                    hsv1 = self.file_data.get(member_path, {}).get('avg_hsv')
                    hsv2 = self.file_data.get(p2_path, {}).get('avg_hsv')
                    if not _color_gate(hsv1, hsv2, **color_gate_params): continue
                    stats['passed_color'] += 1

                    accepted, final_sim = True, sim_p
                    if sim_p < PHASH_STRICT_SKIP:
                        stats['entered_whash'] += 1
                        if not self._ensure_features(member_path, cache1, need_whash=True) or not self._ensure_features(p2_path, scan_cache_manager, need_whash=True): continue
                        w1 = self.file_data.get(member_path, {}).get('whash')
                        w2 = self.file_data.get(p2_path, {}).get('whash')
                        accepted, final_sim = self._accept_pair_with_dual_hash(h1, h2, w1, w2)
                    
                    if accepted and final_sim >= user_thresh:
                        is_match, best_sim = True, max(best_sim, final_sim)

                if is_match:
                    temp_found_pairs.append((p1_path, p2_path, f"{best_sim * 100:.1f}%"))

        found_items = []
        if is_mutual_mode:
            self._update_progress(text="🔄 正在合併相似群組...")
            path_to_leader = {}
            sorted_pairs = sorted([(min(p1, p2), max(p1, p2), sim) for p1, p2, sim in temp_found_pairs])

            for p1, p2, _ in sorted_pairs:
                l1 = path_to_leader.get(p1, p1)
                l2 = path_to_leader.get(p2, p2)
                if l1 != l2:
                    final_l = min(l1, l2)
                    path_to_leader[l1] = path_to_leader[l2] = final_l
                    path_to_leader[p1] = path_to_leader[p2] = final_l

            final_groups = defaultdict(list)
            all_paths_in_pairs = {p for pair in temp_found_pairs for p in pair[:2]}
            for path in all_paths_in_pairs:
                leader = path
                while leader in path_to_leader and path_to_leader[leader] != leader:
                    leader = path_to_leader[leader]
                final_groups[leader].append(path)

            if ad_phash_set:
                self._update_progress(text="🔄 正在與廣告庫進行交叉比對...")
                ad_match_percent = min(user_thresh_percent + 5.0, 99.0)
                ad_match_thresh = hamming_from_sim(ad_match_percent / 100.0, HASH_BITS)
                log_info(f"[交叉比對] 使用相似度閥值: >={ad_match_percent:.1f}% (漢明距離 <= {ad_match_thresh})")

                for leader, children in final_groups.items():
                    is_ad_like = False
                    group_hashes = {gallery_data.get(p,{}).get('phash') for p in [leader] + children if gallery_data.get(p,{}).get('phash')}
                    if any(h and any((h - ah) <= ad_match_thresh for ah in ad_phash_set) for h in group_hashes):
                        is_ad_like = True
                    
                    for child in sorted([p for p in children if p != leader]):
                        h1 = gallery_data.get(leader, {}).get('phash')
                        h2 = gallery_data.get(child, {}).get('phash')
                        if h1 and h2:
                            sim = sim_from_hamming(h1 - h2, HASH_BITS) * 100
                            value_str = f"{sim:.1f}%" + (" (似广告)" if is_ad_like else "")
                            found_items.append((leader, child, value_str))
            else:
                for leader, children in final_groups.items():
                    for child in sorted([p for p in children if p != leader]):
                        h1 = gallery_data.get(leader, {}).get('phash')
                        h2 = gallery_data.get(child, {}).get('phash')
                        if h1 and h2:
                            sim = sim_from_hamming(h1 - h2, HASH_BITS) * 100
                            found_items.append((leader, child, f"{sim:.1f}%"))
        else: # is_ad_mode
            results_by_leader = defaultdict(list)
            for leader, target, sim_str in temp_found_pairs:
                results_by_leader[leader].append((target, float(sim_str.replace('%','')), sim_str))
            for leader, targets in results_by_leader.items():
                for target, _, sim_str in sorted(targets, key=lambda x: x[1], reverse=True): 
                    found_items.append((leader, target, sim_str))
        
        log_performance("[完成] LSH 雙哈希比對階段")
        log_info("--- 比對引擎漏斗統計 ---")
        if stats['filtered_inter'] > 0: log_info(f"因“仅比对不同资料夹”而跳过: {stats['filtered_inter']:,} 次")
        log_info(f"廣告組展開後總比對次數: {stats['comparisons']:,}")
        passed_phash, passed_color, entered_whash = stats['passed_phash'], stats['passed_color'], stats['entered_whash']
        if stats['comparisons'] > 0: log_info(f"通過 pHash 快篩: {passed_phash:,} ({ (passed_phash/stats['comparisons']*100):.1f}%)")
        if passed_phash > 0: log_info(f" └─ 通過顏色過濾閘: {passed_color:,} ({ (passed_color/passed_phash*100):.1f}%)")
        if passed_color > 0: log_info(f"    └─ 進入 wHash 複核: {entered_whash:,} ({ (entered_whash/passed_color*100):.1f}%)")
        final_matches = len({(p1, p2) for p1, p2, _ in temp_found_pairs})
        if passed_color > 0: log_info(f"       └─ 最終有效匹配: {final_matches:,} ({ (final_matches/passed_color*100):.1f}%)")
        log_info("--------------------------")
        
        self.file_data = {**gallery_data, **ad_data}
        return found_items, self.file_data

    def _detect_qr_codes(self, scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        # 【修正】 ... | None -> Union[..., None]
        """QR Code 檢測的主邏輯"""
        continue_processing, self.file_data = self._process_images_with_cache(
            self.tasks_to_process, 
            scan_cache_manager, 
            "QR Code 檢測", 
            _pool_worker_detect_qr_code, 
            'qr_points'
        )
        if not continue_processing: return None
        
        found_qr_images = [(path, path, "QR Code 檢出") for path, data in self.file_data.items() if data and data.get('qr_points')]
        
        return found_qr_images, self.file_data