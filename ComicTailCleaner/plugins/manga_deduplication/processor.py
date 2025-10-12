# ======================================================================
# 檔案：plugins/manga_deduplication/processor.py
# 目的：實現相似卷宗查找器，並呼叫核心引擎進行檔案掃描
# 版本：11.1 (修正版：實現哈希計算與快取共享)
# ======================================================================

from __future__ import annotations
import os
import imagehash
from collections import defaultdict
from typing import Dict, Any, Tuple, List, Optional
from queue import Queue
from multiprocessing import Pool, cpu_count, set_start_method
import sys

try:
    from tkinter import ttk
except ImportError:
    ttk = None

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error, _norm_key
from processors.scanner import ScannedImageCacheManager
# 直接從核心引擎導入 get_files_to_process 和 _natural_sort_key
from core_engine import get_files_to_process, _natural_sort_key

from . import plugin_gui

class MangaDeduplicationPlugin(BasePlugin):
    def __init__(self):
        self.ui_vars = {}; self.pool = None

    def get_id(self) -> str: return "manga_volume_deduplication_smart"
    def get_name(self) -> str: return "相似卷宗查找 (共享引擎版)"
    def get_description(self) -> str: return "呼叫核心掃描引擎，比對每個資料夾末尾N張圖片的指紋，找出相似的漫畫卷宗。"

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any]) -> Optional['ttk.Frame']:
        if ttk is None: return None
        frame = plugin_gui.create_settings_frame(parent_frame, config, self.ui_vars)
        plugin_gui.load_settings(config, self.ui_vars)
        return frame

    def save_settings(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return plugin_gui.save_settings(config, self.ui_vars)

    def _process_images_with_cache(self, tasks: List[str], cache_manager: ScannedImageCacheManager, progress_queue: Optional[Queue], control_events: Optional[Dict]) -> Dict:
        from utils import _open_image_from_any_path, _get_file_stat, _norm_key, log_error
        try:
            import imagehash
        except ImportError:
            imagehash = None

        results = {}
        updated = 0
        total = len(tasks)

        if imagehash is None:
            raise RuntimeError("缺少 imagehash 套件，無法計算 pHash。")

        for idx, p in enumerate(tasks, 1):
            if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
                break

            key = _norm_key(p)
            size, ctime, mtime = _get_file_stat(p)
            if mtime is None: # _get_file_stat 回傳 (None, None, None) 表示檔案不存在
                continue

            cached = cache_manager.get_data(key)
            
            # 命中條件：mtime 不變、且已有 phash
            if cached and abs(cached.get('mtime', 0.0) - mtime) < 1e-6 and cached.get('phash'):
                results[key] = cached
            else:
                try:
                    with _open_image_from_any_path(p) as img:
                        if img is None:
                            continue
                        ph = str(imagehash.phash(img, hash_size=8)) # 64bit pHash
                        meta = {
                            'phash': ph,
                            'mtime': mtime,
                            'size': size,
                            'ctime': ctime,
                        }
                        cache_manager.update_data(key, meta)
                        results[key] = meta
                        updated += 1
                except Exception as e:
                    log_error(f'[外掛] 哈希失敗: {p}: {e}')

            if progress_queue and (idx % 100 == 0 or idx == total):
                progress_queue.put({'type': 'text', 'text': f'🔍 [外掛] 正在計算指紋 {idx}/{total} (更新 {updated})'})

        if updated > 0:
            cache_manager.save_cache()
        return results

    def run(self, config: Dict, progress_queue: Optional[Queue] = None, control_events: Optional[Dict] = None) -> Optional[Tuple[List, Dict, List]]:
        _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
        _is_cancelled = lambda: bool(control_events and control_events.get('cancel') and control_events['cancel'].is_set())
        
        try:
            log_info("[相似卷宗] 準備執行掃描 (共享核心引擎)...")
            _update_progress("🚀 [相似卷宗] 正在呼叫核心掃描引擎...", 0)

            plugin_scan_config = config.copy()
            plugin_scan_config['extract_count'] = int(config.get('plugin_sample_count', 12))
            plugin_scan_config['enable_extract_count_limit'] = config.get('plugin_enable_sample_limit', True)
            
            # --- 【v11.1 修正】 ---
            # 沿用主程式的共享影像快取命名（與模式無關）
            main_cache = ScannedImageCacheManager(config['root_scan_folder'])
            
            files_to_process, _ = get_files_to_process(plugin_scan_config, main_cache, progress_queue, control_events)
            
            if _is_cancelled(): 
                if progress_queue: progress_queue.put({'type':'status_update', 'text':'外掛任務已中止'})
                return None
            if not files_to_process:
                _update_progress("✅ [相似卷宗] 未找到任何圖片檔案。"); return [], {}, []

            file_data = self._process_images_with_cache(files_to_process, main_cache, progress_queue, control_events)
            if file_data is None: return None
            
            files_by_folder = defaultdict(list)
            for f_path in file_data:
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
                    limit = config.get('plugin_enable_sample_limit', True)
                    count = int(config.get('plugin_sample_count', 12))
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