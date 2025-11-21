# ======================================================================
# 檔案：plugins/manga_deduplication/processor.py
# 目的：實現相似卷宗查找器 (v13.2 - 設定標準化)
# 版本：13.2.0 (設定重構：使用 'manga_dedupe_*' 前綴以避免命名衝突)
# ======================================================================

from __future__ import annotations
import os
import imagehash
from collections import defaultdict
from typing import Dict, Any, Tuple, List, Optional, Set
from queue import Queue

try:
    from tkinter import ttk
except ImportError:
    ttk = None

# --- 核心依賴 ---
from plugins.base_plugin import BasePlugin
from core_engine import ImageComparisonEngine, HASH_BITS
from processors.scanner import get_files_to_process, _natural_sort_key, ScannedImageCacheManager
from utils import log_info, log_error, _norm_key, log_warning, _is_virtual_path, _parse_virtual_path

# --- UI 依賴 ---
from . import plugin_gui

class MangaDeduplicationPlugin(BasePlugin):

    def get_id(self) -> str: return "manga_volume_deduplication_smart"
    def get_name(self) -> str: return "相似卷宗查找 (共享引擎版)"
    def get_description(self) -> str: return "呼叫核心掃描引擎，比對每個資料夾末尾N張圖片的指紋，找出相似的漫畫卷宗。"
    
    # --- v-MOD START: 使用新的標準化前綴 ---
    def get_default_config(self) -> Dict[str, Any]:
        """向主程式宣告此外掛的預設設定值。"""
        return {
            'manga_dedupe_enable_sample_limit': True,
            'manga_dedupe_sample_count': 12,
            'manga_dedupe_match_threshold': 8,
        }
    # --- v-MOD END ---

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any], ui_vars: Dict) -> Optional['ttk.Frame']:
        if ttk is None: return None
        return plugin_gui.create_settings_frame(parent_frame, config, ui_vars)

    def save_settings(self, config: Dict[str, Any], ui_vars: Dict) -> Dict[str, Any]:
        return plugin_gui.save_settings(config, ui_vars)

    @staticmethod
    def _tol_bits_from_slider(cfg: dict) -> int:
        s = int(cfg.get("similarity_threshold", 95))
        s_clamped = max(0, min(100, s))
        return int((100 - s_clamped) * HASH_BITS / 100)

    @staticmethod
    def _greedy_match_count(a: List["imagehash.ImageHash"], b: List["imagehash.ImageHash"], tol_bits: int) -> int:
        if not a or not b: return 0
        used = [False] * len(b)
        matched = 0
        for h1 in a:
            for j, h2 in enumerate(b):
                if not used[j] and (h1 - h2) <= tol_bits:
                    used[j] = True
                    matched += 1
                    break
        return matched
    
    def _coerce_hash_obj(self, h):
        if h is None or imagehash is None: return None
        if isinstance(h, imagehash.ImageHash): return h
        try: return imagehash.hex_to_hash(str(h))
        except (TypeError, ValueError): return None

    def run(self, config: Dict, progress_queue: Optional[Queue] = None, control_events: Optional[Dict] = None, app_update_callback: Optional[callable] = None) -> Optional[Tuple[List, Dict, List]]:
        _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
        _is_cancelled = lambda: bool(control_events and control_events.get('cancel') and control_events['cancel'].is_set())
        
        try:
            log_info("[相似卷宗] v13.2 - 核心整合版啟動...")

            # --- v-MOD START: 使用新的標準化鍵名讀取設定 ---
            sample_count = int(config.get('manga_dedupe_sample_count', 12))
            if sample_count < 2:
                log_warning(f"[外掛] '末尾圖片取樣數' ({sample_count}) 過小，強制使用安全值 8。")
                sample_count = 8
            
            MATCH_THRESHOLD = int(config.get("manga_dedupe_match_threshold", 8))
            if MATCH_THRESHOLD < 2:
                log_warning(f"[外掛] '指紋匹配閾值' ({MATCH_THRESHOLD}) 過小，強制使用安全值 5。")
                MATCH_THRESHOLD = 5
            # --- v-MOD END ---
            
            engine = ImageComparisonEngine(config, progress_queue, control_events)
            
            _update_progress("正在準備檔案列表...")
            files_to_process, _ = get_files_to_process(config, engine.scan_cache_manager, progress_queue, control_events)
            
            if _is_cancelled() or not files_to_process:
                return ([], {}, []) if not _is_cancelled() else None

            _update_progress("正在計算或讀取圖片指紋 (共享核心快取)...")
            continue_processing, all_file_data = engine.compute_phashes(
                files_to_process,
                engine.scan_cache_manager,
                "圖片指紋"
            )
            
            if not continue_processing: return None
            
            ad_hashes_set = set()
            ad_folder_path = config.get('ad_folder_path')
            if ad_folder_path and os.path.isdir(ad_folder_path):
                log_info("[外掛] 正在載入廣告庫指紋...")
                ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
                ad_paths = [ent.path for ent in os.scandir(ad_folder_path) if ent.is_file() and ent.name.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]
                
                _, ad_data = engine.compute_phashes(ad_paths, ad_cache_manager, "廣告庫指紋", progress_scope="local")
                
                ad_hashes_set = {self._coerce_hash_obj(data.get('phash')) for data in ad_data.values() if data and data.get('phash')}
                ad_hashes_set.discard(None)
                log_info(f"[外掛] 成功從廣告庫載入 {len(ad_hashes_set)} 個過濾指紋。")

            _update_progress("正在建立資料夾指紋...")
            files_by_folder = defaultdict(list)
            for f_path in files_to_process:
                container_key = _norm_key(os.path.dirname(f_path)) if not _is_virtual_path(f_path) else _norm_key(_parse_virtual_path(f_path)[0])
                if container_key: files_by_folder[container_key].append(f_path)
            
            fingerprints = {}
            for folder, files in files_by_folder.items():
                if _is_cancelled(): return None
                files.sort(key=_natural_sort_key)
                fp = []
                for f in reversed(files):
                    if len(fp) >= sample_count: break
                    data = all_file_data.get(_norm_key(f))
                    if data and 'phash' in data:
                        phash = self._coerce_hash_obj(data['phash'])
                        if phash and phash not in ad_hashes_set:
                            fp.append(phash)
                fingerprints[folder] = fp

            _update_progress("正在比對相似卷宗...")
            folder_list = sorted(list(fingerprints.keys()))
            found_items = []
            tol_bits = self._tol_bits_from_slider(config)

            for i in range(len(folder_list)):
                if _is_cancelled(): return None
                for j in range(i + 1, len(folder_list)):
                    path1, path2 = folder_list[i], folder_list[j]
                    fp1, fp2 = fingerprints[path1], fingerprints[path2]
                    
                    if not fp1 or not fp2: continue
                    
                    match_count = self._greedy_match_count(fp1, fp2, tol_bits)
                    min_len = min(len(fp1), len(fp2))
                    
                    if match_count >= MATCH_THRESHOLD:
                        similarity_str = f"{match_count}/{min_len} 頁相似"
                        found_items.append((min(path1, path2), max(path1, path2), similarity_str))

            gui_file_data = {}
            involved_paths = {p for item in found_items for p in item[:2]}
            for path in involved_paths:
                image_files = files_by_folder.get(_norm_key(path))
                if image_files:
                    gui_file_data[path] = {'display_path': image_files[0]}
            
            log_info(f"[相似卷宗] 掃描完成，找到 {len(found_items)} 組相似項目。")
            return found_items, gui_file_data, []

        except Exception as e:
            log_error(f"[外掛] 執行期間發生嚴重錯誤: {e}", include_traceback=True)
            return [], {}, [("外掛錯誤", str(e))]