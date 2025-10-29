# ======================================================================
# 檔案：plugins/manga_deduplication/processor.py
# 目的：實現相似卷宗查找器，並與 BasePlugin v2.0 契約保持一致
# 版本：12.6.2 (相容性修正：更新函式簽名以匹配 BasePlugin v2.0)
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

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error, _norm_key

from processors.scanner import (
    ScannedImageCacheManager,
    get_files_to_process,
    _natural_sort_key,
)

from . import plugin_gui

class MangaDeduplicationPlugin(BasePlugin):
    def __init__(self):
        # 移除 self.ui_vars，因為它將由主程式傳入
        pass

    def get_id(self) -> str: return "manga_volume_deduplication_smart"
    def get_name(self) -> str: return "相似卷宗查找 (共享引擎版)"
    def get_description(self) -> str: return "呼叫核心掃描引擎，比對每個資料夾末尾N張圖片的指紋，找出相似的漫畫卷宗。"

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any], ui_vars: Dict) -> Optional['ttk.Frame']:
        if ttk is None: return None
        frame = plugin_gui.create_settings_frame(parent_frame, config, ui_vars)
        plugin_gui.load_settings(config, ui_vars)
        return frame

    def save_settings(self, config: Dict[str, Any], ui_vars: Dict) -> Dict[str, Any]:
        return plugin_gui.save_settings(config, ui_vars)

    @staticmethod
    def _tol_bits_from_slider(cfg: dict, default_pct: int = 100) -> int:
        s = int(cfg.get("similarity_threshold", default_pct))
        if s < 0: s = 0
        if s > 100: s = 100
        return int((100 - s) * 64 / 100)

    @staticmethod
    def _greedy_match_count(a: List["imagehash.ImageHash"], b: List["imagehash.ImageHash"], tol_bits: int) -> int:
        if not a or not b: return 0
        used = [False] * len(b)
        matched = 0
        for h1 in a:
            hit = -1
            for j, h2 in enumerate(b):
                if used[j]: continue
                try:
                    if (h1 - h2) <= tol_bits:
                        hit = j
                        break
                except Exception:
                    continue
            if hit >= 0:
                used[hit] = True
                matched += 1
        return matched

    def _process_images_batch(self, tasks: List[str], cache_manager: ScannedImageCacheManager, progress_queue: Optional[Queue], control_events: Optional[Dict]) -> Dict:
        from utils import _open_image_from_any_path, _get_file_stat, _norm_key, log_error
        
        results = {}
        if imagehash is None:
            raise RuntimeError("缺少 imagehash 套件，無法計算 pHash。")

        paths_to_recalc = []
        for p in tasks:
            key = _norm_key(p)
            _, _, mtime = _get_file_stat(p)
            if mtime is None: continue

            cached = cache_manager.get_data(key)
            need_recalc = True
            if cached and abs(cached.get('mtime', 0.0) - mtime) < 1e-6 and cached.get('phash'):
                ph = cached['phash']
                if isinstance(ph, str):
                    try:
                        ph = imagehash.hex_to_hash(ph)
                    except Exception:
                        ph = None
                if ph is not None:
                    try:
                        shape = getattr(ph, "hash", None).shape if hasattr(ph, "hash") else None
                    except Exception:
                        shape = None
                    if shape == (8, 8):
                        cached['phash'] = ph
                        results[key] = cached
                        need_recalc = False
            if need_recalc:
                paths_to_recalc.append(p)
                if cached: results[key] = cached
        
        if paths_to_recalc:
            total_recalc = len(paths_to_recalc)
            if progress_queue: progress_queue.put({'type': 'text', 'text': f'🔍 [外掛] 正在計算 {total_recalc} 個新檔案的指紋...'})
            
            for idx, p in enumerate(paths_to_recalc, 1):
                if control_events and control_events.get('cancel') and control_events['cancel'].is_set(): break
                
                key = _norm_key(p)
                size, ctime, mtime = _get_file_stat(p)
                if mtime is None: continue

                try:
                    with _open_image_from_any_path(p) as img:
                        if img is None: continue
                        ph = imagehash.phash(img, hash_size=8)
                        new_data = {
                            'phash': str(ph),
                            'phash_size': 8,
                            'mtime': mtime, 'size': size, 'ctime': ctime
                        }
                        cache_manager.update_data(key, new_data)
                        
                        new_data['phash'] = ph
                        results[key] = new_data
                except Exception as e:
                    log_error(f'[外掛] 哈希失敗: {p}: {e}')
        return results

    def _build_fingerprints_with_ad_filter(self, files_by_folder: Dict[str, List[str]], ad_hashes: Set[imagehash.ImageHash], sample_count: int, cache_manager: ScannedImageCacheManager, progress_queue: Optional[Queue], control_events: Optional[Dict]) -> Tuple[Dict, Dict]:
        fingerprints = {}
        folders_with_ads = {}
        total_folders = len(files_by_folder)
        
        all_files_to_process = set()
        for folder, files in files_by_folder.items():
            if not files: continue
            tail_span = min(len(files), sample_count * 2)
            all_files_to_process.update(files[-tail_span:])

        if progress_queue: progress_queue.put({'type': 'text', 'text': f'🔬 [外掛] 預處理 {len(all_files_to_process)} 個檔案的指紋...'})
        
        processed_data = self._process_images_batch(list(all_files_to_process), cache_manager, progress_queue, control_events)

        for i, (folder, files) in enumerate(files_by_folder.items()):
            if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
                return {}, {}
            
            if progress_queue: progress_queue.put({'type': 'text', 'text': f'🔬 [外掛] 正在建立資料夾指紋 {i+1}/{total_folders}'})

            fingerprint_hashes: List["imagehash.ImageHash"] = []
            has_ads = False
            
            for file_path in reversed(files):
                if len(fingerprint_hashes) >= sample_count: break

                norm_path = _norm_key(file_path)
                file_data = processed_data.get(norm_path)
                
                if file_data and file_data.get('phash'):
                    ph = file_data['phash']
                    if isinstance(ph, str):
                        try: ph = imagehash.hex_to_hash(ph)
                        except (TypeError, ValueError): continue

                    if ph in ad_hashes:
                        has_ads = True
                        continue
                    
                    if all((ph - old) != 0 for old in fingerprint_hashes):
                        fingerprint_hashes.append(ph)

            fingerprints[folder] = fingerprint_hashes
            if has_ads:
                folders_with_ads[folder] = True
        
        return fingerprints, folders_with_ads

    def run(self, config: Dict, progress_queue: Optional[Queue] = None, control_events: Optional[Dict] = None, app_update_callback: Optional[callable] = None) -> Optional[Tuple[List, Dict, List]]:
        _update_progress = lambda text, value=None: progress_queue.put({'type': 'progress' if value is not None else 'text', 'text': text, 'value': value}) if progress_queue else None
        _is_cancelled = lambda: bool(control_events and control_events.get('cancel') and control_events['cancel'].is_set())
        
        main_cache = None
        ad_cache = None
        
        try:
            log_info("[相似卷宗] 準備執行掃描 (共享核心引擎, v12.6)...")
            log_info(f"[外掛] 收到設定: similarity_threshold={config.get('similarity_threshold')}, "
                     f"plugin_sample_count={config.get('plugin_sample_count')}, "
                     f"plugin_match_threshold={config.get('plugin_match_threshold')}")

            _update_progress("🚀 [相似卷宗] 正在呼叫核心掃描引擎...", 0)
            
            ad_hashes_set = set()
            ad_folder_path = config.get('ad_folder_path')
            if ad_folder_path and os.path.isdir(ad_folder_path):
                log_info("[外掛] 正在載入廣告庫指紋...")
                _update_progress("📦 [外掛] 正在載入廣告庫...")
                ad_cache = ScannedImageCacheManager(ad_folder_path)
                ad_files = [os.path.join(root, f) for root, _, files in os.walk(ad_folder_path) for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]
                
                ad_data = self._process_images_batch(ad_files, ad_cache, None, control_events)
                for data in ad_data.values():
                    if data.get('phash'):
                        ph = data['phash']
                        if isinstance(ph, str):
                            try: ph = imagehash.hex_to_hash(ph)
                            except (TypeError, ValueError): continue
                        ad_hashes_set.add(ph)
                ad_cache.save_cache()
                log_info(f"[外掛] 成功載入 {len(ad_hashes_set)} 個廣告指紋。")

            plugin_scan_config = config.copy()
            plugin_scan_config['enable_extract_count_limit'] = False
            
            main_cache = ScannedImageCacheManager(config['root_scan_folder'])
            
            files_to_process, _ = get_files_to_process(plugin_scan_config, main_cache, progress_queue, control_events)
            
            if _is_cancelled(): 
                if progress_queue: progress_queue.put({'type':'status_update', 'text':'外掛任務已中止'})
                return None
            if not files_to_process:
                _update_progress("✅ [相似卷宗] 未找到任何圖片檔案。"); return [], {}, []

            files_by_folder = defaultdict(list)
            from utils import _is_virtual_path, _parse_virtual_path
            for f_path in files_to_process:
                container_key = ""
                if _is_virtual_path(f_path):
                    container_path, _ = _parse_virtual_path(f_path)
                    if container_path: container_key = _norm_key(container_path)
                else:
                    container_key = _norm_key(os.path.dirname(f_path))
                
                if container_key:
                    files_by_folder[container_key].append(f_path)
            
            for folder in files_by_folder:
                files_by_folder[folder].sort(key=_natural_sort_key)

            sample_count = int(config.get('plugin_sample_count', 12))
            MATCH_THRESHOLD = int(config.get("plugin_match_threshold", 8))

            fingerprints, folders_with_ads = self._build_fingerprints_with_ad_filter(
                files_by_folder, ad_hashes_set, sample_count, main_cache, progress_queue, control_events)

            tol_bits_dbg = self._tol_bits_from_slider(config, default_pct=100)
            lengths = sorted(len(v) for v in fingerprints.values())
            if lengths:
                p50 = lengths[len(lengths)//2]
                p10 = lengths[len(lengths)//10]
                p90 = lengths[(len(lengths)*9)//10]
                log_info(f"[外掛] pHash容忍度={tol_bits_dbg} 位；指紋長度統計 夾數={len(lengths)}, P10={p10}, P50={p50}, P90={p90}")
            else:
                log_info(f"[外掛] pHash容忍度={tol_bits_dbg} 位；無可用指紋")

            _update_progress("🔄 比对指纹...", 75)
            folder_list = sorted(list(fingerprints.keys()))
            found_items = []
            
            if len(folder_list) < 2:
                _update_progress("✅ [相似卷宗] 资料夹数量不足，无需比对。"); return [], {}, []

            for i in range(len(folder_list)):
                if _is_cancelled():
                    if progress_queue: progress_queue.put({'type':'status_update', 'text':'外掛任務已中止'})
                    return None
                
                _update_progress("🔄 比对指纹...", int(75 + (i/len(folder_list)) * 20))

                for j in range(i + 1, len(folder_list)):
                    path1, path2 = folder_list[i], folder_list[j]
                    fp1, fp2 = fingerprints[path1], fingerprints[path2]

                    if not fp1 or not fp2: continue

                    tol_bits = self._tol_bits_from_slider(config, default_pct=100)
                    intersection_size = self._greedy_match_count(fp1, fp2, tol_bits)
                    min_len = min(len(fp1), len(fp2))

                    if min_len == 0: continue
                    
                    current_threshold = MATCH_THRESHOLD
                    if min_len < sample_count:
                        current_threshold = min(MATCH_THRESHOLD, max(1, int(MATCH_THRESHOLD * (min_len / sample_count))))
                    
                    if intersection_size >= current_threshold:
                        has_ads_flag = " (已濾廣告)" if folders_with_ads.get(path1) or folders_with_ads.get(path2) else ""
                        similarity_str = f"{intersection_size}/{min_len} 頁相似{has_ads_flag}"
                        found_items.append((min(path1, path2), max(path1, path2), similarity_str))
            
            _update_progress("✅ 整理结果...", 95)
            gui_file_data = {}
            involved_paths = {p for item in found_items for p in item[:2]}
            for path in involved_paths:
                image_files_for_preview = files_by_folder.get(_norm_key(path))
                if image_files_for_preview:
                    gui_file_data[path] = {'display_path': sorted(image_files_for_preview, key=_natural_sort_key)[0]}
                else: 
                    gui_file_data[path] = {}

            log_info(f"[相似卷宗] 掃描完成，找到 {len(found_items)} 组相似项目。")
            _update_progress("✅ [相似卷宗] 完成！", value=100)
            return found_items, gui_file_data, []
        except Exception as e:
            log_error(f"[外掛] 执行期间发生严重错误: {e}", include_traceback=True)
            if progress_queue: progress_queue.put({'type':'text', 'text': f"❌ 错误: {e}"})
            return [], {}, [("外掛错误", str(e))]
        finally:
            if main_cache:
                main_cache.save_cache()
            if ad_cache:
                ad_cache.save_cache()