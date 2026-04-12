# ======================================================================
# 檔案名稱：core_engine.py
# 模組目的：包含核心的比对引擎與增量更新邏輯
# 版本：2.6.0 (準確度升級：強制啟用 wHash 雙重驗證，消除 pHash 碰撞誤判)
# ======================================================================

import os
import re
import json
import time
import datetime
import sys
import hashlib
from collections import deque, defaultdict
from multiprocessing import Pool, cpu_count, Event, set_start_method
from queue import Queue
from typing import Union, Tuple, Dict, List, Set, Optional, Generator, Any


# --- 第三方库 ---
try:
    import imagehash
except ImportError:
    imagehash = None

# --- 本地模组 ---
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
                   _avg_hsv, _color_gate, _norm_key, _calculate_quick_digest)

try:
    from utils import log_warning
except ImportError:
    def log_warning(msg: str): print(f"[WARN] {msg}")

# 完整導入所有需要的掃描輔助函式
from processors.scanner import (
    ScannedImageCacheManager, 
    FolderStateCacheManager, 
    get_files_to_process,
    _iter_scandir_recursively,
    _natural_sort_key
)

try:
    from processors.qr_engine import (_pool_worker_detect_qr_code, 
                                     _pool_worker_process_image_full, 
                                     _pool_worker_process_image_phash_only)
    QR_ENGINE_ENABLED = True
except ImportError:
    utils.log_warning("[警告] 無法從 processors.qr_engine 導入 QR worker，QR 相關功能將不可用。")
    def _pool_worker_detect_qr_code(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    def _pool_worker_process_image_full(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    def _pool_worker_process_image_phash_only(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    QR_ENGINE_ENABLED = False

# ======================================================================
# Section: 全局常量
# ======================================================================
ENGINE_VERSION = "2.6.1"
HASH_BITS = 64
PHASH_FAST_THRESH   = 0.70
PHASH_STRICT_SKIP   = 0.93  # pHash 夠高時可跳過 wHash 覆核
WHASH_TIER_1        = 0.90
WHASH_TIER_2        = 0.92
WHASH_TIER_3        = 0.95
WHASH_TIER_4        = 0.98
WHASH_RELAXED_THRESH = 0.70  # pHash 及格時 wHash 門檻
WHASH_STRICT_THRESH  = 0.85  # wHash 單獨救援門檻 (需同時滿足 WHASH_MIN_PHASH)
WHASH_MIN_PHASH      = 0.80  # wHash 單獨救援時 pHash 最低下限 (防止無關圖片亂入)
AD_GROUPING_THRESHOLD = 0.95
LSH_BANDS = 8

# --- 快取特徵位元遮罩 ---
FEATURE_PHASH = 1 << 0
FEATURE_WHASH = 1 << 1
FEATURE_COLOR = 1 << 2
FEATURE_QR    = 1 << 3

# ======================================================================
# Section: 核心比對引擎
# ======================================================================

class ImageComparisonEngine:
    def __init__(self, config_dict: dict, progress_queue: Optional[Queue] = None, control_events: Optional[Dict] = None):
        self.config = config_dict; self.progress_queue = progress_queue; self.control_events = control_events
        self.system_qr_scan_capability = QR_ENGINE_ENABLED
        self.pool = None; self.file_data = {}; self.tasks_to_process = []
        self.total_task_count = 0; self.completed_task_count = 0; self.failed_tasks = []
        self.vpath_size_map = {}
        self.quarantine_list = set()
        
        self.scan_cache_manager = ScannedImageCacheManager(self.config.get('root_scan_folder'))
        ad_folder = self.config.get('ad_folder_path')
        self.ad_cache_manager = ScannedImageCacheManager(ad_folder) if ad_folder and os.path.isdir(ad_folder) else None
        
        log_performance("[初始化] 掃描引擎實例")

    def compute_phashes(self,
                        paths: List[str],
                        cache_manager: ScannedImageCacheManager,
                        label: str = "圖片指紋",
                        progress_scope: str = "global"
                        ) -> Tuple[bool, Dict[str, Any]]:
        from processors.qr_engine import _pool_worker_process_image_phash_only
        
        cont, data = self._process_images_with_cache(
            paths,
            cache_manager,
            label,
            _pool_worker_process_image_phash_only,
            'phash',
            progress_scope=progress_scope
        )
        return cont, data
        
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
            
    def _prepare_ad_catalog_state(self) -> Dict:
        ad_folder_path = self.config.get('ad_folder_path')
        if not ad_folder_path or not os.path.isdir(ad_folder_path):
            return {'catalog_digest': ''}

        state_file = os.path.join(ad_folder_path, 'ad_catalog_state.json')
        
        current_state = {'catalog_digest': '', 'manifest_digest': '', 'content_digest': '', 'params_digest': ''}
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    current_state.update(json.load(f))
            except (json.JSONDecodeError, IOError): pass

        comparison_params = {
            'similarity_threshold': self.config.get('similarity_threshold', 95.0),
            'engine_version': ENGINE_VERSION
        }
        params_digest = hashlib.sha256(json.dumps(comparison_params, sort_keys=True).encode()).hexdigest()

        manifest_items = []
        img_exts = ('.png','.jpg','.jpeg','.webp')
        for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
            if ent.is_file() and ent.name.lower().endswith(img_exts):
                try:
                    st = ent.stat(follow_symlinks=False)
                    rel_path = os.path.relpath(ent.path, ad_folder_path).replace('\\', '/')
                    manifest_items.append((rel_path, st.st_size, int(st.st_mtime)))
                except OSError:
                    continue
        
        manifest_items.sort()
        manifest_digest = hashlib.sha256(json.dumps(manifest_items).encode()).hexdigest()

        needs_rebuild = False
        if manifest_digest != current_state.get('manifest_digest'):
            log_info("[Digest] 檢測到廣告庫內容變更，將重新計算內容摘要。")
            needs_rebuild = True
            current_state['manifest_digest'] = manifest_digest
        
        if needs_rebuild:
            ad_cache = ScannedImageCacheManager(ad_folder_path)
            ad_paths = [os.path.join(ad_folder_path, item[0].replace('/', os.sep)) for item in manifest_items]
            _, ad_local_data = self._process_images_with_cache(ad_paths, ad_cache, "更新廣告庫哈希", _pool_worker_process_image_phash_only, 'phash', progress_scope='local')
            ad_cache.save_cache()
            ad_hashes = sorted([str(data['phash']) for data in ad_local_data.values() if data and data.get('phash')])
            content_digest = hashlib.sha256(json.dumps(ad_hashes).encode()).hexdigest()
            current_state['content_digest'] = content_digest
        
        final_digest = hashlib.sha256((current_state.get('content_digest', '') + params_digest).encode()).hexdigest()
        
        if final_digest != current_state.get('catalog_digest'):
            log_info(f"[Digest] 參數或內容已變更，生成新的 Catalog Digest: {final_digest[:8]}...")
            current_state['catalog_digest'] = final_digest
            current_state['params_digest'] = params_digest
            try:
                with open(state_file, 'w', encoding='utf-8') as f:
                    json.dump(current_state, f, indent=2)
            except IOError as e:
                log_error(f"無法更新廣告庫狀態檔案: {e}")

        log_info(f"[Digest] 當前 Catalog Digest: {current_state.get('catalog_digest', '')[:8]}...")
        return current_state

    def find_duplicates(self) -> Union[tuple[list, dict, list], None]:
        try:
            self._update_progress(text="任務開始..."); log_performance("[開始] 掃描任務")
            
            if self.config.get('enable_quarantine', True):
                try:
                    from config import QUARANTINE_FILE
                    with open(QUARANTINE_FILE, 'r', encoding='utf-8') as f:
                        self.quarantine_list = set(json.load(f))
                        if self.quarantine_list:
                            log_info(f"[隔離區] 成功載入 {len(self.quarantine_list)} 個已知錯誤檔案。")
                except (FileNotFoundError, json.JSONDecodeError):
                    self.quarantine_list = set()
            
            mode = self.config.get('comparison_mode', 'mutual_comparison').lower()
            ad_catalog_state = None
            if mode == 'ad_comparison' or (mode == 'mutual_comparison' and self.config.get('enable_ad_cross_comparison')):
                ad_catalog_state = self._prepare_ad_catalog_state()
            
            scan_cache_manager = self.scan_cache_manager
            
            try:
                mode_map = { "ad_comparison": "廣告比對", "mutual_comparison": "互相比對", "qr_detection": "QR Code 檢測" }
                mode_str = mode_map.get(mode, "未知")
                log_info("=" * 50)
                log_info(f"[引擎版本] 核心引擎 v{ENGINE_VERSION}")
                log_info(f"[模式檢查] 當前模式: {mode_str}")
                log_info(f"[模式檢查] - 時間篩選: {'啓用' if self.config.get('enable_time_filter', False) else '關閉'}")
                enable_limit = bool(self.config.get('enable_extract_count_limit', False))
                lim_n = int(self.config.get('extract_count', 0))
                if mode == 'qr_detection' and enable_limit: lim_n = int(self.config.get('qr_pages_per_archive', 10))
                log_info(f"[模式檢查] - 提取數量限制: {'啓用 ('+str(lim_n)+'張)' if enable_limit else '關閉'}")
                log_info(f"[模式檢查] 實際使用的圖片快取: {scan_cache_manager.cache_file_path}")
                log_info("=" * 50)
            except Exception as e: log_error(f"[模式檢查] 模式橫幅日誌生成失敗: {e}")

            initial_files, self.vpath_size_map = get_files_to_process(self.config, scan_cache_manager, self.progress_queue, self.control_events, quarantine_list=self.quarantine_list)
            
            if self._check_control() == 'cancel': return None

            self.tasks_to_process = initial_files
            self.total_task_count = len(self.tasks_to_process)
            
            if not self.tasks_to_process:
                self.progress_queue.put({'type': 'text', 'text':"在指定路徑下未找到任何符合條件的圖片檔案。"})
                return [], {}, []

            if mode == "qr_detection":
                if not QR_ENGINE_ENABLED:
                    log_error("QR 引擎不可用，無法執行 QR Code 檢測。")
                    return [], {}, [("系統錯誤", "QR 引擎未載入")]
                result = self._detect_qr_codes(scan_cache_manager)
            else:
                result = self._find_similar_images(scan_cache_manager, ad_catalog_state)
                
            if result is None: return None
            found, data = result
            return found, data, self.failed_tasks
        finally:
            if self.config.get('enable_quarantine', True) and self.failed_tasks:
                new_failures = {_norm_key(path) for path, error in self.failed_tasks}
                updated_quarantine = self.quarantine_list.union(new_failures)
                if updated_quarantine != self.quarantine_list:
                    log_info(f"[隔離區] 新增 {len(new_failures)} 個錯誤檔案到隔離區，總數: {len(updated_quarantine)}")
                    try:
                        from config import QUARANTINE_FILE
                        with open(QUARANTINE_FILE, 'w', encoding='utf-8') as f:
                            json.dump(list(updated_quarantine), f, ensure_ascii=False, indent=2)
                    except IOError as e:
                        log_error(f"無法寫入隔離區檔案: {e}")
            self._cleanup_pool()
            
    def _process_images_with_cache(self, current_task_list: list[str], cache_manager: ScannedImageCacheManager, description: str, worker_function: callable, data_key: str, progress_scope: str = 'global') -> tuple[bool, dict]:
        if not current_task_list: return True, {}
        local_file_data = {}
        time.sleep(self.config.get('ux_scan_start_delay', 0.1))
        self._update_progress(text=f"📂 正在檢查 {len(current_task_list)} 個{description}的快取...")
        
        local_total = len(current_task_list)
        local_completed = 0
        use_quick_digest = self.config.get('enable_quick_digest', True)
        
        paths_to_recalc, cache_hits = [], 0
        folders_to_rescan = set()
        paths_to_purge = set()

        for path in list(current_task_list):
            _, _, mt = _get_file_stat(path)
            
            if mt is None:
                cached_data_for_purge = cache_manager.get_data(path)
                if cached_data_for_purge:
                    paths_to_purge.add(path)
                
                parent_folder = os.path.dirname(path if not _is_virtual_path(path) else _parse_virtual_path(path)[0])
                if os.path.isdir(parent_folder):
                    folders_to_rescan.add(parent_folder)
                
                if progress_scope == 'global':
                    self.total_task_count = max(0, self.total_task_count - 1)
                else:
                    local_total = max(0, local_total - 1)
                continue

            cached_data = cache_manager.get_data(path)
            is_hit = False
            needs_qd64_upgrade = False

            if cached_data and abs(mt - float(cached_data.get('mtime', 0))) < 1e-6:
                if use_quick_digest:
                    if 'qd64' not in cached_data or not cached_data.get('qd64'):
                        is_hit = True
                        needs_qd64_upgrade = True
                    else:
                        qd64_now = _calculate_quick_digest(path)
                        if qd64_now and cached_data.get('qd64') == qd64_now:
                            is_hit = True
                else:
                    is_hit = True
            
            if is_hit:
                # [關鍵修正] 檢查快取中是否真的包含本次需要的特徵
                features = cached_data.get('features_at', 0)
                if data_key == 'phash' and not (features & FEATURE_PHASH):
                    is_hit = False
                elif data_key == 'whash' and not (features & FEATURE_WHASH):
                    is_hit = False
                elif data_key == 'avg_hsv' and not (features & FEATURE_COLOR):
                    is_hit = False
                elif data_key == 'qr_points' and not (features & FEATURE_QR):
                    is_hit = False

            if is_hit:
                for hash_key in ['phash', 'whash']:
                    if hash_key in cached_data and cached_data[hash_key] and not isinstance(cached_data[hash_key], imagehash.ImageHash):
                        try: cached_data[hash_key] = imagehash.hex_to_hash(str(cached_data[hash_key]))
                        except (TypeError, ValueError): cached_data[hash_key] = None
                
                local_file_data[path] = cached_data
                cache_hits += 1

                if use_quick_digest and needs_qd64_upgrade:
                    try:
                        qd64_now = _calculate_quick_digest(path)
                        if qd64_now:
                            cache_manager.update_data(_norm_key(path), {'qd64': qd64_now, 'mtime': mt})
                    except Exception as e:
                        log_warning(f"[快取升級] 寫入 qd64 失敗: {path}: {e}")

                if progress_scope == 'global':
                    self.completed_task_count += 1
                else:
                    local_completed += 1
            else:
                paths_to_recalc.append(path)
                if cached_data:
                    for hash_key in ['phash', 'whash']:
                        if hash_key in cached_data and cached_data[hash_key] and not isinstance(cached_data[hash_key], imagehash.ImageHash):
                            try: cached_data[hash_key] = imagehash.hex_to_hash(str(cached_data[hash_key]))
                            except (TypeError, ValueError): cached_data[hash_key] = None
                    local_file_data[path] = cached_data

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
                        if (
                            self.config.get("enable_archive_scan", False)
                            and archive_handler
                            and os.path.isfile(full_path)
                            and os.path.splitext(f_lower)[1] in ('.zip', '.cbz', '.rar', '.cbr', '.7z')
                        ):
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

        base_count = len(current_task_list)
        if progress_scope == 'global':
            if base_count > 0:
                log_info(f"快取檢查 - 命中: {cache_hits}/{base_count} | 總進度: {self.completed_task_count}/{self.total_task_count}")
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
        # v-MOD: 讀取進階特徵提取開關；尋親模式強制開啟兩者
        async_results, path_map = [], {}
        is_targeted = self.config.get('enable_targeted_search', False)
        use_rotation   = is_targeted or bool(self.config.get('enable_rotation_matching', False))
        use_preprocess = is_targeted or bool(self.config.get('enable_image_preprocess', False))
        
        for path in paths_to_recalc:
            worker_name = worker_function.__name__
            payload = path
            
            if 'full' in worker_name:
                payload = (path, int(self.config.get('qr_resize_size', 800)), use_rotation, use_preprocess)
            elif 'qr_code' in worker_name:
                payload = (path, int(self.config.get('qr_resize_size', 800)), bool(self.config.get('enable_qr_color_filter', False)))
            elif 'phash_only' in worker_name:
                payload = (path, use_rotation, use_preprocess)
            
            args_to_pass = payload if isinstance(payload, tuple) else (payload,)
            res = self.pool.apply_async(worker_function, args=args_to_pass)
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
                            if use_quick_digest:
                                data['qd64'] = _calculate_quick_digest(path)
                            
                            feature_bit = 0
                            if 'phash' in data: feature_bit |= FEATURE_PHASH
                            if 'whash' in data: feature_bit |= FEATURE_WHASH
                            if 'avg_hsv' in data: feature_bit |= FEATURE_COLOR
                            if 'qr_points' in data: feature_bit |= FEATURE_QR
                            
                            existing_data = cache_manager.get_data(path) or {}
                            data['features_at'] = existing_data.get('features_at', 0) | feature_bit
                            
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

        features_present = ent.get('features_at', 0)
        need_calc_hsv = need_hsv and not (features_present & FEATURE_COLOR)
        need_calc_whash = need_whash and not (features_present & FEATURE_WHASH)

        if not need_calc_hsv and not need_calc_whash: return True

        img = None
        try:
            from PIL import Image, ImageOps
            img = _open_image_from_any_path(path)
            if not img: raise IOError("無法開啟圖片")
            
            img = ImageOps.exif_transpose(img)
            from utils import _auto_crop_white_borders
            img = _auto_crop_white_borders(img)
            
            if need_calc_hsv: ent['avg_hsv'] = _avg_hsv(img)
            if need_calc_whash and imagehash: ent['whash'] = imagehash.whash(img, hash_size=8, mode='haar', remove_max_haar_ll=True)
            
            new_features = 0
            if need_calc_hsv: new_features |= FEATURE_COLOR
            if need_calc_whash: new_features |= FEATURE_WHASH
            ent['features_at'] = features_present | new_features
            
            _, _, mtime = _get_file_stat(path)
            update_payload = {'mtime': mtime, 'features_at': ent['features_at']}
            if 'avg_hsv' in ent and ent['avg_hsv'] is not None: update_payload['avg_hsv'] = list(ent['avg_hsv'])
            if 'whash' in ent and ent['whash'] is not None: update_payload['whash'] = str(ent['whash'])
            
            if self.config.get('enable_quick_digest', True):
                update_payload['qd64'] = _calculate_quick_digest(path)
            
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
        if h is None or imagehash is None:
            return None
        if isinstance(h, imagehash.ImageHash):
            return h
        try:
            return imagehash.hex_to_hash(str(h))
        except (TypeError, ValueError):
            return None

    def _accept_pair_with_dual_hash(self, ad_hash_obj, g_hash_obj, ad_w_hash, g_w_hash, ad_grid=None, g_grid=None) -> tuple[bool, float]:
        h1, h2 = self._coerce_hash_obj(ad_hash_obj), self._coerce_hash_obj(g_hash_obj)
        w1, w2 = self._coerce_hash_obj(ad_w_hash), self._coerce_hash_obj(g_w_hash)
        if not h1 or not h2: return False, 0.0
        
        sim_p = sim_from_hamming(h1 - h2, HASH_BITS)
        
        # --- Grid Hash (敗部復活 / 容錯覆寫) ---
        grid_rescue = False
        if ad_grid and g_grid and len(ad_grid) == 4 and len(g_grid) == 4:
            matched_blocks = 0
            for gb1, gb2 in zip(ad_grid, g_grid):
                gh1, gh2 = self._coerce_hash_obj(gb1), self._coerce_hash_obj(gb2)
                if gh1 and gh2 and sim_from_hamming(gh1 - gh2, HASH_BITS) >= 0.95:
                    matched_blocks += 1
            if matched_blocks >= 3:
                grid_rescue = True
                
        if sim_p < PHASH_FAST_THRESH and not grid_rescue: return False, sim_p
        
        user_t = float(self.config.get('similarity_threshold', 95.0)) / 100.0
        
        if not w1 or not w2: 
            if grid_rescue: return True, max(sim_p, max(user_t, 0.95))
            return (True, sim_p) if sim_p >= PHASH_STRICT_SKIP else (False, sim_p)
        
        sim_w = sim_from_hamming(w1 - w2, HASH_BITS)
        
        if grid_rescue:
            return True, max(sim_p, sim_w, max(user_t, 0.95))
        
        # wHash 覆核 (可由設定關閉)
        if not self.config.get('enable_whash', True):
            return (True, sim_p) if sim_p >= user_t else (False, sim_p)
        
        # 尋親模式允許寬鬆比對
        is_targeted = self.config.get('enable_targeted_search', False)
        if is_targeted:
            return True, max(sim_p, sim_w)
            
        # wHash 第三層漏斗：閾值隨 pHash 反向縮放
        # pHash=0.70 -> wHash 需>= 0.90 ; pHash=0.93 -> wHash 需>= 0.70
        whash_adaptive = 0.90 - max(0.0, min(1.0, (sim_p - 0.70) / 0.23)) * 0.20
        if sim_w >= whash_adaptive:
            return True, max(sim_p, sim_w)
        elif sim_p >= PHASH_STRICT_SKIP:  # pHash 極高時 wHash 實質冗餘，可跳過
            return True, sim_p
            
        return False, sim_p

    def _find_similar_images(self, scan_cache_manager: ScannedImageCacheManager, ad_catalog_state: Optional[Dict] = None) -> Union[tuple[list, dict], None]:
        tasks_to_process = self.tasks_to_process
        is_ad_mode = self.config.get('comparison_mode') == 'ad_comparison'
        current_digest = ""
        
        if is_ad_mode and ad_catalog_state:
            current_digest = ad_catalog_state.get('catalog_digest', "")
            
            unmatched_tasks = []
            for path in self.tasks_to_process:
                cached_data = scan_cache_manager.get_data(path)
                if not cached_data or cached_data.get('last_ad_digest', '') != current_digest:
                    unmatched_tasks.append(path)
            
            if len(unmatched_tasks) < len(self.tasks_to_process):
                log_info(f"[Digest] 總任務數: {len(self.tasks_to_process)}, 其中 {len(unmatched_tasks)} 個任務因 Digest 失效或為新檔案而需要比對。")
            
            tasks_to_process = unmatched_tasks
            self.completed_task_count = len(self.tasks_to_process) - len(tasks_to_process)
            self.total_task_count = len(self.tasks_to_process)

        continue_processing, self.file_data = self._process_images_with_cache(tasks_to_process, scan_cache_manager, "目標雜湊", _pool_worker_process_image_phash_only, 'phash', progress_scope='global')
        if not continue_processing: return None
        
        gallery_data = {k: v for k, v in self.file_data.items() if k in tasks_to_process}
        
        user_thresh_percent = self.config.get('similarity_threshold', 95.0)
        is_mutual_mode = self.config.get('comparison_mode') == 'mutual_comparison'
        ad_folder_path = self.config.get('ad_folder_path'); ad_phash_set = set()
        ad_data_for_marking = {}
        
        if is_mutual_mode and self.config.get('enable_ad_cross_comparison', True):
            if ad_folder_path and os.path.isdir(ad_folder_path):
                self._update_progress(text="📦 正在預處理廣告庫...（此階段為局部進度）")
                ad_paths = []
                for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
                    ad_paths.append(_norm_key(ent.path))

                ad_cache = ScannedImageCacheManager(ad_folder_path)

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
                
        def lerp(p, s, l): 
            weight = (100.0 - max(70.0, min(100.0, float(p)))) / 30.0
            return s + weight * (l - s)
            
        color_gate_params = { 
            'hue_deg_tol': lerp(user_thresh_percent, 18.0, 35.0), 
            'sat_tol': lerp(user_thresh_percent, 0.18, 0.40),
            'low_sat_value_tol': lerp(user_thresh_percent, 0.10, 0.30), 
            'low_sat_achroma_tol': lerp(user_thresh_percent, 0.12, 0.30),
            'low_sat_thresh': 0.18 
        }
        
        ad_data, ad_cache_manager, leader_to_ad_group = {}, None, {}
        
        if is_ad_mode:
            self._update_progress(text="📦 正在預處理廣告庫...（此階段為局部進度）")
            ad_paths = []
            for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
                ad_paths.append(_norm_key(ent.path))
            
            ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
            
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

        # =========================================================
        # v-MOD: 尋親模式 (Targeted Search) 專屬路徑
        # 假設廣告庫每張圖在掃描資料夾一定存在對應，
        # 暴力比對所有組合，只取每張廣告的全局最高分。
        # =========================================================
        if is_ad_mode and self.config.get('enable_targeted_search', False):
            log_info("[尋親模式] 已啟用，將為每張廣告尋找最佳配對（忽略門檻）。")
            targeted_pairs = []
            gallery_list = list(gallery_data.items())
            total_ads = len(ad_data_representatives)
            TARGETED_FLOOR_SIM = 0.40  # 最低可接受相似度，防止完全不相關的配對
            
            for ad_idx, (ad_path, ad_ent) in enumerate(ad_data_representatives.items()):
                if self._check_control() != 'continue': return None
                self._update_progress(
                    text=f"🔍 [尋親] 正在比對第 {ad_idx+1}/{total_ads} 張廣告..."
                )
                best_match_path = None
                best_match_sim = TARGETED_FLOOR_SIM  # 需要超過此下限才會被記錄

                # 取得廣告的所有旋轉指紋
                ad_h_base = self._coerce_hash_obj(ad_ent.get('phash'))
                ad_hashes = [ad_h_base] if ad_h_base else []
                rots = ad_ent.get('phash_rotations', {})
                for rot_key in ['90', '180', '270']:
                    rh = self._coerce_hash_obj(rots.get(rot_key))
                    if rh: ad_hashes.append(rh)
                
                if not ad_hashes: continue
                
                # 暴力對比掃描中每一張圖
                for scan_path, scan_ent in gallery_list:
                    if scan_path in ad_data: continue  # 跳過廣告圖本身
                    h2 = self._coerce_hash_obj(scan_ent.get('phash'))
                    if not h2: continue
                    
                    best_rot_sim = max(
                        (sim_from_hamming(h1 - h2, HASH_BITS) for h1 in ad_hashes if h1),
                        default=0.0
                    )
                    if best_rot_sim > best_match_sim:
                        best_match_sim = best_rot_sim
                        best_match_path = scan_path
                
                if best_match_path:
                    targeted_pairs.append((ad_path, best_match_path, f"{best_match_sim * 100:.1f}%"))
                    log_info(f"  [尋親] ✓ {os.path.basename(ad_path)} → {os.path.basename(best_match_path)} ({best_match_sim*100:.1f}%)")
                else:
                    log_info(f"  [尋親] ✗ {os.path.basename(ad_path)} → 無法找到高於 {TARGETED_FLOOR_SIM*100:.0f}% 的配對。")

            log_info(f"[尋親模式] 完成，共配對 {len(targeted_pairs)}/{total_ads} 張廣告。")
            return (targeted_pairs, {})
        # =========================================================
        # v-MOD END 尋親模式
        # =========================================================
            
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
                    
                    # --- 多角度指紋比對 (v-MOD) ---
                    # 獲取所有可能的旋轉組合
                    h1_rots = [h1]
                    if 'phash_rotations' in (ad_data if not is_mutual_mode else gallery_data).get(member_path, {}):
                        rots = (ad_data if not is_mutual_mode else gallery_data)[member_path]['phash_rotations']
                        h1_rots.extend([self._coerce_hash_obj(rots.get(k)) for k in ['90', '180', '270'] if k in rots])
                    
                    best_sim_p = 0.0
                    best_rot_key = '0' # 記錄哪個旋轉角度最匹配
                    for i, h1_cand in enumerate(h1_rots):
                        if not h1_cand: continue
                        cur_sim = sim_from_hamming(h1_cand - h2, HASH_BITS)
                        if cur_sim > best_sim_p:
                            best_sim_p = cur_sim
                            best_rot_key = ['0', '90', '180', '270'][i]
                    
                    sim_p = best_sim_p
                    
                    # --- 網格哈希也進行旋轉匹配 ---
                    grid1_all = [(ad_data if not is_mutual_mode else gallery_data).get(member_path, {}).get('grid_phash', [])]
                    if 'grid_rotations' in (ad_data if not is_mutual_mode else gallery_data).get(member_path, {}):
                        g_rots = (ad_data if not is_mutual_mode else gallery_data)[member_path]['grid_rotations']
                        grid1_all.extend([g_rots.get(k) for k in ['90', '180', '270'] if k in g_rots])
                    
                    grid2 = gallery_data.get(p2_path, {}).get('grid_phash', [])
                    grid_rescue = False
                    
                    # 嘗試所有旋轉的網格，只要有一個對上 3 塊就算成功
                    for g1_cand in grid1_all:
                        if grid_rescue: break
                        if not g1_cand or not grid2 or len(g1_cand) != 4 or len(grid2) != 4: continue
                        matched_blocks = 0
                        for gb1, gb2 in zip(g1_cand, grid2):
                            gh1, gh2 = self._coerce_hash_obj(gb1), self._coerce_hash_obj(gb2)
                            if gh1 and gh2 and sim_from_hamming(gh1 - gh2, HASH_BITS) >= 0.95:
                                matched_blocks += 1
                        if matched_blocks >= 3:
                            grid_rescue = True

                    if sim_p < PHASH_FAST_THRESH and not grid_rescue: continue
                    stats['passed_phash'] += 1
                    
                    if self.config.get('enable_color_filter', True):
                        cache1 = scan_cache_manager if is_mutual_mode else ad_cache_manager
                        if not self._ensure_features(member_path, cache1, need_hsv=True) or \
                           not self._ensure_features(p2_path, scan_cache_manager, need_hsv=True): continue
                        
                        hsv1 = self.file_data[_norm_key(member_path)].get('avg_hsv')
                        hsv2 = self.file_data[_norm_key(p2_path)].get('avg_hsv')
                        if grid_rescue:
                            # 即使是網格救援，也必須拒絕極端的亮度反轉 (如純白配純黑)
                            if hsv1 and hsv2 and abs(hsv1[2] - hsv2[2]) > 0.6:
                                continue
                        else:
                            if not _color_gate(hsv1, hsv2, **color_gate_params): continue
                    
                    stats['passed_color'] += 1

                    accepted, final_sim = False, sim_p
                    if grid_rescue:
                        accepted, final_sim = True, max(sim_p, max(user_thresh, 0.95))
                    else:
                        stats['entered_whash'] += 1
                        if not self.config.get('enable_whash', True):
                            # wHash 已關閉，只要 pHash 及格即接受
                            if sim_p >= user_thresh:
                                accepted, final_sim = True, sim_p
                        else:
                            cache1 = scan_cache_manager if is_mutual_mode else ad_cache_manager
                            if self._ensure_features(member_path, cache1, need_whash=True) and \
                               self._ensure_features(p2_path, scan_cache_manager, need_whash=True):
                                w1 = self._coerce_hash_obj(self.file_data[_norm_key(member_path)].get('whash'))
                                w2 = self._coerce_hash_obj(self.file_data[_norm_key(p2_path)].get('whash'))
                                if w1 and w2:
                                    sim_w = sim_from_hamming(w1 - w2, HASH_BITS)
                                    whash_adaptive = 0.90 - max(0.0, min(1.0, (sim_p - 0.70) / 0.23)) * 0.20
                                    if sim_w >= whash_adaptive:
                                        accepted, final_sim = True, max(sim_p, sim_w)
                            elif sim_p >= PHASH_STRICT_SKIP:
                                accepted = True

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
            
            if ad_data_for_marking:
                self._update_progress(text="🔄 正在與廣告庫進行交叉比對 (使用統一定義引擎)...")
                for leader, children in final_groups.items():
                    is_ad_like = False
                    
                    # 使用統一邏輯對 leader 進行廣告庫掃描
                    leader_ent = self.file_data.get(_norm_key(leader), {})
                    h2 = self._coerce_hash_obj(leader_ent.get('phash'))
                    grid2 = leader_ent.get('grid_phash')
                    
                    if h2:
                        for ad_path, ad_ent in ad_data_for_marking.items():
                            h1 = self._coerce_hash_obj(ad_ent.get('phash'))
                            grid1 = ad_ent.get('grid_phash')
                            if not h1: continue
                            
                            # --- 多角度廣告判定 (v-MOD) ---
                            h1_rots = [h1]
                            if 'phash_rotations' in ad_ent:
                                h1_rots.extend([self._coerce_hash_obj(ad_ent['phash_rotations'].get(k)) for k in ['90', '180', '270'] if k in ad_ent['phash_rotations']])
                            
                            sim_p = max([sim_from_hamming(can - h2, HASH_BITS) for can in h1_rots if can] + [0.0])
                            
                            grid1_all = [ad_ent.get('grid_phash', [])]
                            if 'grid_rotations' in ad_ent:
                                grid1_all.extend([ad_ent['grid_rotations'].get(k) for k in ['90', '180', '270'] if k in ad_ent['grid_rotations']])
                            
                            grid_rescue = False
                            for g1_cand in grid1_all:
                                if grid_rescue: break
                                if g1_cand and grid2 and len(g1_cand) == 4 and len(grid2) == 4:
                                    matched_blocks = 0
                                    for gb1, gb2 in zip(g1_cand, grid2):
                                        gh1, gh2 = self._coerce_hash_obj(gb1), self._coerce_hash_obj(gb2)
                                        if gh1 and gh2 and sim_from_hamming(gh1 - gh2, HASH_BITS) >= 0.95:
                                            matched_blocks += 1
                                    if matched_blocks >= 3: grid_rescue = True
                                
                            if sim_p < PHASH_FAST_THRESH and not grid_rescue: continue
                            
                            if self.config.get('enable_color_filter', True):
                                if not self._ensure_features(ad_path, ad_cache, need_hsv=True) or not self._ensure_features(leader, scan_cache_manager, need_hsv=True): continue
                                hsv1 = self.file_data[_norm_key(ad_path)].get('avg_hsv')
                                hsv2 = self.file_data[_norm_key(leader)].get('avg_hsv')
                                if grid_rescue:
                                    if hsv1 and hsv2 and abs(hsv1[2] - hsv2[2]) > 0.6: continue
                                else:
                                    if not _color_gate(hsv1, hsv2, **color_gate_params): continue
                                
                            accepted = False
                            if grid_rescue:
                                accepted = True
                            elif not self.config.get('enable_whash', True):
                                if sim_p >= user_thresh: accepted = True
                            else:
                                w1 = self._coerce_hash_obj(ad_ent.get('whash'))
                                w2 = self._coerce_hash_obj(leader_ent.get('whash'))
                                if w1 and w2:
                                    sim_w = sim_from_hamming(w1 - w2, HASH_BITS)
                                    whash_adaptive = 0.90 - max(0.0, min(1.0, (sim_p - 0.70) / 0.23)) * 0.20
                                    if sim_w >= whash_adaptive: accepted = True
                                elif sim_p >= PHASH_STRICT_SKIP: accepted = True
                                
                            if accepted:
                                is_ad_like = True
                                break

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
        
        full_gallery_data = self.file_data
        self.file_data = {**full_gallery_data, **ad_data}
        
        if is_ad_mode and current_digest:
            matched_comic_paths = {_norm_key(item[1]) for item in found_items}
            files_to_update_digest = [
                path for path in tasks_to_process 
                if _norm_key(path) not in matched_comic_paths
            ]
            
            log_info(f"[Digest] 正在為 {len(files_to_update_digest)} 個已處理且『清白』的圖片更新 Digest 標記...")
            for path in files_to_update_digest:
                norm_path = _norm_key(path)
                current_data = self.file_data.get(norm_path, {})
                
                patch = {
                    'last_ad_digest': current_digest,
                    'phash': str(current_data.get('phash')) if current_data.get('phash') else None,
                    'whash': str(current_data.get('whash')) if current_data.get('whash') else None,
                    'avg_hsv': list(current_data.get('avg_hsv')) if current_data.get('avg_hsv') else None,
                    'features_at': current_data.get('features_at', 0)
                }
                final_patch = {k: v for k, v in patch.items() if v is not None}
                scan_cache_manager.update_data(norm_path, final_patch)

            scan_cache_manager.save_cache()
            log_info("[Digest] 標記更新完成。")

        return found_items, self.file_data

    def _detect_qr_codes(self, scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        if self.config.get('enable_qr_hybrid_mode'):
            return self._detect_qr_codes_hybrid(self.tasks_to_process, scan_cache_manager)
        else:
            return self._detect_qr_codes_pure(self.tasks_to_process, scan_cache_manager)

    def _group_qr_results_by_phash(self, flat_qr_list: list, file_data: dict) -> list:
        """
        委派給 qr_engine.group_qr_results_by_phash()。
        門檻直接使用使用者在設定視窗中設定的相似度閾值（與廣告比對、互相比對同一條滑桿），
        語義一致：「達到 X% 相似度才算同一張廣告」。
        """
        from processors.qr_engine import group_qr_results_by_phash as _qr_group
        user_pct = float(self.config.get('similarity_threshold', 95.0))
        qr_thresh = user_pct / 100.0
        log_info(f"[QR 分組] 使用相似度門檻 {user_pct:.0f}%（與其他比對模式共用）")
        return _qr_group(flat_qr_list, file_data, sim_threshold=qr_thresh)



    def _detect_qr_codes_pure(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        log_info("[QR] 正在執行純粹掃描模式...")
        continue_processing, file_data = self._process_images_with_cache(
            files_to_process, scan_cache_manager, "QR Code 檢測", 
            _pool_worker_detect_qr_code, 'qr_points'
        )
        if not continue_processing: return None
        flat_qr = [(path, path, "🆕 新掃描 QR", "qr_item") for path, data in file_data.items() if data and data.get('qr_points')]
        self.file_data = file_data
        grouped = self._group_qr_results_by_phash(flat_qr, file_data)
        return grouped, self.file_data

    def _detect_qr_codes_hybrid(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        log_info("[QR] 正在執行混合掃描模式...")
        ad_folder_path = self.config.get('ad_folder_path')
        if not ad_folder_path or not os.path.isdir(ad_folder_path):
            log_info("[QR混合模式] 廣告資料夾無效，退回純粹掃描模式。")
            return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        
        self._update_progress(text="📦 正在預處理廣告庫...（此階段為局部進度）")
        ad_paths = []
        for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), self.control_events):
            ad_paths.append(_norm_key(ent.path))
        
        ad_cache_manager = ScannedImageCacheManager(ad_folder_path)
        
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
            ctrl = self._check_control()
            if ctrl == 'cancel': return None
            while ctrl == 'pause':
                time.sleep(0.5)
                ctrl = self._check_control()
                if ctrl == 'cancel': return None
            
            ad_p_hash = self._coerce_hash_obj(ad_ent.get('phash'))
            if not ad_p_hash: continue
            candidate_paths = self._lsh_candidates_for(ad_path, ad_p_hash, phash_index)
            for g_path in candidate_paths:
                if g_path in ad_with_phash: continue
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
                    found_ad_matches.append((ad_path, g_path, "已在此廣告分類", "ad_like_group"))
                    gallery_data.setdefault(g_path, {})['qr_points'] = ad_ent['qr_points']
                        
        matched_gallery_paths = {_norm_key(pair[1]) for pair in found_ad_matches}
        remaining_files_for_qr = [path for path in self.tasks_to_process if _norm_key(path) not in matched_gallery_paths]
        
        self._update_progress(text=f"🔍 對剩餘 {len(remaining_files_for_qr)} 個檔案進行 QR 掃描（局部進度）")
        if remaining_files_for_qr:
            ctrl = self._check_control()
            if ctrl == 'cancel': return None
            while ctrl == 'pause':
                time.sleep(0.5)
                ctrl = self._check_control()
                if ctrl == 'cancel': return None
            continue_proc_qr, qr_data = self._process_images_with_cache(
                remaining_files_for_qr, scan_cache_manager, "QR Code 檢測", 
                _pool_worker_detect_qr_code, 'qr_points', progress_scope='local'
            )
            if not continue_proc_qr: return None
            flat_qr = [(path, path, "🆕 新掃描 QR", "qr_item") for path, data in qr_data.items() if data and data.get('qr_points')]
            qr_results = self._group_qr_results_by_phash(flat_qr, qr_data)
            found_ad_matches.extend(qr_results)
            self.file_data.update(qr_data)
        scan_cache_manager.save_cache()
        ad_cache_manager.save_cache()
        return found_ad_matches, self.file_data