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
import threading
from multiprocessing import Pool, set_start_method
from os import cpu_count
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
from core.cache_flow import CacheFlowMixin
from core.similarity_flow import SimilarityFlowMixin

try:
    from processors.qr_engine import (_pool_worker_detect_qr_code,
                                     _pool_worker_detect_qr_colorful_only,
                                     _pool_worker_process_image_full,
                                     _pool_worker_process_image_phash_only,
                                     _pool_worker_ensure_image_features)
    QR_ENGINE_ENABLED = True
except ImportError:
    utils.log_warning("[警告] 無法從 processors.qr_engine 導入 QR worker，QR 相關功能將不可用。")
    def _pool_worker_detect_qr_code(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    def _pool_worker_detect_qr_colorful_only(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    def _pool_worker_process_image_full(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    def _pool_worker_process_image_phash_only(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
    def _pool_worker_ensure_image_features(*args, **kwargs): return (args[0] if args else '', {'error': 'QR Engine not loaded'})
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

def _pool_initializer(lock):
    import utils
    utils.SHARED_IO_LOCK = lock

class ImageComparisonEngine(CacheFlowMixin, SimilarityFlowMixin):
    def __init__(self, config_dict: dict, progress_queue=None, control_events: Optional[Dict] = None):
        self.config = config_dict; self.progress_queue = progress_queue; self.control_events = control_events
        self.system_qr_scan_capability = QR_ENGINE_ENABLED
        self.pool = None; self.file_data = {}; self.tasks_to_process = []
        self.total_task_count = 0; self.completed_task_count = 0; self.failed_tasks = []
        self.vpath_size_map = {}
        self.quarantine_list = set()
        self.cache_stats = {'hit': 0, 'recalc': 0, 'purge': 0, 'rescan_folders': 0}
        
        self.scan_cache_manager = ScannedImageCacheManager(self.config.get('root_scan_folder'))
        ad_folder = self.config.get('ad_folder_path')
        from processors.scanner import MasterAdCacheManager
        self.ad_cache_manager = MasterAdCacheManager(ad_folder) if ad_folder and os.path.isdir(ad_folder) else None
        
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
        
    def _update_progress(self, p_type: str = 'text', value: Union[int, None] = None, text: str = None) -> None:
        if self.progress_queue: self.progress_queue.put({'type': p_type, 'value': value, 'text': text})
        
    def _cleanup_pool(self):
        if self.pool:
            log_info("正在終結現有進程池...");
            if self.progress_queue: self.progress_queue.put({'type': 'status_update', 'text': "正在終止背景任務..."})
            self.pool.terminate(); self.pool.join()
            log_info("進程池已成功終結。"); self.pool = None
            if hasattr(self, 'manager') and self.manager:
                try: self.manager.shutdown(); self.manager = None
                except Exception: pass

    def _normalize_cached_hashes(self, cached_data: Optional[dict]) -> Optional[dict]:
        if not cached_data:
            return cached_data
        for hash_key in ['phash', 'whash']:
            value = cached_data.get(hash_key)
            if value and not isinstance(value, imagehash.ImageHash):
                try:
                    cached_data[hash_key] = imagehash.hex_to_hash(str(value))
                except (TypeError, ValueError, AttributeError):
                    cached_data[hash_key] = None
        return cached_data

    def _build_worker_payload(self, worker_function: callable, path: str):
        worker_name = worker_function.__name__
        is_targeted = self.config.get('enable_targeted_search', False)
        use_rotation = is_targeted or bool(self.config.get('enable_rotation_matching', False))
        use_preprocess = is_targeted or bool(self.config.get('enable_image_preprocess', False))
        use_qr_filter = bool(self.config.get('enable_qr_color_filter', False))

        if 'full' in worker_name:
            return (
                path,
                int(self.config.get('qr_resize_size', 800)),
                use_qr_filter,
                use_rotation,
                use_preprocess,
            )
        if 'qr_code' in worker_name:
            return (
                path,
                int(self.config.get('qr_resize_size', 800)),
                use_qr_filter,
            )
        if 'phash_only' in worker_name:
            return (
                path,
                use_rotation,
                use_preprocess,
            )
        return path

    @staticmethod
    def _feature_bits_from_result(data: dict) -> int:
        feature_bit = 0
        if 'phash' in data:
            feature_bit |= FEATURE_PHASH
        if 'whash' in data:
            feature_bit |= FEATURE_WHASH
        if 'avg_hsv' in data:
            feature_bit |= FEATURE_COLOR
        if 'is_colorful' in data:
            feature_bit |= FEATURE_COLOR
        if 'qr_points' in data:
            feature_bit |= FEATURE_QR
        return feature_bit

    @staticmethod
    def _feature_bits_from_entry(data: Optional[dict]) -> int:
        if not data:
            return 0
        feature_bit = 0
        if data.get('phash'):
            feature_bit |= FEATURE_PHASH
        if data.get('whash'):
            feature_bit |= FEATURE_WHASH
        if data.get('avg_hsv') is not None or 'is_colorful' in data:
            feature_bit |= FEATURE_COLOR
        if 'qr_points' in data:
            feature_bit |= FEATURE_QR
        return feature_bit

    def _collect_file_mtimes(self, current_task_list: list[str], description: str) -> dict[str, Optional[float]]:
        file_mtimes = {}
        target_dirs = defaultdict(list)

        for path in current_task_list:
            if _is_virtual_path(path):
                _, _, mt = _get_file_stat(path)
                file_mtimes[path] = mt
            else:
                target_dirs[os.path.dirname(path)].append(path)

        scandir_count = 0
        total_dirs = len(target_dirs)
        for folder, paths_in_folder in target_dirs.items():
            if self._check_control() == 'cancel':
                return {}
            scandir_count += 1
            if scandir_count % 500 == 0:
                self._update_progress(text=f"📂 正在加速盤點 {len(current_task_list)} 個{description}的快取... ({scandir_count}/{total_dirs})")

            try:
                with os.scandir(folder) as it:
                    entries = {e.name.lower(): e for e in it if e.is_file(follow_symlinks=False)}

                for path in paths_in_folder:
                    bname = os.path.basename(path).lower()
                    if bname in entries:
                        try:
                            file_mtimes[path] = entries[bname].stat(follow_symlinks=False).st_mtime
                        except OSError:
                            file_mtimes[path] = None
                    else:
                        file_mtimes[path] = None
            except OSError:
                for path in paths_in_folder:
                    file_mtimes[path] = None

        return file_mtimes

    def _is_cached_feature_hit(
        self,
        cached_data: Optional[dict],
        mt: Optional[float],
        path: str,
        data_key: str,
        use_quick_digest: bool,
    ) -> tuple[bool, bool]:
        if mt is None or not cached_data:
            return False, False
        if abs(mt - float(cached_data.get('mtime', 0))) >= 1e-6:
            return False, False

        needs_qd64_upgrade = False
        is_hit = False

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

        if not is_hit:
            return False, False

        features = cached_data.get('features_at', 0) | self._feature_bits_from_entry(cached_data)
        if data_key == 'phash' and not (features & FEATURE_PHASH):
            return False, False
        if data_key == 'whash' and not (features & FEATURE_WHASH):
            return False, False
        if data_key == 'avg_hsv' and not (features & FEATURE_COLOR):
            return False, False
        if data_key == 'is_colorful' and 'is_colorful' not in cached_data:
            return False, False
        if data_key == 'qr_points' and not (features & FEATURE_QR):
            return False, False
        return True, needs_qd64_upgrade

    def _purge_stale_cache_entries(self, paths_to_purge: set[str], cache_manager: ScannedImageCacheManager) -> None:
        if not paths_to_purge:
            return
        for stale_path in paths_to_purge:
            cache_manager.remove_data(stale_path)
        log_info(f"[快取] 已清除 {len(paths_to_purge)} 筆失效圖片快取。")

    def _expand_paths_from_rescan_folders(
        self,
        folders_to_rescan: set[str],
        paths_to_recalc: list[str],
        local_file_data: dict,
        progress_scope: str,
        local_total: int,
    ) -> int:
        if not folders_to_rescan:
            return local_total

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
        return local_total

    def _get_entry_from_cache(self, path: str, cache_mgr: ScannedImageCacheManager) -> dict:
        norm_path = _norm_key(path)
        ent = self.file_data.get(norm_path)
        if ent is None:
            ent = cache_mgr.get_data(norm_path) or {}
            self.file_data[norm_path] = ent
        ent['phash'] = self._coerce_hash_obj(ent.get('phash'))
        ent['whash'] = self._coerce_hash_obj(ent.get('whash'))
        if isinstance(ent.get('avg_hsv'), list):
            try:
                ent['avg_hsv'] = tuple(float(x) for x in ent['avg_hsv'])
            except (ValueError, TypeError):
                ent['avg_hsv'] = None
        return ent
            
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

    def _load_quarantine_list(self) -> None:
        self.quarantine_list = set()
        if not self.config.get('enable_quarantine', True):
            return

        try:
            from config import QUARANTINE_FILE
            with open(QUARANTINE_FILE, 'r', encoding='utf-8') as f:
                self.quarantine_list = set(json.load(f))
                if self.quarantine_list:
                    log_info(f"[隔離區] 成功載入 {len(self.quarantine_list)} 個已知錯誤檔案。")
        except (FileNotFoundError, json.JSONDecodeError):
            self.quarantine_list = set()

    def _persist_quarantine_failures(self) -> None:
        if not self.config.get('enable_quarantine', True) or not self.failed_tasks:
            return

        new_failures = {_norm_key(path) for path, error in self.failed_tasks}
        updated_quarantine = self.quarantine_list.union(new_failures)
        if updated_quarantine == self.quarantine_list:
            return

        log_info(f"[隔離區] 新增 {len(new_failures)} 個錯誤檔案到隔離區，總數: {len(updated_quarantine)}")
        try:
            from config import QUARANTINE_FILE
            with open(QUARANTINE_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(updated_quarantine), f, ensure_ascii=False, indent=2)
        except IOError as e:
            log_error(f"無法寫入隔離區檔案: {e}")

    def _get_comparison_mode(self) -> str:
        return self.config.get('comparison_mode', 'mutual_comparison').lower()

    def _prepare_ad_catalog_for_mode(self, mode: str) -> Optional[Dict]:
        if mode == 'ad_comparison' or (mode == 'mutual_comparison' and self.config.get('enable_ad_cross_comparison')):
            return self._prepare_ad_catalog_state()
        return None

    def _log_mode_banner(self, mode: str, scan_cache_manager: ScannedImageCacheManager) -> None:
        try:
            mode_map = { "ad_comparison": "廣告比對", "mutual_comparison": "互互相對", "qr_detection": "QR Code 檢測" }
            mode_str = mode_map.get(mode, "未知")
            log_info("=" * 50)
            log_info(f"[引擎版本] 核心引擎 v{ENGINE_VERSION}")
            log_info(f"[模式檢查] 當前模式: {mode_str}")
            log_info(f"[模式檢查] - 時間篩選: {'啓用' if self.config.get('enable_time_filter', False) else '關閉'}")
            enable_limit = bool(self.config.get('enable_extract_count_limit', False))
            lim_n = int(self.config.get('extract_count', 0))
            if mode == 'qr_detection' and enable_limit:
                lim_n = int(self.config.get('qr_pages_per_archive', 10))
            log_info(f"[模式檢查] - 提取數量限制: {'啓用 ('+str(lim_n)+'張)' if enable_limit else '關閉'}")
            log_info(f"[模式檢查] 實際使用的圖片快取: {scan_cache_manager.cache_file_path}")
            log_info("=" * 50)
        except Exception as e:
            log_error(f"[模式檢查] 模式橫幅日誌生成失敗: {e}")

    def _prepare_scan_tasks(self, scan_cache_manager: ScannedImageCacheManager) -> bool:
        initial_files, self.vpath_size_map = get_files_to_process(
            self.config,
            scan_cache_manager,
            self.progress_queue,
            self.control_events,
            quarantine_list=self.quarantine_list,
        )
        if self._check_control() == 'cancel':
            return False

        self.tasks_to_process = initial_files
        self.total_task_count = len(self.tasks_to_process)
        return True

    def _dispatch_comparison_mode(
        self,
        mode: str,
        scan_cache_manager: ScannedImageCacheManager,
        ad_catalog_state: Optional[Dict],
    ) -> Union[tuple[list, dict], tuple[list, dict, list], None]:
        if not self.tasks_to_process:
            self.progress_queue.put({'type': 'text', 'text':"在指定路徑下未找到任何符合條件的圖片檔案。"})
            return [], {}, []

        if mode == "qr_detection":
            if not QR_ENGINE_ENABLED:
                log_error("QR 引擎不可用，無法執行 QR Code 檢測。")
                return [], {}, [("系統錯誤", "QR 引擎未載入")]
            return self._detect_qr_codes(scan_cache_manager)

        return self._find_similar_images(scan_cache_manager, ad_catalog_state)

    def _normalize_mode_result(self, result) -> Union[tuple[list, dict, list], None]:
        if result is None:
            return None
        if len(result) == 3:
            return result
        found, data = result
        return found, data, self.failed_tasks

    def _run_scan_orchestration(self) -> Union[tuple[list, dict, list], None]:
        self._update_progress(text="任務開始...")
        log_performance("[開始] 掃描任務")
        self._load_quarantine_list()

        mode = self._get_comparison_mode()
        ad_catalog_state = self._prepare_ad_catalog_for_mode(mode)
        scan_cache_manager = self.scan_cache_manager

        self._log_mode_banner(mode, scan_cache_manager)

        if not self._prepare_scan_tasks(scan_cache_manager):
            return None

        result = self._dispatch_comparison_mode(mode, scan_cache_manager, ad_catalog_state)
        return self._normalize_mode_result(result)

    def find_duplicates(self) -> Union[tuple[list, dict, list], None]:
        try:
            return self._run_scan_orchestration()
        finally:
            self._persist_quarantine_failures()
            self._cleanup_pool()

    def _ensure_features(self, path: str, cache_mgr: ScannedImageCacheManager, need_hsv: bool = False, need_whash: bool = False) -> bool:
        norm_path = _norm_key(path)
        ent = self._get_entry_from_cache(path, cache_mgr)

        features_present = ent.get('features_at', 0) | self._feature_bits_from_entry(ent)
        if features_present != ent.get('features_at', 0):
            ent['features_at'] = features_present
        need_calc_hsv = need_hsv and not (features_present & FEATURE_COLOR)
        need_calc_whash = need_whash and not (features_present & FEATURE_WHASH)

        if not need_calc_hsv and not need_calc_whash:
            cache_mgr.update_data(norm_path, {'features_at': features_present})
            return True

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
                try: img.close()
                except Exception: pass

    def _batch_ensure_features(self, paths: list, cache_mgr_map: dict,
                                need_hsv: bool = False, need_whash: bool = False,
                                phase_name: str = "特徵") -> int:
        flag_hsv   = FEATURE_COLOR
        flag_whash = FEATURE_WHASH
        to_load = []
        for p in dict.fromkeys(paths):
            norm_p = _norm_key(p)
            cache_mgr = cache_mgr_map.get(norm_p)
            ent = self.file_data.get(norm_p) or (self._get_entry_from_cache(p, cache_mgr) if cache_mgr else {})
            features_present = ent.get('features_at', 0) | self._feature_bits_from_entry(ent)
            if features_present != ent.get('features_at', 0) and cache_mgr:
                ent['features_at'] = features_present
                cache_mgr.update_data(norm_p, {'features_at': features_present})
            still_need = (need_hsv   and not (features_present & flag_hsv)) or \
                         (need_whash and not (features_present & flag_whash))
            if still_need: to_load.append(p)

        if not to_load:
            log_info(f"[批次順序讀取] {phase_name} 無需補算，全部命中快取。")
            return 0

        def _folder_sort_key(p):
            if _is_virtual_path(p):
                arc, inner = _parse_virtual_path(p)
                return (arc or p, inner or '')
            return (os.path.dirname(p), os.path.basename(p))

        to_load.sort(key=_folder_sort_key)
        log_info(f"[批次順序讀取] 開始載入 {len(to_load)} 個路徑的「{phase_name}」特徵（順序讀）...")
        self._update_progress(text=f"🔄 批次順序載入 {phase_name} 特徵 ({len(to_load)} 個)...")

        if QR_ENGINE_ENABLED and len(to_load) >= 2:
            try:
                return self._batch_ensure_features_parallel(
                    to_load,
                    cache_mgr_map,
                    need_hsv=need_hsv,
                    need_whash=need_whash,
                    phase_name=phase_name,
                )
            except Exception as e:
                log_error(f"[lazy feature] parallel {phase_name} failed; falling back to serial. {e}", include_traceback=True)

        calculated = 0; hb_interval = 30; last_hb = time.time()
        touched_cache_managers = set()
        for idx, p in enumerate(to_load):
            cache_mgr = cache_mgr_map.get(_norm_key(p))
            if not cache_mgr: continue
            if self._ensure_features(p, cache_mgr, need_hsv=need_hsv, need_whash=need_whash):
                calculated += 1
                touched_cache_managers.add(cache_mgr)
            now = time.time()
            if now - last_hb >= hb_interval:
                pct = int(idx / len(to_load) * 100)
                log_info(f"[批次載入] {phase_name}: {idx}/{len(to_load)} ({pct}%) | 計算: {calculated}")
                last_hb = now

        for cache_mgr in touched_cache_managers:
            try:
                cache_mgr.save_cache()
            except Exception as e:
                log_error(f"[批次載入] {phase_name} 快取落盤失敗: {e}")

        log_info(f"[批次順序讀取] 完成：{phase_name} 共 {len(to_load)} 個，其中 {calculated} 個需計算，"
                 f"{len(to_load) - calculated} 個命中快取。")
        return calculated

    def _batch_ensure_features_parallel(self, to_load: list, cache_mgr_map: dict,
                                        need_hsv: bool = False, need_whash: bool = False,
                                        phase_name: str = "特徵") -> int:
        pool_size = self._ensure_worker_pool()
        enable_qd = self.config.get('enable_quick_digest', True)
        use_preprocess = bool(self.config.get('enable_image_preprocess', False))
        hash_resolution = int(self.config.get('hash_resolution', 128))
        log_info(f"[lazy feature] parallel {phase_name}: {len(to_load)} images, workers={pool_size}")
        self._update_progress(text=f"平行補算 {phase_name} ({len(to_load)} 張, {pool_size} workers)...")

        async_results = []
        path_map = {}
        for p in to_load:
            res = self.pool.apply_async(
                _pool_worker_ensure_image_features,
                args=(p, need_hsv, need_whash, use_preprocess, enable_qd, hash_resolution),
            )
            async_results.append(res)
            path_map[res] = p

        calculated = 0
        completed = 0
        hb_interval = 30
        last_hb = time.time()
        touched_cache_managers = set()

        while async_results:
            if self._check_control() == 'cancel':
                self._cleanup_pool()
                return calculated

            remaining = []
            for res in async_results:
                if not res.ready():
                    remaining.append(res)
                    continue

                completed += 1
                path_hint = path_map.get(res, "")
                try:
                    path_done, data = res.get()
                except Exception as e:
                    log_error(f"[lazy feature] worker failed for {path_hint}: {e}", include_traceback=True)
                    continue

                norm_path = _norm_key(path_done)
                cache_mgr = cache_mgr_map.get(norm_path)
                if not cache_mgr:
                    continue
                if not data or data.get('error'):
                    error_msg = data.get('error') if data else "empty worker result"
                    log_error(f"[lazy feature] {phase_name} failed: {path_done}: {error_msg}")
                    self.failed_tasks.append((path_done, error_msg))
                    continue

                existing = self.file_data.get(norm_path) or self._get_entry_from_cache(path_done, cache_mgr) or {}
                features_present = existing.get('features_at', 0) | self._feature_bits_from_entry(existing)
                new_features = self._feature_bits_from_result(data)
                update_payload = {
                    'mtime': data.get('mtime'),
                    'features_at': features_present | new_features,
                }
                for key in ('size', 'ctime', 'avg_hsv', 'whash', 'qd64', 'width', 'height'):
                    if key in data and data[key] is not None:
                        update_payload[key] = data[key]

                cache_mgr.update_data(norm_path, update_payload)
                existing.update(update_payload)
                self.file_data[norm_path] = self._normalize_cached_hashes(existing)
                touched_cache_managers.add(cache_mgr)
                calculated += 1

            async_results = remaining
            now = time.time()
            if now - last_hb >= hb_interval:
                pct = int(completed / len(to_load) * 100)
                log_info(f"[lazy feature] {phase_name}: {completed}/{len(to_load)} ({pct}%) | calculated={calculated}")
                last_hb = now
            time.sleep(0.05)

        for cache_mgr in touched_cache_managers:
            try:
                cache_mgr.save_cache()
            except Exception as e:
                log_error(f"[lazy feature] {phase_name} cache save failed: {e}")

        log_info(f"[lazy feature] parallel {phase_name} complete: requested={len(to_load)}, calculated={calculated}, failed={len(to_load) - calculated}")
        return calculated

    def _coerce_hash_obj(self, h):
        if h is None or imagehash is None: return None
        if isinstance(h, imagehash.ImageHash): return h
        try: return imagehash.hex_to_hash(str(h))
        except (TypeError, ValueError): return None

    @staticmethod
    def _build_digest_patch(current_data: dict) -> dict:
        patch = {
            'phash': str(current_data.get('phash')) if current_data.get('phash') else None,
            'whash': str(current_data.get('whash')) if current_data.get('whash') else None,
            'avg_hsv': list(current_data.get('avg_hsv')) if current_data.get('avg_hsv') else None,
            'grid_phash': current_data.get('grid_phash', []),
            'features_at': current_data.get('features_at', 0),
        }
        return {k: v for k, v in patch.items() if v is not None}

    def _load_ad_hash_dataset(self, ad_folder_path: str, description: str, worker_function: callable, data_key: str, progress_text: str, current_digest: str = "") -> tuple[bool, list[str], Optional[ScannedImageCacheManager], dict]:
        ad_paths = self._collect_image_paths(ad_folder_path)
        if not ad_paths: return True, [], None, {}
        self._update_progress(text=progress_text)
        from processors.scanner import MasterAdCacheManager
        ad_cache_manager = MasterAdCacheManager(ad_folder_path)
        continue_processing, ad_data = self._process_images_with_cache(ad_paths, ad_cache_manager, description, worker_function, data_key, progress_scope='local')
        if continue_processing and ad_data and current_digest:
            if not ad_cache_manager.index_is_current(current_digest):
                log_info(f"[AdIndex] rebuilding because digest changed: {current_digest[:8]}...")
                ad_cache_manager.rebuild_hash_index(ad_data, digest=current_digest)
            else:
                log_info(f"[AdIndex] index current: {current_digest[:8]}...")
        return continue_processing, ad_paths, ad_cache_manager, ad_data

    def _prepare_cross_compare_state(self, ad_folder_path: str, current_digest: str = "") -> dict:
        state = {'ad_data_for_marking': {}, 'ad_cache_manager': None}
        if not ad_folder_path or not os.path.isdir(ad_folder_path): return state
        continue_processing, _, ad_cache_manager, ad_data_for_marking = self._load_ad_hash_dataset(ad_folder_path, "預載入廣告庫", _pool_worker_process_image_phash_only, 'phash', "📦 正在預處理廣告庫...（此階段為局部進度）", current_digest=current_digest)
        if not continue_processing: return state
        state['ad_data_for_marking'] = ad_data_for_marking; state['ad_cache_manager'] = ad_cache_manager
        return state

    def _prepare_ad_mode_state(self, ad_folder_path: str, current_digest: str = "") -> Optional[dict]:
        continue_processing, _, ad_cache_manager, ad_data = self._load_ad_hash_dataset(ad_folder_path, "廣告圖片雜湊", _pool_worker_process_image_phash_only, 'phash', "📦 正在預處理廣告庫...（此階段為局部進度）", current_digest=current_digest)
        if not continue_processing: return None
        self.file_data.update(ad_data)
        self._update_progress(text="🔍 正在使用 LSH 高效預處理廣告庫...")
        ad_lsh_index = self._build_phash_band_index(ad_data)
        ad_path_to_leader = {p: p for p in ad_data}
        grouping_dist = hamming_from_sim(AD_GROUPING_THRESHOLD, HASH_BITS)
        for p1 in sorted(ad_data.keys()):
            if ad_path_to_leader[p1] != p1: continue
            h1 = self._coerce_hash_obj(ad_data.get(p1, {}).get('phash'))
            if not h1: continue
            for p2 in self._lsh_candidates_for(p1, h1, ad_lsh_index):
                if p2 <= p1 or ad_path_to_leader[p2] != p2: continue
                h2 = self._coerce_hash_obj(ad_data.get(p2, {}).get('phash'))
                if h2 and (h1 - h2) <= grouping_dist: ad_path_to_leader[p2] = ad_path_to_leader[p1]
        leader_to_ad_group = {}
        for path, leader in ad_path_to_leader.items(): leader_to_ad_group.setdefault(leader, []).append(path)
        ad_data_representatives = {p: d for p, d in ad_data.items() if p in leader_to_ad_group}
        self._update_progress(text=f"🔍 廣告庫預處理完成，找到 {len(ad_data_representatives)} 個獨立廣告組。")
        return {'ad_data': ad_data, 'ad_cache_manager': ad_cache_manager, 'leader_to_ad_group': leader_to_ad_group, 'ad_member_to_leader': ad_path_to_leader, 'ad_data_representatives': ad_data_representatives}

    def _detect_qr_codes(self, scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        if self.config.get('enable_qr_hybrid_mode'): return self._detect_qr_codes_hybrid(self.tasks_to_process, scan_cache_manager)
        else: return self._detect_qr_codes_pure(self.tasks_to_process, scan_cache_manager)

    def _group_qr_results_by_phash(self, flat_qr_list: list, file_data: dict) -> list:
        from processors.qr_engine import group_qr_results_by_phash as _qr_group
        user_pct = float(self.config.get('similarity_threshold', 95.0))
        qr_thresh = user_pct / 100.0
        log_info(f"[QR 分組] 使用相似度門檻 {user_pct:.0f}%（與其他比對模式共用）")
        return _qr_group(flat_qr_list, file_data, sim_threshold=qr_thresh)

    def _detect_qr_codes_pure(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        log_info("[QR] 正在執行純粹掃描模式..."); remaining_files_for_qr = list(files_to_process)
        if self.config.get('enable_qr_color_filter', False) and remaining_files_for_qr:
            self._update_progress(text=f"🎨 對剩餘 {len(remaining_files_for_qr)} 個檔案進行 QR 彩圖前篩（局部進度）")
            continue_proc_color, color_data = self._process_images_with_cache(remaining_files_for_qr, scan_cache_manager, "QR 彩圖前篩", _pool_worker_detect_qr_colorful_only, 'is_colorful', progress_scope='local')
            if not continue_proc_color: return None
            self.file_data.update(color_data); remaining_files_for_qr = [p for p in remaining_files_for_qr if color_data.get(p, {}).get('is_colorful')]
        continue_processing, file_data = self._process_images_with_cache(remaining_files_for_qr, scan_cache_manager, "QR Code 檢測", _pool_worker_detect_qr_code, 'qr_points'); self.file_data.update(file_data)
        if not continue_processing: return None
        flat_qr = [(path, path, "🆕 新掃描 QR", "qr_item") for path, data in file_data.items() if data and data.get('qr_points')]
        return self._group_qr_results_by_phash(flat_qr, file_data), self.file_data

    def _detect_qr_codes_hybrid(self, files_to_process: list[str], scan_cache_manager: ScannedImageCacheManager) -> Union[tuple[list, dict], None]:
        log_info("[QR] 正在執行混合掃描模式..."); ad_folder_path = self.config.get('ad_folder_path')
        if not ad_folder_path or not os.path.isdir(ad_folder_path): return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        continue_proc_ad, ad_paths, ad_cache_manager, ad_data = self._load_ad_hash_dataset(ad_folder_path, "廣告圖片屬性", _pool_worker_process_image_full, 'qr_points', "📦 正在預處理廣告庫...（此階段為局部進度）")
        if not continue_proc_ad: return None
        self.file_data.update(ad_data); ad_with_phash = {p: d for p, d in ad_data.items() if d and d.get('phash')}
        if not ad_with_phash: return self._detect_qr_codes_pure(files_to_process, scan_cache_manager)
        if ad_cache_manager and hasattr(ad_cache_manager, "rebuild_hash_index"): ad_cache_manager.rebuild_hash_index(ad_with_phash, digest=f"qr_hybrid:{len(ad_with_phash)}")
        found_ad_matches = []; remaining_files_for_qr = list(files_to_process)
        if self.config.get('enable_qr_color_filter', False) and remaining_files_for_qr:
            continue_proc_color, color_data = self._process_images_with_cache(remaining_files_for_qr, scan_cache_manager, "QR 彩圖前篩", _pool_worker_detect_qr_colorful_only, 'is_colorful', progress_scope='local')
            if not continue_proc_color: return None
            self.file_data.update(color_data); remaining_files_for_qr = [p for p in remaining_files_for_qr if color_data.get(p, {}).get('is_colorful')]
        if remaining_files_for_qr:
            continue_proc_qr, qr_data = self._process_images_with_cache(remaining_files_for_qr, scan_cache_manager, "QR Code 檢測", _pool_worker_detect_qr_code, 'qr_points', progress_scope='local')
            if not continue_proc_qr: return None
            self.file_data.update(qr_data); qr_positive_paths = [p for p, d in qr_data.items() if d and d.get('qr_points')]
            user_thresh = self.config.get('similarity_threshold', 95.0) / 100.0; unmatched_qr_paths = []
            for g_path in qr_positive_paths:
                g_ent = self.file_data.get(_norm_key(g_path), {}); g_p_hash = self._coerce_hash_obj(g_ent.get('phash')); matched = False
                candidate_paths = ad_cache_manager.query_hash_index(g_p_hash) if ad_cache_manager and hasattr(ad_cache_manager, "query_hash_index") else set(ad_with_phash.keys())
                for ad_path in candidate_paths:
                    ad_ent = ad_with_phash.get(ad_path)
                    if not ad_ent or not ad_ent.get('qr_points'): continue
                    ad_p_hash = self._coerce_hash_obj(ad_ent.get('phash'))
                    if not ad_p_hash: continue
                    sim_p = sim_from_hamming(ad_p_hash - g_p_hash, HASH_BITS)
                    if sim_p < PHASH_FAST_THRESH: continue
                    is_accepted, final_sim_val = True, sim_p
                    if sim_p < PHASH_STRICT_SKIP:
                        if not self._ensure_features(ad_path, ad_cache_manager, need_whash=True) or not self._ensure_features(g_path, scan_cache_manager, need_whash=True): continue
                        is_accepted, final_sim_val = self._accept_pair_with_dual_hash(ad_p_hash, g_p_hash, self.file_data.get(_norm_key(ad_path), {}).get('whash'), self.file_data.get(_norm_key(g_path), {}).get('whash'))
                    if is_accepted and final_sim_val >= user_thresh: found_ad_matches.append((g_path, ad_path, "似廣告", "ad_like_group")); matched = True; break
                if not matched: unmatched_qr_paths.append(g_path)
            flat_qr = [(path, path, "🆕 新掃描 QR", "qr_item") for path in unmatched_qr_paths]
            found_ad_matches.extend(self._group_qr_results_by_phash(flat_qr, {p: qr_data[p] for p in unmatched_qr_paths if p in qr_data}))
        scan_cache_manager.save_cache(); ad_cache_manager.save_cache()
        return found_ad_matches, self.file_data

    def _collect_image_paths(self, folder_path: str) -> list[str]:
        img_exts = ('.png', '.jpg', '.jpeg', '.webp', '.gif', '.bmp'); paths = []
        if not folder_path or not os.path.isdir(folder_path): return paths
        for ent in _iter_scandir_recursively(folder_path, set(), set(), self.control_events):
            if ent.is_file() and ent.name.lower().endswith(img_exts): paths.append(_norm_key(ent.path))
        return paths
