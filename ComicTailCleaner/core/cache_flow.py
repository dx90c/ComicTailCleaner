# ======================================================================
# 檔案名稱：core/cache_flow.py
# 模組目的：承接 ImageComparisonEngine 的快取流程 helper
# ======================================================================

import os
import sys
import time
from multiprocessing import Pool, set_start_method
from os import cpu_count

from processors.scanner import ScannedImageCacheManager
from utils import (
    _calculate_quick_digest,
    _is_virtual_path,
    _norm_key,
    _parse_virtual_path,
    log_error,
    log_info,
    log_warning,
)


class CacheFlowMixin:
    """Cache-flow helpers for ImageComparisonEngine.

    This mixin is intentionally narrow for the first O1 split. It only hosts
    helpers that were already extracted from the main flow and does not change
    the multiprocessing model or cache semantics.
    """

    def _ensure_worker_pool(self) -> int:
        user_proc_setting = self.config.get('worker_processes', 0)
        pool_size = max(1, min(user_proc_setting, cpu_count())) if user_proc_setting > 0 else max(1, min(cpu_count() // 2, 8))
        if not self.pool:
            if sys.platform.startswith('win'):
                try:
                    set_start_method('spawn', force=True)
                except RuntimeError:
                    pass
            self.pool = Pool(processes=pool_size)
        return pool_size

    def _submit_worker_jobs(self, paths_to_recalc: list[str], worker_function: callable) -> tuple[list, dict]:
        async_results = []
        path_map = {}
        for path in paths_to_recalc:
            payload = self._build_worker_payload(worker_function, path)
            args_to_pass = payload if isinstance(payload, tuple) else (payload,)
            res = self.pool.apply_async(worker_function, args=args_to_pass)
            async_results.append(res)
            path_map[res] = path
        return async_results, path_map

    def _handle_ready_worker_result(
        self,
        res,
        path_map: dict,
        cache_manager: ScannedImageCacheManager,
        local_file_data: dict,
        progress_scope: str,
        local_completed: int,
    ) -> int:
        try:
            path_done, data = res.get()
            if data.get('error'):
                self.failed_tasks.append((path_done, data['error']))
                if "不存在" in data['error']:
                    cache_manager.remove_data(path_done)
            else:
                if self.config.get('enable_quick_digest', True):
                    data['qd64'] = _calculate_quick_digest(path_done)

                feature_bit = self._feature_bits_from_result(data)
                existing = cache_manager.get_data(path_done) or {}
                data['features_at'] = existing.get('features_at', 0) | feature_bit
                local_file_data.setdefault(path_done, {}).update(data)
                cache_manager.update_data(path_done, data)

            if progress_scope == 'global':
                self.completed_task_count += 1
            else:
                local_completed += 1
        except Exception as e:
            path_done = path_map.get(res, "未知路徑")
            error_msg = f"工作進程處理失敗: {e}"
            log_error(error_msg, True)
            self.failed_tasks.append((path_done, error_msg))
            if progress_scope == 'global':
                self.completed_task_count += 1
            else:
                local_completed += 1
        return local_completed

    def _update_processing_progress(self, progress_scope: str, description: str, local_completed: int, local_total: int) -> None:
        if progress_scope == 'global':
            if self.total_task_count > 0:
                current_progress = int(self.completed_task_count / self.total_task_count * 100)
                self._update_progress(p_type='progress', value=current_progress, text=f"⚙️ 處理{description}中... ({self.completed_task_count}/{self.total_task_count})")
        else:
            if local_total > 0:
                current_progress = int(local_completed / local_total * 100)
                self._update_progress(p_type='progress', value=current_progress, text=f"⚙️ [局部] 處理{description}中... ({local_completed}/{local_total})")

    def _emit_qr_processing_heartbeat(self, description: str, progress_scope: str, local_completed: int, local_total: int, last_qr_heartbeat: float) -> float:
        if "QR" not in description or (time.time() - last_qr_heartbeat) < 10:
            return last_qr_heartbeat

        if progress_scope == 'global':
            total = self.total_task_count
            done = self.completed_task_count
        else:
            total = local_total
            done = local_completed
        if total > 0:
            pct = int(done / total * 100)
            log_info(f"[{description} 心跳] {done}/{total} ({pct}%)")
        return time.time()

    def _handle_recalc_setup(
        self,
        paths_to_purge: set[str],
        folders_to_rescan: set[str],
        paths_to_recalc: list[str],
        local_file_data: dict,
        cache_manager: ScannedImageCacheManager,
        progress_scope: str,
        local_total: int,
    ) -> int:
        self._purge_stale_cache_entries(paths_to_purge, cache_manager)
        return self._expand_paths_from_rescan_folders(
            folders_to_rescan,
            paths_to_recalc,
            local_file_data,
            progress_scope,
            local_total,
        )

    def _process_async_worker_loop(
        self,
        async_results: list,
        path_map: dict,
        cache_manager: ScannedImageCacheManager,
        local_file_data: dict,
        progress_scope: str,
        description: str,
        local_completed: int,
        local_total: int,
    ) -> tuple[bool, int]:
        last_qr_heartbeat = time.time()

        while async_results:
            if self._check_control() == 'cancel':
                self._cleanup_pool()
                return False, local_completed

            remaining_results = []
            for res in async_results:
                if res.ready():
                    local_completed = self._handle_ready_worker_result(
                        res,
                        path_map,
                        cache_manager,
                        local_file_data,
                        progress_scope,
                        local_completed,
                    )
                else:
                    remaining_results.append(res)

            async_results = remaining_results
            self._update_processing_progress(progress_scope, description, local_completed, local_total)
            last_qr_heartbeat = self._emit_qr_processing_heartbeat(
                description,
                progress_scope,
                local_completed,
                local_total,
                last_qr_heartbeat,
            )
            time.sleep(0.05)

        return True, local_completed

    def _process_images_with_cache(
        self,
        current_task_list: list[str],
        cache_manager: ScannedImageCacheManager,
        description: str,
        worker_function: callable,
        data_key: str,
        progress_scope: str = 'global',
    ) -> tuple[bool, dict]:
        if not current_task_list:
            return True, {}
        time.sleep(self.config.get('ux_scan_start_delay', 0.1))
        self._update_progress(text=f"📂 正在檢查 {len(current_task_list)} 個{description}的快取...")

        use_quick_digest = self.config.get('enable_quick_digest', True)
        file_mtimes = self._collect_file_mtimes(current_task_list, description)
        if self._check_control() == 'cancel':
            return False, {}

        local_file_data, paths_to_recalc, paths_to_purge, folders_to_rescan, cache_hits, local_completed, local_total = self._collect_cache_work_plan(
            current_task_list,
            cache_manager,
            data_key,
            progress_scope,
            file_mtimes,
            use_quick_digest,
        )
        if not hasattr(self, 'cache_stats'):
            self.cache_stats = {'hit': 0, 'recalc': 0, 'purge': 0, 'rescan_folders': 0}
        self.cache_stats['hit'] += cache_hits
        self.cache_stats['recalc'] += len(paths_to_recalc)
        self.cache_stats['purge'] += len(paths_to_purge)
        self.cache_stats['rescan_folders'] += len(folders_to_rescan)
        
        log_info(
            f"[快取計畫] {description}: cache_hit={cache_hits}, "
            f"recalc={len(paths_to_recalc)}, purge={len(paths_to_purge)}, "
            f"rescan_folders={len(folders_to_rescan)}"
        )

        local_total = self._handle_recalc_setup(
            paths_to_purge,
            folders_to_rescan,
            paths_to_recalc,
            local_file_data,
            cache_manager,
            progress_scope,
            local_total,
        )

        if not paths_to_recalc:
            cache_manager.save_cache()
            return True, local_file_data

        continue_processing, local_completed = self._run_recalc_jobs(
            paths_to_recalc,
            worker_function,
            cache_manager,
            local_file_data,
            progress_scope,
            description,
            local_completed,
            local_total,
        )
        if not continue_processing:
            return False, {}

        cache_manager.save_cache()
        return True, local_file_data

    def _run_recalc_jobs(
        self,
        paths_to_recalc: list[str],
        worker_function: callable,
        cache_manager: ScannedImageCacheManager,
        local_file_data: dict,
        progress_scope: str,
        description: str,
        local_completed: int,
        local_total: int,
    ) -> tuple[bool, int]:
        pool_size = self._ensure_worker_pool()
        self._update_progress(text=f"⚙️ 啟動 {pool_size} 個工作進程，處理 {len(paths_to_recalc)} 筆{description}...")
        async_results, path_map = self._submit_worker_jobs(paths_to_recalc, worker_function)
        return self._process_async_worker_loop(
            async_results,
            path_map,
            cache_manager,
            local_file_data,
            progress_scope,
            description,
            local_completed,
            local_total,
        )

    def _collect_cache_work_plan(
        self,
        current_task_list: list[str],
        cache_manager: ScannedImageCacheManager,
        data_key: str,
        progress_scope: str,
        file_mtimes: dict,
        use_quick_digest: bool,
    ) -> tuple[dict, list[str], set[str], set[str], int, int, int]:
        local_file_data = {}
        paths_to_recalc, cache_hits = [], 0
        folders_to_rescan = set()
        paths_to_purge = set()
        local_total = len(current_task_list)
        local_completed = 0

        for path in list(current_task_list):
            mt = file_mtimes.get(path)

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
            is_hit, needs_qd64_upgrade = self._is_cached_feature_hit(
                cached_data,
                mt,
                path,
                data_key,
                use_quick_digest,
            )

            if is_hit:
                cached_data = self._normalize_cached_hashes(cached_data)
                local_file_data[path] = cached_data
                cache_hits += 1

                inferred_features = self._feature_bits_from_entry(cached_data)
                stored_features = cached_data.get('features_at', 0)
                if inferred_features and (stored_features | inferred_features) != stored_features:
                    merged_features = stored_features | inferred_features
                    cached_data['features_at'] = merged_features
                    cache_manager.update_data(_norm_key(path), {'features_at': merged_features, 'mtime': mt})

                if use_quick_digest and needs_qd64_upgrade:
                    try:
                        qd64_now = _calculate_quick_digest(path)
                        if qd64_now:
                            cache_manager.update_data(_norm_key(path), {'qd64': qd64_now, 'mtime': mt, 'features_at': cached_data.get('features_at', 0)})
                    except Exception as e:
                        log_warning(f"[快取升級] 寫入 qd64 失敗: {path}: {e}")

                if progress_scope == 'global':
                    self.completed_task_count += 1
                else:
                    local_completed += 1
            else:
                paths_to_recalc.append(path)
                if cached_data:
                    cached_data = self._normalize_cached_hashes(cached_data)
                    local_file_data[path] = cached_data

        return local_file_data, paths_to_recalc, paths_to_purge, folders_to_rescan, cache_hits, local_completed, local_total
