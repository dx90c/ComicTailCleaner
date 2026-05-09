# ======================================================================
# 檔案名稱：core/similarity_flow.py
# 模組目的：承接 ImageComparisonEngine 的相似度比對流程 helper (M1-B)
# ======================================================================

import os
import json
import time
import hashlib
from collections import defaultdict
from typing import Union, Tuple, Dict, List, Optional, Any

import config
import utils
from utils import (
    log_info,
    log_error,
    log_warning,
    log_performance,
    _is_virtual_path,
    _parse_virtual_path,
    _norm_key,
    _get_file_stat,
    sim_from_hamming,
    _color_gate,
)
from processors.scanner import _iter_scandir_recursively

try:
    import imagehash
except ImportError:
    imagehash = None

# 為了在 Mixin 中正確使用常量，這裡重新宣告或引用
HASH_BITS = 64
PHASH_FAST_THRESH = 0.70
PHASH_STRICT_SKIP = 0.93
AD_GROUPING_THRESHOLD = 0.95
LSH_BANDS = 8

FEATURE_PHASH = 1 << 0
FEATURE_WHASH = 1 << 1
FEATURE_COLOR = 1 << 2


def bit_count_np(arr):
    """高效的 NumPy 陣列位元計數 (Popcount) 實作。"""
    import numpy as np
    c = arr.astype(np.uint64)
    c = (c & 0x5555555555555555) + ((c >> 1) & 0x5555555555555555)
    c = (c & 0x3333333333333333) + ((c >> 2) & 0x3333333333333333)
    c = (c & 0x0F0F0F0F0F0F0F0F) + ((c >> 4) & 0x0F0F0F0F0F0F0F0F)
    c = (c & 0x00FF00FF00FF00FF) + ((c >> 8) & 0x00FF00FF00FF00FF)
    c = (c & 0x0000FFFF0000FFFF) + ((c >> 16) & 0x0000FFFF0000FFFF)
    c = (c & 0x00000000FFFFFFFF) + ((c >> 32) & 0x00000000FFFFFFFF)
    return c.astype(np.int8)


class SimilarityFlowMixin:
    def _h2i(self, h):
        """安全地將指紋物件轉換為整數數值。"""
        if h is None: return 0
        if isinstance(h, int): return h
        try:
            return int(h)
        except (TypeError, ValueError):
            try:
                # 嘗試從 hex 字串轉換
                return int(str(h), 16)
            except (TypeError, ValueError):
                return 0

    def _valid_hash_obj(self, h) -> bool:
        """Return True only for present, non-zero perceptual hashes."""
        return h is not None and self._h2i(h) != 0

    """Similarity-flow helpers for ImageComparisonEngine (M1-B)."""

    def _find_similar_images(self, scan_cache_manager: Any, ad_catalog_state: Optional[Dict] = None) -> Union[tuple[list, dict], None]:
        context = self._prepare_similarity_context(scan_cache_manager, ad_catalog_state)
        if context is None:
            return None

        if context['is_ad_mode'] and self.config.get('enable_targeted_search', False):
            return self._run_targeted_search(
                context['ad_data_representatives'],
                context['ad_data'],
                context['gallery_data'],
                context['ad_cache_manager'],
                context.get('ad_member_to_leader'),
            )

        temp_found_pairs, found_items, stats = self._run_similarity_funnel(context, scan_cache_manager)
        if self._check_control() != 'continue':
            return None

        log_performance("[完成] LSH 雙哈希比對階段")
        final_matches = len({(_norm_key(p1), _norm_key(p2)) for p1, p2, _ in temp_found_pairs})
        self._log_funnel_stats(stats, final_matches)

        full_gallery_data = self.file_data
        self.file_data = {**full_gallery_data, **context['ad_data']}

        if context['is_ad_mode'] and context['current_digest']:
            self._mark_clean_digest_tasks(found_items, context['tasks_to_process'], context['current_digest'], scan_cache_manager)

        return found_items, self.file_data

    def _run_similarity_funnel(
        self,
        context: dict,
        scan_cache_manager: Any,
    ) -> tuple[list, list, dict]:
        color_gate_params = self._build_color_gate_params(context['user_thresh_percent'])
        phash_index = self._build_phash_band_index(context['gallery_data'])
        user_thresh = context['user_thresh_percent'] / 100.0
        inter_folder_only = self.config.get('enable_inter_folder_only', False) and context['is_mutual_mode']
        stats = {'comparisons': 0, 'passed_phash': 0, 'passed_color': 0, 'entered_whash': 0, 'filtered_inter': 0}
        use_color_filter = self.config.get('enable_color_filter', True)
        use_whash = self.config.get('enable_whash', True)

        candidates_phash, phase_a_start = self._collect_phash_candidates(
            context['ad_data_representatives'],
            context['gallery_data'],
            context['ad_data'],
            context['leader_to_ad_group'],
            phash_index,
            context['is_mutual_mode'],
            context['is_ad_mode'],
            inter_folder_only,
            stats,
            ad_cache_manager=context['ad_cache_manager'],
            ad_member_to_leader=context.get('ad_member_to_leader'),
        )
        if self._check_control() != 'continue':
            return [], [], stats

        phase_b_start = time.time()
        if use_color_filter:
            self._ensure_candidate_hsv(
                candidates_phash,
                context['is_mutual_mode'],
                context['ad_cache_manager'],
                scan_cache_manager,
            )
        log_info(f"[Phase B 完成] HSV 特徵準備耗時: {time.time() - phase_b_start:.2f}s")
        phase_c_start = time.time()
        candidates_hsv = self._filter_candidates_by_color(candidates_phash, color_gate_params, use_color_filter, stats)
        log_info(f"[Phase C 完成] 顏色向量化過濾耗時: {time.time() - phase_c_start:.2f}s")

        phase_d_start = time.time()
        if use_whash:
            self._ensure_candidate_whash(
                candidates_hsv,
                context['is_mutual_mode'],
                context['ad_cache_manager'],
                scan_cache_manager,
            )
        log_info(f"[Phase D 完成] wHash 特徵準備耗時: {time.time() - phase_d_start:.2f}s")
        phase_e_start = time.time()
        temp_found_pairs = self._select_final_matches(candidates_hsv, user_thresh, use_whash, stats, phase_a_start)
        log_info(f"[Phase E 耗時] wHash 向量化複核耗時: {time.time() - phase_e_start:.2f}s")

        build_items_start = time.time()
        found_items = self._build_found_items(
            temp_found_pairs,
            context['is_mutual_mode'],
            context['ad_data_for_marking'],
            context['ad_mark_cache_manager'],
            scan_cache_manager,
            color_gate_params,
            user_thresh,
        )
        log_info(f"[結果整理耗時] 群組合併與結果建構耗時: {time.time() - build_items_start:.2f}s")
        return temp_found_pairs, found_items, stats

    def _prepare_similarity_context(self, scan_cache_manager: Any, ad_catalog_state: Optional[Dict]) -> Optional[dict]:
        tasks_to_process, current_digest, is_ad_mode = self._resolve_similarity_tasks(
            scan_cache_manager,
            ad_catalog_state,
        )
        gallery_data = self._load_similarity_gallery_data(tasks_to_process, scan_cache_manager)
        if gallery_data is None:
            return None

        user_thresh_percent = self.config.get('similarity_threshold', 95.0)
        mode_state = self._build_similarity_mode_context(gallery_data, is_ad_mode, current_digest)
        if mode_state is None:
            return None

        return {
            'tasks_to_process': tasks_to_process,
            'is_ad_mode': is_ad_mode,
            'current_digest': current_digest,
            'gallery_data': gallery_data,
            'user_thresh_percent': user_thresh_percent,
            **mode_state,
        }

    def _build_similarity_mode_context(
        self,
        gallery_data: dict,
        is_ad_mode: bool,
        current_digest: str,
    ) -> Optional[dict]:
        is_mutual_mode = self.config.get('comparison_mode') == 'mutual_comparison'
        ad_folder_path = self.config.get('ad_folder_path')
        state = {
            'is_mutual_mode': is_mutual_mode,
            'ad_data_for_marking': {},
            'ad_mark_cache_manager': None,
            'ad_data': {},
            'ad_cache_manager': None,
            'leader_to_ad_group': {},
            'ad_member_to_leader': {},
            'ad_data_representatives': gallery_data.copy(),
            'ad_folder_path': ad_folder_path,
        }

        if is_mutual_mode and self.config.get('enable_ad_cross_comparison', True):
            cross_compare_state = self._prepare_cross_compare_state(ad_folder_path, current_digest=current_digest)
            state['ad_data_for_marking'] = cross_compare_state['ad_data_for_marking']
            state['ad_mark_cache_manager'] = cross_compare_state['ad_cache_manager']

        if not is_ad_mode:
            return state

        ad_mode_state = self._prepare_ad_mode_state(ad_folder_path, current_digest=current_digest)
        if ad_mode_state is None:
            return None
        state['ad_data'] = ad_mode_state['ad_data']
        state['ad_cache_manager'] = ad_mode_state['ad_cache_manager']
        state['leader_to_ad_group'] = ad_mode_state['leader_to_ad_group']
        state['ad_member_to_leader'] = ad_mode_state['ad_member_to_leader']
        state['ad_data_representatives'] = ad_mode_state['ad_data_representatives']
        return state

    def _load_similarity_gallery_data(
        self,
        tasks_to_process: list[str],
        scan_cache_manager: Any,
    ) -> Optional[dict]:
        continue_processing, self.file_data = self._process_images_with_cache(
            tasks_to_process,
            scan_cache_manager,
            "目標雜湊",
            self._get_phash_worker(),
            'phash',
            progress_scope='global',
        )
        if not continue_processing:
            return None
        return {k: v for k, v in self.file_data.items() if k in tasks_to_process}

    def _get_phash_worker(self):
        from processors.qr_engine import _pool_worker_process_image_phash_only
        return _pool_worker_process_image_phash_only

    def _resolve_similarity_tasks(
        self,
        scan_cache_manager: Any,
        ad_catalog_state: Optional[Dict],
    ) -> tuple[list[str], str, bool]:
        tasks_to_process = self.tasks_to_process
        current_digest = ""
        is_ad_mode = self.config.get('comparison_mode') == 'ad_comparison'

        if is_ad_mode and ad_catalog_state:
            current_digest = ad_catalog_state.get('catalog_digest', "")
            unmatched_tasks = self._apply_digest_filter(self.tasks_to_process, scan_cache_manager, current_digest)
            if len(unmatched_tasks) < len(self.tasks_to_process):
                log_info(
                    f"[Digest] 總任務數: {len(self.tasks_to_process)}, "
                    f"其中 {len(unmatched_tasks)} 個任務因 Digest 失效或為新檔案而需要比對。"
                )
            tasks_to_process = unmatched_tasks
            self.completed_task_count = len(self.tasks_to_process) - len(tasks_to_process)
            self.total_task_count = len(self.tasks_to_process)

        return tasks_to_process, current_digest, is_ad_mode

    def _mark_clean_digest_tasks(self, found_items: list, tasks_to_process: list, current_digest: str, scan_cache_manager: Any) -> None:
        if not current_digest:
            return
        matched_comic_paths = {_norm_key(item[1]) for item in found_items}
        files_to_update_digest = [path for path in tasks_to_process if _norm_key(path) not in matched_comic_paths]
        log_info(f"[Digest] 正在為 {len(files_to_update_digest)} 個已處理且『清白』的圖片更新 Digest 標記...")
        for path in files_to_update_digest:
            norm_path = _norm_key(path)
            current_data = self.file_data.get(norm_path, {})
            scan_cache_manager.update_data(norm_path, self._build_digest_patch(current_data))
        scan_cache_manager.save_cache()
        log_info("[Digest] 標記更新完成。")

    def _build_found_items(
        self,
        temp_found_pairs: list,
        is_mutual_mode: bool,
        ad_data_for_marking: dict,
        ad_cache_manager: Any,
        scan_cache_manager: Any,
        color_gate_params: dict,
        user_thresh: float,
    ) -> list:
        found_items = []
        if not is_mutual_mode:
            results_by_leader = defaultdict(list)
            for leader, target, sim_str in temp_found_pairs:
                results_by_leader[leader].append((target, float(sim_str.replace('%', '')), sim_str))
            for leader, targets in results_by_leader.items():
                for target, _, sim_str in sorted(targets, key=lambda x: x[1], reverse=True):
                    found_items.append((leader, target, sim_str))
            return found_items

        self._update_progress(text="🔄 正在合併相似羣組...")
        norm_to_orig = {}
        for p1, p2, _ in temp_found_pairs:
            norm_to_orig[_norm_key(p1)] = p1
            norm_to_orig[_norm_key(p2)] = p2

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
            while leader in path_to_leader and path_to_leader[leader] != leader:
                leader = path_to_leader[leader]
            final_groups[leader].append(path)

        mark_ad_like = bool(ad_data_for_marking)
        if mark_ad_like:
            self._update_progress(text="🔄 正在與廣告庫進行交叉比對 (使用統一定義引擎)...")

        for norm_leader, norm_children in final_groups.items():
            is_ad_like = False
            if mark_ad_like:
                is_ad_like = self._is_ad_like_group(
                    norm_leader,
                    ad_data_for_marking,
                    ad_cache_manager,
                    scan_cache_manager,
                    color_gate_params,
                    user_thresh,
                )
            
            orig_leader = norm_to_orig.get(norm_leader, norm_leader)
            for norm_child in sorted([p for p in norm_children if p != norm_leader]):
                orig_child = norm_to_orig.get(norm_child, norm_child)
                h1 = self._coerce_hash_obj(self.file_data.get(norm_leader, {}).get('phash'))
                h2 = self._coerce_hash_obj(self.file_data.get(norm_child, {}).get('phash'))
                if self._valid_hash_obj(h1) and self._valid_hash_obj(h2):
                    sim = sim_from_hamming(h1 - h2, HASH_BITS) * 100
                    value_str = f"{sim:.1f}%"
                    if is_ad_like:
                        value_str += " (似廣告)"
                    found_items.append((orig_leader, orig_child, value_str))
        return found_items

    def _is_ad_like_group(
        self,
        leader: str,
        ad_data_for_marking: dict,
        ad_cache_manager: Any,
        scan_cache_manager: Any,
        color_gate_params: dict,
        user_thresh: float,
    ) -> bool:
        leader_ent = self.file_data.get(_norm_key(leader), {})
        h2 = self._coerce_hash_obj(leader_ent.get('phash'))
        grid2 = leader_ent.get('grid_phash')
        if not self._valid_hash_obj(h2):
            return False

        for ad_path, ad_ent in ad_data_for_marking.items():
            h1 = self._coerce_hash_obj(ad_ent.get('phash'))
            grid1 = ad_ent.get('grid_phash')
            if not self._valid_hash_obj(h1):
                continue

            h1_rots = [h1]
            if 'phash_rotations' in ad_ent:
                h1_rots.extend([self._coerce_hash_obj(ad_ent['phash_rotations'].get(k)) for k in ['90', '180', '270'] if k in ad_ent['phash_rotations']])
            sim_p = max([sim_from_hamming(can - h2, HASH_BITS) for can in h1_rots if self._valid_hash_obj(can)] + [0.0])

            grid1_all = [ad_ent.get('grid_phash', [])]
            if 'grid_rotations' in ad_ent:
                grid1_all.extend([ad_ent['grid_rotations'].get(k) for k in ['90', '180', '270'] if k in ad_ent['grid_rotations']])

            grid_rescue = False
            if grid2 and len(grid2) == 16:
                grid2_coerced = [self._coerce_hash_obj(gb) for gb in grid2]
                for g1_cand in grid1_all:
                    if grid_rescue:
                        break
                    if not g1_cand or len(g1_cand) != 16:
                        continue
                    matched_blocks = 0
                    for gb1, gh2 in zip(g1_cand, grid2_coerced):
                        gh1 = self._coerce_hash_obj(gb1)
                        if self._valid_hash_obj(gh1) and self._valid_hash_obj(gh2) and sim_from_hamming(gh1 - gh2, HASH_BITS) >= 0.95:
                            matched_blocks += 1
                    if matched_blocks >= 12:
                        grid_rescue = True

            if sim_p < PHASH_FAST_THRESH and not grid_rescue:
                continue

            if self.config.get('enable_color_filter', True):
                if sim_p < PHASH_STRICT_SKIP:
                    ad_feature_cache = ad_cache_manager or scan_cache_manager
                    if not self._ensure_features(ad_path, ad_feature_cache, need_hsv=True) or not self._ensure_features(leader, scan_cache_manager, need_hsv=True):
                        continue
                    hsv1 = self.file_data[_norm_key(ad_path)].get('avg_hsv')
                    hsv2 = self.file_data[_norm_key(leader)].get('avg_hsv')
                    if grid_rescue:
                        if hsv1 and hsv2 and abs(hsv1[2] - hsv2[2]) > 0.6:
                            continue
                    elif not _color_gate(hsv1, hsv2, **color_gate_params):
                        continue

            if grid_rescue:
                return True
            if not self.config.get('enable_whash', True):
                if sim_p >= user_thresh:
                    return True
            else:
                w1 = self._coerce_hash_obj(ad_ent.get('whash'))
                w2 = self._coerce_hash_obj(leader_ent.get('whash'))
                if self._valid_hash_obj(w1) and self._valid_hash_obj(w2):
                    sim_w = sim_from_hamming(w1 - w2, HASH_BITS)
                    whash_adaptive = 0.90 - max(0.0, min(1.0, (sim_p - 0.70) / 0.23)) * 0.20
                    if sim_w >= whash_adaptive:
                        return True
                elif sim_p >= PHASH_STRICT_SKIP:
                    return True
        return False

    def _filter_candidates_by_color(self, candidates_phash: list, color_gate_params: dict, use_color_filter: bool, stats: dict) -> list:
        log_info(f"[Phase C] 顏色過濾 {len(candidates_phash)} 個 pHash 候選...")
        if not candidates_phash: return []
        if not use_color_filter:
            stats['passed_color'] += len(candidates_phash)
            return candidates_phash

        import numpy as np
        
        hsv_pairs = []
        skip_mask = []
        rescue_mask = []
        
        for (_, m_path, p2_path, sim_p, grid_rescue) in candidates_phash:
            hsv1 = self.file_data.get(_norm_key(m_path), {}).get('avg_hsv') or (0, 0, 0)
            hsv2 = self.file_data.get(_norm_key(p2_path), {}).get('avg_hsv') or (0, 0, 0)
            hsv_pairs.append([hsv1, hsv2])
            skip_mask.append(sim_p >= PHASH_STRICT_SKIP)
            rescue_mask.append(grid_rescue)

        HSV = np.array(hsv_pairs, dtype=np.float32)
        SK = np.array(skip_mask, dtype=bool)
        GR = np.array(rescue_mask, dtype=bool)
        
        h_tol = color_gate_params['hue_deg_tol']
        s_tol = color_gate_params['sat_tol']
        ls_thresh = color_gate_params['low_sat_thresh']
        ls_v_tol = color_gate_params['low_sat_value_tol']
        ls_a_tol = color_gate_params['low_sat_achroma_tol']

        H1, S1, V1 = HSV[:, 0, 0], HSV[:, 0, 1], HSV[:, 0, 2]
        H2, S2, V2 = HSV[:, 1, 0], HSV[:, 1, 1], HSV[:, 1, 2]

        is_low_sat = np.maximum(S1, S2) < ls_thresh
        ls_ok = (np.abs(V1 - V2) <= ls_v_tol) & \
                (np.abs(V1*(1.0-S1) - V2*(1.0-S2)) <= ls_a_tol) & \
                (np.abs(S1 - S2) <= 0.15)
        
        dh = np.abs(H1 - H2)
        hue_diff = np.minimum(dh, 360.0 - dh)
        c_ok = (hue_diff <= h_tol) & (np.abs(S1 - S2) <= s_tol)
        
        gate_passed = np.where(is_low_sat, ls_ok, c_ok)
        gr_passed = (np.abs(V1 - V2) <= 0.6)
        
        final_mask = SK | (GR & gr_passed) | (~SK & ~GR & gate_passed)
        results = [c for i, c in enumerate(candidates_phash) if final_mask[i]]
        stats['passed_color'] += len(results)
        
        log_info(f"[Phase C 完成] 顏色過濾後剩餘: {len(results)} 個候選")
        return results

    def _ensure_candidate_whash(
        self,
        candidates_hsv: list,
        is_mutual_mode: bool,
        ad_cache_manager: Any,
        scan_cache_manager: Any,
    ) -> None:
        if not candidates_hsv:
            return
        non_grid = [(mp, p2) for (_, mp, p2, _, gr) in candidates_hsv if not gr]
        if not non_grid:
            return
        log_info(f"[Phase D] 批次順序讀取 {len(non_grid)} 個候選的 wHash...")
        primary_cache = scan_cache_manager if is_mutual_mode else ad_cache_manager
        cache_mgr_map_d, all_paths_d = self._build_candidate_cache_map(
            non_grid,
            primary_cache,
            scan_cache_manager,
        )
        self._batch_ensure_features(all_paths_d, cache_mgr_map_d, need_whash=True, phase_name="wHash")

    def _ensure_candidate_hsv(
        self,
        candidates: list,
        is_mutual_mode: bool,
        ad_cache_manager: Any,
        scan_cache_manager: Any,
    ) -> None:
        if not candidates:
            return
        log_info(f"[Phase B] 批次順序讀取 {len(candidates)} 個候選的 HSV...")
        primary_cache = scan_cache_manager if is_mutual_mode else ad_cache_manager
        cache_mgr_map, all_paths_b = self._build_candidate_cache_map(
            [(member_path, p2_path) for (_, member_path, p2_path, _, _) in candidates],
            primary_cache,
            scan_cache_manager,
        )
        self._batch_ensure_features(all_paths_b, cache_mgr_map, need_hsv=True, phase_name="HSV")

    def _collect_phash_candidates(
        self,
        ad_data_representatives: dict,
        gallery_data: dict,
        ad_data: dict,
        leader_to_ad_group: dict,
        phash_index: list,
        is_mutual_mode: bool,
        is_ad_mode: bool,
        inter_folder_only: bool,
        stats: dict,
        ad_cache_manager: Any = None,
        ad_member_to_leader: Optional[dict] = None,
    ) -> tuple[list, float]:
        log_info("[Phase A] 開始純記憶體 pHash 篩選 (向量化優化版)...")
        self._update_progress(text="🔍 [Phase A] pHash 快篩中 (向量化加速)...")
        candidates_phash = []
        phase_a_start = time.time()

        import numpy as np
        
        gallery_items = list(gallery_data.items())
        gallery_paths = [it[0] for it in gallery_items]
        gallery_hashes = np.array([self._h2i(it[1].get('phash')) for it in gallery_items], dtype=np.uint64)
        
        # 廣告模式必須以「成員圖」做 pHash 候選，而不是只用代表圖。
        # 代表圖只負責最後分組顯示；實際進 Phase E 的 member_path 必須是命中的那張廣告圖。
        ad_source_data = ad_data if is_ad_mode and not is_mutual_mode and ad_data else ad_data_representatives

        ad_paths = []
        ad_hashes_matrix = []
        ad_grid_matrix = []
        
        for ad_path, ad_ent in ad_source_data.items():
            h_base = self._h2i(ad_ent.get('phash'))
            if h_base == 0:
                continue
            rots = ad_ent.get('phash_rotations', {})
            # 缺失的旋轉 hash 不可補 0；0 會和壞快取/黑圖 hash 形成假 100%。
            h90 = self._h2i(rots.get('90')) or h_base
            h180 = self._h2i(rots.get('180')) or h_base
            h270 = self._h2i(rots.get('270')) or h_base
            ad_paths.append(ad_path)
            ad_hashes_matrix.append([h_base, h90, h180, h270])
            
            g_base = [self._h2i(x) for x in ad_ent.get('grid_phash', [])]
            if len(g_base) != 16: g_base = [0]*16
            g_rots = ad_ent.get('grid_rotations', {})
            def get_g_rot(k):
                r = [self._h2i(x) for x in g_rots.get(k, [])]
                return r if len(r) == 16 else [0]*16
            ad_grid_matrix.append([g_base, get_g_rot('90'), get_g_rot('180'), get_g_rot('270')])

        if not ad_paths:
            log_warning("[Phase A] 沒有有效的 pHash 來源，跳過候選生成。")
            return candidates_phash, phase_a_start

        AD_H = np.array(ad_hashes_matrix, dtype=np.uint64)
        AD_G = np.array(ad_grid_matrix, dtype=np.uint64)
        
        # [AD-LSH-02] 建立 Grid 倒排索引 (僅廣告比模式且非互比時啟用)
        grid_index = None
        if is_ad_mode and not is_mutual_mode:
            grid_index = self._build_grid_block_index(gallery_data)
        
        for i, ad_path in enumerate(ad_paths):
            if self._check_control() != 'continue': break
            
            ad_h_base = AD_H[i, 0]
            lsh_candidates = self._lsh_candidates_for(ad_path, ad_h_base, phash_index)
            
            # [AD-LSH-02] Fallback 補救邏輯
            if not lsh_candidates and grid_index is not None:
                stats['fallback_trigger_count'] = stats.get('fallback_trigger_count', 0) + 1
                ad_grid_blocks = AD_G[i, 0] # 使用 0 度旋轉作為投票基準
                votes = defaultdict(int)
                for b_idx, block_val in enumerate(ad_grid_blocks):
                    if block_val == 0: continue
                    for path in grid_index.get((b_idx, block_val), []):
                        votes[path] += 1
                
                # 門檻：matched_blocks >= 12
                fb_list = [p for p, v in votes.items() if v >= 12]
                
                # 分層 Cap
                MAX_FB_PER_AD = 200
                if len(fb_list) > MAX_FB_PER_AD:
                    fb_list = fb_list[:MAX_FB_PER_AD]
                    stats['fallback_cap_hit_ad'] = stats.get('fallback_cap_hit_ad', 0) + 1
                
                total_fb_so_far = stats.get('fallback_candidates_added', 0)
                MAX_FB_TOTAL = 5000
                if total_fb_so_far + len(fb_list) > MAX_FB_TOTAL:
                    allowed = MAX_FB_TOTAL - total_fb_so_far
                    fb_list = fb_list[:allowed]
                    stats['fallback_cap_hit_total'] = stats.get('fallback_cap_hit_total', 0) + 1
                
                if fb_list:
                    lsh_candidates = set(fb_list)
                    stats['fallback_candidates_added'] = total_fb_so_far + len(fb_list)
                    # 記錄這些是由補救產生的，供後續統計最終通過數
                    if 'fallback_pairs' not in stats: stats['fallback_pairs'] = set()
                    leader_path = ad_member_to_leader.get(ad_path, ad_path) if ad_member_to_leader else ad_path
                    for scan_item_path in lsh_candidates:
                        stats['fallback_pairs'].add((leader_path, scan_item_path))

            if not lsh_candidates: continue
            
            filtered_candidates = []
            for p2_path in lsh_candidates:
                if is_mutual_mode:
                    if p2_path <= ad_path:
                        continue
                    if inter_folder_only:
                        p1_parent_base = ad_path if not _is_virtual_path(ad_path) else _parse_virtual_path(ad_path)[0]
                        p2_parent_base = p2_path if not _is_virtual_path(p2_path) else _parse_virtual_path(p2_path)[0]
                        if os.path.dirname(p1_parent_base) == os.path.dirname(p2_parent_base):
                            stats['filtered_inter'] += 1
                            continue
                if is_ad_mode and p2_path in ad_data:
                    continue
                filtered_candidates.append(p2_path)
            if not filtered_candidates:
                continue

            lsh_set = set(filtered_candidates)
            lsh_mask = np.array([p in lsh_set for p in gallery_paths], dtype=bool)
            if not np.any(lsh_mask): continue
            
            target_hashes = gallery_hashes[lsh_mask]
            target_paths = [gallery_paths[idx] for idx, val in enumerate(lsh_mask) if val]
            valid_target_mask = target_hashes != 0
            if not np.any(valid_target_mask):
                continue
            if not np.all(valid_target_mask):
                target_hashes = target_hashes[valid_target_mask]
                target_paths = [p for p, keep in zip(target_paths, valid_target_mask) if keep]
            
            xor_results = AD_H[i][:, np.newaxis] ^ target_hashes
            hamming_dists = bit_count_np(xor_results)
            sims = 1.0 - (hamming_dists / 64.0)
            max_sims = np.max(sims, axis=0)
            
            stats['comparisons'] += len(target_paths)
            
            rescue_needed_mask = (max_sims < 0.70)
            grid_rescue_final = np.zeros(len(target_paths), dtype=bool)
            
            rescue_candidates_mask = rescue_needed_mask & (max_sims >= 0.4)
            if np.any(rescue_candidates_mask):
                for idx in np.where(rescue_candidates_mask)[0]:
                    g_path = target_paths[idx]
                    grid2 = [self._h2i(x) for x in gallery_data.get(g_path, {}).get('grid_phash', [])]
                    if len(grid2) != 16: continue
                    G2 = np.array(grid2, dtype=np.uint64)
                    g_xor = AD_G[i] ^ G2
                    valid_blocks = (AD_G[i] != 0) & (G2 != 0)
                    g_matches = np.sum(valid_blocks & (1.0 - (bit_count_np(g_xor) / 64.0) >= 0.95), axis=1)
                    if np.any(g_matches >= 12):
                        grid_rescue_final[idx] = True

            passed_mask = (max_sims >= 0.70) | grid_rescue_final
            for idx in np.where(passed_mask)[0]:
                scan_item_path = target_paths[idx]
                sim_p = float(max_sims[idx])
                is_gr = bool(grid_rescue_final[idx])
                
                # 語意整理 (L2-SIM-SAFETY):
                # leader_path: 廣告群組代表圖 (用於 UI 顯示)
                # member_path: 實際命中的廣告圖 (用於細項比對)
                # scan_item_path: 當前掃描中的圖片
                if is_ad_mode and not is_mutual_mode:
                    leader_path = ad_member_to_leader.get(ad_path, ad_path) if ad_member_to_leader else ad_path
                    member_path = ad_path
                else:
                    leader_path = ad_path
                    member_path = ad_path
                    
                candidates_phash.append((leader_path, member_path, scan_item_path, sim_p, is_gr))
                stats['passed_phash'] += 1
                

        return candidates_phash, phase_a_start

    def _select_final_matches(self, candidates_hsv: list, user_thresh: float, use_whash: bool, stats: dict, phase_a_start: float) -> list:
        log_info(f"[Phase E] wHash 最終過濾 {len(candidates_hsv)} 個候選...")
        if not candidates_hsv: return []
        
        import numpy as np
        stats['entered_whash'] += len(candidates_hsv)
        
        sim_p_arr = np.array([c[3] for c in candidates_hsv], dtype=np.float32)
        gr_arr = np.array([c[4] for c in candidates_hsv], dtype=bool)
        
        w1_list, w2_list = [], []
        for (_, m_path, p2_path, _, _) in candidates_hsv:
            w1_list.append(self._h2i(self.file_data.get(_norm_key(m_path), {}).get('whash')))
            w2_list.append(self._h2i(self.file_data.get(_norm_key(p2_path), {}).get('whash')))
        
        W1 = np.array(w1_list, dtype=np.uint64)
        W2 = np.array(w2_list, dtype=np.uint64)
        
        if use_whash:
            # 增加安全檢查：如果雜湊值為 0，通常代表讀圖失敗或純色，不應視為有效匹配
            valid_w = (W1 != 0) & (W2 != 0)
            diffs = bit_count_np(W1 ^ W2)
            sim_w = 1.0 - (diffs / HASH_BITS)
            wh_adaptive = 0.90 - np.maximum(0.0, np.minimum(1.0, (sim_p_arr - 0.70) / 0.23)) * 0.20
            
            # 如果 wHash 無效且 pHash 又太低，則不予通過
            passed_w = valid_w & (sim_w >= wh_adaptive)
            passed_strict = (sim_p_arr >= PHASH_STRICT_SKIP) & (W1 != 0) & (W2 != 0)
            accepted = gr_arr | passed_w | passed_strict
            final_sims = np.where(gr_arr, np.maximum(sim_p_arr, 0.95),
                         np.where(passed_w, np.maximum(sim_p_arr, sim_w), sim_p_arr))
            log_info(
                f"[Phase E 診斷] valid_w={int(np.count_nonzero(valid_w))}/{len(candidates_hsv)}, "
                f"passed_w={int(np.count_nonzero(passed_w))}, "
                f"strict_phash={int(np.count_nonzero(passed_strict))}, "
                f"grid_rescue={int(np.count_nonzero(gr_arr))}"
            )
        else:
            accepted = gr_arr | (sim_p_arr >= user_thresh)
            final_sims = np.where(gr_arr, np.maximum(sim_p_arr, 0.95), sim_p_arr)
            
        final_mask = accepted & (final_sims >= user_thresh)
        best_match = {}
        skipped_self_matches = 0
        for i, (leader_path, m_path, scan_item_path, _, _) in enumerate(candidates_hsv):
            if not final_mask[i]: continue
            
            # Self-comparison guard (L2-SIM-SAFETY): 
            # 確保不會自己跟自己比，這通常發生在互比模式或路徑重疊情境。
            if _norm_key(m_path) == _norm_key(scan_item_path):
                skipped_self_matches += 1
                continue
                
            key = (leader_path, scan_item_path)
            f_sim = final_sims[i]
            prev = best_match.get(key)
            if prev is None or f_sim > prev[1]:
                best_match[key] = (m_path, f_sim)

        if skipped_self_matches:
            log_warning(f"[Phase E guard] 阻擋了 {skipped_self_matches} 筆自我比對候選 (路徑相同)")

        # [AD-LSH-02] 精準統計補救候選最終通過數 (避免 member 展開造成的重複計數)
        if 'fallback_pairs' in stats:
            stats['fallback_total_passed'] = len(set(best_match.keys()) & stats['fallback_pairs'])

        temp_found_pairs = [
            (leader_path, scan_item_path, f"{best_sim * 100:.1f}%")
            for (leader_path, scan_item_path), (_, best_sim) in best_match.items()
        ]
        total_elapsed = time.time() - phase_a_start
        log_info(f"[Phase E 完成] 最終匹配: {len(temp_found_pairs)} 對 | 總比對耗時: {total_elapsed:.1f}s")
        return temp_found_pairs

    def _apply_digest_filter(self, tasks: list[str], scan_cache_manager: Any, current_digest: str) -> list[str]:
        unmatched_tasks = []
        for path in tasks:
            cached_data = scan_cache_manager.get_data(path)
            if not cached_data or cached_data.get('last_ad_digest', '') != current_digest:
                unmatched_tasks.append(path)
        return unmatched_tasks

    def _run_targeted_search(
        self,
        ad_data_representatives: dict,
        ad_data: dict,
        gallery_data: dict,
        ad_cache_manager: Any = None,
        ad_member_to_leader: Optional[dict] = None,
    ):
        log_info("[尋親模式] 已啟用，將為每張廣告尋找最佳配對（忽略門檻）。")
        targeted_pairs = []
        gallery_list = list(gallery_data.items())
        total_ads = len(ad_data_representatives)
        targeted_floor_sim = 0.40
        ad_member_to_leader = ad_member_to_leader or {}

        if ad_cache_manager and hasattr(ad_cache_manager, "query_hash_index"):
            best_matches = {
                leader: {'path': None, 'sim': targeted_floor_sim}
                for leader in ad_data_representatives.keys()
            }
            for scan_path, scan_ent in gallery_list:
                h2 = self._coerce_hash_obj(scan_ent.get('phash'))
                if not self._valid_hash_obj(h2): continue
                candidate_paths = ad_cache_manager.query_hash_index(h2)
                if not candidate_paths: continue
                for ad_path in candidate_paths:
                    ad_ent = ad_data.get(ad_path, {})
                    h1 = self._coerce_hash_obj(ad_ent.get('phash'))
                    if not self._valid_hash_obj(h1): continue
                    h1_rots = [h1]
                    rots = ad_ent.get('phash_rotations', {}) or {}
                    h1_rots.extend([self._coerce_hash_obj(rots.get(k)) for k in ('90', '180', '270') if rots.get(k)])
                    best_rot_sim = max((sim_from_hamming(h1c - h2, HASH_BITS) for h1c in h1_rots if self._valid_hash_obj(h1c)), default=0.0)
                    leader = ad_member_to_leader.get(ad_path, ad_path)
                    if leader not in best_matches:
                        best_matches[leader] = {'path': None, 'sim': targeted_floor_sim}
                    if best_rot_sim > best_matches[leader]['sim']:
                        best_matches[leader] = {'path': scan_path, 'sim': best_rot_sim}

            for ad_idx, ad_path in enumerate(ad_data_representatives.keys()):
                match = best_matches.get(ad_path, {'path': None, 'sim': targeted_floor_sim})
                self._update_progress(text=f"🔍 [尋親] 正在彙整第 {ad_idx + 1}/{total_ads} 張廣告...")
                if match['path']:
                    targeted_pairs.append((ad_path, match['path'], f"{match['sim'] * 100:.1f}%"))
                    log_info(f"  [尋親] ✓ {os.path.basename(ad_path)} → {os.path.basename(match['path'])} ({match['sim']*100:.1f}%)")
                else:
                    log_info(f"  [尋親] ✗ {os.path.basename(ad_path)} → 無法找到高於 {targeted_floor_sim*100:.0f}% 的配對。")
            log_info(f"[尋親模式] 完成，共配對 {len(targeted_pairs)}/{total_ads} 張廣告。")
            return targeted_pairs, {}

        for ad_idx, (ad_path, ad_ent) in enumerate(ad_data_representatives.items()):
            if self._check_control() != 'continue': return None
            self._update_progress(text=f"🔍 [尋親] 正在比對第 {ad_idx + 1}/{total_ads} 張廣告...")
            best_match_path, best_match_sim = None, targeted_floor_sim
            ad_h_base = self._coerce_hash_obj(ad_ent.get('phash'))
            ad_hashes = [ad_h_base] if self._valid_hash_obj(ad_h_base) else []
            rots = ad_ent.get('phash_rotations', {})
            for rot_key in ['90', '180', '270']:
                rh = self._coerce_hash_obj(rots.get(rot_key))
                if self._valid_hash_obj(rh): ad_hashes.append(rh)
            if not ad_hashes: continue

            for scan_path, scan_ent in gallery_list:
                if scan_path in ad_data: continue
                h2 = self._coerce_hash_obj(scan_ent.get('phash'))
                if not self._valid_hash_obj(h2): continue
                best_rot_sim = max((sim_from_hamming(h1 - h2, HASH_BITS) for h1 in ad_hashes if self._valid_hash_obj(h1)), default=0.0)
                if best_rot_sim > best_match_sim:
                    best_match_sim, best_match_path = best_rot_sim, scan_path

            if best_match_path:
                targeted_pairs.append((ad_path, best_match_path, f"{best_match_sim * 100:.1f}%"))
                log_info(f"  [尋親] ✓ {os.path.basename(ad_path)} → {os.path.basename(best_match_path)} ({best_match_sim*100:.1f}%)")
            else:
                log_info(f"  [尋親] ✗ {os.path.basename(ad_path)} → 無法找到高於 {targeted_floor_sim*100:.0f}% 的配對。")

        log_info(f"[尋親模式] 完成，共配對 {len(targeted_pairs)}/{total_ads} 張廣告。")
        return targeted_pairs, {}

    def _build_color_gate_params(self, user_thresh_percent: float) -> dict:
        def lerp(p, start, limit):
            weight = (100.0 - max(70.0, min(100.0, float(p)))) / 30.0
            return start + weight * (limit - start)
        return {
            'hue_deg_tol': lerp(user_thresh_percent, 18.0, 35.0),
            'sat_tol': lerp(user_thresh_percent, 0.18, 0.40),
            'low_sat_value_tol': lerp(user_thresh_percent, 0.10, 0.30),
            'low_sat_achroma_tol': lerp(user_thresh_percent, 0.12, 0.30),
            'low_sat_thresh': 0.18,
        }

    def _log_funnel_stats(self, stats: dict, final_matches: int):
        log_info("--- 比對引擎漏斗統計 ---")
        if stats.get('filtered_inter', 0) > 0:
            log_info(f"因 \"僅比對不同資料夾\" 而跳過: {stats['filtered_inter']:,} 次")
        
        fb_triggered = stats.get('fallback_trigger_count', 0)
        fb_added = stats.get('fallback_candidates_added', 0)
        fb_passed = stats.get('fallback_total_passed', 0)
        if fb_triggered > 0:
            msg = f"Grid-Block 補救觸發: {fb_triggered} 次, 補回候選: {fb_added} 筆, 最終匹配: {fb_passed} 筆"
            cap_ad = stats.get('fallback_cap_hit_ad', 0)
            cap_total = stats.get('fallback_cap_hit_total', 0)
            if cap_ad or cap_total:
                msg += f" (截斷發生: 每圖{cap_ad}次, 總量{cap_total}次)"
            log_info(msg)

        comparisons, passed_phash = stats.get('comparisons', 0), stats.get('passed_phash', 0)
        passed_color, entered_whash = stats.get('passed_color', 0), stats.get('entered_whash', 0)
        log_info(f"廣告組展開後總比對次數: {comparisons:,}")
        log_info(f"通過 pHash 快篩: {passed_phash:,} ({(passed_phash/max(comparisons, 1))*100:.1f}%)")
        log_info(f" └─ 通過顏色過濾閘: {passed_color:,} ({(passed_color/max(passed_phash, 1))*100:.1f}%)")
        log_info(f"    └─ 進入 wHash 複核: {entered_whash:,} ({(entered_whash/max(passed_color, 1))*100:.1f}%)")
        log_info(f"       └─ 最終有效匹配: {final_matches:,} ({(final_matches/max(entered_whash, 1))*100:.1f}%)")
        log_info("--------------------------")

    def _build_grid_block_index(self, gallery_file_data: dict, max_bucket_size=1000):
        """建立 Grid Block 倒排索引，用於 Phase A 補救 (AD-LSH-02)。"""
        from collections import defaultdict
        index = defaultdict(list)
        oversaturated = set()
        for path, ent in gallery_file_data.items():
            grid = ent.get('grid_phash', [])
            if len(grid) != 16: continue
            for i, block in enumerate(grid):
                val = self._h2i(block)
                if val == 0: continue
                key = (i, val)
                if key in oversaturated: continue
                bucket = index[key]
                if len(bucket) >= max_bucket_size:
                    index.pop(key)
                    oversaturated.add(key)
                    continue
                bucket.append(_norm_key(path))
        return index

    def _build_candidate_cache_map(self, pairs: list, primary_cache_manager: Any, gallery_cache_manager: Any) -> tuple[dict, list]:
        cache_mgr_map, ordered_paths = {}, []
        for left_path, right_path in pairs:
            cache_mgr_map[_norm_key(left_path)] = primary_cache_manager
            cache_mgr_map[_norm_key(right_path)] = gallery_cache_manager
            ordered_paths.extend([left_path, right_path])
        return cache_mgr_map, ordered_paths

    def _build_digest_patch(self, current_data: dict) -> dict:
        patch = {
            'phash': str(current_data.get('phash')) if current_data.get('phash') else None,
            'whash': str(current_data.get('whash')) if current_data.get('whash') else None,
            'avg_hsv': list(current_data.get('avg_hsv')) if current_data.get('avg_hsv') else None,
            'grid_phash': current_data.get('grid_phash', []),
            'features_at': current_data.get('features_at', 0),
        }
        return {k: v for k, v in patch.items() if v is not None}

    def _collect_image_paths(self, folder_path: str) -> list[str]:
        img_exts = ('.png', '.jpg', '.jpeg', '.webp', '.gif', '.bmp')
        paths = []
        if not folder_path or not os.path.isdir(folder_path): return paths
        for ent in _iter_scandir_recursively(folder_path, set(), set(), self.control_events):
            if ent.is_file() and ent.name.lower().endswith(img_exts):
                paths.append(_norm_key(ent.path))
        return paths

    def _accept_pair_with_dual_hash(self, ad_hash_obj, g_hash_obj, ad_w_hash, g_w_hash, ad_grid=None, g_grid=None) -> tuple[bool, float]:
        h1, h2 = self._coerce_hash_obj(ad_hash_obj), self._coerce_hash_obj(g_hash_obj)
        w1, w2 = self._coerce_hash_obj(ad_w_hash), self._coerce_hash_obj(g_w_hash)
        if not self._valid_hash_obj(h1) or not self._valid_hash_obj(h2): return False, 0.0
        sim_p = sim_from_hamming(h1 - h2, HASH_BITS)
        grid_rescue = False
        if ad_grid and g_grid and len(ad_grid) == 16 and len(g_grid) == 16:
            m = sum(
                1 for b1, b2 in zip(ad_grid, g_grid)
                if self._h2i(b1) != 0 and self._h2i(b2) != 0
                and sim_from_hamming(self._h2i(b1) - self._h2i(b2), 64) >= 0.95
            )
            if m >= 12: grid_rescue = True
        if sim_p < PHASH_FAST_THRESH and not grid_rescue: return False, sim_p
        user_t = float(self.config.get('similarity_threshold', 95.0)) / 100.0
        if not self._valid_hash_obj(w1) or not self._valid_hash_obj(w2):
            if grid_rescue: return True, max(sim_p, max(user_t, 0.95))
            return (True, sim_p) if sim_p >= PHASH_STRICT_SKIP else (False, sim_p)
        sim_w = sim_from_hamming(w1 - w2, HASH_BITS)
        if grid_rescue: return True, max(sim_p, sim_w, max(user_t, 0.95))
        if not self.config.get('enable_whash', True): return (True, sim_p) if sim_p >= user_t else (False, sim_p)
        if self.config.get('enable_targeted_search', False): return True, max(sim_p, sim_w)
        whash_adaptive = 0.90 - max(0.0, min(1.0, (sim_p - 0.70) / 0.23)) * 0.20
        if sim_w >= whash_adaptive or sim_p >= PHASH_STRICT_SKIP: return True, max(sim_p, sim_w)
        return False, sim_p

    def _build_phash_band_index(self, gallery_file_data: dict, bands=LSH_BANDS):
        seg_bits = HASH_BITS // bands; mask = (1 << seg_bits) - 1
        index = [defaultdict(list) for _ in range(bands)]
        for path, ent in gallery_file_data.items():
            phash_obj = self._coerce_hash_obj(ent.get('phash'))
            if not self._valid_hash_obj(phash_obj): continue
            v = self._h2i(phash_obj)
            for b in range(bands):
                key = (v >> (b * seg_bits)) & mask
                index[b][key].append(_norm_key(path))
        return index

    def _lsh_candidates_for(self, ad_path: str, ad_hash_obj: Any, index: list, bands=LSH_BANDS):
        seg_bits = HASH_BITS // bands; mask = (1 << seg_bits) - 1
        v = self._h2i(ad_hash_obj)
        cand = set()
        for b in range(bands):
            key = (v >> (b * seg_bits)) & mask
            cand.update(index[b].get(key, []))
        if _norm_key(ad_path) in cand: cand.remove(_norm_key(ad_path))
        return cand
