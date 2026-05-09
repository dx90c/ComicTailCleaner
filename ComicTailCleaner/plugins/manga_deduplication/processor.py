# ======================================================================
# 檔案：plugins/manga_deduplication/processor.py
# 目的：相似卷宗查找器 (v17.1.0 - 專業重構版)
# ======================================================================

from __future__ import annotations
import os
import re
import sqlite3
import unicodedata
import imagehash
from collections import defaultdict
from typing import Dict, Any, Tuple, List, Optional, Set
from queue import Queue

try:
    from tkinter import ttk
except ImportError:
    ttk = None

from plugins.base_plugin import BasePlugin
from core_engine import ImageComparisonEngine, HASH_BITS
from processors.scanner import get_files_to_process, _natural_sort_key, ScannedImageCacheManager
from utils import log_info, log_error, _norm_key, log_warning, _is_virtual_path, _parse_virtual_path
import config as app_config
from . import plugin_gui


# ======================================================================
# Section: 骨幹快取管理器
# ======================================================================

class SkeletonCacheManager:
    _TABLE_DDL = """
        CREATE TABLE IF NOT EXISTS folder_skeletons (
            path     TEXT PRIMARY KEY,
            skeleton TEXT NOT NULL,
            vol_num  INTEGER,
            mtime    REAL NOT NULL
        )
    """

    def __init__(self):
        db_dir = getattr(app_config, 'DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
        self._db_path = os.path.join(db_dir, 'folder_skeleton_cache.db')
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_conn()

    def _ensure_conn(self):
        try:
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.execute(self._TABLE_DDL)
            self._conn.commit()
        except Exception as e:
            log_warning(f"[SkeletonCache] Failed: {e}")
            self._conn = None

    def batch_load(self) -> dict:
        if not self._conn: return {}
        try:
            cursor = self._conn.execute("SELECT path, skeleton, vol_num, mtime FROM folder_skeletons")
            return {row[0]: {'skeleton': row[1], 'vol_num': row[2], 'mtime': row[3]} for row in cursor.fetchall()}
        except Exception: return {}

    def batch_save(self, entries: list):
        if not self._conn or not entries: return
        try:
            self._conn.executemany("INSERT OR REPLACE INTO folder_skeletons (path, skeleton, vol_num, mtime) VALUES (?, ?, ?, ?)", entries)
            self._conn.commit()
        except Exception as e: log_warning(f"[SkeletonCache] Save failed: {e}")

    def close(self):
        if self._conn:
            try: self._conn.close()
            except Exception: pass
            self._conn = None


# ======================================================================
# Section: Skeleton Extraction v2 (Module 5)
# ======================================================================

# v17.1.0 Professional Cleaning Chain
_RE_METADATA_BRACKETS = re.compile(r'\[.*?\]|\(.*?\)|【.*?】|（.*?）')
_RE_VOLUME_LABELS     = re.compile(r'(?i)(vol|ch|chapter|ep|page|book|v|第|卷|巻|話|话|集|冊|册|号|號)\.?\s*\d+', re.IGNORECASE)
_RE_TRAILING_DIGITS   = re.compile(r'\s+#?\d+\s*$')
_RE_CLEANUP_NOISE     = re.compile(r'([-_\s\.])+')


def _normalize_fullwidth(s: str) -> str:
    return unicodedata.normalize('NFKC', s)


def _extract_skeleton(folder_name: str) -> Tuple[str, Optional[int]]:
    """
    [v2.0] Extract skeleton name and volume number from folder name.
    """
    original_name = folder_name
    name = _normalize_fullwidth(folder_name)

    # 1. Extract volume number
    vol_num: Optional[int] = None
    m_vol = re.search(r'(?i)(?:vol|v|第|ch|集|冊|卷)\.?\s*(\d+)', name)
    if m_vol:
        vol_num = int(m_vol.group(1))
    
    # 2. Sequential Cleanup Chain (Module 5)
    name = _RE_METADATA_BRACKETS.sub(' ', name)
    name = _RE_VOLUME_LABELS.sub(' ', name)
    name = _RE_TRAILING_DIGITS.sub(' ', name)
    
    # 3. Final noise compression and normalization
    name = _RE_CLEANUP_NOISE.sub(' ', name).strip().lower()

    # 4. Fallback for edge cases
    if not name or len(name) < 2:
        name = _normalize_fullwidth(original_name).lower().strip()

    return name, vol_num


# ======================================================================
# Section: Selection Strategy
# ======================================================================

class VolumeStrategy:
    def __init__(self):
        self.uncensored_keywords = ["無修正", "decensored", "uncensored", "步兵", "流出"]
        self.chinese_keywords    = ["漢化", "中文", "chinese", "翻譯", "汉化"]
        self.ongoing_keywords    = ["進行中", "ongoing"]

    def _get_score(self, path: str) -> int:
        score = 0
        name = os.path.basename(path).lower()
        if any(k in name for k in self.ongoing_keywords):    score -= 500
        if any(k in name for k in self.uncensored_keywords): score += 100
        return score

    def calculate(self, all_groups: List[Tuple]) -> Set[str]:
        to_select = set()
        for p1, p2, sim in all_groups:
            s1, s2 = self._get_score(p1), self._get_score(p2)
            if   s1 > s2: to_select.add(p2)
            elif s2 > s1: to_select.add(p1)
        return to_select


# ======================================================================
# Section: Plugin Body
# ======================================================================

class MangaDeduplicationPlugin(BasePlugin):

    def get_id(self)   -> str: return "manga_volume_deduplication_smart"
    def get_name(self) -> str: return "相似卷宗查找 (智慧分組版)"

    def get_default_config(self) -> Dict[str, Any]:
        return {
            'manga_dedupe_enable_sample_limit': True,
            'manga_dedupe_sample_count':        12,
            'manga_dedupe_match_threshold':     8,
            'manga_dedupe_cross_lang':          True,   
            'manga_dedupe_dup_threshold':       80,     
        }

    def get_settings_frame(self, parent_frame, config, ui_vars):
        return plugin_gui.create_settings_frame(parent_frame, config, ui_vars)

    def save_settings(self, config, ui_vars):
        return plugin_gui.save_settings(config, ui_vars)

    def get_styles(self) -> Dict[str, Dict[str, str]]:
        return {
            "uncensored_tag": {"background": "#C8E6C9", "foreground": "#2E7D32"},
            "duplicate_tag":  {"background": "#FFF9C4", "foreground": "#F57F17"},
            "series_only_tag":{"background": "#E3F2FD", "foreground": "#1565C0"},
        }

    def get_selection_strategy(self, config):
        return VolumeStrategy()

    @staticmethod
    def _tol_bits_from_slider(cfg: dict) -> int:
        s = max(0, min(100, int(cfg.get("similarity_threshold", 95))))
        return int((100 - s) * HASH_BITS / 100)

    @staticmethod
    def _greedy_match_pairs(a: list, b: list, tol_bits: int):
        if not a or not b: return 0, None
        used = [False] * len(b)
        matched = 0
        last_pair = None
        for h1, f1_path in a:
            for j, (h2, f2_path) in enumerate(b):
                if not used[j] and (h1 - h2) <= tol_bits:
                    matched += 1; used[j] = True
                    last_pair = (f1_path, f2_path)
                    break
        return matched, last_pair

    def _coerce_hash_obj(self, h):
        if h is None or imagehash is None: return None
        if isinstance(h, imagehash.ImageHash): return h
        try: return imagehash.hex_to_hash(str(h))
        except: return None

    def _check_uncensored(self, path: str) -> bool:
        keywords = ["無修正", "decensored", "uncensored", "步兵", "流出"]
        return any(k in os.path.basename(path).lower() for k in keywords)

    def _count_real_pages(self, folder_path: str) -> int:
        if not os.path.isdir(folder_path): return 0
        count = 0
        img_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff'}
        try:
            with os.scandir(folder_path) as it:
                for entry in it:
                    if entry.is_file() and os.path.splitext(entry.name)[1].lower() in img_exts:
                        count += 1
        except OSError: pass
        return count

    def run(self,
            config:              Dict[str, Any],
            progress_queue:      Optional[Queue] = None,
            control_events:      Optional[Dict]  = None,
            app_update_callback: Optional[callable] = None
            ) -> Optional[Tuple[List, Dict, List]]:

        _upd = lambda text, v=None: progress_queue.put(
            {'type': 'progress' if v is not None else 'text', 'text': text, 'value': v}
        ) if progress_queue else None
        _cancelled = lambda: bool(
            control_events and control_events.get('cancel') and control_events['cancel'].is_set()
        )

        skeleton_cache = SkeletonCacheManager()

        try:
            log_info("[Similarity Detector] v17.1.0 starting...")
            sample_count    = max(2, int(config.get('manga_dedupe_sample_count',    12)))
            MATCH_THRESHOLD = max(2, int(config.get('manga_dedupe_match_threshold', 8)))
            cross_lang      = bool(config.get('manga_dedupe_cross_lang', True))
            dup_pct         = max(1, min(100, int(config.get('manga_dedupe_dup_threshold', 80))))

            engine = ImageComparisonEngine(config, progress_queue, control_events)
            _upd("Preparing files...")
            files_to_process, _ = get_files_to_process(config, engine.scan_cache_manager, progress_queue, control_events)
            if _cancelled() or not files_to_process: return ([], {}, []) if not _cancelled() else None

            _upd("Loading fingerprints...")
            continue_proc, all_file_data = engine.compute_phashes(files_to_process, engine.scan_cache_manager, "Fingerprints")
            if not continue_proc: return None

            ad_hashes_set: Set = set()
            ad_folder_path = config.get('ad_folder_path')
            if ad_folder_path and os.path.isdir(ad_folder_path):
                from processors.scanner import MasterAdCacheManager, _iter_scandir_recursively
                ad_cache = MasterAdCacheManager(ad_folder_path)
                ad_paths = []
                img_exts = ('.png', '.jpg', '.jpeg', '.webp')
                for ent in _iter_scandir_recursively(ad_folder_path, set(), set(), control_events):
                    if ent.is_file() and ent.name.lower().endswith(img_exts):
                        ad_paths.append(_norm_key(ent.path))
                
                _, ad_data = engine.compute_phashes(ad_paths, ad_cache, "AdMaster", progress_scope="local")
                ad_hashes_set = {self._coerce_hash_obj(d.get('phash')) for d in ad_data.values() if d and d.get('phash')}
                ad_hashes_set.discard(None)

            _upd("Building folder index...")
            files_by_folder: Dict[str, List[str]] = defaultdict(list)
            for f_path in files_to_process:
                container = _norm_key(_parse_virtual_path(f_path)[0]) if _is_virtual_path(f_path) else _norm_key(os.path.dirname(f_path))
                if container: files_by_folder[container].append(f_path)

            fingerprints: Dict[str, List] = {}
            for folder, files in files_by_folder.items():
                if _cancelled(): return None
                files.sort(key=_natural_sort_key)
                fp = []
                for f in reversed(files):
                    if len(fp) >= sample_count: break
                    data = all_file_data.get(_norm_key(f))
                    if data and 'phash' in data:
                        h = self._coerce_hash_obj(data['phash'])
                        if h and h not in ad_hashes_set: fp.append((h, f))
                fingerprints[folder] = fp

            folder_list = sorted(fingerprints.keys())
            tol_bits    = self._tol_bits_from_slider(config)

            _upd("Extracting skeletons...")
            cached_skeletons = skeleton_cache.batch_load()
            skeleton_info: Dict[str, Tuple[str, Optional[int]]] = {}
            new_entries: List[Tuple] = []

            for folder in folder_list:
                if _cancelled(): return None
                folder_name = os.path.basename(folder)
                try: mtime = os.stat(folder).st_mtime
                except OSError: mtime = 0.0

                cached = cached_skeletons.get(folder)
                if cached and abs(cached.get('mtime', -1) - mtime) < 1.0:
                    skeleton_info[folder] = (cached['skeleton'], cached.get('vol_num'))
                else:
                    skel, vol_num = _extract_skeleton(folder_name)
                    skeleton_info[folder] = (skel, vol_num)
                    new_entries.append((folder, skel, vol_num, mtime))

            if new_entries: skeleton_cache.batch_save(new_entries)

            series_buckets: Dict[str, List[str]] = defaultdict(list)
            for folder in folder_list:
                skel, _ = skeleton_info[folder]
                series_buckets[skel].append(folder)

            _upd("Comparing similarity...")
            intra_duplicates: Set[Tuple[str, str]] = set()  
            matched_display: Dict[str, str] = {} 
            for skel, folders in series_buckets.items():
                if len(folders) < 2: continue
                for i in range(len(folders)):
                    for j in range(i + 1, len(folders)):
                        f1, f2 = folders[i], folders[j]
                        fp1, fp2 = fingerprints.get(f1, []), fingerprints.get(f2, [])
                        if not fp1 or not fp2: continue
                        match, pair = self._greedy_match_pairs(fp1, fp2, tol_bits)
                        min_len = min(len(fp1), len(fp2))
                        if min_len > 0 and (match / min_len * 100) >= dup_pct:
                            intra_duplicates.add((min(f1, f2), max(f1, f2)))
                            if pair:
                                if f1 not in matched_display: matched_display[f1] = pair[0]
                                if f2 not in matched_display: matched_display[f2] = pair[1]

            inter_duplicates: Set[Tuple[str, str]] = set()
            if cross_lang:
                _upd("Searching cross-language duplicates...")
                hash_to_folders: Dict[str, Set[str]] = defaultdict(set)
                for folder, fp_list in fingerprints.items():
                    for h, f_path in fp_list:
                        bucket = str(h)[:14]
                        hash_to_folders[bucket].add(folder)

                candidate_pairs: Set[Tuple[str, str]] = set()
                for bucket, folders_in_bucket in hash_to_folders.items():
                    if len(folders_in_bucket) < 2: continue
                    f_list = sorted(folders_in_bucket)
                    for i in range(len(f_list)):
                        for j in range(i + 1, len(f_list)):
                            f1, f2 = f_list[i], f_list[j]
                            if skeleton_info.get(f1, ('',None))[0] != skeleton_info.get(f2, ('',None))[0]:
                                candidate_pairs.add((min(f1,f2), max(f1,f2)))

                for f1, f2 in candidate_pairs:
                    if _cancelled(): return None
                    fp1, fp2 = fingerprints.get(f1, []), fingerprints.get(f2, [])
                    match, pair = self._greedy_match_pairs(fp1, fp2, tol_bits)
                    if min(len(fp1), len(fp2)) > 0 and (match / min(len(fp1), len(fp2)) * 100) >= dup_pct:
                        inter_duplicates.add((f1, f2))
                        if pair:
                            if f1 not in matched_display: matched_display[f1] = pair[0]
                            if f2 not in matched_display: matched_display[f2] = pair[1]

            class UnionFind:
                def __init__(self): self.parent = {}
                def find(self, i):
                    if self.parent.setdefault(i, i) == i: return i
                    self.parent[i] = self.find(self.parent[i])
                    return self.parent[i]
                def union(self, i, j):
                    r1, r2 = self.find(i), self.find(j)
                    if r1 != r2:
                        if len(r1) <= len(r2): self.parent[r2] = r1
                        else: self.parent[r1] = r2

            dsu = UnionFind()
            for f1, f2 in inter_duplicates:
                s1, s2 = skeleton_info.get(f1, ('',None))[0], skeleton_info.get(f2, ('',None))[0]
                if s1 and s2: dsu.union(s1, s2)

            merged_series: Dict[str, List[str]] = defaultdict(list)
            for skel, folders in series_buckets.items():
                merged_series[dsu.find(skel)].extend(folders)

            found_items: List[Tuple] = []
            processed_folders: Set[str] = set()
            all_dups = {tuple(sorted(p)) for p in (list(intra_duplicates) + list(inter_duplicates))}

            for r_skel, s_folders in sorted(merged_series.items()):
                if len(s_folders) < 2: continue
                s_folders_sorted = sorted(s_folders)
                gk = s_folders_sorted[0]
                for f in s_folders_sorted:
                    if f in processed_folders: continue
                    skel, vol = skeleton_info.get(f, (r_skel, None))
                    lbl = f"Vol.{vol}" if vol is not None else "?"
                    cnt = self._count_real_pages(f) if not _is_virtual_path(f) else len(files_by_folder.get(_norm_key(f), []))
                    comps = [o for o in s_folders_sorted if o != f and tuple(sorted((f, o))) in all_dups]
                    if comps:
                        tag = "duplicate_tag"
                        msg = f"{lbl} | {cnt}P | Duplicate ({os.path.basename(comps[0])})"
                    else:
                        tag = "uncensored_tag" if self._check_uncensored(f) else "series_only_tag"
                        msg = f"{lbl} | {cnt}P"
                    found_items.append((gk, f, msg, tag))
                    processed_folders.add(f)

            gui_data: Dict[str, Any] = {}
            for path in {p for _, p, _, _ in found_items} | {gk for gk, _, _, _ in found_items}:
                img_files = files_by_folder.get(_norm_key(path))
                dp = matched_display.get(path, img_files[0] if img_files else path)
                sk, _ = skeleton_info.get(_norm_key(path), (os.path.basename(path), None))
                gui_data[path] = {
                    'display_path': dp,
                    'page_count':   self._count_real_pages(path) if not _is_virtual_path(path) else len(img_files or []),
                    'display_name': dsu.find(sk) if sk else sk,
                }

            return found_items, gui_data, []
        except Exception as e:
            log_error(f"[MangaDedupe] Fatal: {e}", True)
            return [], {}, [("Error", str(e))]
        finally: skeleton_cache.close()
