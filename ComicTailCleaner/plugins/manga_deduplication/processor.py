# ======================================================================
# 檔案：plugins/manga_deduplication/processor.py
# 目的：相似卷宗查找器 (v14.0 - 智慧分組版)
# 版本：14.0.0
#   - [NEW] 兩段式識別：名稱骨幹分組 + pHash 反向索引跨名重複偵測
#   - [NEW] SQLite 持久化骨幹快取（插件自管，不依賴核心引擎）
#   - [OPTIMIZE] O(N²) → O(N×sample) 指紋查找，支援三萬個資料夾
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
# Section: 骨幹快取管理器（插件自管 SQLite，不依賴核心引擎快取）
# ======================================================================

class SkeletonCacheManager:
    """
    輕量的 SQLite 管理器，用於持久化每個資料夾的「骨幹名稱」。
    表結構：
        path    TEXT PRIMARY KEY  – 正規化資料夾路徑
        skeleton TEXT             – 清洗後的系列骨幹名稱
        vol_num INTEGER           – 集數 (可為 NULL)
        mtime   REAL              – 最後修改時間，用於失效判斷
    """

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
            log_warning(f"[骨幹快取] 無法初始化 SQLite: {e}，將以純記憶體模式運行")
            self._conn = None

    def batch_load(self) -> dict:
        """一次性載入所有快取到記憶體，減少查詢次數"""
        if not self._conn:
            return {}
        try:
            cursor = self._conn.execute(
                "SELECT path, skeleton, vol_num, mtime FROM folder_skeletons"
            )
            return {
                row[0]: {'skeleton': row[1], 'vol_num': row[2], 'mtime': row[3]}
                for row in cursor.fetchall()
            }
        except Exception:
            return {}

    def batch_save(self, entries: list):
        """批量寫入，entries 為 (path, skeleton, vol_num, mtime) 的列表"""
        if not self._conn or not entries:
            return
        try:
            self._conn.executemany(
                "INSERT OR REPLACE INTO folder_skeletons (path, skeleton, vol_num, mtime) "
                "VALUES (?, ?, ?, ?)",
                entries
            )
            self._conn.commit()
        except Exception as e:
            log_warning(f"[骨幹快取] 批量寫入失敗: {e}")

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# ======================================================================
# Section: 名稱骨幹提取
# ======================================================================

# 預編譯正則，避免重複編譯開銷
_RE_BRACKET_SQUARE  = re.compile(r'\[.*?\]')          # [...]
_RE_BRACKET_ROUND   = re.compile(r'\(.*?\)')           # (...)
_RE_BRACKET_FULL_1  = re.compile(r'【.*?】')            # 【...】
_RE_BRACKET_FULL_2  = re.compile(r'（.*?）')            # （...）
_RE_VOL_NUMBER = re.compile(
    r'(?:vol|v|第|ch|chapter|ep|part|tome|band|'
    r'巻|卷|話|话|集|冊|册|号|號)'
    r'\s*\.?\s*(\d+)|'          # vol.01 / 第01卷
    r'#\s*(\d+)|'               # #05
    r'(\d+)\s*(?:巻|卷|話|话|集|冊|册|号|號)',  # 01卷
    re.IGNORECASE
)
_RE_TRAILING_NUMBER = re.compile(r'\s+(\d+)\s*$')      # 結尾獨立數字：「標題 03」
_RE_NOISE           = re.compile(r'[-_=・\s]+')         # 殘餘分隔符正規化


def _normalize_fullwidth(s: str) -> str:
    """全型轉半型：Ａ→A，１→1 等"""
    return unicodedata.normalize('NFKC', s)


def _extract_skeleton(folder_name: str) -> Tuple[str, Optional[int]]:
    """
    從資料夾名稱提取系列骨幹與集數。

    Returns:
        (skeleton, vol_num)
        - skeleton: 清洗後的小寫骨幹字串（空白時返回 lowercase 原名）
        - vol_num:  集數整數，未偵測到則為 None
    """
    name = _normalize_fullwidth(folder_name)

    # 1. 移除各種括號及其內容
    name = _RE_BRACKET_SQUARE.sub(' ', name)
    name = _RE_BRACKET_ROUND.sub(' ', name)
    name = _RE_BRACKET_FULL_1.sub(' ', name)
    name = _RE_BRACKET_FULL_2.sub(' ', name)

    # 2. 提取集數
    vol_num: Optional[int] = None
    m = _RE_VOL_NUMBER.search(name)
    if m:
        num_str = m.group(1) or m.group(2) or m.group(3)
        if num_str:
            vol_num = int(num_str)
        # 從名稱中移除集數片段
        name = name[:m.start()] + ' ' + name[m.end():]

    # 如果清洗後仍有「結尾獨立數字」且尚未偵測到集數，視為集數
    if vol_num is None:
        m2 = _RE_TRAILING_NUMBER.search(name)
        if m2:
            vol_num = int(m2.group(1))
            name = name[:m2.start()]

    # 3. 正規化殘餘字元
    name = _RE_NOISE.sub(' ', name).strip().lower()

    # 4. 防呆：如果清洗後為空，以原始小寫名字作為骨幹
    if not name:
        name = _normalize_fullwidth(folder_name).lower().strip()

    return name, vol_num


# ======================================================================
# Section: 選取策略
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
# Section: 插件主體
# ======================================================================

class MangaDeduplicationPlugin(BasePlugin):

    def get_id(self)   -> str: return "manga_volume_deduplication_smart"
    def get_name(self) -> str: return "相似卷宗查找 (智慧分組版)"

    def get_default_config(self) -> Dict[str, Any]:
        return {
            'manga_dedupe_enable_sample_limit': True,
            'manga_dedupe_sample_count':        12,
            'manga_dedupe_match_threshold':     8,
            'manga_dedupe_cross_lang':          True,   # 啟用跨名重複偵測
            'manga_dedupe_dup_threshold':       80,     # 判定重複所需相似頁面 %
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

    # ── 輔助：pHash 容差 ────────────────────────────────────────────
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
        except OSError:
            pass
        return count

    # ── 主要執行邏輯 ────────────────────────────────────────────────
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
            log_info("[相似卷宗] v14.0 - 智慧分組版啟動...")

            # ── 讀取設定 ────────────────────────────────────────────
            sample_count    = max(2, int(config.get('manga_dedupe_sample_count',    12)))
            MATCH_THRESHOLD = max(2, int(config.get('manga_dedupe_match_threshold', 8)))
            cross_lang      = bool(config.get('manga_dedupe_cross_lang', True))
            dup_pct         = max(1, min(100, int(config.get('manga_dedupe_dup_threshold', 80))))

            # ── 引擎初始化 & 檔案列舉 ───────────────────────────────
            engine = ImageComparisonEngine(config, progress_queue, control_events)

            _upd("正在準備檔案列表...")
            files_to_process, _ = get_files_to_process(
                config, engine.scan_cache_manager, progress_queue, control_events
            )
            if _cancelled() or not files_to_process:
                return ([], {}, []) if not _cancelled() else None

            # ── 計算圖片指紋（使用核心引擎，複用既有快取）──────────
            _upd("正在取得圖片指紋（複用快取）...")
            continue_proc, all_file_data = engine.compute_phashes(
                files_to_process, engine.scan_cache_manager, "圖片指紋"
            )
            if not continue_proc: return None

            # ── 過濾廣告圖 ──────────────────────────────────────────
            ad_hashes_set: Set = set()
            ad_folder_path = config.get('ad_folder_path')
            if ad_folder_path and os.path.isdir(ad_folder_path):
                log_info("[相似卷宗] 載入廣告庫以排除廣告頁面...")
                ad_cache = ScannedImageCacheManager(ad_folder_path)
                ad_paths = [
                    e.path for e in os.scandir(ad_folder_path)
                    if e.is_file() and e.name.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))
                ]
                _, ad_data = engine.compute_phashes(ad_paths, ad_cache, "廣告庫指紋", progress_scope="local")
                ad_hashes_set = {
                    self._coerce_hash_obj(d.get('phash'))
                    for d in ad_data.values() if d and d.get('phash')
                }
                ad_hashes_set.discard(None)

            # ── 建立資料夾 → 圖片清單的對照表 ──────────────────────
            _upd("建立資料夾指紋庫...")
            files_by_folder: Dict[str, List[str]] = defaultdict(list)
            for f_path in files_to_process:
                if _is_virtual_path(f_path):
                    container = _norm_key(_parse_virtual_path(f_path)[0])
                else:
                    container = _norm_key(os.path.dirname(f_path))
                if container:
                    files_by_folder[container].append(f_path)

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
                        if h and h not in ad_hashes_set:
                            fp.append((h, f))
                fingerprints[folder] = fp

            folder_list = sorted(fingerprints.keys())
            tol_bits    = self._tol_bits_from_slider(config)

            # ================================================================
            # 第一段：名稱骨幹提取（增量快取）
            # ================================================================
            _upd("分析資料夾名稱骨幹（查詢快取）...")
            cached_skeletons = skeleton_cache.batch_load()
            skeleton_info: Dict[str, Tuple[str, Optional[int]]] = {}
            new_entries: List[Tuple] = []

            for folder in folder_list:
                if _cancelled(): return None
                folder_name = os.path.basename(folder)
                # 取得資料夾 mtime 用於失效判斷
                try:
                    mtime = os.stat(folder).st_mtime
                except OSError:
                    mtime = 0.0

                cached = cached_skeletons.get(folder)
                if cached and abs(cached.get('mtime', -1) - mtime) < 1.0:
                    # 快取命中且未過期
                    skeleton_info[folder] = (cached['skeleton'], cached.get('vol_num'))
                else:
                    # 需要重新計算
                    skel, vol_num = _extract_skeleton(folder_name)
                    skeleton_info[folder] = (skel, vol_num)
                    new_entries.append((folder, skel, vol_num, mtime))

            # 批量寫入新算出的骨幹
            if new_entries:
                log_info(f"[相似卷宗] 新增/更新骨幹快取 {len(new_entries)} 筆")
                skeleton_cache.batch_save(new_entries)

            # ================================================================
            # 第二段：依骨幹分組 → 組內同卷判定 + 跨組異名重複偵測
            # ================================================================
            # 第二段 A：依骨幹建立系列桶
            series_buckets: Dict[str, List[str]] = defaultdict(list)
            for folder in folder_list:
                skel, _ = skeleton_info[folder]
                series_buckets[skel].append(folder)

            _upd("比對相似卷宗...")

            # 第二段 B：組內比對（找「同系列同卷不同版本」）
            # 策略：同一系列的資料夾，若指紋重疊率 ≥ dup_pct，視為重複副本
            intra_duplicates: Set[Tuple[str, str]] = set()  # (folder_a, folder_b)
            matched_display: Dict[str, str] = {} # 記錄可以完美成對的預覽圖
            for skel, folders in series_buckets.items():
                if len(folders) < 2: continue
                if _cancelled(): return None
                for i in range(len(folders)):
                    for j in range(i + 1, len(folders)):
                        f1, f2 = folders[i], folders[j]
                        fp1, fp2 = fingerprints.get(f1, []), fingerprints.get(f2, [])
                        if not fp1 or not fp2: continue
                        match, pair = self._greedy_match_pairs(fp1, fp2, tol_bits)
                        min_len = min(len(fp1), len(fp2))
                        if min_len > 0 and (match / min_len * 100) >= dup_pct:
                            intra_duplicates.add((min(f1, f2), max(f1, f2)))
                            if pair and f1 not in matched_display: matched_display[f1] = pair[0]
                            if pair and f2 not in matched_display: matched_display[f2] = pair[1]

            # 第二段 C：跨系列異名重複偵測（pHash 反向索引，僅在啟用時執行）
            inter_duplicates: Set[Tuple[str, str]] = set()
            if cross_lang:
                _upd("建立 pHash 反向索引（跨語言偵測）...")
                # 建立 hash → folders 的反向索引
                hash_to_folders: Dict[str, Set[str]] = defaultdict(set)
                for folder, fp_list in fingerprints.items():
                    for h, f_path in fp_list:
                        # 使用 hash 字串前 14 字元作為桶（容差近似桶）
                        bucket = str(h)[:14]
                        hash_to_folders[bucket].add(folder)

                # 找出有指紋重疊但骨幹不同的資料夾對
                candidate_pairs: Set[Tuple[str, str]] = set()
                for bucket, folders_in_bucket in hash_to_folders.items():
                    if len(folders_in_bucket) < 2: continue
                    folder_list_in_bucket = sorted(folders_in_bucket)
                    for i in range(len(folder_list_in_bucket)):
                        for j in range(i + 1, len(folder_list_in_bucket)):
                            f1_b = folder_list_in_bucket[i]
                            f2_b = folder_list_in_bucket[j]
                            skel1, _ = skeleton_info.get(f1_b, ('', None))
                            skel2, _ = skeleton_info.get(f2_b, ('', None))
                            # 只處理骨幹不同的對（骨幹相同的已在 intra 中處理）
                            if skel1 != skel2:
                                candidate_pairs.add((min(f1_b, f2_b), max(f1_b, f2_b)))

                # 對候選對進行精確計數確認
                for f1, f2 in candidate_pairs:
                    if _cancelled(): return None
                    fp1, fp2 = fingerprints.get(f1, []), fingerprints.get(f2, [])
                    if not fp1 or not fp2: continue
                    match, pair = self._greedy_match_pairs(fp1, fp2, tol_bits)
                    min_len = min(len(fp1), len(fp2))
                    if min_len > 0 and (match / min_len * 100) >= dup_pct:
                        inter_duplicates.add((f1, f2))
                        if pair and f1 not in matched_display: matched_display[f1] = pair[0]
                        if pair and f2 not in matched_display: matched_display[f2] = pair[1]

            # ================================================================
            # 組裝輸出：轉換為 main_window.py 的 (gk, ip, vs, tag) 格式
            # ================================================================
            _upd("整理顯示資料...")

            # ── [NEW] DSU 合併跨名骨幹 ──
            class UnionFind:
                def __init__(self):
                    self.parent = {}
                def find(self, i):
                    if self.parent.setdefault(i, i) == i: return i
                    self.parent[i] = self.find(self.parent[i])
                    return self.parent[i]
                def union(self, i, j):
                    root_i = self.find(i)
                    root_j = self.find(j)
                    if root_i != root_j:
                        # 選擇較短的骨幹名稱作為根，顯示時更容易理解
                        if len(root_i) <= len(root_j):
                            self.parent[root_j] = root_i
                        else:
                            self.parent[root_i] = root_j

            dsu = UnionFind()
            for f1, f2 in inter_duplicates:
                skel1, _ = skeleton_info.get(f1, ('', None))
                skel2, _ = skeleton_info.get(f2, ('', None))
                if skel1 and skel2 and skel1 != skel2:
                    dsu.union(skel1, skel2)

            # 將所有資料夾重新按合併後的虛擬骨幹（Root Skeleton）分組
            merged_series: Dict[str, List[str]] = defaultdict(list)
            for skel, folders in series_buckets.items():
                root_skel = dsu.find(skel)
                merged_series[root_skel].extend(folders)

            # 過濾出需要顯示的系列：合併後群組裡有 2+ 個成員
            multi_folder_series: Dict[str, List[str]] = {
                root_skel: sorted(folders)
                for root_skel, folders in merged_series.items()
                if len(folders) >= 2
            }

            # 為了能快速查詢任兩資料夾是否有重複關係
            all_dups = set()
            for f1, f2 in intra_duplicates:
                all_dups.add(tuple(sorted((f1, f2))))
            for f1, f2 in inter_duplicates:
                all_dups.add(tuple(sorted((f1, f2))))

            found_items: List[Tuple] = []
            processed_folders: Set[str] = set()

            for root_skel, s_folders_sorted in sorted(multi_folder_series.items()):
                if _cancelled(): return None
                
                group_key = s_folders_sorted[0]  # 詞典序最小的路徑作為此系列群組的 group_key

                for folder in s_folders_sorted:
                    if folder in processed_folders: continue
                    skel, vol_num = skeleton_info.get(folder, (root_skel, None))
                    vol_label  = f"Vol.{vol_num}" if vol_num is not None else "?"
                    page_count = self._count_real_pages(folder) if not _is_virtual_path(folder) else len(files_by_folder.get(_norm_key(folder), []))

                    # 尋找是否此資料夾與同群組內其他資料夾有重複關係
                    companions = [
                        other for other in s_folders_sorted
                        if other != folder and tuple(sorted((folder, other))) in all_dups
                    ]

                    if companions:
                        # 有重複副本，取第一個關聯項
                        companion = companions[0]
                        skel_other, _ = skeleton_info.get(companion, ('', None))
                        if skel == skel_other:
                            # 原始骨幹一樣 -> 同系列副本
                            val_str = f"{vol_label} | {page_count}P | 🔴 重複副本"
                            val_str += f" (與 {os.path.basename(companion)} 相同)"
                        else:
                            # 原始骨幹不同 -> 跨名系列副本
                            val_str = f"{vol_label} | {page_count}P | ⚠️ 跨名重複副本"
                            val_str += f" (與 {os.path.basename(companion)} 相同，原名：{skel})"
                        tag = "duplicate_tag"
                    else:
                        val_str = f"{vol_label} | {page_count}P"
                        tag      = "series_only_tag"
                        if self._check_uncensored(folder):
                            val_str += " [★無修正★]"
                            tag = "uncensored_tag"

                    found_items.append((group_key, folder, val_str, tag))
                    processed_folders.add(folder)

            # ── 建立 gui_file_data ──────────────────────────────────
            gui_file_data: Dict[str, Any] = {}
            all_display_paths = {path for _, path, _, _ in found_items} | {gk for gk, _, _, _ in found_items}

            for path in all_display_paths:
                real_count = 0
                ctime      = None
                display_path = path
                try:
                    st    = os.stat(path)
                    ctime = st.st_ctime
                except OSError:
                    pass

                if _is_virtual_path(path):
                    image_files = files_by_folder.get(_norm_key(path))
                    real_count  = len(image_files) if image_files else 0
                    if image_files: display_path = image_files[0]
                else:
                    real_count   = self._count_real_pages(path)
                    image_files  = files_by_folder.get(_norm_key(path))
                    if image_files: display_path = image_files[0]
                
                # [v-MOD] 如果這本漫畫之前有配對成功的圖片，優先使用它，讓 UI 出現相同的對照圖
                if path in matched_display:
                    display_path = matched_display[path]

                # 反查它所屬合併群組的代表骨幹，作為 UI 上的群組標題
                skel_name, _ = skeleton_info.get(_norm_key(path), (os.path.basename(path), None))
                root_skel_for_dict = dsu.find(skel_name) if skel_name else skel_name
                
                gui_file_data[path] = {
                    'display_path': display_path,
                    'page_count':   real_count,
                    'ctime':        ctime,
                    'display_name': root_skel_for_dict,  # 供 UI 顯示合併後的系列名稱
                }

            dup_count = sum(1 for _, _, vs, _ in found_items if '重複副本' in vs)
            log_info(
                f"[相似卷宗] 掃描完成。"
                f"虛擬系列組: {len(multi_folder_series)}，"
                f"發現重複 (含組內與跨名): {dup_count}"
            )
            return found_items, gui_file_data, []

        except Exception as e:
            log_error(f"[相似卷宗外掛] 發生嚴重錯誤: {e}", include_traceback=True)
            return [], {}, [("外掛錯誤", str(e))]
        finally:
            skeleton_cache.close()