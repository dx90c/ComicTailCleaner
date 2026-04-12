# ======================================================================
# 檔案：plugins/cache_database_cleaner/processor.py
# 目的：掃描 EMM SQLite 資料庫，找出實體已消失的「幽靈卷宗」
# 版本：1.0.0
# ======================================================================

import os
import sqlite3
import datetime
from typing import Dict, Any, Optional, List, Tuple

from plugins.base_plugin import BasePlugin
from utils import log_info, log_error

class CacheDatabaseCleanerPlugin(BasePlugin):
    def get_id(self) -> str:
        return "cache_database_cleaner"

    def get_name(self) -> str:
        return "👻 失效卷宗清理"

    def get_description(self) -> str:
        return (
            "比對 EMM (exhentai-manga-manager) 資料庫，\n"
            "找出並展示實體硬碟上已經不存在的卷宗，\n"
            "讓您能快速清理腐壞的 SQL 記錄。\n\n"
            "⚠️ 需先在「擴充功能」設定中配置 EMM 資料庫路徑。"
        )

    def get_plugin_type(self) -> str:
        # 'secondary_mode'：出現在設定第二頁的「EMM 輔助掃描模式」區塊，
        # 不占用主設定頁的「比對模式」框空間。
        return 'secondary_mode'

    def get_styles(self) -> Dict[str, Dict[str, str]]:
        return {
            "ghost_item": {"background": "#FFEEEE", "foreground": "#880000"},
        }

    def run(
        self,
        config: Dict[str, Any],
        progress_queue: Optional[Any] = None,
        control_events: Optional[Dict[str, Any]] = None,
        app_update_callback=None,
    ) -> Optional[Tuple[List, Dict, List]]:
        """
        回傳格式與內建模式相同，讓主程式能正常解包：
            (found_items, file_data, errors)
        found_items: List[ (group_key, item_path, sim_label, tag) ]
        file_data:   Dict[ path, {size, ctime, page_count, display_name, ...} ]
        errors:      List[ path_that_failed ]
        """
        def _upd(text, val=None):
            if progress_queue:
                progress_queue.put({
                    'type': 'progress' if val is not None else 'text',
                    'text': text,
                    'value': val,
                })

        _upd("🚀 [幽靈獵手] 開始準備掃描...", 0)
        log_info("[幽靈獵手] 任務開始。")

        # ── 1. 找到資料庫 ──────────────────────────────────────────────
        db_dir = config.get('eh_data_directory', '')
        if not db_dir:
            _upd("⚠️ [幽靈獵手] 請先在「擴充功能 (前置處理)」中設定 EMM 資料庫資料夾路徑！", 100)
            return [], {}, []

        db_file = os.path.join(db_dir, "database.sqlite")
        if not os.path.isfile(db_file):
            _upd(f"⚠️ [幽靈獵手] 找不到 database.sqlite：{db_file}", 100)
            return [], {}, []

        # ── 2. 讀取資料庫所有有效 (exist=1) 路徑 ───────────────────────
        _upd("正在讀取資料庫有效記錄 (exist=1)...", 10)
        try:
            with sqlite3.connect(db_file, timeout=10) as conn:
                cols_info = conn.execute("PRAGMA table_info(Mangas)").fetchall()
                col_names = [r[1] for r in cols_info]

                # 優先用 filepath_normalized（已正規化），備援用 filepath
                if 'filepath_normalized' in col_names:
                    rows = conn.execute(
                        "SELECT filepath_normalized, filepath, title FROM Mangas WHERE exist = 1"
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT filepath, filepath, title FROM Mangas WHERE exist = 1"
                    ).fetchall()
        except Exception as e:
            log_error(f"[幽靈獵手] 讀取資料庫失敗: {e}", include_traceback=True)
            _upd(f"❌ 讀取資料庫失敗: {e}", 100)
            return [], {}, []

        total = len(rows)
        _upd(f"庫內有效記錄 {total} 筆，正在逐一驗證硬碟...", 20)
        log_info(f"[幽靈獵手] 共 {total} 筆有效記錄待驗證。")

        # ── 3. 存活驗證 ────────────────────────────────────────────────
        # 用一個固定的 group_key 把所有幽靈集中在同一個父節點下
        GROUP_KEY = "__ghost_group__"
        VIRTUAL_DISPLAY_NAME = "💀 發現失效的幽靈卷宗"

        found_items: List[Tuple] = []  # (group_key, item_path, label, tag)
        file_data: Dict[str, Any] = {
            GROUP_KEY: {"display_name": VIRTUAL_DISPLAY_NAME}
        }
        errors: List[str] = []

        for idx, (norm_path, raw_path, title) in enumerate(rows):
            # 取消偵測
            if control_events and control_events.get('cancel') and control_events['cancel'].is_set():
                log_info("[幽靈獵手] 掃描被使用者取消。")
                _upd("⚠️ 任務已取消。", 100)
                return None

            # 兩條路徑分開處理：
            # db_key_path：保留 DB 原始格式（正斜線），確保 SQL UPDATE 的 WHERE 子句能命中
            # check_path ：轉換為 OS 慣用格式（反斜線），供 os.path.exists() 使用
            db_key_path = (norm_path or raw_path or "").strip()
            check_path = db_key_path.replace('/', os.sep)
            if not db_key_path:
                continue

            if not os.path.exists(check_path):
                display_label = "此路徑的實體檔案／資料夾已不存在"
                # 存 db_key_path（正斜線）讓 sync_deleted_files 能對應 filepath_normalized
                found_items.append((GROUP_KEY, db_key_path, display_label, "ghost_item"))

                # file_data 也用相同的 key
                try:
                    stat = os.stat(check_path)
                    file_data[db_key_path] = {
                        'size': stat.st_size,
                        'ctime': stat.st_ctime,
                    }
                except OSError:
                    file_data[db_key_path] = {
                        'size': 0,
                        'ctime': None,
                        'display_name': title or os.path.basename(check_path),
                    }

            # 進度更新（每 100 筆或最後一筆）
            if (idx + 1) % 100 == 0 or idx == total - 1:
                pct = 20 + int((idx + 1) / total * 75)
                _upd(f"驗證中... ({idx + 1}/{total})", pct)

        ghost_count = len(found_items)
        log_info(f"[幽靈獵手] 掃描完成，找到 {ghost_count} 個幽靈卷宗。")
        _upd(f"✅ 掃描完成！共找到 {ghost_count} 個幽靈卷宗。", 100)

        return found_items, file_data, errors
