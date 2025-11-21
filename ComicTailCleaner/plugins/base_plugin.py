# ======================================================================
# 檔案：plugins/base_plugin.py
# 目的：定義所有外掛必須遵守的基礎介面 (契約)
# 版本：2.1 (新增 get_default_config 以支援設定自動化)
# ======================================================================

from __future__ import annotations
import abc
from typing import Dict, Any, Tuple, List, Optional
from queue import Queue

# 為了避免循環導入，我們只在型別註解中使用 'ttk.Frame'
# 這需要 Python 3.7+ 的 from __future__ import annotations
try:
    from tkinter import ttk
except ImportError:
    ttk = None

class BasePlugin(abc.ABC):
    """
    所有外掛的基礎類別，定義了外掛必須實現的介面。
    這就像一個「標準插頭」，確保所有外掛都能被主程式正確識別和呼叫。
    """

    @abc.abstractmethod
    def get_id(self) -> str:
        """
        返回外掛的唯一標識符 (ID)。
        必須是唯一的、全小寫的字串，可包含底線。
        例如: "manga_volume_deduplication"
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_name(self) -> str:
        """返回外掛在 UI 上顯示的名稱。"""
        raise NotImplementedError

    def get_description(self) -> str:
        """
        (可選) 返回當滑鼠懸浮在外掛選項上時，顯示的詳細描述。
        如果未實現，則返回空字串。
        """
        return ""

    def get_default_config(self) -> Dict[str, Any]:
        """
        (可選) 向主程式宣告此外掛的預設設定值。
        主程式啟動時會收集所有外掛的預設值，並與主設定合併。

        **重要**: 為了避免與主程式或其他外掛的設定鍵衝突，
        強烈建議所有鍵名都使用與外掛 ID 相關的唯一前綴。
        例如，如果外掛 ID 是 'manga_dedupe'，則鍵名應為 'manga_dedupe_sample_count'。
        """
        return {}

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any], ui_vars: Dict) -> Optional['ttk.Frame']:
        """
        (可選) 建立並返回一個包含此外掛專屬設定的 tkinter/ttk 框架。
        主程式會將這個框架放置在設定視窗的適當位置。
        
        Args:
            parent_frame: 應該放置設定元件的父 ttk.Frame。
            config: 當前的全域設定字典，供讀取預設值。
            ui_vars: 一個共享的字典，用於儲存此外掛的 UI 變數 (tk.StringVar, tk.BooleanVar 等)。
        
        Returns:
            如果外掛有專屬設定，則返回建立的 ttk.Frame；否則返回 None。
        """
        return None

    def save_settings(self, config: Dict[str, Any], ui_vars: Dict) -> Dict[str, Any]:
        """
        (可選) 從 ui_vars 字典中讀取值，並將其存入 config 字典後返回。
        主程式會在儲存設定時呼叫此方法。
        """
        return config

    def get_plugin_type(self) -> str:
        """
        【v2.0 新增】
        宣告外掛的類型。主程式會根據這個類型來決定如何以及何時執行此外掛。
        - 'mode': 一個獨立的比對模式 (預設)。會顯示在「比對模式」的單選框中。
        - 'preprocessor': 一個在主任務執行前運行的前置處理器。會顯示為一個獨立的勾選框。
        """
        return 'mode'

    @abc.abstractmethod
    def run(self, 
            config: Dict[str, Any], 
            progress_queue: Optional[Queue] = None, 
            control_events: Optional[Dict[str, Any]] = None,
            app_update_callback: Optional[callable] = None
            ) -> Optional[Any]:
        """
        執行外掛的核心邏輯。這是所有外掛必須實現的主要方法。

        Args:
            config: 當前的全域設定字典。
            progress_queue: 用於向主 UI 回報進度與狀態的佇列。
            control_events: 一個包含 'cancel' 和 'pause' 事件的字典，用於控制流程。
            app_update_callback: (僅 preprocessor 可用) 一個回呼函式，用於在執行耗時操作時保持主 UI 響應。

        Returns:
            - 如果是 'mode' 類型，應返回 Tuple[List, Dict, List] 格式的比對結果。
            - 如果是 'preprocessor' 類型，執行完畢後返回 None 即可。
        """
        raise NotImplementedError