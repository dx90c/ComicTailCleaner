# ======================================================================
# 檔案：plugins/base_plugin.py
# 目的：定義所有外掛必須遵守的基礎介面 (契約)
# 版本：2.2 (新增 樣式與選取策略 接口)
# ======================================================================

from __future__ import annotations
import abc
from typing import Dict, Any, Tuple, List, Optional, Set
from queue import Queue

try:
    from tkinter import ttk
except ImportError:
    ttk = None

class BasePlugin(abc.ABC):
    """所有外掛的基礎類別"""

    @abc.abstractmethod
    def get_id(self) -> str:
        """返回外掛唯一 ID"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_name(self) -> str:
        """返回外掛顯示名稱"""
        raise NotImplementedError

    def get_description(self) -> str:
        return ""

    def get_default_config(self) -> Dict[str, Any]:
        return {}

    def get_settings_frame(self, parent_frame: 'ttk.Frame', config: Dict[str, Any], ui_vars: Dict) -> Optional['ttk.Frame']:
        return None

    def save_settings(self, config: Dict[str, Any], ui_vars: Dict) -> Dict[str, Any]:
        return config

    def get_plugin_type(self) -> str:
        return 'mode'

    # --- v-MOD: 新增樣式接口 ---
    def get_styles(self) -> Dict[str, Dict[str, str]]:
        """
        定義 Treeview 的顯示樣式。
        Returns:
            {
                "tag_name": {"background": "#RRGGBB", "foreground": "#RRGGBB"},
                ...
            }
        """
        return {}
    # --- v-MOD END ---

    # --- v-MOD: 新增選取策略接口 ---
    def get_selection_strategy(self, config: Dict[str, Any]):
        """
        回傳一個策略物件或函式，用於「選取建議」。
        該物件必須有一個 calculate(all_groups) 方法，回傳 Set[path_to_select]。
        """
        return None
    # --- v-MOD END ---
    def on_app_ready(self, app_instance: Any) -> None:
        """
        (可選) 當主視窗 (MainWindow) 初始化完成並顯示後，會呼叫此方法。
        
        Args:
            app_instance: MainWindow 的實例。外掛可以透過這個物件存取
                          主程式的 config, UI 元件, 甚至呼叫 start_scan()。
                          
        用途:
            用於實現自動化腳本、捷徑啟動參數處理、或是修改主程式的預設行為。
        """
        pass
    @abc.abstractmethod
    def run(self, 
            config: Dict[str, Any], 
            progress_queue: Optional[Queue] = None, 
            control_events: Optional[Dict[str, Any]] = None,
            app_update_callback: Optional[callable] = None
            ) -> Optional[Any]:
        raise NotImplementedError