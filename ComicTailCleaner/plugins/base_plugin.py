# ======================================================================
# 檔案：plugins/base_plugin.py
# 目的：定義所有外掛必須遵守的基礎介面 (高級版)
# 版本：2.1 (修正循環導入問題)
# ======================================================================

import abc
from typing import List, Dict, Any, Tuple, Optional
from queue import Queue
from tkinter import ttk

class BasePlugin(abc.ABC):
    @abc.abstractmethod
    def get_id(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    def get_description(self) -> str:
        return ""

    @abc.abstractmethod
    def run(self, 
            config: Dict[str, Any], 
            
            progress_queue: Optional[Queue] = None, 
            control_events: Optional[Dict[str, Any]] = None
            ) -> Optional[Tuple[List[tuple], Dict[str, Any], List[tuple]]]:
        raise NotImplementedError

    def get_settings_frame(self, parent_frame: ttk.Frame, config: Dict[str, Any]) -> Optional[ttk.Frame]:
        return None

    def save_settings(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return config