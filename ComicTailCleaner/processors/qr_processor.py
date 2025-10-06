# ======================================================================
# 檔案：processors/qr_processor.py
# 目的：QR 處理流程的輕薄封裝層，將任務委派給核心引擎
# 版本：2.1.0 (簡化版)
# ======================================================================

from __future__ import annotations
from typing import Any, Dict, Optional, Tuple, List
from queue import Queue

# 核心引擎是唯一的外部依賴
from core_engine import ImageComparisonEngine

# 日誌工具的後備定義
try:
    from utils import log_info, log_error
except ImportError:
    def log_info(msg): print(f"[INFO] {msg}")
    def log_error(msg): print(f"[ERROR] {msg}")

__all__ = ["QrProcessor", "QRProcessor"]

class QrProcessor:
    """
    輕薄封裝：設定 comparison_mode='qr_detection'，然後呼叫核心引擎執行。
    這種設計避免了循環依賴，並將所有複雜的列舉、快取和進程管理
    工作統一交給 core_engine 處理。
    """
    def __init__(
        self,
        config: Dict[str, Any],
        progress_queue: Optional[Queue] = None,
        control_events: Optional[dict] = None,
    ):
        # 複製設定，避免修改原始字典
        self.config = dict(config) if config else {}
        
        # 強制設定為 QR 模式，確保 core_engine 走正確的邏輯分支
        self.config["comparison_mode"] = "qr_detection"
        
        # 保存進度佇列和控制事件，以便傳遞給核心引擎
        self.progress_queue = progress_queue
        self.control_events = control_events

    def run(self) -> Optional[Tuple[List[tuple], Dict[str, Any], List[tuple]]]:
        """
        執行 QR 偵測任務。
        """
        try:
            log_info("[QR Processor] 建立核心引擎實例並以 QR 模式運行...")
            # 建立核心引擎，並傳入已強制設定為 QR 模式的 config
            engine = ImageComparisonEngine(
                self.config,
                self.progress_queue,
                self.control_events,
            )
            # 呼叫核心引擎的統一入口函式，它會根據模式自動分派任務
            return engine.find_duplicates()
        except Exception as e:
            log_error(f"[QR Processor] 執行過程中發生未預期的錯誤: {e}")
            # 向上拋出異常，讓呼叫者 (例如 GUI) 能夠捕獲並處理
            raise

# 為了與舊的程式碼兼容，提供一個大寫的別名
QRProcessor = QrProcessor