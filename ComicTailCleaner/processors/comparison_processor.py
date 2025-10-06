# ======================================================================
# 檔案名稱：processors/comparison_processor.py
# 版本：1.1.0 (簡化為調度器)
# ======================================================================
from .base_processor import BaseProcessor
from utils import log_performance
from core_engine import ImageComparisonEngine

class ComparisonProcessor(BaseProcessor):
    """互相比對 / 廣告比對：直接委派給現有的 ImageComparisonEngine"""
    def run(self):
        try:
            self._update_progress(text="任務開始...")
            log_performance("[開始] 相似度比對任務")

            engine = ImageComparisonEngine(self.config, self.progress_queue, self.control_events)
            result = engine.find_duplicates()

            # **契約**：核心在取消/暫停時會回傳 None
            if result is None:
                return None

            # 正常：回傳 (found, data, errors)
            return result
        finally:
            self._cleanup_pool()
