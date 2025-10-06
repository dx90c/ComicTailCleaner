# ======================================================================
# 檔案名稱：processors/base_processor.py
# 版本：1.1.0 (補全通用方法)
# ======================================================================
from queue import Queue
from typing import Optional

class _DummyEvent:
    def is_set(self) -> bool:
        return False

class BaseProcessor:
    """所有 Processor 的共同基底"""
    def __init__(self, config: dict,
                 progress_queue: Optional[Queue] = None,
                 control_events: Optional[dict] = None) -> None:
        self.config = config or {}
        self.progress_queue = progress_queue
        self.control_events = control_events or {
            'cancel': _DummyEvent(),
            'pause': _DummyEvent(),
        }
        self.pool = None  # 有需要時才設置

    def _update_progress(self, p_type: str = 'text',
                         value: Optional[int] = None,
                         text: Optional[str] = None) -> None:
        """把進度訊息丟回 GUI"""
        if not self.progress_queue:
            return
        payload = {'type': p_type}
        if value is not None:
            payload['value'] = value
        if text is not None:
            payload['text'] = text
        self.progress_queue.put(payload)

    def _check_control(self) -> str:
        ev = self.control_events or {}
        if ev.get('cancel') and ev['cancel'].is_set():
            return 'cancel'
        if ev.get('pause') and ev['pause'].is_set():
            return 'pause'
        return 'continue'

    def _cleanup_pool(self) -> None:
        """若有掛 multiprocessing.Pool，就安全收掉"""
        if self.pool:
            try:
                self.pool.terminate()
                self.pool.join()
            except Exception:
                pass
            finally:
                self.pool = None

    def run(self):
        raise NotImplementedError
