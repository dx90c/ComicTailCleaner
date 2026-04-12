import os
import shutil
import json
import logging
import uuid
import threading
try:
    import send2trash
except ImportError:
    send2trash = None
from utils import log_error, log_info

class UndoManager:
    def __init__(self, data_dir: str):
        self.log_file = os.path.join(data_dir, "undo_log.json")
        self.history = []
        self._lock = threading.Lock()
        
        # 啟動時檢查：是否有上次未完成的延遲刪除（災難復原），改為背景非同步執行
        threading.Thread(target=self._async_startup_recovery, daemon=True).start()

    def _async_startup_recovery(self):
        with self._lock:
            self._load_log()
            if self.history:
                log_info(f"發現 {self.get_total_pending_count()} 個上次未完成的刪除項目，執行背景災難復原...")
                self.commit_deletions()

    def _load_log(self):
        self.history = []
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, "r", encoding="utf-8") as f:
                    self.history = json.load(f)
            except Exception as e:
                log_error(f"無法讀取還原紀錄: {e}")
                self.history = []

    def _save_log(self):
        try:
            with open(self.log_file, "w", encoding="utf-8") as f:
                json.dump(self.history, f, indent=4, ensure_ascii=False)
        except Exception as e:
            log_error(f"無法儲存還原紀錄: {e}")

    def mark_for_deletion(self, file_paths: list[str]) -> tuple[list[str], list[str]]:
        """
        將檔案標記為待刪除並記錄，回傳 (成功紀錄路徑清單, 失敗路徑清單)。
        只在記憶體中記錄，不進行實體搬移。
        """
        successful_paths = []
        failed_paths = []
        current_batch = []
        
        for p in file_paths:
            if not os.path.exists(p):
                # 即使檔案已不存在，我們雖然記錄到成功，但不寫入 batch 給之後 commit 用（因為它已經沒了）
                # 這裡按照舊版邏輯：不在硬碟的，回傳 failed 讓主程式知道它原本就不在
                log_error(f"檔案不存在，無法加到刪除標籤: {p}")
                failed_paths.append(p)
                continue
                
            abs_path = os.path.abspath(p)
            current_batch.append(abs_path)
            successful_paths.append(p)
                
        if current_batch:
            with self._lock:
                self.history.append(current_batch)
                self._save_log()
            
        return successful_paths, failed_paths

    def get_undo_count(self) -> int:
        """回傳可以還原的批次數量"""
        return len(self.history)
        
    def get_total_pending_count(self) -> int:
        """回傳目前所有等待刪除的檔案總數量 (Flatten count)"""
        with self._lock:
            return sum(len(batch) for batch in self.history)
        
    def get_all_pending_paths(self) -> list[str]:
        """取得所有等待刪除的路徑"""
        paths = []
        for batch in self.history:
            paths.extend(batch)
        return paths

    def undo_last_mark(self) -> tuple[int, int, list[str]]:
        """
        取消最後一次被標記刪除的整批圖片，回傳 (成功還原數量, 0, 成功還原的路徑清單)
        """
        if not self.history:
            return 0, 0, []
            
        with self._lock:
            last_batch = self.history.pop()
            self._save_log()
        
        return len(last_batch), 0, last_batch
        
    def commit_deletions(self) -> int:
        """
        執行物理刪除，將所有標記的檔案送入系統資源回收筒，並清空紀錄。
        回傳被刪除的檔案數量。
        """
        deleted_count = 0
        all_paths_to_delete = []
        with self._lock:
            for batch in self.history:
                for p in batch:
                    if os.path.exists(p):
                        all_paths_to_delete.append(p)
                        
        if all_paths_to_delete:
            try:
                if send2trash:
                    # send2trash 支援陣列，這能強制 Windows 走單一 Shell API 呼叫，速度會快 1000 倍
                    send2trash.send2trash(all_paths_to_delete)
                    deleted_count += len(all_paths_to_delete)
                else:
                    for p in all_paths_to_delete:
                        os.remove(p)
                        deleted_count += 1
            except Exception as e:
                log_error(f"物理刪除失敗: {e}")
        
        with self._lock:
            self.history = []
            try:
                if os.path.exists(self.log_file):
                    os.remove(self.log_file)
            except Exception:
                pass
            
        return deleted_count
