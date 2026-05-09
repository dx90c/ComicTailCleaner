import json
import os
import threading

try:
    import send2trash
except ImportError:
    send2trash = None

from utils import log_error, log_info


_JUNK_FILENAMES = frozenset({"thumbs.db", ".ds_store", "desktop.ini", ".picasa.ini"})


def _is_effectively_empty_folder(path: str) -> bool:
    if not os.path.isdir(path):
        return False
    try:
        for entry in os.scandir(path):
            if entry.name.lower() not in _JUNK_FILENAMES:
                return False
        return True
    except (FileNotFoundError, PermissionError, OSError):
        return False


def _is_deletion_safe_now(path: str) -> bool:
    if not os.path.exists(path):
        return False
    if os.path.isdir(path):
        return _is_effectively_empty_folder(path)
    return os.path.isfile(path)


class UndoManager:
    def __init__(self, data_dir: str):
        self.log_file = os.path.join(data_dir, "undo_log.json")
        self.history = []
        self._lock = threading.Lock()

        with self._lock:
            self._load_log()
            if self.history:
                log_info(
                    f"[Undo] 已恢復 {self.get_total_pending_count()} 筆待刪除項目，"
                    "等待使用者手動套用或復原。"
                )

    def _load_log(self):
        self.history = []
        if not os.path.exists(self.log_file):
            return
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                self.history = json.load(f)
        except Exception as e:
            log_error(f"[Undo] 載入刪除記錄失敗: {e}")
            self.history = []

    def _save_log(self):
        try:
            with open(self.log_file, "w", encoding="utf-8") as f:
                json.dump(self.history, f, indent=4, ensure_ascii=False)
        except Exception as e:
            log_error(f"[Undo] 儲存刪除記錄失敗: {e}")

    def mark_for_deletion(self, file_paths: list[str]) -> tuple[list[str], list[str]]:
        successful_paths = []
        failed_paths = []
        current_batch = []

        for path in file_paths:
            if not os.path.exists(path):
                log_error(f"[Undo] 檔案不存在，無法加入待刪除清單: {path}")
                failed_paths.append(path)
                continue
            if os.path.isdir(path) and not _is_effectively_empty_folder(path):
                log_error(f"[Undo] 資料夾不是空的，拒絕加入待刪除清單: {path}")
                failed_paths.append(path)
                continue

            abs_path = os.path.abspath(path)
            current_batch.append(abs_path)
            successful_paths.append(path)

        if current_batch:
            with self._lock:
                self.history.append(current_batch)
                self._save_log()

        return successful_paths, failed_paths

    def get_undo_count(self) -> int:
        return len(self.history)

    def get_total_pending_count(self) -> int:
        with self._lock:
            return sum(len(batch) for batch in self.history)

    def get_all_pending_paths(self) -> list[str]:
        paths = []
        for batch in self.history:
            paths.extend(batch)
        return paths

    def undo_last_mark(self) -> tuple[int, int, list[str]]:
        if not self.history:
            return 0, 0, []

        with self._lock:
            last_batch = self.history.pop()
            self._save_log()

        return len(last_batch), 0, last_batch

    def commit_deletions(self) -> int:
        deleted_count = 0
        all_paths_to_delete = []

        with self._lock:
            for batch in self.history:
                for path in batch:
                    if _is_deletion_safe_now(path):
                        all_paths_to_delete.append(path)
                    elif os.path.exists(path):
                        log_error(f"[Undo] 套用前重新檢查失敗，跳過非空資料夾或非檔案路徑: {path}")

        if all_paths_to_delete:
            try:
                if send2trash:
                    send2trash.send2trash(all_paths_to_delete)
                    deleted_count += len(all_paths_to_delete)
                else:
                    for path in all_paths_to_delete:
                        if os.path.isdir(path):
                            os.rmdir(path)
                        else:
                            os.remove(path)
                        deleted_count += 1
            except Exception as e:
                log_error(f"[Undo] 套用刪除失敗: {e}")

        with self._lock:
            self.history = []
            try:
                if os.path.exists(self.log_file):
                    os.remove(self.log_file)
            except Exception:
                pass

        return deleted_count
