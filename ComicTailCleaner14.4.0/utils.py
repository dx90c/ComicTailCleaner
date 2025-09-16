# ======================================================================
# 檔案名稱：utils.py
# 模組目的：提供通用的輔助函式，如日誌、路徑處理、依賴檢查等
# 版本：1.0.1 (修正 Python 版本相容性)
# ======================================================================

import os
import sys
import datetime
import traceback
import json
import io
import colorsys
import subprocess
import threading
from typing import Union # 【修正】導入 Union 類型

# 延遲導入或選擇性導入，以避免不必要的依賴
try:
    from PIL import Image, UnidentifiedImageError
except ImportError:
    Image = None
    UnidentifiedImageError = None

try:
    import pkg_resources
except ImportError:
    pkg_resources = None

try:
    from tkinter import messagebox
except ImportError:
    class MockMessageBox:
        def showwarning(self, title, message): print(f"WARNING: {title}\n{message}")
        def showerror(self, title, message): print(f"ERROR: {title}\n{message}")
        def askyesno(self, title, message): print(f"QUESTION: {title}\n{message}"); return False
        def showinfo(self, title, message): print(f"INFO: {title}\n{message}")
    messagebox = MockMessageBox()


# 從本地模組導入常數和 archive_handler
try:
    import archive_handler
    ARCHIVE_SUPPORT_ENABLED = True
except ImportError:
    archive_handler = None
    ARCHIVE_SUPPORT_ENABLED = False

from config import VPATH_PREFIX, VPATH_SEPARATOR

# 全域狀態變數
QR_SCAN_ENABLED = False
PERFORMANCE_LOGGING_ENABLED = False
psutil = None
CACHE_LOCK = threading.Lock()

# === 日誌函式 ===
def log_error(message: str, include_traceback: bool = False):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] ERROR: {message}\n"
    if include_traceback:
        log_content += traceback.format_exc() + "\n"
    log_file = "error_log.txt"
    print(log_content, end='', flush=True)
    try:
        with open(log_file, "a", encoding="utf-8-sig", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"FATAL: 無法寫入錯誤日誌檔案: {e}\n原始錯誤: {message}", flush=True)

def log_info(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] INFO: {message}\n"
    log_file = "info_log.txt"
    print(log_content, end='', flush=True)
    try:
        with open(log_file, "a", encoding="utf-8-sig", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"FATAL: 無法寫入資訊日誌檔案: {e}", flush=True)

def log_performance(message: str):
    global psutil
    if PERFORMANCE_LOGGING_ENABLED and psutil:
        try:
            process = psutil.Process(os.getpid())
            cpu_percent = process.cpu_percent(interval=None)
            memory_mb = process.memory_info().rss / (1024 * 1024)
            performance_info = f" (CPU: {cpu_percent:.1f}%, Mem: {memory_mb:.1f} MB)"
            log_info(f"{message}{performance_info}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            log_info(message)
    else:
        log_info(message)

# === 虛擬路徑與通用圖片開啟接口 ===
def _is_virtual_path(path: str) -> bool:
    return path.startswith(VPATH_PREFIX)

def _parse_virtual_path(vpath: str) -> tuple[Union[str, None], Union[str, None]]:
    # 【修正】 'str | None' -> 'Union[str, None]'
    if not _is_virtual_path(vpath):
        return None, None
    try:
        _, content = vpath.split(VPATH_PREFIX, 1)
        archive_path, inner_path = content.split(VPATH_SEPARATOR, 1)
        return archive_path, inner_path
    except (ValueError, IndexError):
        log_error(f"解析虛擬路徑失敗: {vpath}")
        return None, None

def _open_image_from_any_path(path: str) -> Union[Image.Image, None]:
    # 【修正】 'Image.Image | None' -> 'Union[Image.Image, None]'
    if Image is None: return None
    try:
        if _is_virtual_path(path):
            archive_path, inner_path = _parse_virtual_path(path)
            if archive_path and inner_path and archive_handler:
                image_bytes = archive_handler.get_image_bytes(archive_path, inner_path)
                if image_bytes:
                    return Image.open(io.BytesIO(image_bytes))
        else:
            if os.path.exists(path):
                return Image.open(path)
    except (UnidentifiedImageError, IOError, Exception):
        pass
    return None

def _get_file_stat(path: str) -> tuple[Union[int, None], Union[float, None], Union[float, None]]:
    # 【修正】 '... | None' -> 'Union[..., None]'
    real_path = path
    if _is_virtual_path(path):
        real_path, _ = _parse_virtual_path(path)
    
    try:
        if real_path and os.path.exists(real_path):
            st = os.stat(real_path)
            return st.st_size, st.st_ctime, st.st_mtime
    except (OSError, TypeError):
        pass
    return None, None, None

# === 演算法與數學函式 ===
def sim_from_hamming(d: int, bits: int) -> float:
    return 1.0 - (d / bits)

def hamming_from_sim(sim: float, bits: int) -> int:
    return max(0, int(round((1.0 - sim) * bits)))

def _avg_hsv(img: Image.Image) -> Union[tuple[float, float, float], None]:
    # 【修正】 '... | None' -> 'Union[..., None]'
    try:
        import numpy as np
        small = img.convert("RGB").resize((32, 32), Image.Resampling.BILINEAR)
        arr = np.asarray(small, dtype=np.float32) / 255.0
        hsv_arr = np.apply_along_axis(lambda p: colorsys.rgb_to_hsv(p[0], p[1], p[2]), 2, arr)
        h, s, v = hsv_arr[:, :, 0], hsv_arr[:, :, 1], hsv_arr[:, :, 2]
        return float(np.mean(h) * 360.0), float(np.mean(s)), float(np.mean(v))
    except (ImportError, ValueError, Exception):
        return None

def _color_gate(hsv1, hsv2, *, hue_deg_tol, sat_tol, low_sat_thresh, low_sat_value_tol, low_sat_achroma_tol) -> bool:
    if not hsv1 or not hsv2: return False
    try:
        h1, s1, v1 = hsv1
        h2, s2, v2 = hsv2
    except (TypeError, IndexError, ValueError):
        return False

    if max(s1, s2) < low_sat_thresh:
        if abs(v1 - v2) > low_sat_value_tol: return False
        a1, a2 = v1 * (1.0 - s1), v2 * (1.0 - s2)
        if abs(a1 - a2) > low_sat_achroma_tol: return False
        if abs(s1 - s2) > 0.08: return False
        return True

    dh = abs(h1 - h2)
    hue_diff = min(dh, 360.0 - dh)
    if hue_diff > hue_deg_tol: return False
    if abs(s1 - s2) > sat_tol: return False
    return True

# === 系統與環境函式 ===
def _open_folder(folder_path: str):
    try:
        if os.path.isdir(folder_path):
            if sys.platform == "win32":
                os.startfile(os.path.normpath(folder_path))
            elif sys.platform == "darwin":
                subprocess.Popen(["open", folder_path])
            else:
                subprocess.Popen(["xdg-open", folder_path])
    except Exception as e:
        log_error(f"無法自動開啟資料夾 '{folder_path}': {e}", True)

def check_and_install_packages():
    global QR_SCAN_ENABLED, PERFORMANCE_LOGGING_ENABLED, psutil, ARCHIVE_SUPPORT_ENABLED

    if getattr(sys, 'frozen', False):
        log_info("在打包環境中運行，跳過依賴檢查。")
        try: import cv2; import numpy; QR_SCAN_ENABLED = True
        except ImportError: QR_SCAN_ENABLED = False
        try: import psutil as psutil_lib; psutil = psutil_lib; PERFORMANCE_LOGGING_ENABLED = True
        except ImportError: PERFORMANCE_LOGGING_ENABLED = False
        return

    log_info("正在檢查必要的 Python 套件...")

    required = {'Pillow': 'Pillow>=9.0.0', 'imagehash': 'imagehash>=4.2.1', 'send2trash': 'send2trash>=1.8.0'}
    optional = {
        'opencv-python': 'opencv-python>=4.5.0', 'numpy': 'numpy>=1.20.0',
        'psutil': 'psutil>=5.8.0', 'rarfile': 'rarfile>=4.0'
    }
    
    missing_core, missing_optional = [], []

    if pkg_resources:
        for name, req_str in required.items():
            try: pkg_resources.require(req_str)
            except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict): missing_core.append(name)
        for name, req_str in optional.items():
            try: pkg_resources.require(req_str)
            except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict): missing_optional.append(name)
    else:
        try: from PIL import Image; Image.new('RGB', (1, 1))
        except (ImportError, AttributeError): missing_core.append('Pillow')
        try: import imagehash; imagehash.average_hash(Image.new('RGB', (8,8)))
        except (ImportError, AttributeError): missing_core.append('imagehash')
        try: import send2trash
        except (ImportError, AttributeError): missing_core.append('send2trash')
        try: import cv2; import numpy
        except (ImportError, AttributeError): missing_optional.extend(['opencv-python', 'numpy'])
        try: import psutil as psutil_lib
        except (ImportError, AttributeError): missing_optional.append('psutil')
        try: import rarfile
        except (ImportError, AttributeError): missing_optional.append('rarfile')

    if missing_core:
        req_strings = [required[pkg] for pkg in missing_core]
        package_str = " ".join(req_strings)
        response = messagebox.askyesno(
            "缺少核心依賴",
            f"缺少必要套件：{', '.join(missing_core)}。\n\n是否嘗試自動安裝？\n（將執行命令：pip install {package_str}）",
        )
        if response:
            try:
                log_info(f"正在執行: {sys.executable} -m pip install {package_str}")
                subprocess.check_call([sys.executable, "-m", "pip", "install", *req_strings])
                messagebox.showinfo("安裝成功", "核心套件安裝成功，請重新啟動程式。")
                sys.exit(0)
            except subprocess.CalledProcessError as e:
                messagebox.showerror("安裝失敗", f"自動安裝套件失敗：{e}\n請手動打開命令提示字元並執行 'pip install {package_str}'")
                sys.exit(1)
        else:
            sys.exit(1)

    QR_SCAN_ENABLED = 'opencv-python' not in missing_optional and 'numpy' not in missing_optional
    if 'psutil' not in missing_optional:
        try:
            import psutil as psutil_lib
            psutil = psutil_lib
            PERFORMANCE_LOGGING_ENABLED = True
        except ImportError:
            PERFORMANCE_LOGGING_ENABLED = False

    if not ARCHIVE_SUPPORT_ENABLED:
        messagebox.showwarning("可選功能缺失", "未找到 `archive_handler.py` 模組檔案。\n壓縮檔掃描功能將被禁用。")

    if missing_optional:
        warning_message = f"缺少可選套件：{', '.join(missing_optional)}。\n\n"
        if not QR_SCAN_ENABLED:
            warning_message += "QR Code 相關功能將被禁用。\n要啟用，請安裝：pip install opencv-python numpy\n\n"
        if not PERFORMANCE_LOGGING_ENABLED:
            warning_message += "性能日誌功能將被禁用。\n要啟用，請安裝：pip install psutil\n\n"
        if 'rarfile' in missing_optional:
             warning_message += "RAR/CBR 壓縮檔支援被禁用。\n要啟用，請安裝：pip install rarfile\n並確保 UnRAR 工具存在於系統路徑或程式目錄。"
        messagebox.showwarning("缺少可選依賴", warning_message)
        log_info(f"警告: 缺少 {', '.join(missing_optional)}，相關功能已禁用。")

    log_info("所有必要套件檢查通過。")

# === 設定檔讀寫 ===
def load_config(config_path: str, default_config: dict) -> dict:
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config_from_file = json.load(f)
                merged_config = default_config.copy()
                merged_config.update(config_from_file)
                return merged_config
    except (json.JSONDecodeError, IOError, Exception) as e:
        log_error(f"讀取設定檔 '{config_path}' 失敗: {e}，將使用預設設定。")
    return default_config.copy()

def save_config(config: dict, config_path: str):
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
    except (IOError, Exception) as e:
        log_error(f"保存設定檔 '{config_path}' 時發生錯誤: {e}", True)