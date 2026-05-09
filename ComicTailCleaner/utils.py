# ======================================================================
# 檔案名稱：utils.py
# 模組目的：提供通用的輔助函式，如日誌、路徑處理、依賴檢查等
# 版本：1.2.0 (新增 xxhash 支援與 quick_digest 計算)
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
import re
from typing import Union, Optional, Tuple
from config import INFO_LOG_FILE, ERROR_LOG_FILE, DATA_DIR, LOG_DIR

# 延遲導入或選擇性導入
try:
    from PIL import Image, UnidentifiedImageError, ImageOps, ImageDraw
except ImportError:
    Image = UnidentifiedImageError = ImageOps = ImageDraw = None

try:
    import pkg_resources
except ImportError:
    pkg_resources = None

try:
    from tkinter import messagebox
except ImportError:
    class MockMessageBox: # CLI 備用
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
PERFORMANCE_LOGGING_ENABLED = False
psutil = None
CACHE_LOCK = threading.Lock()
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME_LOG_FILE = None
_TEE_LOCK = threading.RLock()
_TEE_FILE = None
_TEE_INSTALLED = False
_ORIGINAL_STDOUT = sys.stdout
_ORIGINAL_STDERR = sys.stderr

# 在匯入時就進行可靠的 QR 功能檢查
try:
    import cv2 as _cv2
    import numpy
    QR_SCAN_ENABLED = hasattr(_cv2, "QRCodeDetector")
except Exception:
    QR_SCAN_ENABLED = False

try:
    import xxhash
except ImportError:
    xxhash = None

__all__ = [
    'log_info', 'log_error', 'log_warning', 'log_performance',
    '_norm_key', '_is_virtual_path', '_parse_virtual_path', '_sanitize_path_for_filename',
    '_open_image_from_any_path', '_get_file_stat', 'sim_from_hamming', 'hamming_from_sim',
    '_avg_hsv', '_color_gate', '_open_folder', '_reveal_in_explorer', 'check_and_install_packages',
    'load_config', 'save_config', 'reset_runtime_log', 'append_runtime_log_session_header', 'install_runtime_log_tee',
    'close_runtime_log_tee', 'CACHE_LOCK', 'ARCHIVE_SUPPORT_ENABLED', 'QR_SCAN_ENABLED',
    '_auto_crop_white_borders'
]

# --- 【臨時功能：自動裁切白邊】 ---
def _auto_crop_white_borders(img: "Image.Image", tolerance: int = 240) -> "Image.Image":
    """
    自動裁切圖片四周的白邊 (亮度大於 tolerance 的視為白邊)
    回傳裁切後的圖片拷貝，若無白邊或出錯則回傳原圖。
    """
    try:
        from PIL import Image, ImageOps
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        
        gray = img.convert("L")
        # 自動對比優化：確保淺灰色掃描底色也能被拉到接近 255 的純白，以便正確裁切
        gray = ImageOps.autocontrast(gray, cutoff=2)
        
        lut = [255 if i < tolerance else 0 for i in range(256)]
        mask = gray.point(lut)
        bbox = mask.getbbox()
        if bbox:
            img = img.crop(bbox)
            
        # 標準化為直向 (Portrait) 以解決掃描器90度旋轉問題
        if getattr(img, 'width', 0) > getattr(img, 'height', 0):
            img = img.rotate(90, expand=True)
            
        return img
    except Exception:
        pass
    return img

# --- 【v1.2.0 新增】 ---
def _calculate_quick_digest(path: str) -> Optional[str]:
    """Hash only the first 64KB to avoid loading the whole file into memory."""
    if not xxhash:
        return None
    try:
        hasher = xxhash.xxh64()
        if _is_virtual_path(path):
            data = _open_image_from_any_path(path, read_bytes=True)
            if data is None:
                return None
            hasher.update(data[:65536])
        else:
            with open(path, 'rb') as f:
                hasher.update(f.read(65536))
        return hasher.hexdigest()
    except Exception:
        return None
# --- 輔助功能 ---

def _norm_key(p: str) -> str:
    """統一路徑表示，從 core_engine 移至此處成為公共函式。"""
    if not p: return p
    # 移除 file 協議前綴
    if p.lower().startswith("file:///"): p = p[8:]
    
    # 處理自訂的 zip 協議
    if p.lower().startswith("zip://"):
        m = re.match(r'^(zip://)(.+?)(!/.*)$', p, re.IGNORECASE)
        if m:
            prefix, real, inner = m.groups()
            # 將外部實體路徑標準化
            real_norm = os.path.normcase(os.path.normpath(real))
            # 確保內部路徑使用 unix 分隔符
            inner_norm = inner.replace("\\", "/")
            return f"zip://{real_norm}{inner_norm}"
        return p # 如果格式不匹配，返回原樣
        
    # 對於普通文件路徑，進行標準化
    return os.path.normcase(os.path.normpath(p))

# === 日誌函式 (修正版) ===

class _RuntimeTee:
    def __init__(self, stream):
        self.stream = stream

    @property
    def encoding(self):
        return getattr(self.stream, "encoding", None) or "utf-8"

    def write(self, text):
        if not isinstance(text, str):
            text = str(text)
        try:
            self.stream.write(text)
        except UnicodeEncodeError:
            enc = self.encoding
            self.stream.write(text.encode(enc, errors="replace").decode(enc, errors="replace"))
        except Exception:
            pass
        with _TEE_LOCK:
            if _TEE_FILE:
                try:
                    _TEE_FILE.write(text)
                except Exception:
                    pass

    def flush(self):
        try:
            self.stream.flush()
        except Exception:
            pass
        with _TEE_LOCK:
            if _TEE_FILE:
                try:
                    _TEE_FILE.flush()
                except Exception:
                    pass

    def isatty(self):
        return bool(getattr(self.stream, "isatty", lambda: False)())


def _safe_console_write(text: str):
    try:
        print(text, end='', flush=True)
    except UnicodeEncodeError:
        try:
            encoding = getattr(sys.stdout, 'encoding', None) or 'utf-8'
            safe_text = text.encode(encoding, errors='replace').decode(encoding, errors='replace')
            print(safe_text, end='', flush=True)
        except Exception:
            print(text.encode('ascii', errors='replace').decode('ascii'), end='', flush=True)


def _append_runtime_log(text: str):
    if _TEE_INSTALLED:
        return
    if not RUNTIME_LOG_FILE:
        return
    try:
        with open(RUNTIME_LOG_FILE, "a", encoding="utf-8", buffering=1) as f:
            f.write(text)
    except Exception:
        pass


def _new_runtime_log_path() -> str:
    stamp = datetime.datetime.now().strftime("%m%d-%H%M%S")
    path = os.path.join(LOG_DIR, f"LOG{stamp}.txt")
    if os.path.exists(path):
        path = os.path.join(LOG_DIR, f"LOG{stamp}-{os.getpid()}.txt")
    return path


def _cleanup_timestamp_runtime_logs(max_logs: int = 2):
    try:
        archives = []
        for name in os.listdir(LOG_DIR):
            if re.fullmatch(r"LOG\d{4}-\d{6}(?:-\d+)?\.txt", name):
                path = os.path.join(LOG_DIR, name)
                try:
                    archives.append((os.path.getmtime(path), path))
                except OSError:
                    pass
        archives.sort(reverse=True)
        for _, old_path in archives[max_logs:]:
            try:
                os.remove(old_path)
            except OSError:
                pass
    except Exception:
        pass


def _rotate_accumulative_log(file_path: str, max_size_mb: float = 1.0):
    """
    自適應日誌輪轉：若檔案超過指定大小，則更名為 .bak（覆蓋舊備份）。
    注意：此函式嚴禁呼叫 log_info/log_error 以免造成無窮遞迴。
    """
    if not os.path.exists(file_path):
        return
    try:
        if os.path.getsize(file_path) > max_size_mb * 1024 * 1024:
            bak_path = file_path + ".bak"
            os.replace(file_path, bak_path)
    except (OSError, PermissionError):
        pass


def reset_runtime_log():
    global RUNTIME_LOG_FILE
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"# ComicTailCleaner run log\n# started_at={timestamp}\n\n"
    try:
        # 1. 累積日誌輪轉 (防止無限增長)
        _rotate_accumulative_log(INFO_LOG_FILE)
        _rotate_accumulative_log(ERROR_LOG_FILE)

        # 2. 建立新的 Runtime Log
        RUNTIME_LOG_FILE = _new_runtime_log_path()
        with open(RUNTIME_LOG_FILE, "w", encoding="utf-8", buffering=1) as f:
            f.write(header)
        legacy_log = os.path.join(LOG_DIR, "LOG.txt")
        if os.path.exists(legacy_log):
            try:
                os.remove(legacy_log)
            except OSError:
                pass
        _cleanup_timestamp_runtime_logs(max_logs=2)
    except Exception:
        pass


def append_runtime_log_session_header():
    if not RUNTIME_LOG_FILE:
        reset_runtime_log()
        return
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = (
        "\n"
        "================================================================\n"
        f"# ComicTailCleaner comparison started_at={timestamp}\n"
        "================================================================\n\n"
    )
    try:
        with open(RUNTIME_LOG_FILE, "a", encoding="utf-8", buffering=1) as f:
            f.write(header)
    except Exception:
        pass


def install_runtime_log_tee(reset: bool = True):
    global _TEE_FILE, _TEE_INSTALLED
    if _TEE_INSTALLED:
        return
    if reset:
        reset_runtime_log()
    else:
        if not RUNTIME_LOG_FILE:
            reset_runtime_log()
        else:
            append_runtime_log_session_header()
    try:
        _TEE_FILE = open(RUNTIME_LOG_FILE, "a", encoding="utf-8", buffering=1)
        sys.stdout = _RuntimeTee(_ORIGINAL_STDOUT)
        sys.stderr = _RuntimeTee(_ORIGINAL_STDERR)
        _TEE_INSTALLED = True
    except Exception:
        _TEE_FILE = None
        _TEE_INSTALLED = False


def close_runtime_log_tee():
    global _TEE_FILE, _TEE_INSTALLED
    if not _TEE_INSTALLED:
        return
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    sys.stdout = _ORIGINAL_STDOUT
    sys.stderr = _ORIGINAL_STDERR
    with _TEE_LOCK:
        if _TEE_FILE:
            try:
                _TEE_FILE.flush()
                _TEE_FILE.close()
            except Exception:
                pass
            _TEE_FILE = None
    _TEE_INSTALLED = False


def log_error(message: str, include_traceback: bool = False):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] ERROR: {message}\n"
    if include_traceback:
        log_content += traceback.format_exc() + "\n"
    _safe_console_write(log_content)
    _append_runtime_log(log_content)
    try:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8-sig", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"FATAL: 無法寫入錯誤日誌檔案: {e}\n原始錯誤: {message}", flush=True)

def log_info(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] INFO: {message}\n"
    _safe_console_write(log_content)
    _append_runtime_log(log_content)
    try:
        with open(INFO_LOG_FILE, "a", encoding="utf-8-sig", buffering=1) as f:
            f.write(log_content)
    except Exception as e:
        print(f"FATAL: 無法寫入資訊日誌檔案: {e}", flush=True)

def log_warning(msg: str):
    """記錄一條警告訊息，並重定向到 info log。"""
    log_info(f"WARNING: {msg}")

def log_performance(message: str):
    global psutil
    if PERFORMANCE_LOGGING_ENABLED and psutil:
        try:
            process = psutil.Process(os.getpid())
            cpu_percent = process.cpu_percent(interval=None)
            memory_mb = process.memory_info().rss / (1024 * 1024)
            log_info(f"{message} (CPU: {cpu_percent:.1f}%, Mem: {memory_mb:.1f} MB)")
        except (psutil.NoSuchProcess, psutil.AccessDenied, Exception):
            log_info(message)
    else:
        log_info(message)

# === 路徑處理函式 ===
def _is_virtual_path(path: str) -> bool:
    return path.startswith(VPATH_PREFIX)

def _parse_virtual_path(vpath: str) -> Tuple[Optional[str], Optional[str]]:
    if not _is_virtual_path(vpath):
        return None, None
    try:
        _, content = vpath.split(VPATH_PREFIX, 1)
        # 改用 rsplit，並限制切分 1 次。這樣會從最右邊的 '!' 開始切，
        # 確保前面路徑裡的 '!' 不會干擾。
        archive_path, inner_path = content.rsplit(VPATH_SEPARATOR, 1)
        return archive_path, inner_path
        
        
    except (ValueError, IndexError):
        log_error(f"解析虛擬路徑失敗: {vpath}")
        return None, None

def _sanitize_path_for_filename(path: str) -> str:
    """清理路徑中的非法字元"""
    if not path:
        return ""
    basename = os.path.basename(os.path.normpath(path))
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', basename)
    return sanitized

def _open_image_from_any_path(path: str, read_bytes: bool = False) -> Optional[Union[Image.Image, bytes]]:
    if Image is None:
        return None

    def _read_data():
        if _is_virtual_path(path):
            archive_path, inner_path = _parse_virtual_path(path)
            if archive_path and inner_path and archive_handler:
                return archive_handler.get_image_bytes(archive_path, inner_path)
            return None
        if os.path.exists(path):
            with open(path, 'rb') as f:
                return f.read()
        return None

    try:
        lock = getattr(sys.modules[__name__], 'SHARED_IO_LOCK', None)
        if lock is not None:
            with lock:
                image_bytes = _read_data()
        else:
            image_bytes = _read_data()

        if image_bytes is None:
            return None
        if read_bytes:
            return image_bytes

        with Image.open(io.BytesIO(image_bytes)) as img:
            img.load()
            return img.copy()
    except (UnidentifiedImageError, IOError, Exception):
        return None

def _get_file_stat(path: str) -> Tuple[Optional[int], Optional[float], Optional[float]]:
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

def _avg_hsv(img: Image.Image) -> Optional[Tuple[float, float, float]]:
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
        if abs(s1 - s2) > 0.15: return False
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

def _reveal_in_explorer(path: str):
    """
    在檔案總管中開啟並**高亮選取**指定的路徑（可以是檔案或資料夾）。
    - Windows : explorer /select,"path"
    - macOS   : open -R "path"
    - Linux   : xdg-open 上層目錄（Linux 不支援直接選取）
    """
    try:
        norm = os.path.normpath(path)
        if sys.platform == "win32":
            subprocess.Popen(['explorer', '/select,', norm])
        elif sys.platform == "darwin":
            subprocess.Popen(['open', '-R', norm])
        else:
            # Linux 退路：開上層目錄
            parent = os.path.dirname(norm)
            subprocess.Popen(['xdg-open', parent])
    except Exception as e:
        log_error(f"無法在檔案總管中顯示路徑 '{path}': {e}", True)


    global QR_SCAN_ENABLED, PERFORMANCE_LOGGING_ENABLED, psutil, ARCHIVE_SUPPORT_ENABLED

    if getattr(sys, 'frozen', False):
        log_info("在打包環境中運行，跳過依賴檢查。")
        try: 
            import cv2
            import numpy
            QR_SCAN_ENABLED = True
        except ImportError: 
            QR_SCAN_ENABLED = False
        try: 
            import psutil as psutil_lib
            psutil = psutil_lib
            PERFORMANCE_LOGGING_ENABLED = True
        except ImportError: 
            PERFORMANCE_LOGGING_ENABLED = False
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

    # 重新評估 QR_SCAN_ENABLED
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
        if 'opencv-python' in missing_optional or 'numpy' in missing_optional:
            warning_message += "QR Code 相關功能將被禁用。\n要啟用，請安裝：pip install opencv-python numpy\n\n"
        if 'psutil' in missing_optional:
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
