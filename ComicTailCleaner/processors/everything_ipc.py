"""
processors/everything_ipc.py
Everything SDK IPC 整合層 (v2.0 - 自動位元判斷 + 正確查詢語法)

三層遞進保護：
  1. 如果 Everything 正在跑 → 直接借用 MFT 秒搜
  2. 如果找不到 DLL → 自動從官方下載 (約 100KB)
  3. 如果連不上 / 下載失敗 → 靜默退回 os.scandir 模式
"""
import os
import sys
import ctypes
import time
import datetime
import subprocess
import urllib.request
import zipfile

from utils import log_info, log_error, _norm_key
from config import DATA_DIR, EVERYTHING_DLL_PATH, BIN_DIR

EVERYTHING_IPC_ERROR_IPC = 2   # Everything 自己定義的 IPC 連線失敗錯誤碼


class EverythingIPCManager:
    """Wrapper for Everything SDK — 自動選擇 32/64 位 DLL。"""

    def __init__(self):
        self.dll = None
        self.is_ready = False

    # ------------------------------------------------------------------ #
    #  內部：載入 DLL + 握手確認                                           #
    # ------------------------------------------------------------------ #
    def _try_load_dll(self, dll_path: str) -> bool:
        """嘗試載入指定路徑的 DLL 並做連線測試。成功回傳 True。"""
        try:
            dll = ctypes.windll.LoadLibrary(dll_path)

            # 型別宣告（必要，否則可能 crash）
            dll.Everything_SetSearchW.argtypes              = [ctypes.c_wchar_p]
            dll.Everything_QueryW.argtypes                  = [ctypes.c_bool]
            dll.Everything_GetNumResults.restype            = ctypes.c_int
            dll.Everything_GetResultFullPathNameW.argtypes  = [ctypes.c_int, ctypes.c_wchar_p, ctypes.c_int]
            dll.Everything_GetLastError.restype             = ctypes.c_int
            dll.Everything_CleanUp.argtypes                 = []
            dll.Everything_GetMajorVersion.restype          = ctypes.c_int

            # 握手：探測版本
            major = dll.Everything_GetMajorVersion()

            # 連線測試：送一個最簡單的查詢
            dll.Everything_SetSearchW("test")
            dll.Everything_QueryW(True)
            err = dll.Everything_GetLastError()

            if err == EVERYTHING_IPC_ERROR_IPC:
                log_info(f"[Everything SDK] DLL 載入成功 (v{major}.x)，但 Everything 服務未在背景運行。")
                return False

            log_info(f"[Everything SDK] ✅ 連線成功！Everything v{major}.x，MFT 秒搜啟動！")
            self.dll = dll
            return True

        except OSError as e:
            # Windows 403: 位元不符時會在這裡炸掉
            log_error(f"[Everything SDK] 載入 {os.path.basename(dll_path)} 失敗 (可能是位元不符): {e}")
            return False
        except Exception as e:
            log_error(f"[Everything SDK] 未知錯誤: {e}")
            return False

    def _initialize(self) -> bool:
        if self.is_ready:
            return True

        # os.makedirs(DATA_DIR, exist_ok=True) # 已在 config.py 中建立

        # ---- 策略：先嘗試已下載的 DLL (優先使用新結構路徑) ---- #
        dll_candidates = [
            EVERYTHING_DLL_PATH,
            os.path.join(BIN_DIR, "Everything32.dll"),
            os.path.join(DATA_DIR, "Everything32.dll"),
        ]

        any_exists = any(os.path.exists(p) for p in dll_candidates)

        if not any_exists:
            log_info("未偵測到 Everything SDK DLL，正在從 Voidtools 官方下載（約 100KB）...")
            try:
                zip_path = os.path.join(DATA_DIR, "Everything-SDK.zip")
                urllib.request.urlretrieve("https://www.voidtools.com/Everything-SDK.zip", zip_path)
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    names = zf.namelist()
                    for dll_name in ["Everything64.dll", "Everything32.dll"]:
                        # SDK zip 裡面有 dll/Everything64.dll 這樣的路徑
                        inner = next((n for n in names if n.endswith(dll_name)), None)
                        if inner:
                            data = zf.read(inner)
                            # 下載時優先存入新結構的 bin/ 目錄
                            target = os.path.join(BIN_DIR, dll_name)
                            with open(target, 'wb') as f:
                                f.write(data)
                try:
                    os.remove(zip_path)
                except Exception:
                    pass
                log_info("🎉 Everything SDK 下載完成！")
            except Exception as e:
                log_error(f"下載 Everything SDK 失敗: {e}")
                return False

        # ---- 自動嘗試：先試與 Python 位元一致的，再試另一個 ---- #
        is_64bits = sys.maxsize > 2**32
        ordered = (
            [dll_candidates[0], dll_candidates[1]] if is_64bits
            else [dll_candidates[1], dll_candidates[0]]
        )

        for dll_path in ordered:
            if not os.path.exists(dll_path):
                continue
            if self._try_load_dll(dll_path):
                self.is_ready = True
                return True

        log_info("[Everything SDK] 兩個版本的 DLL 都無法連線，退回 os.scandir 模式。")
        return False

    # ------------------------------------------------------------------ #
    #  公開 API                                                             #
    # ------------------------------------------------------------------ #
    def is_everything_running(self) -> bool:
        return self._initialize()

    def search(self, root_path: str, extensions: list,
               excluded_paths: list, excluded_names: list,
               min_mtime: float = None, max_mtime: float = None,
               time_mode: str = 'mtime') -> list:
        """
        在 root_path 下搜尋符合 extensions 的所有檔案。
        回傳正規化後的完整路徑清單。

        time_mode: 'ctime' → 使用 Everything 的 dc: (Date Created)
                   其他    → 使用 Everything 的 dm: (Date Modified)
        """
        if not self._initialize():
            return []

        # ✅ 正確語法：ext:jpg;png;zip（無星號、無點、分號分隔）
        ext_str = ";".join([e.lstrip("*.").lower() for e in extensions])

        # Everything 對路徑用反斜線較穩定，但不需要後綴斜線
        norm_root = os.path.normpath(root_path)

        # 基底查詢：path:"X:\folder" ext:jpg;png;zip
        query = f'path:"{norm_root}" ext:{ext_str}'

        # 套用排除清單
        for ep in excluded_paths:
            query += f' !path:"{os.path.normpath(ep)}"'
        for en in excluded_names:
            query += f' !"{en}"'

        # 根據使用者設定決定時間過濾維度
        # ctime → dc: (Date Created，Windows 下載/移動檔案時會更新)
        # mtime / hybrid → dm: (Date Modified，原始壓製時間)
        time_prefix = "dc" if time_mode == 'ctime' else "dm"

        # 套用時間篩選
        if min_mtime or max_mtime:
            fmt = "%Y-%m-%dT%H:%M:%S"
            t_start = datetime.datetime.fromtimestamp(min_mtime).strftime(fmt) if min_mtime else ""
            t_end   = datetime.datetime.fromtimestamp(max_mtime).strftime(fmt) if max_mtime else ""
            if t_start and t_end:
                query += f" {time_prefix}:{t_start}..{t_end}"
            elif t_start:
                query += f" {time_prefix}:>={t_start}"
            elif t_end:
                query += f" {time_prefix}:<={t_end}"

        log_info(f"[Everything Query] {query}")

        self.dll.Everything_SetSearchW(query)
        self.dll.Everything_QueryW(True)

        err = self.dll.Everything_GetLastError()
        if err != 0:
            log_error(f"[Everything SDK] 查詢錯誤碼: {err}")
            return []

        num_results = self.dll.Everything_GetNumResults()
        log_info(f"[Everything SDK] 瞬間發現 {num_results} 個匹配檔案。")

        results = []
        buf = ctypes.create_unicode_buffer(32768)
        for i in range(num_results):
            self.dll.Everything_GetResultFullPathNameW(i, buf, 32768)
            results.append(_norm_key(buf.value))

        self.dll.Everything_CleanUp()
        return results

    # ------------------------------------------------------------------ #
    #  dc: 索引管理：自動偵測 / 自動啟用                                  #
    # ------------------------------------------------------------------ #

    def find_everything_exe(self) -> str:
        """透過登錄檔或常見路徑找出 Everything.exe。"""
        try:
            import winreg
            for root_key in [winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER]:
                try:
                    key = winreg.OpenKey(root_key,
                        r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\Everything.exe")
                    path, _ = winreg.QueryValueEx(key, None)
                    winreg.CloseKey(key)
                    if path and os.path.exists(path):
                        return path
                except OSError:
                    pass
        except ImportError:
            pass
        for candidate in [
            os.path.join(os.environ.get('ProgramFiles',    'C:\\Program Files'),        'Everything', 'Everything.exe'),
            os.path.join(os.environ.get('ProgramFiles(x86)', 'C:\\Program Files (x86)'), 'Everything', 'Everything.exe'),
        ]:
            if os.path.exists(candidate):
                return candidate
        return None

    def find_everything_ini(self) -> str:
        """找出 Everything.ini 設定檔路徑（優先 AppData，其次 exe 所在目錄）。"""
        # 一般使用者安裝：AppData
        appdata = os.environ.get('APPDATA', '')
        candidate = os.path.join(appdata, 'Everything', 'Everything.ini')
        if os.path.exists(candidate):
            return candidate
        # 可攜版 / 服務版：與 exe 同目錄
        exe = self.find_everything_exe()
        if exe:
            candidate = os.path.join(os.path.dirname(exe), 'Everything.ini')
            if os.path.exists(candidate):
                return candidate
        return None

    def check_dc_indexed(self) -> bool:
        """回傳 True 若 Everything 已開啟「索引建立日期」選項。"""
        ini_path = self.find_everything_ini()
        if not ini_path:
            log_info("[Everything SDK] 找不到 Everything.ini，預設視為未開啟 dc 索引。")
            return False
        try:
            with open(ini_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                for line in f:
                    if line.strip().lower().startswith('index_date_created='):
                        return line.strip().split('=', 1)[1].strip() == '1'
            return False  # 鍵不存在 → 預設為關閉
        except Exception as e:
            log_error(f"[Everything SDK] 讀取 Everything.ini 失敗: {e}")
            return False

    def enable_dc_index_and_restart(self) -> bool:
        """自動修改 Everything.ini 開啟建立日期索引，並重啟 Everything。
        回傳 True 代表 INI 修改成功（Everything 將在背景重建索引）。"""
        ini_path = self.find_everything_ini()
        if not ini_path:
            log_error("[Everything SDK] 找不到 Everything.ini，無法自動啟用 dc 索引。")
            return False
        try:
            with open(ini_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                lines = f.readlines()
            found = False
            for i, line in enumerate(lines):
                if line.strip().lower().startswith('index_date_created='):
                    lines[i] = 'index_date_created=1\n'
                    found = True
                    break
            if not found:
                lines.append('index_date_created=1\n')
            with open(ini_path, 'w', encoding='utf-8-sig') as f:
                f.writelines(lines)
            log_info(f"[Everything SDK] ✅ Everything.ini 已修改：index_date_created=1")
        except Exception as e:
            log_error(f"[Everything SDK] 修改 Everything.ini 失敗: {e}")
            return False

        # 重啟 Everything 以套用新設定
        exe_path = self.find_everything_exe()
        if exe_path:
            try:
                subprocess.run(['taskkill', '/F', '/IM', 'Everything.exe'],
                               capture_output=True, timeout=5)
                time.sleep(1)
                subprocess.Popen(
                    [exe_path],
                    creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
                )
                time.sleep(2)
                log_info("[Everything SDK] Everything 已重新啟動，正在背景建立「建立日期」索引。")
            except Exception as e:
                log_error(f"[Everything SDK] 重啟 Everything 失敗（請手動重啟）: {e}")
        else:
            log_info("[Everything SDK] 找不到 Everything.exe，INI 已改好，請手動重新啟動 Everything 以套用設定。")
        return True
