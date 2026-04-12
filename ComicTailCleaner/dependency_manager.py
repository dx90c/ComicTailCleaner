# ======================================================================
# 檔案名稱：dependency_manager.py
# 模組目的：自動掃描依賴、生成 requirements.txt 並透過 GUI 提示安裝
# 版本：1.1.0 (新增 Tkinter 彈窗通知)
# ======================================================================

import os
import sys
import subprocess
import threading
import json
import tkinter as tk
from tkinter import ttk, messagebox

# ======================================================================

def _show_gui_message(title, message, type='info'):
    """顯示一個臨時的 Tkinter 對話框"""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    
    result = None
    if type == 'yesno':
        result = messagebox.askyesno(title, message, parent=root)
    elif type == 'error':
        messagebox.showerror(title, message, parent=root)
    else:
        messagebox.showinfo(title, message, parent=root)
        
    root.destroy()
    return result

class InstallProgressWindow(tk.Toplevel):
    def __init__(self, requirements):
        super().__init__()
        self.title("正在安裝依賴套件")
        self.geometry("400x120")
        self.resizable(False, False)
        self.attributes("-topmost", True)
        self.protocol("WM_DELETE_WINDOW", self._disable_close)
        
        # 使視窗居中
        self.update_idletasks()
        x = (self.winfo_screenwidth() // 2) - (400 // 2)
        y = (self.winfo_screenheight() // 2) - (120 // 2)
        self.geometry(f"+{x}+{y}")
        
        self.requirements = requirements
        self.success = False
        self.error_msg = ""
        
        lbl = ttk.Label(self, text="正在透過 pip 安裝套件，請稍候...\n(可能需要幾分鐘的時間)", justify=tk.CENTER)
        lbl.pack(pady=15)
        
        self.progress = ttk.Progressbar(self, mode='indeterminate', length=300)
        self.progress.pack(pady=5)
        self.progress.start(15)
        
        # 在背景執行緒啟動安裝
        threading.Thread(target=self._run_install, daemon=True).start()

    def _disable_close(self):
        pass

    def _run_install(self):
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )
            self.success = True
        except subprocess.CalledProcessError as e:
            self.success = False
            self.error_msg = f"錯誤碼: {e.returncode}"
        finally:
            self.after(0, self.destroy)

def check_and_install():
    """檢查並安裝缺失的套件 (GUI 版)"""
    
    if not os.path.exists('requirements.txt'):
        # 如果沒有 requirements 就不管他
        return

    req_file = 'requirements.txt'
    cache_file = '.deps_cache'
    
    try:
        current_mtime = os.path.getmtime(req_file)
    except OSError:
        return
        
    current_py_ver = sys.version
    
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            if cache.get('mtime') == current_mtime and cache.get('py_version') == current_py_ver:
                return # 完全命中快取，一秒跳過
        except Exception:
            pass

    # 1. 檢查缺失
    missing = []
    with open(req_file, 'r', encoding='utf-8') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    for req in requirements:
        # 簡單解析：分離套件名與版本 (處理 ==, >=, <= 等)
        pkg_name = req.split('>')[0].split('=')[0].split('<')[0].strip()
        try:
            try:
                from importlib.metadata import version, PackageNotFoundError
                version(pkg_name)
            except ImportError:
                import pkg_resources
                pkg_resources.get_distribution(pkg_name)
        except Exception:
            missing.append(req)

    if not missing:
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({'mtime': current_mtime, 'py_version': current_py_ver}, f)
        except Exception:
            pass
        return # 全部都裝好了

    # 2. GUI 通知用戶
    msg = "偵測到以下必要套件缺失或版本不符：\n\n"
    msg += "\n".join([f"• {m}" for m in missing[:10]])
    if len(missing) > 10:
        msg += f"\n... 以及其他 {len(missing)-10} 個"
    
    msg += "\n\n是否嘗試自動安裝這些套件？"

    should_install = _show_gui_message("環境檢查", msg, type='yesno')

    if should_install:
        # 啟動進度條視窗
        root = tk.Tk()
        root.withdraw()
        inst_win = InstallProgressWindow(missing)
        root.wait_window(inst_win)
        
        if inst_win.success:
            _show_gui_message("安裝成功", "所有套件已安裝完成！\n程式將關閉，請重新啟動以套用變更。")
            sys.exit(0)
        else:
            error_msg = f"自動安裝失敗 ({inst_win.error_msg})。\n\n請手動開啟終端機執行：\npip install -r requirements.txt"
            _show_gui_message("安裝失敗", error_msg, type='error')
            sys.exit(1)
    else:
        _show_gui_message("提示", "請參考根目錄下的 requirements.txt 手動安裝依賴。\n程式可能無法正常運作。")