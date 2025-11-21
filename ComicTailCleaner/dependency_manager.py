# ======================================================================
# 檔案名稱：dependency_manager.py
# 模組目的：自動掃描依賴、生成 requirements.txt 並透過 GUI 提示安裝
# 版本：1.1.0 (新增 Tkinter 彈窗通知)
# ======================================================================

import os
import sys
import ast
import subprocess
import pkg_resources
import tkinter as tk
from tkinter import messagebox

# === 設定區 ===

# 專案中會掃描的資料夾
SCAN_DIRS = ['core', 'gui', 'plugins', 'processors', '.']

# 忽略的系統內建模組 (白名單)
IGNORE_MODULES = {
    'os', 'sys', 're', 'json', 'time', 'datetime', 'math', 'random', 
    'collections', 'queue', 'threading', 'multiprocessing', 'shutil', 
    'io', 'typing', 'abc', 'tempfile', 'subprocess', 'traceback', 
    'tkinter', 'urllib', 'ssl', 'webbrowser', 'concurrent', 'pathlib',
    'enum', 'copy', 'platform', 'hashlib', 'base64', 'uuid', 'ctypes',
    'config', 'utils', 'core_engine', 'gui', 'plugins', 'processors',
    'dependency_manager' # 排除自己
}

# Import 名稱 -> PyPI 套件名稱的對照表
PACKAGE_MAPPING = {
    'PIL': 'Pillow>=9.0.0',
    'cv2': 'opencv-python>=4.5.0',
    'imagehash': 'imagehash>=4.3.1',
    'send2trash': 'send2trash>=1.8.0',
    'numpy': 'numpy>=1.21.0',
    'psutil': 'psutil>=5.9.0',
    'pyautogui': 'pyautogui>=0.9.53',
    'pyperclip': 'pyperclip>=1.8.2',
    'tkcalendar': 'tkcalendar>=1.6.1',
    'nanoid': 'nanoid>=2.0.0',
    'rarfile': 'rarfile>=4.0'
}

# 預設必須安裝的基礎套件
BASE_REQUIREMENTS = {
    'Pillow>=9.0.0',
    'send2trash>=1.8.0'
}

# ======================================================================

def get_imports_from_file(filepath):
    """使用 AST 靜態分析檔案中的 import 語句"""
    imports = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            root = ast.parse(f.read(), filepath)
        
        for node in ast.walk(root):
            if isinstance(node, ast.Import):
                for n in node.names:
                    imports.add(n.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except Exception:
        pass 
    return imports

def scan_project_dependencies():
    """掃描整個專案並回傳所需的 PyPI 套件列表"""
    detected_imports = set()
    
    for d in SCAN_DIRS:
        if d == '.':
            files = [f for f in os.listdir('.') if f.endswith('.py')]
            for f in files: detected_imports.update(get_imports_from_file(f))
        elif os.path.exists(d):
            for root, dirs, files in os.walk(d):
                for file in files:
                    if file.endswith(".py"):
                        path = os.path.join(root, file)
                        detected_imports.update(get_imports_from_file(path))

    required_packages = set(BASE_REQUIREMENTS)
    
    for module in detected_imports:
        if module in IGNORE_MODULES: continue
        # 忽略本地 plugins 資料夾名稱
        if os.path.exists(os.path.join('plugins', module)): continue

        if module in PACKAGE_MAPPING:
            required_packages.add(PACKAGE_MAPPING[module])
        else:
            pass # 未知模組不自動加入，避免錯誤

    return sorted(list(required_packages))

def generate_requirements_file():
    """生成 requirements.txt，回傳是否已更新"""
    reqs = scan_project_dependencies()
    new_content = "\n".join(reqs)
    
    # 檢查內容是否變更
    current_content = ""
    if os.path.exists('requirements.txt'):
        with open('requirements.txt', 'r', encoding='utf-8') as f:
            current_content = f.read().strip()
    
    if current_content != new_content:
        try:
            with open('requirements.txt', 'w', encoding='utf-8') as f:
                f.write(new_content)
            return True # 檔案已更新
        except Exception:
            return False
    return False 

def _show_gui_message(title, message, type='info'):
    """顯示一個臨時的 Tkinter 對話框"""
    root = tk.Tk()
    root.withdraw() # 隱藏主視窗
    # 確保視窗在最上層
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

def check_and_install():
    """檢查並安裝缺失的套件 (GUI 版)"""
    
    # 1. 更新清單
    generate_requirements_file()
    
    if not os.path.exists('requirements.txt'):
        return

    # 2. 檢查缺失
    missing = []
    with open('requirements.txt', 'r', encoding='utf-8') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    for req in requirements:
        # 簡單解析：分離套件名與版本
        pkg_name = req.split('>')[0].split('=')[0].split('<')[0].strip()
        try:
            pkg_resources.get_distribution(pkg_name)
        except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict):
            missing.append(req)

    if not missing:
        return # 全部都裝好了

    # 3. GUI 通知用戶
    msg = "偵測到以下必要套件缺失或版本不符：\n\n"
    msg += "\n".join([f"• {m}" for m in missing[:10]]) # 最多顯示10個避免視窗太長
    if len(missing) > 10:
        msg += f"\n... 以及其他 {len(missing)-10} 個"
    
    msg += "\n\n完整的依賴清單已生成於 'requirements.txt'。"
    msg += "\n\n是否嘗試自動安裝這些套件？"

    should_install = _show_gui_message("環境檢查", msg, type='yesno')

    if should_install:
        try:
            # 使用 subprocess 呼叫 pip
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
            
            _show_gui_message("安裝成功", "所有套件已安裝完成！\n程式將關閉，請重新啟動以套用變更。")
            sys.exit(0) # 成功後退出重啟
            
        except subprocess.CalledProcessError as e:
            error_msg = f"自動安裝失敗 (錯誤碼: {e.returncode})。\n\n請手動開啟終端機執行：\npip install -r requirements.txt"
            _show_gui_message("安裝失敗", error_msg, type='error')
            sys.exit(1) # 失敗退出
    else:
        # 用戶選擇不自動安裝
        _show_gui_message("提示", "請參考根目錄下的 requirements.txt 手動安裝依賴。\n程式可能無法正常運作。")
        # 這裡不強制退出，但程式可能會在後面崩潰