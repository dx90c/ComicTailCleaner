# ======================================================================
# 檔案名稱：app.py
# 模組目的：ComicTailCleaner 的主程式入口
# 版本：1.1.0 (新增自動依賴管理)
# ======================================================================

import sys
import os
from multiprocessing import set_start_method, freeze_support

# --- v-MOD START: 自動依賴檢查 ---
# 在導入任何其他本地模組(尤其是 gui)之前，先檢查依賴
# 只有在非打包環境 (.py 腳本執行) 下才檢查，打包成 exe 後不需要
if not getattr(sys, 'frozen', False):
    try:
        import dependency_manager
        dependency_manager.check_and_install()
    except ImportError:
        print("[警告] 找不到 dependency_manager.py，跳過自動依賴檢查。")
    except Exception as e:
        print(f"[警告] 依賴檢查發生錯誤: {e}")
# --- v-MOD END ---

from tkinter import messagebox
from gui.main_window import MainWindow
# utils.check_and_install_packages 我們已經用更強大的 dependency_manager 取代了
# 所以原本 utils 裡的檢查可以保留作為 fallback，或者在 utils 裡移除

def main() -> None:
    """
    主執行函式。
    """
    if sys.platform.startswith('win'):
        try:
            set_start_method('spawn', force=True)
        except RuntimeError:
            pass

    app = None
    try:
        app = MainWindow()
        # app.withdraw() # dependency_manager 已經在最前面處理好了，這裡可以直接顯示
        app.deiconify()
        app.mainloop()

    except SystemExit:
        # 安裝套件後會觸發 SystemExit
        if app: app.destroy()
        return

    except Exception as e:
        from utils import log_error
        log_error(f"程式啟動時發生未預期的嚴重錯誤: {e}", include_traceback=True)
        # 這裡因為還沒有主視窗，用一個簡單的 tk root 來顯示錯誤框
        try:
            import tkinter as tk
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("啟動失敗", f"錯誤訊息: {e}")
            root.destroy()
        except:
            print(f"啟動失敗: {e}")
        if app: app.destroy()
        return

if __name__ == '__main__':
    freeze_support()
    main()