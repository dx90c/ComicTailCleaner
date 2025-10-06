# ======================================================================
# 檔案名稱：app.py
# 模組目的：ComicTailCleaner 的主程式入口
# 版本：1.0.0
# ======================================================================

import sys
from multiprocessing import set_start_method, freeze_support
from tkinter import messagebox

# 從我們的本地模組中導入必要的元件
# gui 模組負責所有視窗介面
# utils 模組提供通用工具 (日誌、依賴檢查等)
from gui import MainWindow
from utils import log_error, check_and_install_packages

def main() -> None:
    """
    主執行函式。
    負責設定環境、建立應用程式實例並進入主迴圈。
    """
    # 為了在 Windows 或打包成 .exe 檔案後，多進程功能能穩定運作，
    # 'spawn' 是最推薦的啟動方法。
    if sys.platform.startswith('win'):
        try:
            # 強制設定啟動方法。如果程式被其他腳本導入，這可以避免衝突。
            set_start_method('spawn', force=True)
        except RuntimeError:
            # 如果啟動方法已經被設定，會拋出 RuntimeError，可以安全地忽略。
            pass

    app = None  # 預先定義 app 變數，確保在 try...except 區塊中都能存取
    try:
        # 1. 建立主視窗 MainWindow 的實例
        app = MainWindow()

        # 2. 暫時隱藏主視窗
        #    這麼做是為了在背景進行初始化和依賴檢查時，
        #    使用者不會看到一個空白或未完成的視窗，提升使用者體驗。
        app.withdraw()

        # 3. 執行套件依賴檢查
        #    這個函式會檢查所有必要的第三方函式庫是否存在，並提示使用者安裝。
        check_and_install_packages()

        # 4. 顯示主視窗並進入程式主迴圈
        #    deiconify() 會讓先前隱藏的視窗重新顯示。
        #    mainloop() 會啟動 Tkinter 的事件迴圈，開始等待使用者操作 (點擊按鈕、捲動等)。
        #    程式會停留在此，直到視窗被關閉。
        app.deiconify()
        app.mainloop()

    except SystemExit:
        # 當 check_and_install_packages 函式庫要求使用者安裝套件並重啟時，
        # 它會呼叫 sys.exit()，這會引發 SystemExit 例外。
        # 我們捕捉這個例外，記錄日誌，並確保程式乾淨地退出。
        log_error("程式因 SystemExit 而關閉 (通常在套件安裝後發生)。")
        if app:
            app.destroy()  # 確保 Tkinter 視窗被銷毀
        return

    except Exception as e:
        # 捕捉所有其他在啟動過程中可能發生的未知錯誤。
        log_error(f"程式啟動時發生未預期的嚴重錯誤: {e}", include_traceback=True)
        messagebox.showerror(
            "啟動失敗",
            f"程式啟動時發生嚴重錯誤，無法繼續執行。\n\n"
            f"請檢查 'error_log.txt' 檔案以獲取詳細資訊。\n\n"
            f"錯誤訊息: {e}"
        )
        if app:
            app.destroy()
        return

if __name__ == '__main__':
    # 這是一個 Python 的標準寫法。
    # `if __name__ == '__main__':` 確保只有當這個檔案被直接執行時，
    # 裡面的程式碼才會被運行。如果它被其他檔案作為模組導入，則不會執行。

    # 對於使用 multiprocessing 的跨平台應用程式，尤其是在打包成執行檔時，
    # freeze_support() 是必須的，它能確保子進程被正確地創建。
    freeze_support()

    # 呼叫主函式，啟動我們的應用程式。
    main()