這是一份針對 **v16.0.2** 架構優化後的打包指引。

由於我們引入了 `gui/` 資料夾、新的外掛依賴 (`pyautogui`, `pyperclip` 等) 以及 SQLite，打包指令需要做相應的調整。為了盡量縮小體積，我保留了排除大型未用庫的參數，並加入了 UPX 壓縮建議。

### ComicTailCleaner 專案打包指引 (v16.0.2)

**文件目的**: 將 Python 專案打包成單一 Windows 可執行檔 (.exe)，並透過參數優化檔案體積。

---

### 一、前置準備 (最重要的一步)

為了避免打包進系統中不相關的雜物（這是 EXE 肥大的主因），強烈建議使用 **乾淨的虛擬環境 (Virtual Environment)**。

1.  **建立虛擬環境**:
    ```bash
    python -m venv venv
    ```
2.  **進入虛擬環境**:
    ```bash
    venv\Scripts\activate
    ```
3.  **只安裝必要套件** (這一步決定了體積大小):
    ```bash
    pip install pyinstaller pillow imagehash opencv-python numpy send2trash psutil pyautogui pyperclip tkcalendar nanoid rarfile
    ```
    *(注意：不要安裝 pandas, matplotlib 等沒用到的巨型套件)*

4.  **準備檔案結構**:
    請將所有要打包的檔案放在同一個資料夾（例如 `build_dir`），結構應如下：
    ```text
    build_dir/
    ├── app.py               (程式入口)
    ├── config.py
    ├── utils.py
    ├── core_engine.py
    ├── archive_handler.py
    ├── dependency_manager.py
    ├── gui/                 (GUI 模組資料夾)
    ├── core/                (如果有的話)
    ├── plugins/             (外掛資料夾)
    ├── processors/          (處理器資料夾)
    ├── UnRAR.exe            (必要工具)
    ├── icon.ico             (圖示)
    └── upx.exe              (推薦：放入 UPX 壓縮工具可減少約 30% 體積)
    ```

---

### 二、打包命令 (優化版)

請在終端機切換到上述目錄，然後執行以下指令。

#### 📋 單行版本 (直接複製貼上)

```bash
pyinstaller --noconfirm --clean --windowed --onefile --upx-dir="." --icon="icon.ico" --add-data "plugins;plugins" --add-data "UnRAR.exe;." --hidden-import="gui" --hidden-import="processors" --hidden-import="plugins" --hidden-import="sqlite3" --hidden-import="pyautogui" --hidden-import="pyperclip" --hidden-import="tkcalendar" --collect-all="imagehash" --exclude-module="matplotlib" --exclude-module="pandas" --exclude-module="scipy.stats" --exclude-module="notebook" --exclude-module="test" --exclude-module="setuptools" "app.py"
```

#### 📝 多行解析版 (了解細節)

```bash
pyinstaller --noconfirm --clean --windowed --onefile ^
 --upx-dir="." ^                         # 使用 UPX 壓縮 (需下載 upx.exe 放同目錄)
 --icon="icon.ico" ^                     # 設定圖示
 --add-data "plugins;plugins" ^          # 核心：將外掛資料夾完整打包，包含圖片素材
 --add-data "UnRAR.exe;." ^              # 核心：支援 RAR/CBR
 --hidden-import="gui" ^                 # 新增：確保掃描到 gui 套件
 --hidden-import="processors" ^          # 新增：確保掃描到 processors 套件
 --hidden-import="sqlite3" ^             # 新增：v16 核心改用 SQLite
 --hidden-import="pyautogui" ^           # 新增：EH 外掛依賴
 --hidden-import="pyperclip" ^           # 新增：EH 外掛依賴
 --hidden-import="tkcalendar" ^          # 新增：日期選擇器
 --collect-all="imagehash" ^             # 強制收集 imagehash 及其依賴 (如 pywt)
 --exclude-module="matplotlib" ^         # 排除肥大且未使用的庫
 --exclude-module="pandas" ^             # 排除肥大且未使用的庫
 --exclude-module="scipy.stats" ^        # 排除部分 scipy 模組 (imagehash 只需部分 scipy)
 --exclude-module="notebook" ^           # 排除 Jupyter 相關垃圾
 --exclude-module="setuptools" ^         # 排除開發工具
 "app.py"
```

---

### 三、常見問題與注意事項

1.  **關於設定檔 (`config.json`)**：
    *   新版程式 (`v16.0.2`) 具備強大的預設值生成能力。**不建議**打包 `config.json` 進去。
    *   讓程式在使用者電腦第一次執行時自動生成 `data/config.json`，這樣最乾淨，也不會覆蓋使用者的設定。

2.  **關於 `pyautogui` 與圖示識別**：
    *   指令中的 `--add-data "plugins;plugins"` 至關重要。它確保了 `plugins/eh_database_tools/assets/` 下的按鈕截圖被正確打包。如果沒加這行，自動化功能會失效。

3.  **防毒軟體誤報**：
    *   使用 `--onefile` (單檔案) + `UPX` 壓縮的 EXE 很容易被 Windows Defender 誤判為病毒。
    *   **解決方案**：如果只是自己用，沒關係。如果要發布給別人，建議拿掉 `--upx-dir="."` 參數，體積會變大一點，但被誤殺機率降低。

4.  **關於 `dependency_manager.py`**：
    *   這個檔案是用來檢查開發環境的。打包後的 EXE 不需要它運作（`app.py` 裡有判斷 `frozen` 狀態會跳過檢查），但 PyInstaller 會自動把它包進去，這無傷大雅。

