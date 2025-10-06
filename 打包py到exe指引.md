好的，完全理解。我們需要一份更新到 `v15.0.0` 的、專業且清晰的打包指引文件，以反映專案模組化後的新結構 (`src` 目錄) 和新的依賴（外掛系統）。

我已經為您仿造 `v14.x` 的風格，撰寫了一份全新的 `打包py到exe指引.md`。這份文件不僅更新了指令，更重要的是**更新了整個操作流程**，以適應您現在更專業的專案結構。

您可以直接用以下內容覆蓋掉舊的 `打包py到exe指引.md` 檔案。

---

### **ComicTailCleaner 專案建置說明 (v15.0.0)**

**文件目的**: 本文件旨在提供將 `ComicTailCleaner` v15.0.0 Python 專案打包成 Windows 可執行檔 (.exe) 的標準化流程。本文檔中的命令已針對新的模組化架構 (`app.py` 入口) 和外掛系統 (`plugins` 資料夾) 進行了特別調校。

#### **一、前置準備**

在執行打包命令前，請務必確保滿足以下條件：

1.  **安裝 PyInstaller**: 您的 Python 環境中已安裝 `pyinstaller`。
    ```cmd
    pip install pyinstaller
    ```
2.  **安裝所有依賴**: 專案所需的所有 Python 套件皆已安裝 (`Pillow`, `imagehash`, `opencv-python`, `numpy`, `scipy`, `send2trash`, `psutil`, `rarfile` 等)。

3.  **準備必要檔案 (重要)**:
    與舊版不同，所有必要的原始碼與資源檔案現在都應統一放置在 `src` 資料夾內。打包前，請確認您的 `src` 資料夾結構如下：
    ```
    src/
    ├── app.py                 # 主程式入口
    ├── gui.py
    ├── core_engine.py
    ├── config.py
    ├── utils.py
    ├── archive_handler.py
    ├── plugins/               # 完整的外掛資料夾
    │   └── ...
    ├── processors/            # 完整的處理器資料夾
    │   └── ...
    ├── config.json            # 乾淨的預設設定檔
    ├── UnRAR.exe              # RAR 支援工具
    ├── icon.ico               # (建議) 應用程式圖示
    └── upx.exe                # (建議) UPX 壓縮工具
    ```

#### **二、打包命令**

本版本推薦使用**單檔案模式 (One-File)**，以方便使用者分發。

##### **建議指令 (單檔案模式)**

```cmd
pyinstaller --noconfirm --clean --windowed --onefile --upx-dir="." --icon="icon.ico" --add-data "plugins;plugins" --add-data "config.json;." --add-data "UnRAR.exe;." --hidden-import="pkg_resources.py2_warn" --hidden-import="psutil" --hidden-import="send2trash" --hidden-import="imagehash" --hidden-import="cv2" --hidden-import="numpy" --hidden-import="scipy" --hidden-import="pywt" --hidden-import="rarfile" --collect-all="imagehash" --collect-all="pywt" --exclude-module="PyQt5" --exclude-module="PySide2" --exclude-module="wx" --exclude-module="matplotlib" --exclude-module="pandas" --exclude-module="torch" --exclude-module="tensorflow" "app.py"
```

**多行版本 (方便閱讀)**
```bash
pyinstaller --noconfirm --clean --windowed --onefile ^
 --upx-dir="." ^
 --icon="icon.ico" ^
 --add-data "plugins;plugins" ^
 --add-data "config.json;." ^
 --add-data "UnRAR.exe;." ^
 --hidden-import="pkg_resources.py2_warn" ^
 --hidden-import="psutil" ^
 --hidden-import="send2trash" ^
 --hidden-import="imagehash" ^
 --hidden-import="cv2" ^
 --hidden-import="numpy" ^
 --hidden-import="scipy" ^
 --hidden-import="pywt" ^
 --hidden-import="rarfile" ^
 --collect-all="imagehash" ^
 --collect-all="pywt" ^
 --exclude-module="PyQt5" ^
 --exclude-module="PySide2" ^
 --exclude-module="wx" ^
 --exclude-module="matplotlib" ^
 --exclude-module="pandas" ^
 --exclude-module="torch" ^
 --exclude-module="tensorflow" ^
 "app.py"
```

---

#### **三、關鍵參數詳解**

*   `--onefile`: 將所有內容打包成一個獨立的 EXE 檔案。
*   `--windowed`: 指定這是一個圖形化介面 (GUI) 程式，執行時不顯示命令列視窗。
*   `--upx-dir="."`: 使用位於當前目錄的 UPX 工具壓縮最終的 EXE 檔案以減小體積。
*   `--icon="icon.ico"`: 為產生的 EXE 檔案指定圖示。
*   `--add-data "plugins;plugins"`: **(關鍵新增)** 將整個 `plugins` 資料夾及其所有內容打包進去。這是確保外掛功能在 `.exe` 中正常運作的核心。
*   `--add-data "config.json;."`: 將預設的 `config.json` 檔案打包進去。
*   `--add-data "UnRAR.exe;."`: 將 `UnRAR.exe` 工具打包進去，以支援 RAR/CBR 格式。
*   `--hidden-import=...`: 手動告知 PyInstaller 有哪些它未能自動偵測到的「隱藏導入」模組。
*   `--collect-all=...`: 以最強力的方式，完整收集一個模組所有相關的子模組、數據檔案等。
*   `--exclude-module=...`: 明確排除我們未使用的大型函式庫，這是**減小檔案體積的最有效手段**。
*   `"app.py"`: 指定 `app.py` 作為程式的入口點。

#### **四、使用流程**

1.  **準備檔案**: 按照「一、前置準備」中的說明，將所有必要的檔案和資料夾整理到 `src` 目錄下。
2.  **打開終端機**: 在檔案總管中，進入 `src` 資料夾。在路徑欄輸入 `cmd` 並按 Enter，即可在當前目錄打開命令提示字元。
3.  **執行命令**: 複製「二、打包命令」中的**單行版本**指令，貼到命令提示字元視窗中，然後按 Enter 執行。
4.  **獲取成品**: 打包成功後，在 `src` 資料夾內會出現一個 `dist` 資料夾。您需要的 `app.exe` 檔案就在其中。

---
**備註**: 舊版指引中的「標準化命名」步驟已不再需要，因為我們現在有了穩定且統一的 `app.py` 入口點。