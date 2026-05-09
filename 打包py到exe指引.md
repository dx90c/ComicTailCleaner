# RELEASE-PACK-02 v17.1.0 PyInstaller 打包指引

## 1. 目標

本文件是 ComicTailCleaner v17.1.0 正式發布版專用的 exe 打包指引。

v17 打包採兩階段：

1. 先用 `scratch/build_release_package.py --execute` 複製一份乾淨 source。
2. 再在乾淨 source 目錄中執行 PyInstaller。

這樣可避免把開發資料、EH/EMM DB、cache、LOG、指紋、安全網腳本與 UpdateRecords 打進發布版。

## 2. 前置準備

建議使用乾淨虛擬環境：

```cmd
python -m venv .venv_pack
.venv_pack\Scripts\activate
python -m pip install --upgrade pip
```

安裝 v17 建議依賴：

```cmd
pip install pyinstaller pillow imagehash opencv-python numpy send2trash psutil pyautogui pyperclip keyboard tkcalendar tqdm nanoid rarfile pyzbar
```

注意：

- 不建議安裝 `pandas`、`matplotlib`、`notebook` 等未使用的大型套件。
- 若 `pyzbar` 需要額外 zbar runtime，打包後 QR 進階解碼可能需要另行驗證；目前 QR 仍有 OpenCV fallback。

## 3. 一鍵腳本

新增 v17 專用腳本：

```text
scratch\build_pyinstaller_exe.py
```

預設 dry-run，只顯示會執行的流程：

```cmd
python scratch\build_pyinstaller_exe.py
```

實際建立乾淨 source 並打包 exe：

```cmd
python scratch\build_pyinstaller_exe.py --execute
```

預設輸出檔名會自動加上分鐘時間戳，格式如下：

```text
ComicTailCleaner_v17_1_0_YYYYMMDD_HHMM.exe
```

若需要臨時測試名，仍可手動指定：

```cmd
python scratch\build_pyinstaller_exe.py --execute --app-name ComicTailCleaner_v17_1_0_test
```

輸出流程：

```text
dist_clean\ComicTailCleaner_clean_YYYYMMDD_HHMMSS\
dist_clean\pyinstaller_work\
dist_clean\pyinstaller_dist\
```

最終 exe 預期在：

```text
dist_clean\pyinstaller_dist\ComicTailCleaner_v17_1_0_YYYYMMDD_HHMM.exe
```

## 4. PyInstaller 核心規則

v17 必須包含：

- `plugins/`
- `data/bin/Everything32.dll`
- `data/bin/Everything64.dll`
- `data/eh_database_tools/EhTagTranslation/`
- `UnRAR.exe`

v17 必須排除：

- `data/configs/config.json`
- `data/caches/`
- `data/logs/`
- `data/eh_database_tools/database.sqlite`
- `data/eh_database_tools/metadata.sqlite`
- `data/eh_database_tools/Backups/`
- `scratch/`
- `UpdateRecords/`

## 5. 手動 PyInstaller 參考命令

若不使用腳本，可先進入乾淨 source 目錄，再執行：

```cmd
pyinstaller --noconfirm --clean --windowed --onefile ^
 --name "ComicTailCleaner_v17_1_0_YYYYMMDD_HHMM" ^
 --add-data "plugins;plugins" ^
 --add-data "data\eh_database_tools\EhTagTranslation;data\eh_database_tools\EhTagTranslation" ^
 --add-binary "UnRAR.exe;." ^
 --add-binary "data\bin\Everything32.dll;data\bin" ^
 --add-binary "data\bin\Everything64.dll;data\bin" ^
 --hidden-import "gui" ^
 --hidden-import "processors" ^
 --hidden-import "plugins" ^
 --hidden-import "sqlite3" ^
 --hidden-import "tkcalendar" ^
 --hidden-import "tqdm" ^
 --hidden-import "nanoid" ^
 --hidden-import "pyautogui" ^
 --hidden-import "pyperclip" ^
 --hidden-import "keyboard" ^
 --hidden-import "psutil" ^
 --hidden-import "websocket" ^
 --hidden-import "rarfile" ^
 --hidden-import "cv2" ^
 --hidden-import "numpy" ^
 --hidden-import "pyzbar.pyzbar" ^
 --collect-all "imagehash" ^
 --collect-binaries "pyzbar" ^
 --exclude-module "matplotlib" ^
 --exclude-module "pandas" ^
 --exclude-module "notebook" ^
 --exclude-module "IPython" ^
 --exclude-module "pytest" ^
 --exclude-module "torch" ^
 --exclude-module "torchvision" ^
 --exclude-module "torchaudio" ^
 --exclude-module "PyQt5" ^
 --exclude-module "PyQt6" ^
 --exclude-module "PySide2" ^
 --exclude-module "PySide6" ^
 --exclude-module "sklearn" ^
 --exclude-module "tensorflow" ^
 app.py
```

注意：`scipy` 不列入正式版預設排除。`imagehash.phash/whash` 可能依賴 `scipy`，若要測試極限瘦身，只能使用 `--exclude-scipy` 另外打測試版，並完成三比、QR、相似卷宗與指紋巡檢驗證後再評估。

## 6. 驗證方式

打包前：

```text
一鍵指紋巡檢_L123.bat
```

打包後：

1. 將 exe 複製到乾淨測試資料夾。
2. 啟動 exe。
3. 確認 `data/configs/`、`data/caches/`、`data/logs/` 會在 exe 同層產生。
4. 確認不會帶入開發機的 `config.json`、EH DB、cache 或 LOG。
5. 開啟設定頁，確認外掛頁可載入。
6. 做一次小範圍掃描或 sandbox 測試。

## 7. 與 v16 指引差異

v16 指引直接在專案目錄打包；v17 改為先產生乾淨 source，降低誤包私人資料與診斷檔的風險。

v17 額外注意：

- Everything DLL 位於 `data/bin/`。
- EhTag 翻譯資料位於 `data/eh_database_tools/EhTagTranslation/`。
- EH/EMM DB 絕不可打包。
- 指紋巡檢、安全網與 UpdateRecords 只屬於開發目錄，不屬於發布版。

## 8. 更新紀錄

- **2026-05-10 01:35 (Codex)**:
    - 複核 UPX 瘦身效果有限的原因：CFG 會讓 PyInstaller 跳過大量 DLL，真正肥大更可能來自環境中未使用的大型套件被 hook 掃入。
    - `scratch/build_pyinstaller_exe.py` 新增低風險排除：`torch`、`torchvision`、`torchaudio`、`PyQt5/PyQt6`、`PySide2/PySide6`、`sklearn`、`tensorflow`。
    - `scipy` 保留於正式版，另提供 `--exclude-scipy` 實驗參數，避免破壞 `imagehash.phash/whash` 導致三比、QR 或相似卷宗退化。
- **2026-05-09 21:28 (Codex)**:
    - 複核 EXE 版 EH 外掛無法啟動 EMM 的修補方向：保留 `websocket` hidden-import、EMM subprocess cwd、環境繼承與 `emm_subprocess.log` 診斷。
    - 補強 `processor.py`：缺少 `websocket` 等依賴時，進度回報不會因 `update_progress` 尚未建立而二次崩潰；EMM subprocess 使用 `os.environ.copy()`，並在 `Popen` 後關閉父程序端 log handle。
- **2026-05-09 21:07 (Codex)**:
    - 更新 `scratch/build_pyinstaller_exe.py` 預設 exe 命名規則：BAT 不指定 `--app-name` 時，會輸出 `ComicTailCleaner_v17_1_0_YYYYMMDD_HHMM.exe`。
    - `pyinstaller_build_report_*.txt/.json` 會同步記錄實際 `app_name`，方便對照是哪一次打包產物。
- **2026-05-09 21:10 (Codex)**:
    - 將 `config.py` 的公開版本號固定為 `APP_VERSION = "17.1.0"`，正式版 UI 與設定視窗不再顯示開發資料夾後綴 `502` 或重構標籤。
    - 注意：已產出的舊 exe 不會自動改名或改顯示版本；需重新執行 `打包_v17.1.0_乾淨版EXE_execute.bat` 產生新版 exe。
- **2026-05-09 20:53 (Antigravity)**: 
    - 修正 `config.py`：引入 `ASSET_DIR` 與 `DATA_DIR` 區分邏輯，確保打包後 DLL 資源能從 `_MEIPASS` 正確釋放。
    - 修正 `archive_handler.py`：更新 `UnRAR.exe` 偵測路徑，支援單檔 `.exe` 內部資源釋放。
    - 驗收：使用者確認生成之 `.exe` 功能正常。
