# ComicTailCleaner 專案建置說明 (BUILD_INSTRUCTIONS.md)

本文件記錄了如何使用 PyInstaller 將 `ComicTailCleaner` Python 腳本打包成 Windows 可執行檔 (.exe) 的詳細步驟與命令。這些命令是經過多次測試後，能夠成功處理所有複雜依賴（如 `imagehash`, `scipy`）的最終版本。

## 前置準備

在執行打包命令前，請確保滿足以下條件：

1.  您的 Python 環境中已安裝 `pyinstaller`。
2.  專案所需的所有 Python 套件皆已安裝 (例如：`Pillow`, `imagehash`, `opencv-python`, `numpy`, `scipy` 等)。
3.  專案根目錄下存在一個乾淨的、不包含任何個人路徑的 `config.json` 預設設定檔。

## 打包命令

根據不同的需求，我們有兩種打包模式可供選擇。

### 模式一：單目錄模式 (One-Directory)
此模式建議在 **開發或除錯** 階段使用。它會產生一個包含主程式 EXE 及所有依賴檔案的資料夾。

*   **優點**：啟動速度快。
*   **缺點**：發布時需提供整個資料夾。

```cmd
pyinstaller --noconfirm --clean --windowed --add-data "config.json;." --hidden-import=Pillow --hidden-import=imagehash --hidden-import=send2trash --hidden-import=cv2 --hidden-import=numpy --hidden-import=scipy --hidden-import=six --hidden-import=pywt --copy-metadata=Pillow --copy-metadata=imagehash --copy-metadata=send2trash --copy-metadata=opencv-python --copy-metadata=numpy --copy-metadata=scipy --copy-metadata=six --copy-metadata=PyWavelets --collect-all=imagehash --collect-all=scipy "ComicTailCleaner_v12.6.3.py"
```

### 模式二：單檔案模式 (One-File)
此模式建議在 **正式發布版本** 時使用。它會將所有內容打包成一個獨立的 EXE 檔案，方便使用者下載與執行。

*   **優點**：乾淨俐落，便於分享。
*   **缺點**：首次啟動速度較慢（因需在背景解壓縮）。

```cmd
pyinstaller --noconfirm --clean --windowed --onefile --add-data "config.json;." --hidden-import=Pillow --hidden-import=imagehash --hidden-import=send2trash --hidden-import=cv2 --hidden-import=numpy --hidden-import=scipy --hidden-import=six --hidden-import=pywt --copy-metadata=Pillow --copy-metadata=imagehash --copy-metadata=send2trash --copy-metadata=opencv-python --copy-metadata=numpy --copy-metadata=scipy --copy-metadata=six --copy-metadata=PyWavelets --collect-all=imagehash --collect-all=scipy "ComicTailCleaner_v12.6.3.py"
```

---

## 關鍵參數詳解

*   `--onefile`：將所有內容打包成一個獨立的 EXE 檔案。
*   `--windowed`：指定這是一個圖形化介面 (GUI) 程式，執行時不顯示黑色的命令列視窗。
*   `--add-data "config.json;."`：將 `config.json` 檔案打包進去。分號前的 `config.json` 是來源檔案，分號後的 `.` 代表將其放置在 EXE 執行時的根目錄。
*   `--hidden-import=...`：手動告知 PyInstaller 有哪些它未能自動偵測到的「隱藏導入」模組。主要用於打包模組的 **程式碼**。
*   `--copy-metadata=...`：手動複製指定套件的 **元數據 (Metadata)**。這對於需要進行版本校驗的函式庫 (如 `pkg_resources`) 至關重要。
*   `--collect-all=...`：以最強力的方式，完整收集一個模組所有相關的子模組、數據檔案、二進制檔等。特別適用於像 `scipy` 這類結構複雜的函式庫。
*   `--clean`：在每次建置前，自動清除 PyInstaller 的快取及臨時檔案，以確保一個乾淨的打包環境。
*   `"ComicTailCleaner_v12.6.3.py"`：您的主程式腳本檔名。未來發布新版本時，請記得更新此處的檔名。

## 使用流程

1.  在命令提示字元 (CMD) 或終端機中，切換到本專案的根目錄。
2.  確認 `config.json` 檔案已存在於根目錄。
3.  複製您需要的模式（單目錄或單檔案）對應的完整命令。
4.  在命令提示字元中貼上並執行。
5.  打包成功後，在專案根目錄下會出現一個 `dist` 資料夾，您需要的檔案就在其中。
