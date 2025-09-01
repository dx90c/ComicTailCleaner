### **ComicTailCleaner 專案建置說明 (v14.2.1)**

**文件目的**: 本文件旨在提供將 `ComicTailCleaner` v14.2.1 Python 腳本打包成 Windows 可執行檔 (.exe) 的標準化流程。本文檔中的命令經過特別調校，以確保新的 **LSH 雙哈希引擎**及其所有複雜依賴（如 `scipy`, `pywt`）能夠被成功打包。

#### **一、前置準備**

在執行打包命令前，請務必確保滿足以下條件：

1.  **安裝 PyInstaller**: 您的 Python 環境中已安裝 `pyinstaller`。
    ```cmd
    pip install pyinstaller
    ```
2.  **安裝所有依賴**: 專案所需的所有 Python 套件皆已安裝 (`Pillow`, `imagehash`, `opencv-python`, `numpy`, `scipy`, `send2trash`, `psutil` 等)。
3.  **準備必要檔案**:
    *   **`config.json`**: 專案根目錄下必須存在一個乾淨的、不包含任何個人路徑的 `config.json` 預設設定檔。
    *   **(可選但建議)** **`icon.ico`**: 在專案根目錄下準備一個圖示檔案 (例如 `icon.ico`)，用於生成帶有圖示的 EXE 檔案。

#### **二、標準化命名 (重要步驟)**

為確保打包命令的一致性和可重用性，請在執行打包前，將您當前版本的腳本**複製並重新命名**。

*   將 `ComicTailCleaner_v14.2.1.py` **複製**一份，並命名為 `ComicTailCleaner.py`。

後續所有命令都將基於 `ComicTailCleaner.py` 這個標準檔名。

#### **三、打包命令**

根據您的發布需求，選擇以下任一模式。

##### **模式一：單目錄模式 (One-Directory)**
此模式會產生一個包含主程式 EXE 及所有依賴檔案的資料夾，**啟動速度較快**。

```cmd
pyinstaller --noconfirm --clean --windowed --add-data "config.json;." --hidden-import=psutil --copy-metadata=psutil --hidden-import=Pillow --hidden-import=imagehash --hidden-import=send2trash --hidden-import=cv2 --hidden-import=numpy --hidden-import=scipy --hidden-import=six --hidden-import=pywt --copy-metadata=Pillow --copy-metadata=imagehash --copy-metadata=send2trash --copy-metadata=opencv-python --copy-metadata=numpy --copy-metadata=scipy --copy-metadata=six --copy-metadata=PyWavelets --collect-all=imagehash --collect-all=scipy "ComicTailCleaner.py"
```

##### **模式二：單檔案模式 (One-File)**
此模式會將所有內容打包成一個獨立的 EXE 檔案，**便於分享**，但首次啟動較慢。

```cmd
pyinstaller --noconfirm --clean --windowed --onefile --add-data "config.json;." --hidden-import=psutil --copy-metadata=psutil --hidden-import=Pillow --hidden-import=imagehash --hidden-import=send2trash --hidden-import=cv2 --hidden-import=numpy --hidden-import=scipy --hidden-import=six --hidden-import=pywt --copy-metadata=Pillow --copy-metadata=imagehash --copy-metadata=send2trash --copy-metadata=opencv-python --copy-metadata=numpy --copy-metadata=scipy --copy-metadata=six --copy-metadata=PyWavelets --collect-all=imagehash --collect-all=scipy "ComicTailCleaner.py"
```

---

#### **四、關鍵參數詳解**

*   `--onefile`: 將所有內容打包成一個獨立的 EXE 檔案。
*   `--windowed`: 指定這是一個圖形化介面 (GUI) 程式，執行時不顯示黑色的命令列視窗。
*   `--icon="icon.ico"`: **(建議)** 為產生的 EXE 檔案指定圖示。
*   `--add-data "config.json;."`: **(關鍵修正)** 將 `config.json` 檔案打包進去。分號前的 `config.json` 是來源檔案，分號後的 `.` 代表將其放置在 EXE 執行時的根目錄。
*   `--hidden-import=...`: 手動告知 PyInstaller 有哪些它未能自動偵測到的「隱藏導入」模組。這對於確保所有**程式碼**都被打包至關重要。
*   `--copy-metadata=...`: 手動複製指定套件的**元數據 (Metadata)**。這對於需要進行版本校驗的函式庫 (如 `pkg_resources`) 尤其重要。
*   `--collect-all=...`: 以最強力的方式，完整收集一個模組所有相關的子模組、數據檔案、二進制檔等。特別適用於像 `scipy` 和 `imagehash` 這類結構複雜的函式庫。
*   `--clean`: 在每次建置前，自動清除 PyInstaller 的快取及臨時檔案，確保打包環境的純淨。
*   `"ComicTailCleaner.py"`: 您的主程式腳本檔名。

#### **五、使用流程**

1.  在命令提示字元 (CMD) 或終端機中，**切換到本專案的根目錄**。
2.  **確認必要檔案**: 確保 `config.json` 和 (可選的) `icon.ico` 已存在於根目錄。
3.  **執行標準化命名**: 將 `ComicTailCleaner_v14.2.1.py` 複製為 `ComicTailCleaner.py`。
4.  **複製並執行命令**: 選擇您需要的模式，複製其對應的完整命令，並在終端機中貼上執行。
5.  **獲取成品**: 打包成功後，在專案根目錄下會出現一個 `dist` 資料夾。您需要的 EXE 檔案（或資料夾）就在其中。