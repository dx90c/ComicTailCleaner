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

小打包

pyinstaller --noconfirm --clean --windowed --onefile --upx-dir="." --add-data "config.json;." --hidden-import=psutil --hidden-import=imagehash --hidden-import=send2trash --hidden-import=cv2 --hidden-import=numpy --hidden-import=scipy --hidden-import=pywt --collect-all=imagehash --collect-all=pywt --exclude-module=PyQt5 --exclude-module=PySide2 --exclude-module=wx --exclude-module=matplotlib --exclude-module=pandas --exclude-module=torch --exclude-module=tensorflow --exclude-module=scipy.stats --exclude-module=scipy.optimize --exclude-module=scipy.interpolate "ComicTailCleaner_v14.2.1.py"




---

### **v14.3.0 单档案打包指令**

**核心思想**：我们继承所有 v14.2.1 的优化指令，并再次确认所有必要的函式库都已被正确包含。

**最终建议指令：**

```bash
pyinstaller --noconfirm --clean --windowed --onefile --upx-dir="." ^
 --add-data "config.json;." ^
 --hidden-import="psutil" ^
 --hidden-import="imagehash" ^
 --hidden-import="send2trash" ^
 --hidden-import="cv2" ^
 --hidden-import="numpy" ^
 --hidden-import="scipy" ^
 --hidden-import="PyWavelets" ^
 --collect-all="imagehash" ^
 --collect-all="pywt" ^
 --exclude-module="PyQt5" ^
 --exclude-module="PySide2" ^
 --exclude-module="wx" ^
 --exclude-module="matplotlib" ^
 --exclude-module="pandas" ^
 --exclude-module="torch" ^
 --exclude-module="tensorflow" ^
 "ComicTailCleaner_v14.3.0.py"
```
*(注：我使用了 `^` 符号 (在 Windows 命令提示字元中) 来将长指令换行，这样更具可读性。您也可以直接复制成一行来执行。)*

---

### **指令解读与微调说明**

1.  **`--onefile`**: 生成单一的 `.exe` 档案，这是我们的核心目标。
2.  **`--windowed`**: 这是一个 GUI 应用程式，执行时不要显示黑色的命令提示字元视窗。
3.  **`--clean`**: 在打包前清理旧的建构快取。
4.  **`--upx-dir="."`**: 使用 UPX 压缩工具来减小最终 `.exe` 的体积。请确保您已经下载了 [UPX](https://github.com/upx/upx/releases) 并将其 `upx.exe` 档案放在与您的 `.py` 档案相同的目录下。
5.  **`--add-data "config.json;."`**: 将 `config.json` 档案包含进来，并放在最终执行档的根目录下。这是正确的。
6.  **`--hidden-import="..."`**: 这些是 `PyInstaller` 静态分析时可能找不到的“隐藏导入”。
    *   `psutil`, `imagehash`, `send2trash`, `cv2`, `numpy`, `scipy`：这些都是我们程式码中明确或间接使用到的函式库，继续包含它们是正确的。
    *   `--hidden-import="PyWavelets"`: 这是一个**关键的新增/确认**。`imagehash` 的 `wHash` 演算法依赖于 `PyWavelets` 函式库（通常简写为 `pywt`）。虽然 `PyInstaller` 的 `collect-all` 应该能找到它，但明确地将其作为隐藏导入可以增加打包的成功率。
7.  **`--collect-all="..."`**: 强制收集一个函式库的所有相关档案。
    *   `imagehash`, `pywt`：继续保留这两项是确保杂凑演算法正常工作的最佳实践。
8.  **`--exclude-module="..."`**: 明确排除我们**没有**使用到的大型 GUI 或科学计算函式库。这是减小最终档案体积的**最有效手段**。您提供的排除列表非常棒，我们完全继承。
    *   （补充说明）我们已经排除了 `scipy` 的大部分子模组，但因为 `imagehash` 的某些演算法可能会间接触发对 `scipy` 的依赖，所以我们依然在 `--hidden-import` 中保留了 `scipy` 的主模组，这是一个稳妥的做法。
9.  **`"ComicTailCleaner_v14.3.0.py"`**: 确保最后指定的是您**最新版本**的 Python 档案名称。


pyinstaller --noconfirm --clean --windowed --onefile --upx-dir="." --add-data "config.json;." --hidden-import="psutil" --hidden-import="imagehash" --hidden-import="send2trash" --hidden-import="cv2" --hidden-import="numpy" --hidden-import="scipy" --hidden-import="PyWavelets" --collect-all="imagehash" --collect-all="pywt" --exclude-module="PyQt5" --exclude-module="PySide2" --exclude-module="wx" --exclude-module="matplotlib" --exclude-module="pandas" --exclude-module="torch" --exclude-module="tensorflow" "ComicTailCleaner14.3.0.py"
