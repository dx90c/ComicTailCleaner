# ComicTailCleaner (漫畫尾頁廣告清理)

[![Latest Release](https://img.shields.io/github/v/release/dx90c/ComicTailCleaner?display_name=tag)](https://github.com/dx90c/ComicTailCleaner/releases/latest)

一個專為漫畫愛好者設計的實用工具，旨在自動化清理漫畫資料夾末尾的廣告頁或重複頁面。本工具透過高效的感知雜湊演算法 (Perceptual Hashing) 與 QR Code 偵測，能精準找出內容相似或包含廣告的圖片，一鍵清理，提升您的閱讀體驗。

<p align="center">
  <img src="https://raw.githubusercontent.com/dx90c/ComicTailCleaner/main/screenshot.png" alt="程式主介面-掃描結果" width="80%">
</p>

<details>
  <summary><strong>► 點此展開／收合：查看「設定」介面</strong></summary>
  <br>
  <p align="center">
    <img src="https://raw.githubusercontent.com/dx90c/ComicTailCleaner/main/screenshot1.png" alt="設定介面" width="50%">
  </p>
</details>
---

## ✨ 主要功能

*   **多種比對模式**：
    *   **互相比對**：掃描指定資料夾內的圖片，找出彼此內容相似的重複頁面。
    *   **廣告比對**：與您預先建立的「廣告樣本庫」進行比對，精準刪除已知的廣告頁。
    *   **QR Code 偵測**：快速掃描圖片是否包含 QR Code，並可結合廣告庫進行混合模式掃描，效率與準確性兼具。
*   **高度可自訂化**：
    *   可自由調整圖片相似度的判斷閾值 (預設 98%)。
    *   可設定從每個資料夾末尾提取的圖片數量，專注於清理尾頁。
    *   支援時間篩選，只處理特定日期範圍內建立的資料夾。
    *   支援排除特定資料夾，避免誤刪。
*   **直覺的圖形化介面 (GUI)**：
    *   提供雙欄預覽，方便即時比對相似圖片的內容。
    *   清晰的列表顯示所有找到的待處理項目。
    *   支援全選、建議選取、反選等快捷操作。
*   **安全可靠**：
    *   刪除的檔案預設會移至 **系統回收桶**，給您反悔的機會。
    *   內建快取機制，大幅提升重複掃描的速度。

---

## 🚀 如何使用 (給一般使用者)

本程式無需安裝 Python，下載即可執行。

1.  **前往發行頁面**：
    *   點擊這裡前往 [**最新發行 (Releases) 頁面**](https://github.com/dx90c/ComicTailCleaner/releases/latest)。

2.  **下載執行檔**：
    *   在頁面下方的 "Assets" 區塊中，下載名為 `ComicTailCleaner_vX.X.X.exe` 的檔案。

3.  **執行程式**：
    *   下載後直接點兩下執行 `.exe` 檔案即可。
    *   程式第一次儲存設定時，會在 `.exe` 檔案旁邊自動建立一個 `config.json` 檔案。

---

## 🛠️ 開發者指南 (For Developers)

如果您想從原始碼執行或自行修改、建置本專案，請參考以下步驟。

### 1. 從原始碼執行

**前置需求**
*   Python 3.10 或更新版本
*   Git

**步驟**
```bash
# 1. 複製專案原始碼
git clone https://github.com/dx90c/ComicTailCleaner.git
cd ComicTailCleaner

# 2. 安裝所有必要的 Python 依賴套件
# (建議先建立一個 requirements.txt 檔案)
pip install -r requirements.txt

# 3. 執行主程式
python ComicTailCleaner_v12.6.3.py
```

### 2. 建立依賴清單 (requirements.txt)
為了方便管理，建議在專案根目錄建立一個 `requirements.txt` 檔案。您可以使用以下命令自動生成：
```bash
pip freeze > requirements.txt
```
它應該包含以下主要依賴：
*   `Pillow`
*   `imagehash`
*   `send2trash`
*   `opencv-python`
*   `numpy`
*   `scipy`
*   `PyWavelets`

### 3. 從原始碼建置 (打包成 .exe)

本專案的打包過程較為複雜，因為依賴了 `scipy` 等大型函式庫。所有詳細的步驟、參數說明與最終可用的命令稿，都已記錄在專門的指引檔案中。

**請參考：[打包py到exe指引.md](./打包py到exe指引.md)**

---

## 📄 授權 (License)

本專案採用 [MIT License](LICENSE) 進行授權。
