# ComicTailCleaner

# E-Download 漫畫尾頁廣告剔除工具 v11.8

這是一款用於自動檢測並剔除漫畫尾頁廣告的工具，支援以下功能：

- 比對廣告圖庫，找出相似圖片並列出。
- 可選擇互相比對漫畫內部重複頁面。
- 支援 QR Code 掃描，檢測是否包含導向連結的廣告圖。
- 圖像哈希比對技術，使用 `imagehash` 判斷相似度。
- GUI 操作介面，支援圖片預覽與勾選刪除。
- 可掃描所有資料夾或僅掃描最後 N 頁。

## 安裝需求

請使用 Python 3.7 以上版本。需安裝以下第三方套件：

```bash
pip install pillow imagehash opencv-python pyzbar
