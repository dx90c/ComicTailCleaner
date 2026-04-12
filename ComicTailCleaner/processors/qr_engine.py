# ======================================================================
# 檔案：processors/qr_engine.py
# 目的：提供高準確率的 QR 偵測 Worker 函式 (可多進程呼叫)
# 版本：14.3.3 (v-MOD: 進階特徵提取開關 - 旋轉/前處理改為條件式)
# ======================================================================

from __future__ import annotations
import os
from typing import Tuple, Dict, Any, List, Optional

# ---- 第三方庫相依 ----
try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = None
    np = None

try:
    from PIL import Image, ImageOps, UnidentifiedImageError
except ImportError:
    Image = None
    ImageOps = None
    UnidentifiedImageError = Exception

try:
    import imagehash
except ImportError:
    imagehash = None

try:
    from pyzbar.pyzbar import decode as pyzbar_decode
    from pyzbar.pyzbar import ZBarSymbol
except ImportError:
    pyzbar_decode = None
    ZBarSymbol = None

# -------- 核心 QR 偵測函式 (保持 v14.3.2 的黃金標準) --------
def _detect_qr_on_image(img: "Image.Image") -> Optional[List]:
    """
    使用 PyZbar 與 OpenCV 強化濾波來尋找圖像中的狡猾 QR 碼。
    回傳：相容的 QR 邊界輪廓點 (List)
    """
    if cv2 is None or np is None or Image is None:
        return None

    img_cv = np.array(img.convert('RGB'))
    if img_cv.shape[0] == 0 or img_cv.shape[1] == 0:
        raise ValueError("圖像尺寸異常，無法進行影像處理")
    
    # [1] 高清首抽：直接以 PyZbar 灰階萃取 (高寬容度)
    if pyzbar_decode:
        try:
            gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)
            decoded = pyzbar_decode(gray, symbols=[ZBarSymbol.QRCODE])
            if decoded:
                poly = decoded[0].polygon
                if poly and len(poly) == 4:
                    return [[[p.x, p.y] for p in poly]]
        except Exception: pass
    
    # [2] OpenCV 原生：傳統解碼器兜底
    qr_detector = cv2.QRCodeDetector()
    retval, decoded_info, points, _ = qr_detector.detectAndDecodeMulti(img_cv)
    if retval and points is not None and len(points) > 0:
        return points.tolist()
        
    # [3] 重型裝甲：若前兩者錯失，則套用 CLAHE(局部對比強化) 與 自適應二值化(暴力去雜訊)
    if pyzbar_decode:
        try:
            gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(gray)
            
            decoded = pyzbar_decode(enhanced, symbols=[ZBarSymbol.QRCODE])
            if decoded:
                poly = decoded[0].polygon
                if poly and len(poly) == 4:
                    return [[[p.x, p.y] for p in poly]]
            
            # 暴力二值化 (專治網點與泛黃紙張)
            blurred = cv2.GaussianBlur(enhanced, (5, 5), 0)
            thresh = cv2.adaptiveThreshold(blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
            decoded = pyzbar_decode(thresh, symbols=[ZBarSymbol.QRCODE])
            if decoded:
                poly = decoded[0].polygon
                if poly and len(poly) == 4:
                    return [[[p.x, p.y] for p in poly]]
        except Exception: pass

    return None

def _fast_get_qr_regions(img_cv: np.ndarray, min_area=400, max_area_ratio=0.5) -> List[Tuple[int, int, int, int]]:
    """
    極速紋理濾鏡 (ROI Locator)：
    透過縮圖、Sobel 邊緣偵測與形態學(Morphological Close)
    在 20ms 內快速圈出「可能是 QR Code 的區塊」。
    有效過濾網點與全白頁面，避免後續進行秒級的全圖暴力掃描。
    回傳：[(x, y, w, h), ...]
    """
    if cv2 is None or np is None: return []
    
    gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY) if img_cv.ndim == 3 else img_cv
    # 統一縮放到 800 來找特徵 (太小會吃掉 QR，太大會拖慢速度)
    scale = 800 / max(gray.shape)
    if scale < 1.0:
        small = cv2.resize(gray, (0,0), fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    else:
        small = gray
        scale = 1.0

    # 找尋黑白強烈交替的邊緣
    grad_x = cv2.Sobel(small, cv2.CV_32F, 1, 0, ksize=3)
    grad_y = cv2.Sobel(small, cv2.CV_32F, 0, 1, ksize=3)
    grad = cv2.magnitude(grad_x, grad_y)
    grad = cv2.convertScaleAbs(grad)

    _, thresh = cv2.threshold(grad, 50, 255, cv2.THRESH_BINARY)
    
    # 閉運算將密集的 QR 點糊成一個實心的白色方塊
    # 縮小 Kernel 大小至 (11, 11)，避免將 QR Code 旁邊獨立的文字區塊也合併進來（造成長寬比失真）
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (11, 11))
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
    
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    rois = []
    total_area = small.shape[0] * small.shape[1]
    
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h
        # 只要不小於極限值 (太小不可能解碼)，就保留
        if area >= min_area:
            aspect_ratio = w / float(h)
            # 放寬的長寬比限制：避免長條漫畫對話框/黑線進入 PyZbar 拖慢速度
            # 由於我們縮小了形態學核心，QR 大機率會維持正方形，範圍設在 0.3 ~ 3.5 即可兼顧
            if 0.3 <= aspect_ratio <= 3.5:
                rois.append((int(x/scale), int(y/scale), int(w/scale), int(h/scale)))
            
    return rois

def _fast_is_colorful(img_cv: np.ndarray, color_threshold=15.0) -> bool:
    """
    極速彩色判定濾鏡 (Colorfulness Filter)：
    過濾掉「偽彩色」(如泛黃掃描頁面)，只保留真正含有廣告色彩的圖片。
    在 LAB 空間計算 a 和 b 通道的標準差。
    耗時: ~1ms。
    """
    if cv2 is None or np is None: return True
    # 縮小以進行極速運算
    scale = 400.0 / max(img_cv.shape[:2])
    if scale < 1.0:
        small = cv2.resize(img_cv, (0, 0), fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    else:
        small = img_cv
        
    lab = cv2.cvtColor(small, cv2.COLOR_RGB2LAB)
    l, a, b = cv2.split(lab)
    
    std_a = np.std(a)
    std_b = np.std(b)
    mean_a = np.mean(a)
    mean_b = np.mean(b)
    
    # 結合標準差與均值偏移量 (排除整體泛黃但顏色單一的紙張)
    color_val = np.sqrt(std_a**2 + std_b**2) + 0.3 * np.sqrt((mean_a - 128)**2 + (mean_b - 128)**2)
    return color_val > color_threshold

# -------- Worker：QR-Only (【核心修正】增加 EXIF 校正) --------
def _pool_worker_detect_qr_code(image_path: str, resize_size: int, enable_color_filter: bool = False) -> Tuple[str, Dict[str, Any]]:
    """
    多進程 Worker：偵測 QR Code，並返回包含 mtime 的完整元數據以供快取。
    """
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})
        
    try:
        st = os.stat(image_path)
        metadata = {'size': st.st_size, 'ctime': st.st_ctime, 'mtime': st.st_mtime}
    except OSError as e:
        return (image_path, {'error': f"無法獲取檔案狀態: {e}"})

    try:
        with Image.open(image_path) as pil_img:
            if not pil_img or pil_img.width == 0 or pil_img.height == 0:
                metadata.update({'error': f"圖片尺寸異常或無法讀取: {image_path}"})
                return (image_path, metadata)
            
            # v-MOD: EXIF 方向校正
            if hasattr(ImageOps, 'exif_transpose'):
                try: pil_img = ImageOps.exif_transpose(pil_img)
                except Exception: pass

            img_cv = np.array(pil_img.convert('RGB'))
            
            # 【優化：色彩濾鏡】
            if enable_color_filter:
                if not _fast_is_colorful(img_cv):
                    metadata['qr_points'] = None
                    return (image_path, metadata)
            
            # 策略 1: 縮放圖
            resized_img = pil_img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            points = _detect_qr_on_image(resized_img)
            
            # 【關鍵改善：ROI 快速濾鏡】
            # 取代原本「縮圖失敗就直接整張高畫質硬上」的愚公移山做法。
            # 先用 OpenCV 形態學在 20ms 內框出「可能是 QR」的座標。
            if not points:
                from processors.qr_engine import _fast_get_qr_regions
                img_cv = np.array(pil_img.convert('RGB'))
                rois = _fast_get_qr_regions(img_cv)
                
                # 若找到了嫌疑區域，只對那些小區塊進行深度掃描
                if rois:
                    for x, y, w, h in rois:
                        # 切割嫌疑區塊，四周多給 60 pixel 緩衝，確保 PyZbar 能讀到 QR 的 Quiet Zone（白邊）
                        pad = 60
                        crop_box = (
                            max(0, x - pad), 
                            max(0, y - pad), 
                            min(pil_img.width, x + w + pad), 
                            min(pil_img.height, y + h + pad)
                        )
                        crop_img = pil_img.crop(crop_box)
                        crop_points = _detect_qr_on_image(crop_img)
                        if crop_points:
                            # 補償座標回原圖位置
                            points = []
                            for p in crop_points[0]:
                                points.append([p[0] + crop_box[0], p[1] + crop_box[1]])
                            points = [points]
                            break
            metadata['qr_points'] = points
            
            # 若偵測到 QR，順便計算 pHash 以供後端分組去重
            if points and imagehash:
                try:
                    ph = imagehash.phash(pil_img, hash_size=8)
                    metadata['phash'] = str(ph)
                    from core_engine import FEATURE_PHASH, FEATURE_QR
                    metadata['features_at'] = (metadata.get('features_at', 0) | FEATURE_PHASH | FEATURE_QR)
                except Exception:
                    pass
            
            return (image_path, metadata)
            
    except UnidentifiedImageError:
        metadata.update({'error': f"無法識別圖片格式: {image_path}"})
        return (image_path, metadata)
    except Exception as e:
        metadata.update({'error': f"QR檢測失敗 {image_path}: {e}"})
        return (image_path, metadata)

# -------- Worker：pHash-only --------
def _pool_worker_process_image_phash_only(
    image_path: str,
    use_rotation: bool = False,
    use_preprocess: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """
    多進程 Worker：計算 pHash，並確保返回元數據。

    use_rotation:   True → 額外計算 90/180/270° 的旋轉指紋 (進階旋轉容差比對)
    use_preprocess: True → 先執行去白邊 + 對比度拉平 (進階圖像前處理加強)
    """
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})

    try:
        st = os.stat(image_path)
        metadata = {'size': st.st_size, 'ctime': st.st_ctime, 'mtime': st.st_mtime}
    except OSError as e:
        return (image_path, {'error': f"無法獲取檔案狀態: {e}"})

    try:
        with Image.open(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                metadata.update({'error': f"圖片尺寸異常或無法讀取: {image_path}"})
                return (image_path, metadata)
            
            img.draft('RGB', (512, 512))
            img = ImageOps.exif_transpose(img)
            
            # v-MOD: 進階前處理：去白邊 + 對比度拉平（預設關閉）
            if use_preprocess:
                from utils import _auto_crop_white_borders
                img = _auto_crop_white_borders(img)
                img = ImageOps.equalize(img.convert('L')).convert('RGB')
            
            metadata['width'] = img.width
            metadata['height'] = img.height
            
            # 基礎 pHash (0 度，永遠計算)
            metadata['phash'] = imagehash.phash(img, hash_size=8)
            
            def get_grid(image):
                tw, th = image.size
                if tw < 16 or th < 16: return []
                thw, thh = tw // 2, th // 2
                tboxes = [(0,0,thw,thh), (thw,0,tw,thh), (0,thh,thw,th), (thw,thh,tw,th)]
                return [str(imagehash.phash(image.crop(b), hash_size=8)) for b in tboxes]

            metadata['grid_phash'] = get_grid(img)
            
            # v-MOD: 旋轉指紋（use_rotation=True 才計算）
            if use_rotation:
                metadata['phash_rotations'] = {
                    '90':  str(imagehash.phash(img.rotate(90, expand=True), hash_size=8)),
                    '180': str(imagehash.phash(img.rotate(180, expand=True), hash_size=8)),
                    '270': str(imagehash.phash(img.rotate(270, expand=True), hash_size=8))
                }
                metadata['grid_rotations'] = {
                    '90':  get_grid(img.rotate(90, expand=True)),
                    '180': get_grid(img.rotate(180, expand=True)),
                    '270': get_grid(img.rotate(270, expand=True))
                }
            else:
                metadata['phash_rotations'] = {}
                metadata['grid_rotations'] = {}
                
            return (image_path, metadata)
    except Exception as e:
        metadata.update({'error': f"處理 pHash 失敗 {image_path}: {e}"})
        return (image_path, metadata)

# -------- Worker：pHash + QR --------
def _pool_worker_process_image_full(
    image_path: str,
    resize_size: int,
    use_rotation: bool = False,
    use_preprocess: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """
    多進程 Worker：混合模式，計算 pHash、偵測 QR，並返回完整元數據。

    use_rotation:   True → 額外計算旋轉指紋
    use_preprocess: True → 先執行去白邊 + 對比度拉平
    """
    if not os.path.exists(image_path):
        return (image_path, {'error': f"圖片檔案不存在: {image_path}"})

    try:
        st = os.stat(image_path)
        metadata = {'size': st.st_size, 'ctime': st.st_ctime, 'mtime': st.st_mtime}
    except OSError as e:
        return (image_path, {'error': f"無法獲取檔案狀態: {e}"})

    try:
        with Image.open(image_path) as img:
            if not img or img.width == 0 or img.height == 0:
                metadata.update({'error': f"圖片尺寸異常或無法讀取: {image_path}"})
                return (image_path, metadata)
            
            img.draft('RGB', (resize_size, resize_size))
            img = ImageOps.exif_transpose(img)
            
            # v-MOD: 進階前處理
            if use_preprocess:
                from utils import _auto_crop_white_borders
                img = _auto_crop_white_borders(img)
                img = ImageOps.equalize(img.convert('L')).convert('RGB')
            
            metadata['width'] = img.width
            metadata['height'] = img.height
            
            metadata['phash'] = imagehash.phash(img, hash_size=8)
            
            def get_grid(image):
                tw, th = image.size
                if tw < 16 or th < 16: return []
                thw, thh = tw // 2, th // 2
                tboxes = [(0,0,thw,thh), (thw,0,tw,thh), (0,thh,thw,th), (thw,thh,tw,th)]
                return [str(imagehash.phash(image.crop(b), hash_size=8)) for b in tboxes]

            metadata['grid_phash'] = get_grid(img)
            
            # v-MOD: 旋轉指紋
            if use_rotation:
                metadata['phash_rotations'] = {
                    '90':  str(imagehash.phash(img.rotate(90, expand=True), hash_size=8)),
                    '180': str(imagehash.phash(img.rotate(180, expand=True), hash_size=8)),
                    '270': str(imagehash.phash(img.rotate(270, expand=True), hash_size=8))
                }
                metadata['grid_rotations'] = {
                    '90':  get_grid(img.rotate(90, expand=True)),
                    '180': get_grid(img.rotate(180, expand=True)),
                    '270': get_grid(img.rotate(270, expand=True))
                }
            else:
                metadata['phash_rotations'] = {}
                metadata['grid_rotations'] = {}
            
            # QR Code 偵測
            resized_img = img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            qr_points_val = _detect_qr_on_image(resized_img)
            # 策略 2: ROI 原圖掃描 (避免全圖暴力掃描)
            if not qr_points_val:
                img_cv = np.array(img.convert('RGB'))
                rois = _fast_get_qr_regions(img_cv)
                if rois:
                    for x, y, w, h in rois:
                        pad = 60
                        crop_box = (
                            max(0, x - pad), max(0, y - pad), 
                            min(img.width, x + w + pad), min(img.height, y + h + pad)
                        )
                        crop_img = img.crop(crop_box)
                        crop_points = _detect_qr_on_image(crop_img)
                        if crop_points:
                            points = []
                            for p in crop_points[0]:
                                points.append([p[0] + crop_box[0], p[1] + crop_box[1]])
                            qr_points_val = [points]
                            break
                            
            metadata['qr_points'] = qr_points_val
                
        return (image_path, metadata)
    except UnidentifiedImageError:
        metadata.update({'error': f"無法識別圖片格式: {image_path}"})
        return (image_path, metadata)
    except Exception as e:
        metadata.update({'error': f"完整圖片處理失敗 {image_path}: {e}"})
        return (image_path, metadata)


# ======================================================================
# QR 結果後處理：依 pHash 相似度分組（相同廣告的多個副本歸為一組）
# ======================================================================

def group_qr_results_by_phash(
    flat_qr_list: List[tuple],
    file_data: dict,
    sim_threshold: float = 0.80,
) -> List[tuple]:
    """
    將「掃到 QR」的圖片依 pHash 相似度分組，返回與互相比對模式相容的
    (leader, child, val_str, tag) 四元組格式。

    參數
    ----
    flat_qr_list : [(path, path, val_str, tag), ...]
        掃描完畢後還未分組的原始 QR 清單。
    file_data : dict
        各圖片的 metadata dict（須含 'phash' key）。
    sim_threshold : float
        pHash 相似度門檻，預設 0.80。建議與使用者設定的 similarity_threshold 勾稽
        （例如若使用者設 90%，則傳入 0.80 作為 QR 分組的寬鬆版）。

    回傳
    ----
    分組後的四元組清單：
    - 單張圖片: (path, path, val_str, tag)
    - 組長:     (leader, leader, "QR 廣告 (共 N 張)", tag)
    - 子節點:   (leader, child,  val_str, tag)
    """
    if not imagehash or not flat_qr_list:
        return flat_qr_list

    HASH_BITS = 64  # imagehash phash(hash_size=8) → 64 bits

    def _coerce(h):
        if h is None:
            return None
        if isinstance(h, imagehash.ImageHash):
            return h
        try:
            return imagehash.hex_to_hash(str(h))
        except Exception:
            return None

    def _sim(h1, h2):
        """Hamming 距離轉換為 0~1 相似度"""
        dist = h1 - h2          # imagehash 的 __sub__ 回傳漢明距離(int)
        return 1.0 - dist / HASH_BITS

    # ── 分離有/無 pHash 的項目 ────────────────────────────────────
    items_with_hash: List[tuple] = []   # (path, val_str, tag, hash_obj)
    items_without_hash: List[tuple] = []

    for path, _dup_path, val_str, tag in flat_qr_list:
        norm = path.lower().replace('\\', '/')
        raw_h = None
        entry = file_data.get(path) or file_data.get(norm)
        
        if entry and entry.get('phash'):
            raw_h = entry['phash']
        else:
            # ── 現場補算保險 ──
            # 如果快取裡沒 hash (舊資料)，現場補算，避免無法分組
            try:
                with Image.open(path) as img:
                    raw_h = str(imagehash.phash(img))
                    # 順便存回 entry 裡，下次就不用補算了
                    if entry is not None:
                        entry['phash'] = raw_h
            except Exception:
                raw_h = None

        h = _coerce(raw_h)
        if h:
            items_with_hash.append((path, val_str, tag, h))
        else:
            items_without_hash.append((path, path, val_str, tag))


    # ── Union-Find 分組 ───────────────────────────────────────────
    n = len(items_with_hash)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        pi, pj = find(i), find(j)
        if pi != pj:
            parent[pi] = pj

    for i in range(n):
        for j in range(i + 1, n):
            h1, h2 = items_with_hash[i][3], items_with_hash[j][3]
            if _sim(h1, h2) >= sim_threshold:
                union(i, j)

    # ── 按組長收集成員 ─────────────────────────────────────────────
    from collections import defaultdict
    groups: dict = defaultdict(list)
    for i, (path, val_str, tag, _) in enumerate(items_with_hash):
        groups[find(i)].append((path, val_str, tag))

    grouped: List[tuple] = []
    for _root, members in groups.items():
        if len(members) == 1:
            path, val_str, tag = members[0]
            grouped.append((path, path, val_str, tag))
        else:
            leader_path = members[0][0]
            count = len(members)
            grouped.append((leader_path, leader_path, f"QR 廣告 (共 {count} 張)", members[0][2]))
            for path, val_str, tag in members[1:]:
                grouped.append((leader_path, path, val_str, tag))

    grouped.extend(items_without_hash)

    import logging
    logging.getLogger(__name__).info(
        f"[QR 分組] 門檻={sim_threshold:.0%} · 原始 {len(flat_qr_list)} 筆 → "
        f"{len({r[0] for r in grouped})} 組"
    )
    return grouped