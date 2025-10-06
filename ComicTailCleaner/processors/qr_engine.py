# ======================================================================
# 檔案：processors/qr_engine.py
# 目的：提供高準確率的 QR 偵測 Worker 函式 (可多進程呼叫)
# 版本：14.3.2+++ EXIF (最終修正：加入 EXIF 方向校正，解決旋轉圖片的誤判問題)
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

# -------- 核心 QR 偵測函式 (保持 v14.3.2 的黃金標準) --------
def _detect_qr_on_image(img: "Image.Image") -> Optional[List]:
    """
    使用 OpenCV 偵測圖片中的 QR Code。
    成功標準：只要成功偵測到位置(points)，就視為命中。
    """
    if cv2 is None or np is None or Image is None:
        return None

    img_cv = np.array(img.convert('RGB'))
    if img_cv.shape[0] == 0 or img_cv.shape[1] == 0:
        raise ValueError("圖像尺寸異常，無法進行 OpenCV 處理")
    
    qr_detector = cv2.QRCodeDetector()
    retval, decoded_info, points, _ = qr_detector.detectAndDecodeMulti(img_cv)
    
    if retval and points is not None and len(points) > 0:
        return points.tolist()
        
    return None

# -------- Worker：QR-Only (【核心修正】增加 EXIF 校正) --------
def _pool_worker_detect_qr_code(image_path: str, resize_size: int) -> Tuple[str, Dict[str, Any]]:
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
            
            # 【關鍵修正】在所有操作前，先校正圖片方向！
            pil_img = ImageOps.exif_transpose(pil_img)
            
            # 策略 1: 縮放圖
            resized_img = pil_img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            points = _detect_qr_on_image(resized_img)
            
            # 策略 2: 原圖
            if not points:
                points = _detect_qr_on_image(pil_img)
            
            metadata['qr_points'] = points
            return (image_path, metadata)
            
    except UnidentifiedImageError:
        metadata.update({'error': f"無法識別圖片格式: {image_path}"})
        return (image_path, metadata)
    except Exception as e:
        metadata.update({'error': f"QR檢測失敗 {image_path}: {e}"})
        return (image_path, metadata)

# -------- Worker：pHash-only (【核心修正】增加 EXIF 校正) --------
def _pool_worker_process_image_phash_only(image_path: str) -> Tuple[str, Dict[str, Any]]:
    """
    多進程 Worker：計算 pHash，並確保返回元數據。
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
            
            # 【關鍵修正】計算哈希前同樣需要校正方向
            img = ImageOps.exif_transpose(img)
            
            metadata['phash'] = imagehash.phash(img, hash_size=8)
            return (image_path, metadata)
    except Exception as e:
        metadata.update({'error': f"處理 pHash 失敗 {image_path}: {e}"})
        return (image_path, metadata)

# -------- Worker：pHash + QR (【核心修正】增加 EXIF 校正) --------
def _pool_worker_process_image_full(image_path: str, resize_size: int) -> Tuple[str, Dict[str, Any]]:
    """
    多進程 Worker：混合模式，計算 pHash、偵測 QR，並返回完整元數據。
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
            
            # 【關鍵修正】在所有計算開始前，統一校正方向
            img = ImageOps.exif_transpose(img)
            
            # 1. 計算 pHash
            metadata['phash'] = imagehash.phash(img, hash_size=8)
            
            # 2. 檢測 QR Code
            resized_img = img.copy()
            resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
            qr_points_val = _detect_qr_on_image(resized_img)
            if not qr_points_val:
                qr_points_val = _detect_qr_on_image(img)
            metadata['qr_points'] = qr_points_val
                
        return (image_path, metadata)
    except UnidentifiedImageError:
        metadata.update({'error': f"無法識別圖片格式: {image_path}"})
        return (image_path, metadata)
    except Exception as e:
        metadata.update({'error': f"完整圖片處理失敗 {image_path}: {e}"})
        return (image_path, metadata)