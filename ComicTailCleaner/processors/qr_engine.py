from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

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


def _detect_qr_on_image(img: "Image.Image") -> Optional[List]:
    if cv2 is None or np is None or Image is None:
        return None

    img_cv = np.array(img.convert("RGB"))
    if img_cv.shape[0] == 0 or img_cv.shape[1] == 0:
        raise ValueError("空圖片無法進行 QR 偵測")

    if pyzbar_decode:
        try:
            gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY)
            decoded = pyzbar_decode(gray, symbols=[ZBarSymbol.QRCODE])
            if decoded:
                poly = decoded[0].polygon
                if poly and len(poly) == 4:
                    return [[[p.x, p.y] for p in poly]]
        except Exception:
            pass

    qr_detector = cv2.QRCodeDetector()
    retval, decoded_info, points, _ = qr_detector.detectAndDecodeMulti(img_cv)
    if retval and points is not None and len(points) > 0:
        return points.tolist()

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

            blurred = cv2.GaussianBlur(enhanced, (5, 5), 0)
            thresh = cv2.adaptiveThreshold(
                blurred,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                11,
                2,
            )
            decoded = pyzbar_decode(thresh, symbols=[ZBarSymbol.QRCODE])
            if decoded:
                poly = decoded[0].polygon
                if poly and len(poly) == 4:
                    return [[[p.x, p.y] for p in poly]]
        except Exception:
            pass

    return None


def _fast_get_qr_regions(
    img_cv: np.ndarray,
    min_area: int = 400,
    max_area_ratio: float = 0.5,
) -> List[Tuple[int, int, int, int]]:
    if cv2 is None or np is None:
        return []

    gray = cv2.cvtColor(img_cv, cv2.COLOR_RGB2GRAY) if img_cv.ndim == 3 else img_cv
    scale = 800 / max(gray.shape)
    if scale < 1.0:
        small = cv2.resize(gray, (0, 0), fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    else:
        small = gray
        scale = 1.0

    grad_x = cv2.Sobel(small, cv2.CV_32F, 1, 0, ksize=3)
    grad_y = cv2.Sobel(small, cv2.CV_32F, 0, 1, ksize=3)
    grad = cv2.magnitude(grad_x, grad_y)
    grad = cv2.convertScaleAbs(grad)
    _, thresh = cv2.threshold(grad, 50, 255, cv2.THRESH_BINARY)

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (11, 11))
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    rois = []
    total_area = small.shape[0] * small.shape[1]
    max_area = total_area * max_area_ratio
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h
        if area < min_area or area > max_area:
            continue
        aspect_ratio = w / float(h)
        if 0.3 <= aspect_ratio <= 3.5:
            rois.append((int(x / scale), int(y / scale), int(w / scale), int(h / scale)))
    return rois


def _fast_is_colorful(img_cv: np.ndarray, color_threshold: float = 15.0) -> bool:
    if cv2 is None or np is None:
        return True

    scale = 400.0 / max(img_cv.shape[:2])
    if scale < 1.0:
        small = cv2.resize(img_cv, (0, 0), fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    else:
        small = img_cv

    lab = cv2.cvtColor(small, cv2.COLOR_RGB2LAB)
    _, a, b = cv2.split(lab)
    std_a = np.std(a)
    std_b = np.std(b)
    mean_a = np.mean(a)
    mean_b = np.mean(b)
    color_val = np.sqrt(std_a ** 2 + std_b ** 2) + 0.3 * np.sqrt((mean_a - 128) ** 2 + (mean_b - 128) ** 2)
    return color_val > color_threshold


def _get_4x4_grid_hashes(image: "Image.Image") -> List[str]:
    if not image or not imagehash:
        return []
    tw, th = image.size
    if tw < 32 or th < 32:
        return []
    bw, bh = tw // 4, th // 4
    hashes = []
    for row in range(4):
        for col in range(4):
            box = (col * bw, row * bh, (col + 1) * bw, (row + 1) * bh)
            hashes.append(str(imagehash.phash(image.crop(box), hash_size=8)))
    return hashes


def _pool_worker_detect_qr_colorful_only(
    image_path: str,
    pil_img: "Image.Image" = None,
) -> Tuple[str, Dict[str, Any]]:
    from utils import _get_file_stat, _open_image_from_any_path

    st_size, st_ctime, st_mtime = _get_file_stat(image_path)
    if st_mtime is None:
        return (image_path, {"error": f"圖片檔案不存在: {image_path}"})
    metadata = {"size": st_size, "ctime": st_ctime, "mtime": st_mtime}

    try:
        if pil_img is None:
            pil_img = _open_image_from_any_path(image_path)
        if pil_img is None or pil_img.width == 0 or pil_img.height == 0:
            metadata["is_colorful"] = False
            return (image_path, metadata)

        if hasattr(ImageOps, "exif_transpose"):
            try:
                pil_img = ImageOps.exif_transpose(pil_img)
            except Exception:
                pass

        img_cv = np.array(pil_img.convert("RGB"))
        metadata["is_colorful"] = bool(_fast_is_colorful(img_cv))
        return (image_path, metadata)
    except Exception as e:
        metadata["error"] = f"彩圖前篩失敗: {image_path}: {e}"
        return (image_path, metadata)
    finally:
        if pil_img:
            try:
                pil_img.close()
            except Exception:
                pass


def _pool_worker_detect_qr_code(
    image_path: str,
    resize_size: int,
    enable_color_filter: bool = False,
    pil_img: "Image.Image" = None,
) -> Tuple[str, Dict[str, Any]]:
    from utils import _get_file_stat, _open_image_from_any_path

    st_size, st_ctime, st_mtime = _get_file_stat(image_path)
    if st_mtime is None:
        return (image_path, {"error": f"圖片檔案不存在: {image_path}"})
    metadata = {"size": st_size, "ctime": st_ctime, "mtime": st_mtime}

    try:
        if pil_img is None:
            pil_img = _open_image_from_any_path(image_path)
        if pil_img is None:
            raise UnidentifiedImageError("無法開啟圖片")
        if pil_img.width == 0 or pil_img.height == 0:
            metadata["error"] = f"空圖片無法進行 QR 偵測: {image_path}"
            return (image_path, metadata)

        if hasattr(ImageOps, "exif_transpose"):
            try:
                pil_img = ImageOps.exif_transpose(pil_img)
            except Exception:
                pass

        img_cv = np.array(pil_img.convert("RGB"))
        if enable_color_filter and not _fast_is_colorful(img_cv):
            metadata["is_colorful"] = False
            metadata["qr_points"] = None
            return (image_path, metadata)
        metadata["is_colorful"] = True

        resized_img = pil_img.copy()
        resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
        points = _detect_qr_on_image(resized_img)

        if not points:
            rois = _fast_get_qr_regions(img_cv)
            for x, y, w, h in rois:
                pad = 60
                crop_box = (
                    max(0, x - pad),
                    max(0, y - pad),
                    min(pil_img.width, x + w + pad),
                    min(pil_img.height, y + h + pad),
                )
                crop_img = pil_img.crop(crop_box)
                crop_points = _detect_qr_on_image(crop_img)
                if crop_points:
                    points = [[[p[0] + crop_box[0], p[1] + crop_box[1]] for p in crop_points[0]]]
                    break

        metadata["qr_points"] = points
        if points and imagehash:
            try:
                ph = imagehash.phash(pil_img, hash_size=8)
                metadata["phash"] = str(ph)
                from core_engine import FEATURE_PHASH, FEATURE_QR
                metadata["features_at"] = metadata.get("features_at", 0) | FEATURE_PHASH | FEATURE_QR
            except Exception:
                pass
        return (image_path, metadata)
    except UnidentifiedImageError:
        metadata["error"] = f"無法開啟圖片: {image_path}"
        return (image_path, metadata)
    except Exception as e:
        metadata["error"] = f"QR 偵測失敗 {image_path}: {e}"
        return (image_path, metadata)
    finally:
        if pil_img:
            try:
                pil_img.close()
            except Exception:
                pass


def _pool_worker_process_image_phash_only(
    image_path: str,
    use_rotation: bool = False,
    use_preprocess: bool = False,
    hash_resolution: int = 128,
    pil_img: "Image.Image" = None,
) -> Tuple[str, Dict[str, Any]]:
    from utils import _get_file_stat, _open_image_from_any_path

    st_size, st_ctime, st_mtime = _get_file_stat(image_path)
    if st_mtime is None:
        return (image_path, {"error": f"圖片檔案不存在: {image_path}"})
    metadata = {"size": st_size, "ctime": st_ctime, "mtime": st_mtime}

    try:
        if pil_img is None:
            pil_img = _open_image_from_any_path(image_path)
        if pil_img is None:
            raise UnidentifiedImageError("無法開啟圖片")
        if pil_img.width == 0 or pil_img.height == 0:
            metadata["error"] = f"空圖片無法計算 pHash: {image_path}"
            return (image_path, metadata)

        img = ImageOps.exif_transpose(pil_img.convert("RGB"))
        if use_preprocess:
            from utils import _auto_crop_white_borders
            img = _auto_crop_white_borders(img)
            img = ImageOps.equalize(img.convert("L")).convert("RGB")

        metadata["width"], metadata["height"] = img.width, img.height
        h32 = imagehash.phash(img, hash_size=8)
        h128 = imagehash.phash(img, hash_size=16)
        h512 = imagehash.phash(img, hash_size=32)
        metadata.update({
            "phash": str(h32),
            "phash_32": str(h32),
            "phash_128": h128.hash.tobytes(),
            "phash_512": h512.hash.tobytes(),
            "grid_phash": _get_4x4_grid_hashes(img),
        })

        if use_rotation:
            img_90 = img.rotate(90, expand=True)
            img_180 = img.rotate(180, expand=True)
            img_270 = img.rotate(270, expand=True)
            metadata["phash_rotations"] = {
                "90": str(imagehash.phash(img_90, hash_size=8)),
                "180": str(imagehash.phash(img_180, hash_size=8)),
                "270": str(imagehash.phash(img_270, hash_size=8)),
            }
            metadata["grid_rotations"] = {
                "90": _get_4x4_grid_hashes(img_90),
                "180": _get_4x4_grid_hashes(img_180),
                "270": _get_4x4_grid_hashes(img_270),
            }
        else:
            metadata["phash_rotations"] = {}
            metadata["grid_rotations"] = {}

        return (image_path, metadata)
    except Exception as e:
        metadata["error"] = f"pHash 計算失敗 {image_path}: {e}"
        return (image_path, metadata)
    finally:
        if pil_img:
            try:
                pil_img.close()
            except Exception:
                pass


def _pool_worker_ensure_image_features(
    image_path: str,
    need_hsv: bool = False,
    need_whash: bool = False,
    use_preprocess: bool = False,
    enable_quick_digest: bool = True,
    hash_resolution: int = 128,
) -> Tuple[str, Dict[str, Any]]:
    from utils import _avg_hsv, _auto_crop_white_borders, _calculate_quick_digest, _get_file_stat, _open_image_from_any_path

    st_size, st_ctime, st_mtime = _get_file_stat(image_path)
    if st_mtime is None:
        return (image_path, {"error": f"file stat failed: {image_path}"})

    metadata: Dict[str, Any] = {"size": st_size, "ctime": st_ctime, "mtime": st_mtime}
    pil_img = None
    try:
        pil_img = _open_image_from_any_path(image_path)
        if pil_img is None:
            raise UnidentifiedImageError("image open failed")
        if pil_img.width == 0 or pil_img.height == 0:
            metadata["error"] = f"invalid image size: {image_path}"
            return (image_path, metadata)

        img = ImageOps.exif_transpose(pil_img.convert("RGB"))
        img = _auto_crop_white_borders(img)
        if use_preprocess:
            img = ImageOps.equalize(img.convert("L")).convert("RGB")

        metadata["width"], metadata["height"] = img.width, img.height
        if need_hsv:
            avg_hsv = _avg_hsv(img)
            if avg_hsv is not None:
                metadata["avg_hsv"] = list(avg_hsv)
        if need_whash:
            if imagehash is None:
                metadata["error"] = "imagehash unavailable"
                return (image_path, metadata)
            metadata["whash"] = str(imagehash.whash(img, hash_size=8, mode="haar", remove_max_haar_ll=True))
        if enable_quick_digest:
            qd64 = _calculate_quick_digest(image_path)
            if qd64:
                metadata["qd64"] = qd64
        return (image_path, metadata)
    except Exception as e:
        metadata["error"] = f"feature calculation failed: {image_path}: {e}"
        return (image_path, metadata)
    finally:
        if pil_img:
            try:
                pil_img.close()
            except Exception:
                pass


def _pool_worker_process_image_full(
    image_path: str,
    resize_size: int,
    enable_color_filter: bool = False,
    use_rotation: bool = False,
    use_preprocess: bool = False,
    hash_resolution: int = 128,
    pil_img: "Image.Image" = None,
) -> Tuple[str, Dict[str, Any]]:
    from utils import _get_file_stat, _open_image_from_any_path

    st_size, st_ctime, st_mtime = _get_file_stat(image_path)
    if st_mtime is None:
        return (image_path, {"error": f"圖片檔案不存在: {image_path}"})
    metadata = {"size": st_size, "ctime": st_ctime, "mtime": st_mtime}

    try:
        if pil_img is None:
            pil_img = _open_image_from_any_path(image_path)
        if pil_img is None:
            raise UnidentifiedImageError("無法開啟圖片")
        if pil_img.width == 0 or pil_img.height == 0:
            metadata["error"] = f"空圖片無法計算雜湊: {image_path}"
            return (image_path, metadata)

        img = pil_img.convert("RGB")
        # 移除可能導致解碼不全的 draft()，改用穩定的 thumbnail 或直接計算
        img = ImageOps.exif_transpose(img)
        if use_preprocess:
            from utils import _auto_crop_white_borders
            img = _auto_crop_white_borders(img)
            img = ImageOps.equalize(img.convert("L")).convert("RGB")

        metadata["width"], metadata["height"] = img.width, img.height
        img_cv = np.array(img.convert("RGB"))
        if enable_color_filter and not _fast_is_colorful(img_cv):
            metadata["is_colorful"] = False
            metadata["qr_points"] = None
            return (image_path, metadata)
        metadata["is_colorful"] = True

        h32 = imagehash.phash(img, hash_size=8)
        h128 = imagehash.phash(img, hash_size=16)
        h512 = imagehash.phash(img, hash_size=32)
        metadata.update({
            "phash": str(h32),
            "phash_32": str(h32),
            "phash_128": h128.hash.tobytes(),
            "phash_512": h512.hash.tobytes(),
            "grid_phash": _get_4x4_grid_hashes(img),
        })

        if use_rotation:
            img_90 = img.rotate(90, expand=True)
            img_180 = img.rotate(180, expand=True)
            img_270 = img.rotate(270, expand=True)
            metadata["phash_rotations"] = {
                "90": str(imagehash.phash(img_90, hash_size=8)),
                "180": str(imagehash.phash(img_180, hash_size=8)),
                "270": str(imagehash.phash(img_270, hash_size=8)),
            }
            metadata["grid_rotations"] = {
                "90": _get_4x4_grid_hashes(img_90),
                "180": _get_4x4_grid_hashes(img_180),
                "270": _get_4x4_grid_hashes(img_270),
            }
        else:
            metadata["phash_rotations"] = {}
            metadata["grid_rotations"] = {}

        resized_img = img.copy()
        resized_img.thumbnail((resize_size, resize_size), Image.Resampling.LANCZOS)
        qr_points = _detect_qr_on_image(resized_img)
        if not qr_points:
            rois = _fast_get_qr_regions(img_cv)
            for x, y, w, h in rois:
                pad = 60
                crop_box = (
                    max(0, x - pad),
                    max(0, y - pad),
                    min(img.width, x + w + pad),
                    min(img.height, y + h + pad),
                )
                crop_img = img.crop(crop_box)
                crop_points = _detect_qr_on_image(crop_img)
                if crop_points:
                    qr_points = [[[p[0] + crop_box[0], p[1] + crop_box[1]] for p in crop_points[0]]]
                    break
        metadata["qr_points"] = qr_points
        return (image_path, metadata)
    except UnidentifiedImageError:
        metadata["error"] = f"無法開啟圖片: {image_path}"
        return (image_path, metadata)
    except Exception as e:
        metadata["error"] = f"完整圖片處理失敗 {image_path}: {e}"
        return (image_path, metadata)
    finally:
        if pil_img:
            try:
                pil_img.close()
            except Exception:
                pass


def group_qr_results_by_phash(
    flat_qr_list: List[tuple],
    file_data: dict,
    sim_threshold: float = 0.80,
) -> List[tuple]:
    if not imagehash or not flat_qr_list:
        return flat_qr_list

    hash_bits = 64

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
        return 1.0 - ((h1 - h2) / hash_bits)

    items_with_hash: List[tuple] = []
    items_without_hash: List[tuple] = []

    for path, _dup_path, val_str, tag in flat_qr_list:
        entry = file_data.get(path) or file_data.get(path.lower().replace("\\", "/"))
        raw_h = entry.get("phash") if entry else None
        h = _coerce(raw_h)
        if h:
            items_with_hash.append((path, val_str, tag, h))
        else:
            items_without_hash.append((path, path, val_str, tag))

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
            if _sim(items_with_hash[i][3], items_with_hash[j][3]) >= sim_threshold:
                union(i, j)

    groups: dict = defaultdict(list)
    for i, (path, val_str, tag, _) in enumerate(items_with_hash):
        groups[find(i)].append((path, val_str, tag))

    grouped: List[tuple] = []
    for members in groups.values():
        if len(members) == 1:
            path, val_str, tag = members[0]
            grouped.append((path, path, val_str, tag))
            continue

        leader_path = members[0][0]
        grouped.append((leader_path, leader_path, f"QR 分組 (共 {len(members)} 張)", members[0][2]))
        for path, val_str, tag in members:
            if path == leader_path:
                continue
            grouped.append((leader_path, path, val_str, tag))

    grouped.extend(items_without_hash)
    return grouped
