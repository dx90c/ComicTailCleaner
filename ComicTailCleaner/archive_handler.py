# ======================================================================
# 檔案名稱：archive_handler.py
# 模組目的：為 ComicTailCleaner 提供獨立的壓縮檔掃描與處理能力
# 版本：1.2.0 (健壯性與效能優化)
# ======================================================================

import os
import zipfile
import tarfile
import io
from collections import namedtuple
from typing import Iterable, Set, IO, Optional, Union

# --- 可選的 RAR 支援 ---
try:
    import rarfile
    # 優先使用程式目錄下的 UnRAR.exe，其次是系統路徑中的 unrar
    local_unrar_tool = "UnRAR.exe"
    if os.path.exists(local_unrar_tool):
        rarfile.UNRAR_TOOL = local_unrar_tool
        RAR_SUPPORTED = True
    else:
        from shutil import which
        if which("unrar"):
            rarfile.UNRAR_TOOL = "unrar"
            RAR_SUPPORTED = True
        else:
            RAR_SUPPORTED = False
except ImportError:
    RAR_SUPPORTED = False

# --- 公開的資料結構 ---
ArchiveEntry = namedtuple('ArchiveEntry', ['archive_path', 'inner_path', 'file_size', 'open_bytes'])
CleanResult = namedtuple('CleanResult', ['original_count', 'deleted_count', 'final_count', 'note'])

# --- 內部輔助函式 ---
def _is_image(filename: str) -> bool:
    """檢查檔名是否為支援的圖片格式。"""
    return filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))

def _get_sorted_image_entries(archive_file_obj: IO[bytes]) -> list:
    """
    自動偵測壓縮檔類型並回傳排序過的內部圖片成員列表。
    支援 zip, tar, rar。
    """
    members = []
    original_position = archive_file_obj.tell()
    
    # 嘗試 ZIP
    try:
        archive_file_obj.seek(0)
        if zipfile.is_zipfile(archive_file_obj):
            with zipfile.ZipFile(archive_file_obj, 'r') as zf:
                members = [info for info in zf.infolist() if not info.is_dir() and _is_image(info.filename)]
                members.sort(key=lambda info: info.filename)
            return members
    except Exception:
        pass
    finally:
        archive_file_obj.seek(original_position)

    # 嘗試 TAR
    try:
        archive_file_obj.seek(0)
        with tarfile.open(fileobj=archive_file_obj, mode='r:*') as tf:
            members = [info for info in tf.getmembers() if info.isfile() and _is_image(info.name)]
            members.sort(key=lambda info: info.name)
        return members
    except (tarfile.ReadError, Exception):
        pass
    finally:
        archive_file_obj.seek(original_position)

    # 嘗試 RAR
    try:
        if RAR_SUPPORTED:
            archive_file_obj.seek(0)
            with rarfile.RarFile(archive_file_obj, 'r') as rf:
                members = [info for info in rf.infolist() if not info.is_dir() and _is_image(info.filename)]
                members.sort(key=lambda info: info.filename)
            return members
    except Exception:
        pass
    finally:
        archive_file_obj.seek(original_position)
        
    return members

# --- 公開 API ---
def get_supported_formats() -> list[str]:
    """回傳所有支援的壓縮檔副檔名列表。"""
    formats = ['.zip', '.cbz', '.tar', '.cbt', '.tar.gz', '.gz', '.tar.bz2', '.bz2']
    if RAR_SUPPORTED:
        formats.extend(['.rar', '.cbr'])
    return formats

def get_image_bytes(archive_path: str, inner_path: str) -> Union[bytes, None]:
    """從指定的壓縮檔中讀取特定內部路徑的圖片，並回傳其二進位內容。"""
    try:
        with open(archive_path, 'rb') as f:
            # 嘗試 ZIP
            try:
                f.seek(0)
                if zipfile.is_zipfile(f):
                    with zipfile.ZipFile(f, 'r') as zf:
                        return zf.read(inner_path)
            except Exception: pass
            
            # 嘗試 TAR
            try:
                f.seek(0)
                with tarfile.open(fileobj=f, mode='r:*') as tf:
                    m = tf.getmember(inner_path)
                    fp = tf.extractfile(m)
                    return fp.read() if fp else None
            except Exception: pass

            # 嘗試 RAR
            try:
                if RAR_SUPPORTED:
                    f.seek(0)
                    with rarfile.RarFile(f, 'r') as rf:
                        return rf.read(inner_path)
            except Exception: pass
    except Exception:
        return None
    return None

def iter_archive_images(archive_path: str) -> Iterable[ArchiveEntry]:
    """
    疊代一個壓縮檔內的所有圖片，產生 ArchiveEntry 物件。
    這是一個生成器函式，可以高效地處理大型壓縮檔。
    """
    try:
        with open(archive_path, 'rb') as f:
            members = _get_sorted_image_entries(f)
            for member_info in members:
                inner_path = getattr(member_info, 'filename', getattr(member_info, 'name', ''))
                file_size = getattr(member_info, 'file_size', getattr(member_info, 'size', 0))
                
                # 使用閉包來延遲讀取圖片內容，只有在需要時才真正解壓縮
                def open_bytes_closure(path=archive_path, inner=inner_path):
                    return get_image_bytes(path, inner) or b''
                    
                yield ArchiveEntry(
                    archive_path=archive_path, 
                    inner_path=inner_path, 
                    file_size=file_size, 
                    open_bytes=open_bytes_closure
                )
    except Exception as e:
        from utils import log_error
        log_error(f"無法疊代壓縮檔 '{archive_path}': {e}")
        return

def plan_trailing_deletions(archive_path: str, tail_pages: int) -> Set[str]:
    """規劃要從壓縮檔尾部刪除的圖片列表。"""
    if tail_pages <= 0: return set()
    try:
        with open(archive_path, 'rb') as f:
            members = _get_sorted_image_entries(f)
            if len(members) > tail_pages:
                to_delete = members[-tail_pages:]
                return {getattr(m, 'filename', getattr(m, 'name', '')) for m in to_delete}
    except Exception as e:
        from utils import log_error
        log_error(f"無法規劃 '{archive_path}' 的刪除計畫: {e}")
    return set()

def apply_trailing_deletions(archive_path: str, to_delete: Set[str], keep_backup: bool = True) -> CleanResult:
    """
    實際執行刪除操作：建立一個不含指定檔案的新壓縮檔，然後取代舊檔。
    """
    if not to_delete:
        return CleanResult(0, 0, 0, "無需刪除任何檔案。")

    is_rar = archive_path.lower().endswith(('.rar', '.cbr')) and RAR_SUPPORTED
    tmp_path = archive_path + ".tmp_clean"
    original_count, final_count = 0, 0
    
    try:
        with open(archive_path, 'rb') as original_f:
            all_members = _get_sorted_image_entries(original_f)
            original_count = len(all_members)
            
            # 如果是 RAR 或 TAR，新的輸出檔統一為 .cbz (ZIP格式)
            output_path = os.path.splitext(archive_path)[0] + '.cbz' if is_rar or tarfile.is_tarfile(archive_path) else archive_path
            tmp_path = output_path + ".tmp_clean"
            original_f.seek(0)
            
            is_tar = False
            try:
                with tarfile.open(fileobj=original_f, mode='r:*'): is_tar = True
            except (tarfile.ReadError, Exception): is_tar = False
            finally: original_f.seek(0)

            if is_rar:
                with zipfile.ZipFile(tmp_path, 'w', compression=zipfile.ZIP_STORED) as zf_out:
                    with rarfile.RarFile(original_f, 'r') as rf_in:
                        for member in all_members:
                            if member.filename not in to_delete:
                                zf_out.writestr(member.filename, rf_in.read(member))
                                final_count += 1
            elif is_tar:
                with tarfile.open(fileobj=original_f, mode='r:*') as tf_in:
                    with zipfile.ZipFile(tmp_path, 'w', compression=zipfile.ZIP_STORED) as zf_out:
                        for member in all_members:
                            if member.name not in to_delete:
                                file_content = tf_in.extractfile(member)
                                if file_content:
                                    zf_out.writestr(member.name, file_content.read())
                                    final_count += 1
            elif zipfile.is_zipfile(original_f):
                with zipfile.ZipFile(tmp_path, 'w') as zf_out:
                    with zipfile.ZipFile(original_f, 'r') as zf_in:
                        for member in all_members:
                            if member.filename not in to_delete:
                                compress_type = zipfile.ZIP_STORED if member.filename.lower().endswith(('.png', '.jpg', '.jpeg')) else zipfile.ZIP_DEFLATED
                                data = zf_in.read(member.filename)
                                zf_out.writestr(member.filename, data, compress_type=compress_type)
                                final_count += 1
        
        # 替換檔案
        bak_path = archive_path + ".bak"
        if os.path.exists(bak_path): os.remove(bak_path)
        
        if keep_backup:
            os.rename(archive_path, bak_path)
        else:
            os.remove(archive_path)
            
        os.rename(tmp_path, output_path)
        
        deleted_count = original_count - final_count
        note = "成功。RAR/TAR 已轉存為 CBZ。" if is_rar or is_tar else "成功。"
        return CleanResult(original_count, deleted_count, final_count, note)
        
    except Exception as e:
        if os.path.exists(tmp_path): os.remove(tmp_path)
        return CleanResult(original_count, 0, original_count, f"錯誤: {e}")

def clean_trailing_pages(archive_path: str, tail_pages: int, *, dry_run: bool = False, keep_backup: bool = True) -> CleanResult:
    """高階 API：規劃並執行刪除壓縮檔尾頁。"""
    to_delete = plan_trailing_deletions(archive_path, tail_pages)
    
    try:
        with open(archive_path, 'rb') as f:
            count = len(_get_sorted_image_entries(f))
    except Exception as e:
        return CleanResult(0, 0, 0, f"無法讀取原始檔案: {e}")
        
    if dry_run:
        return CleanResult(count, len(to_delete), count - len(to_delete), "模擬執行成功。")
        
    if not to_delete:
        return CleanResult(count, 0, count, "沒有需要清理的頁面。")
        
    return apply_trailing_deletions(archive_path, to_delete, keep_backup)