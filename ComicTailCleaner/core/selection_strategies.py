# ======================================================================
# 檔案：core/selection_strategies.py
# 目的：定義各種比對模式下的「自動選取建議」策略
# 版本：1.2.0 (優化：互相比對模式下，同資料夾優先保留基準圖)
# ======================================================================

from typing import List, Tuple, Set, Dict
import os

def get_strategy(mode: str, config: dict):
    """工廠函式：根據模式名稱回傳對應的策略物件"""
    if mode == 'ad_comparison':
        return AdStrategy(config)
    elif mode == 'mutual_comparison':
        return MutualStrategy(config)
    elif mode == 'qr_detection':
        return QrStrategy(config)
    return None

class AdStrategy:
    """
    廣告比對策略
    規則：選取建議 = 閾值 + (100 - 閾值) / 2
    """
    def __init__(self, config):
        self.base_threshold = float(config.get('similarity_threshold', 95.0))

    def calculate(self, all_groups: List[Tuple]) -> Set[str]:
        to_select = set()
        diff = 100.0 - self.base_threshold
        safe_buffer = diff / 2.0
        selection_threshold = self.base_threshold + safe_buffer

        for leader, child, sim_str in all_groups:
            try:
                clean_str = sim_str.split('%')[0].strip()
                sim_val = float(clean_str)
                if sim_val >= selection_threshold:
                    to_select.add(child)
            except (ValueError, IndexError):
                continue
        return to_select

class MutualStrategy:
    """
    互相比對策略
    1. 不同資料夾：積分競賽，保留優質資料夾，刪除劣質資料夾的內容。
    2. 同資料夾：優先保留基準圖 (Leader)，刪除重複圖 (Child)，並參考檔名長度。
    """
    def __init__(self, config):
        pass

    def calculate(self, all_groups: List[Tuple]) -> Set[str]:
        folder_stats = {}
        
        def get_folder(path):
            return os.path.dirname(path)

        # 找出所有 100% 相似的項目 (互相比對只建議自動刪除完全一樣的)
        exact_duplicates = []
        for p1, p2, sim_str in all_groups:
            if "100.0%" in str(sim_str):
                exact_duplicates.append((p1, p2))

        if not exact_duplicates:
            return set()

        # --- 階段一：資料夾積分賽 (針對不同資料夾) ---
        for p1, p2 in exact_duplicates:
            f1, f2 = get_folder(p1), get_folder(p2)
            if f1 == f2: continue # 同資料夾不參與積分
            
            if f1 not in folder_stats: folder_stats[f1] = {'score': 0}
            if f2 not in folder_stats: folder_stats[f2] = {'score': 0}
            
            # 評分 1: 檔名長度 (短的 +1) -> 假設短檔名是原始檔
            n1, n2 = os.path.basename(p1), os.path.basename(p2)
            if len(n1) < len(n2): folder_stats[f1]['score'] += 1
            elif len(n2) < len(n1): folder_stats[f2]['score'] += 1
            
            # 評分 2: 檔案大小 (大的 +1) -> 雖然 pHash 一樣，但以此作為保底
            try:
                s1, s2 = os.path.getsize(p1), os.path.getsize(p2)
                if s1 > s2: folder_stats[f1]['score'] += 1
                elif s2 > s1: folder_stats[f2]['score'] += 1
            except: pass

        # --- 階段二：執行選取 ---
        to_select = set()
        
        for p1, p2 in exact_duplicates:
            # p1 是 Leader (基準圖), p2 是 Child (重複圖)
            f1, f2 = get_folder(p1), get_folder(p2)
            
            # === 同資料夾處理邏輯 (本次修正重點) ===
            if f1 == f2:
                # 邏輯：優先刪除 p2 (Child)，保留 p1 (Base)
                # 但為了防呆（如果 p1 是 "img (1).jpg" 而 p2 是 "img.jpg"），我們加一個檔名長度判斷
                
                n1 = os.path.basename(p1)
                n2 = os.path.basename(p2)
                
                if len(n2) > len(n1):
                    # p2 檔名較長 (可能是副本)，刪除 p2 (符合保留基準圖原則)
                    to_select.add(p2)
                elif len(n1) > len(n2):
                    # p1 檔名較長 (基準圖反而像是副本)，這時為了檔案命名的整潔，我們刪除 p1
                    # 注意：這會讓介面勾選父項目，視覺上可能較少見，但邏輯是正確的
                    to_select.add(p1)
                else:
                    # 檔名長度一樣，或者無法判斷
                    # 嚴格執行「保留基準 (p1)，刪除多餘 (p2)」
                    to_select.add(p2)
                continue
            # ====================================

            # 不同資料夾處理邏輯 (維持原樣)
            score1 = folder_stats.get(f1, {}).get('score', 0)
            score2 = folder_stats.get(f2, {}).get('score', 0)
            
            if score1 > score2:
                to_select.add(p2) # p2 資料夾分數低，刪 p2
            elif score2 > score1:
                to_select.add(p1) # p1 資料夾分數低，刪 p1
            else:
                # 資料夾分數平手，不選取 (安全策略)
                pass
                
        return to_select

class QrStrategy:
    """QR 策略"""
    def __init__(self, config):
        pass
    
    def calculate(self, all_groups: List[Tuple]) -> Set[str]:
        # QR 模式全選
        to_select = set()
        for p1, p2, info in all_groups:
            to_select.add(p1)
        return to_select