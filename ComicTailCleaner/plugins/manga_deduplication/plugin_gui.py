# ======================================================================
# 檔案：plugins/manga_deduplication/plugin_gui.py
# 目的：為相似卷宗查找外掛提供專屬的 GUI 設定介面
# 版本：2.4 (設定重構：使用 'manga_dedupe_*' 前綴以避免命名衝突)
# ======================================================================

import tkinter as tk
from tkinter import ttk

def create_settings_frame(parent_frame, config, ui_vars):
    """創建此外掛的專屬設定 UI 元件"""

    # --- v-MOD: 使用新的標準化前綴 ---
    KEY_ENABLE_LIMIT = 'manga_dedupe_enable_sample_limit'
    KEY_SAMPLE_COUNT = 'manga_dedupe_sample_count'
    KEY_MATCH_THRESHOLD = 'manga_dedupe_match_threshold'
    # --- v-MOD END ---

    # --- 設定項 0: 是否啓用數量限制 ---
    ui_vars[KEY_ENABLE_LIMIT] = tk.BooleanVar()
    cb = ttk.Checkbutton(parent_frame, text="啓用末尾圖片取樣限制", variable=ui_vars[KEY_ENABLE_LIMIT])
    cb.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 5))

    # --- 設定項 1: 取樣數量 ---
    ttk.Label(parent_frame, text="末尾圖片取樣數:").grid(row=1, column=0, sticky="w", pady=2)
    ui_vars[KEY_SAMPLE_COUNT] = tk.StringVar()
    spinbox1 = ttk.Spinbox(parent_frame, from_=2, to=100, textvariable=ui_vars[KEY_SAMPLE_COUNT], width=5)
    spinbox1.grid(row=1, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="(以此數量建立指紋)").grid(row=1, column=2, sticky="w")
    
    # --- 設定項 2: 匹配閾值 ---
    ttk.Label(parent_frame, text="指紋匹配閾值:").grid(row=2, column=0, sticky="w", pady=2)
    ui_vars[KEY_MATCH_THRESHOLD] = tk.StringVar()
    spinbox2 = ttk.Spinbox(parent_frame, from_=2, to=100, textvariable=ui_vars[KEY_MATCH_THRESHOLD], width=5)
    spinbox2.grid(row=2, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="(指紋中至少 N 張相似即匹配)").grid(row=2, column=2, sticky="w")
    
    # --- 聯動 UI 狀態 ---
    def toggle_spinbox_state(*args):
        state = tk.NORMAL if ui_vars[KEY_ENABLE_LIMIT].get() else tk.DISABLED
        spinbox1.config(state=state)

    ui_vars[KEY_ENABLE_LIMIT].trace_add("write", toggle_spinbox_state)
    
    # --- v-MOD: 載入設定邏輯移至此處，確保UI元件已建立 ---
    ui_vars[KEY_ENABLE_LIMIT].set(config.get(KEY_ENABLE_LIMIT, True))
    ui_vars[KEY_SAMPLE_COUNT].set(config.get(KEY_SAMPLE_COUNT, 12))
    ui_vars[KEY_MATCH_THRESHOLD].set(config.get(KEY_MATCH_THRESHOLD, 8))
    toggle_spinbox_state() # 手動觸發一次狀態更新
    # --- v-MOD END ---
    
    return parent_frame

def save_settings(config, ui_vars):
    """從 UI 變量中讀取值並存入 config 字典"""
    # --- v-MOD: 使用新的標準化前綴 ---
    KEY_ENABLE_LIMIT = 'manga_dedupe_enable_sample_limit'
    KEY_SAMPLE_COUNT = 'manga_dedupe_sample_count'
    KEY_MATCH_THRESHOLD = 'manga_dedupe_match_threshold'
    
    if KEY_ENABLE_LIMIT in ui_vars:
        config[KEY_ENABLE_LIMIT] = ui_vars[KEY_ENABLE_LIMIT].get()
    
    try:
        if KEY_SAMPLE_COUNT in ui_vars:
            config[KEY_SAMPLE_COUNT] = int(ui_vars[KEY_SAMPLE_COUNT].get())
    except (ValueError, KeyError, tk.TclError):
        # 如果出錯，保留 config 中可能已存在的舊值或預設值
        config.setdefault(KEY_SAMPLE_COUNT, 12)
        
    try:
        if KEY_MATCH_THRESHOLD in ui_vars:
            config[KEY_MATCH_THRESHOLD] = int(ui_vars[KEY_MATCH_THRESHOLD].get())
    except (ValueError, KeyError, tk.TclError):
        config.setdefault(KEY_MATCH_THRESHOLD, 8)
    # --- v-MOD END ---
        
    return config

# 移除舊的 load_settings 函式，因為邏輯已整合進 create_settings_frame