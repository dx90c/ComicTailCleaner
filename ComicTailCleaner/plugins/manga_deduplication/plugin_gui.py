# ======================================================================
# 檔案：plugins/manga_deduplication/plugin_gui.py
# 目的：為相似卷宗查找外掛提供專屬的、完整的 GUI 設定介面
# 版本：2.3 (修正 UI 控件狀態更新的引用方式)
# ======================================================================

import tkinter as tk
from tkinter import ttk

def create_settings_frame(parent_frame, config, ui_vars):
    """創建此外掛的專屬設定 UI 元件"""

    # --- 設定項 0: 是否啓用數量限制 ---
    ui_vars['plugin_enable_sample_limit'] = tk.BooleanVar()
    cb = ttk.Checkbutton(parent_frame, text="啓用末尾圖片取樣限制", variable=ui_vars['plugin_enable_sample_limit'])
    cb.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 5))

    # --- 設定項 1: 取樣數量 ---
    ttk.Label(parent_frame, text="末尾圖片取樣數:").grid(row=1, column=0, sticky="w", pady=2)
    ui_vars['plugin_sample_count'] = tk.StringVar()
    spinbox1 = ttk.Spinbox(parent_frame, from_=2, to=100, textvariable=ui_vars['plugin_sample_count'], width=5)
    spinbox1.grid(row=1, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="(以此數量建立指紋)").grid(row=1, column=2, sticky="w")
    # 【修正】直接保存控件引用
    ui_vars['plugin_sample_count_spinbox'] = spinbox1

    # --- 設定項 2: 匹配閾值 ---
    ttk.Label(parent_frame, text="指紋匹配閾值:").grid(row=2, column=0, sticky="w", pady=2)
    ui_vars['plugin_match_threshold'] = tk.StringVar()
    spinbox2 = ttk.Spinbox(parent_frame, from_=2, to=100, textvariable=ui_vars['plugin_match_threshold'], width=5)
    spinbox2.grid(row=2, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="(指紋中至少 N 張相似即匹配)").grid(row=2, column=2, sticky="w")
    
    # --- 聯動 UI 狀態 ---
    def toggle_spinbox_state(*args):
        state = tk.NORMAL if ui_vars['plugin_enable_sample_limit'].get() else tk.DISABLED
        if 'plugin_sample_count_spinbox' in ui_vars:
            ui_vars['plugin_sample_count_spinbox'].config(state=state)

    ui_vars['plugin_enable_sample_limit'].trace_add("write", toggle_spinbox_state)
    
    return parent_frame

def load_settings(config, ui_vars):
    """將設定值載入到 UI 變量中"""
    ui_vars['plugin_enable_sample_limit'].set(config.get('plugin_enable_sample_limit', True))
    ui_vars['plugin_sample_count'].set(config.get('plugin_sample_count', 12))
    ui_vars['plugin_match_threshold'].set(config.get('plugin_match_threshold', 8))
    
    # 手動觸發一次狀態更新
    state = tk.NORMAL if ui_vars['plugin_enable_sample_limit'].get() else tk.DISABLED
    if 'plugin_sample_count_spinbox' in ui_vars:
        ui_vars['plugin_sample_count_spinbox'].config(state=state)

def save_settings(config, ui_vars):
    """從 UI 變量中讀取值並存入 config 字典"""
    if 'plugin_enable_sample_limit' in ui_vars:
        config['plugin_enable_sample_limit'] = ui_vars['plugin_enable_sample_limit'].get()
    
    try:
        if 'plugin_sample_count' in ui_vars:
            config['plugin_sample_count'] = int(ui_vars['plugin_sample_count'].get())
    except (ValueError, KeyError, tk.TclError):
        config.setdefault('plugin_sample_count', 12)
        
    try:
        if 'plugin_match_threshold' in ui_vars:
            config['plugin_match_threshold'] = int(ui_vars['plugin_match_threshold'].get())
    except (ValueError, KeyError, tk.TclError):
        config.setdefault('plugin_match_threshold', 8)
        
    return config