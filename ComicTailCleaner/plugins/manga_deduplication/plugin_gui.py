# ======================================================================
# 檔案：plugins/manga_deduplication/plugin_gui.py
# 目的：為相似卷宗查找外掛提供專屬的 GUI 設定介面
# 版本：3.0 (新增：跨名重複偵測開關與重複判定門檻)
# ======================================================================

import tkinter as tk
from tkinter import ttk

# ── 設定 Key 常數 ───────────────────────────────────────────────────
KEY_ENABLE_LIMIT  = 'manga_dedupe_enable_sample_limit'
KEY_SAMPLE_COUNT  = 'manga_dedupe_sample_count'
KEY_MATCH_THRESH  = 'manga_dedupe_match_threshold'
KEY_CROSS_LANG    = 'manga_dedupe_cross_lang'
KEY_DUP_THRESHOLD = 'manga_dedupe_dup_threshold'


def create_settings_frame(parent_frame, config, ui_vars):
    """創建此外掛的專屬設定 UI 元件"""

    # ── 設定項 0：是否啟用取樣數量限制 ────────────────────────────
    ui_vars[KEY_ENABLE_LIMIT] = tk.BooleanVar()
    cb_limit = ttk.Checkbutton(
        parent_frame,
        text="啟用末尾圖片取樣限制",
        variable=ui_vars[KEY_ENABLE_LIMIT]
    )
    cb_limit.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 5))

    # ── 設定項 1：取樣數量 ──────────────────────────────────────────
    ttk.Label(parent_frame, text="末尾圖片取樣數:").grid(row=1, column=0, sticky="w", pady=2)
    ui_vars[KEY_SAMPLE_COUNT] = tk.StringVar()
    spinbox_sample = ttk.Spinbox(
        parent_frame, from_=2, to=100,
        textvariable=ui_vars[KEY_SAMPLE_COUNT], width=5
    )
    spinbox_sample.grid(row=1, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="(以此數量建立指紋)").grid(row=1, column=2, sticky="w")

    # ── 設定項 2：指紋匹配閾值（同系列組內比對）───────────────────
    ttk.Label(parent_frame, text="指紋匹配閾值:").grid(row=2, column=0, sticky="w", pady=2)
    ui_vars[KEY_MATCH_THRESH] = tk.StringVar()
    spinbox_match = ttk.Spinbox(
        parent_frame, from_=2, to=100,
        textvariable=ui_vars[KEY_MATCH_THRESH], width=5
    )
    spinbox_match.grid(row=2, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="(指紋中至少 N 張相似即匹配)").grid(row=2, column=2, sticky="w")

    # ────────────────────────── 分隔線 ──────────────────────────────
    ttk.Separator(parent_frame, orient="horizontal").grid(
        row=3, column=0, columnspan=3, sticky="ew", pady=8
    )

    # ── 設定項 3（新增）：啟用跨名重複偵測（異名副本識別）──────────
    ui_vars[KEY_CROSS_LANG] = tk.BooleanVar()
    cb_cross = ttk.Checkbutton(
        parent_frame,
        text="啟用跨名重複偵測（識別日/中/英名稱不同但內容相同的副本）",
        variable=ui_vars[KEY_CROSS_LANG]
    )
    cb_cross.grid(row=4, column=0, columnspan=3, sticky="w", pady=(0, 5))

    # ── 設定項 4（新增）：重複判定的最低相似頁面比例 ───────────────
    ttk.Label(parent_frame, text="副本判定門檻:").grid(row=5, column=0, sticky="w", pady=2)
    ui_vars[KEY_DUP_THRESHOLD] = tk.StringVar()
    spinbox_dup = ttk.Spinbox(
        parent_frame, from_=10, to=100,
        textvariable=ui_vars[KEY_DUP_THRESHOLD], width=5
    )
    spinbox_dup.grid(row=5, column=1, sticky="w", padx=5)
    ttk.Label(parent_frame, text="% 取樣頁相似才判定為重複副本").grid(row=5, column=2, sticky="w")

    # ── 聯動 UI 狀態 ──────────────────────────────────────────────
    def toggle_sample_spinbox(*args):
        state = tk.NORMAL if ui_vars[KEY_ENABLE_LIMIT].get() else tk.DISABLED
        spinbox_sample.config(state=state)

    def toggle_dup_spinbox(*args):
        state = tk.NORMAL if ui_vars[KEY_CROSS_LANG].get() else tk.DISABLED
        spinbox_dup.config(state=state)

    ui_vars[KEY_ENABLE_LIMIT].trace_add("write", toggle_sample_spinbox)
    ui_vars[KEY_CROSS_LANG].trace_add("write", toggle_dup_spinbox)

    # ── 載入設定值 ─────────────────────────────────────────────────
    ui_vars[KEY_ENABLE_LIMIT].set(config.get(KEY_ENABLE_LIMIT, True))
    ui_vars[KEY_SAMPLE_COUNT].set(config.get(KEY_SAMPLE_COUNT, 12))
    ui_vars[KEY_MATCH_THRESH].set(config.get(KEY_MATCH_THRESH, 8))
    ui_vars[KEY_CROSS_LANG].set(config.get(KEY_CROSS_LANG, True))
    ui_vars[KEY_DUP_THRESHOLD].set(config.get(KEY_DUP_THRESHOLD, 80))

    # 觸發初始狀態
    toggle_sample_spinbox()
    toggle_dup_spinbox()

    return parent_frame


def save_settings(config, ui_vars):
    """從 UI 變量中讀取值並存入 config 字典"""

    if KEY_ENABLE_LIMIT in ui_vars:
        config[KEY_ENABLE_LIMIT] = ui_vars[KEY_ENABLE_LIMIT].get()

    if KEY_CROSS_LANG in ui_vars:
        config[KEY_CROSS_LANG] = ui_vars[KEY_CROSS_LANG].get()

    for key, default in [
        (KEY_SAMPLE_COUNT,  12),
        (KEY_MATCH_THRESH,  8),
        (KEY_DUP_THRESHOLD, 80),
    ]:
        try:
            if key in ui_vars:
                config[key] = int(ui_vars[key].get())
        except (ValueError, KeyError):
            config.setdefault(key, default)

    return config