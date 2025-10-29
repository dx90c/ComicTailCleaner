# ======================================================================
# 檔案：plugins/eh_database_tools/plugin_gui.py
# 模組：exhentai-manga-manager 資料庫更新工具 ─ 設定介面（無 Notebook，一體化）
# 版本：2.3 (相容 gui.py 1.8.4+ 的智慧型啟用 API)
# ======================================================================

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, filedialog
from typing import Dict, Any, Optional, List, Tuple

def get_name() -> str:
    return "資料庫更新工具"

def _ask_file(parent: ttk.Frame, var: tk.StringVar, title: str,
              filetypes: Optional[List[Tuple[str, str]]] = None) -> None:
    p = filedialog.askopenfilename(
        parent=parent, title=title,
        filetypes=filetypes or [("All files", "*.*")]
    )
    if p: var.set(p)

def _ask_dir(parent: ttk.Frame, var: tk.StringVar, title: str) -> None:
    p = filedialog.askdirectory(parent=parent, title=title)
    if p: var.set(p)

def _set_children_state(widget: tk.Widget, state: str) -> None:
    try:
        widget.configure(state=state)
    except tk.TclError:
        pass
    for child in widget.winfo_children():
        _set_children_state(child, state)

def create_settings_frame(parent_frame: ttk.Frame,
                          config: Dict[str, Any],
                          ui_vars: Dict[str, tk.Variable]) -> Optional[ttk.Frame]:
    plugin_id = "eh_database_tools"

    frame = ttk.Frame(parent_frame)
    frame.columnconfigure(1, weight=1)

    # === MODIFICATION START: 標準化啟用變數 ===

    # 1. 使用主 GUI 期望的標準化鍵名
    enable_key = f"enable_{plugin_id}"
    
    # 2. 從 ui_vars 取得或建立共用的 tk.BooleanVar
    #    這確保了主 GUI 和外掛操作的是同一個變數實體
    enable_var = ui_vars.get(enable_key)
    if not isinstance(enable_var, tk.BooleanVar):
        # 從 config 讀取初始值，此處外掛預設為啟用
        initial_value = config.get(enable_key, True)
        enable_var = tk.BooleanVar(value=initial_value)
        ui_vars[enable_key] = enable_var
    
    # === MODIFICATION END ===

    get = config.get
    # 其他變數維持原狀
    ui_vars[f"{plugin_id}_data_dir"]            = tk.StringVar(value=get('eh_data_directory', ''))
    ui_vars[f"{plugin_id}_backup_dir"]          = tk.StringVar(value=get('eh_backup_directory', ''))
    ui_vars[f"{plugin_id}_quarantine_path"]     = tk.StringVar(value=get('eh_quarantine_path', '_EMPTY_FOLDERS_QUARANTINE'))
    ui_vars[f"{plugin_id}_syringe_dir"]         = tk.StringVar(value=get('eh_syringe_directory', ''))
    ui_vars[f"{plugin_id}_mmd_json_path"]       = tk.StringVar(value=get('eh_mmd_json_path', ''))
    ui_vars[f"{plugin_id}_csv_path"]            = tk.StringVar(value=get('eh_csv_path', 'download_dashboard.csv'))
    ui_vars[f"{plugin_id}_automation_enabled"]  = tk.BooleanVar(value=get('automation_enabled', False))
    ui_vars[f"{plugin_id}_manga_manager_path"]  = tk.StringVar(value=get('eh_manga_manager_path', ''))
    ui_vars[f"{plugin_id}_automation_speed"]    = tk.StringVar(value=(get('automation_speed', 'fast') or 'fast'))

    # 將啟用勾選框綁定到標準化的共用變數
    chk_enable = ttk.Checkbutton(
        frame,
        text="啟用exhentai-manga-manager資料庫更新工具",
        variable=enable_var
    )
    chk_enable.grid(row=0, column=0, columnspan=3, sticky="w", padx=2, pady=(2, 4))

    box = ttk.Frame(frame)
    box.grid(row=1, column=0, columnspan=3, sticky="ew")
    for c in (0, 1, 2):
        box.columnconfigure(c, weight=1 if c == 1 else 0)

    desc = ttk.Label(
        box,
        text="進行exhentai-manga-manager的資料庫更新、取得元數據，並對MahoMangaDownloader下載列表進行整理，生成CSV並標註作者及社團的羅馬拼音。",
        anchor="w", justify="left", foreground="#000"
    )
    desc.grid(row=0, column=0, columnspan=3, sticky="ew", padx=2, pady=(0, 6))
    def _wrap(e): desc.configure(wraplength=max(200, e.width - 8))
    desc.bind("<Configure>", _wrap)

    r = 1
    def add_row(label_text: str, var: tk.StringVar, browse: Optional[str] = None, filetypes: Optional[List[Tuple[str, str]]] = None) -> None:
        nonlocal r
        ttk.Label(box, text=label_text).grid(row=r, column=0, sticky="w", padx=4, pady=1)
        ent = ttk.Entry(box, textvariable=var)
        ent.grid(row=r, column=1, sticky="ew", padx=4, pady=1)
        if browse == "file":
            ttk.Button(box, text="瀏覽…", command=lambda: _ask_file(box, var, f"選擇 {label_text}", filetypes)).grid(row=r, column=2, sticky="w", padx=4, pady=1)
        elif browse == "dir":
            ttk.Button(box, text="瀏覽…", command=lambda: _ask_dir(box, var, f"選擇 {label_text}")).grid(row=r, column=2, sticky="w", padx=4, pady=1)
        r += 1

    add_row("exhentai-manga-manager 資料庫資料夾：", ui_vars[f"{plugin_id}_data_dir"], browse="dir")
    add_row("備份儲存資料夾：", ui_vars[f"{plugin_id}_backup_dir"], browse="dir")
    add_row("空資料夾隔離區：", ui_vars[f"{plugin_id}_quarantine_path"], browse="dir")
    add_row("EhTag DB 資料夾：", ui_vars[f"{plugin_id}_syringe_dir"], browse="dir") #DB去這裡下載 https://github.com/EhTagTranslation/Database/releases
    add_row("MMD JSON 檔案路徑：", ui_vars[f"{plugin_id}_mmd_json_path"], browse="file", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
    add_row("CSV 儀表板路徑：", ui_vars[f"{plugin_id}_csv_path"], browse="file", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])

    auto_var = ui_vars[f"{plugin_id}_automation_enabled"]
    ttk.Checkbutton(box, text="啟用 UI 自動化更新元數據", variable=auto_var).grid(row=r, column=0, sticky="w", padx=4, pady=(6, 2))
    ttk.Label(box, text="速度檔位：").grid(row=r, column=1, sticky="e", padx=(0, 4), pady=(6, 2))
    speed_cb = ttk.Combobox(box, width=10, state="readonly", values=["fast", "normal", "safe"], textvariable=ui_vars[f"{plugin_id}_automation_speed"])
    speed_cb.grid(row=r, column=2, sticky="w", padx=4, pady=(6, 2))
    r += 1

    ttk.Label(box, text="exhentai-manga-manager 執行檔：").grid(row=r, column=0, sticky="w", padx=24, pady=1)
    ent_mgr = ttk.Entry(box, textvariable=ui_vars[f"{plugin_id}_manga_manager_path"])
    ent_mgr.grid(row=r, column=1, sticky="ew", padx=4, pady=1)
    ttk.Button(box, text="瀏覽…", command=lambda: _ask_file(box, ui_vars[f"{plugin_id}_manga_manager_path"], "選擇執行檔", [("Executable files", "*.exe"), ("All files", "*.*")])).grid(row=r, column=2, sticky="w", padx=4, pady=1)

    def _toggle_auto(*_):
        st = tk.NORMAL if auto_var.get() else tk.DISABLED
        ent_mgr.configure(state=st)
        speed_cb.configure(state=("readonly" if auto_var.get() else "disabled"))
    auto_var.trace_add("write", _toggle_auto)
    _toggle_auto()

    def _toggle_enable(*_):
        # 讀取標準化的共用變數
        state = tk.NORMAL if enable_var.get() else tk.DISABLED
        _set_children_state(box, state)
        # 確保描述標籤永遠可見，不受 state 影響
        try: desc.configure(state=tk.NORMAL)
        except tk.TclError: pass
        
    # 將切換函式綁定到標準化的共用變數
    enable_var.trace_add("write", _toggle_enable)
    _toggle_enable()

    # 將建立的 UI 容器 pack 到父容器中，使其可見
    frame.pack(fill="x", expand=True)
    return frame

def save_settings(config: Dict[str, Any], ui_vars: Dict[str, tk.Variable]) -> Dict[str, Any]:
    pid = "eh_database_tools"
    
    # === MODIFICATION: 使用標準化鍵名保存設定 ===
    enable_key = f"enable_{pid}"
    if enable_key in ui_vars:
        config[enable_key] = bool(ui_vars[enable_key].get())
    # === MODIFICATION END ===
    
    config['eh_data_directory']        = ui_vars[f"{pid}_data_dir"].get().strip()
    config['eh_backup_directory']      = ui_vars[f"{pid}_backup_dir"].get().strip()
    config['eh_quarantine_path']       = ui_vars[f"{pid}_quarantine_path"].get().strip()
    config['eh_syringe_directory']     = ui_vars[f"{pid}_syringe_dir"].get().strip()
    config['eh_mmd_json_path']         = ui_vars[f"{pid}_mmd_json_path"].get().strip()
    config['eh_csv_path']              = ui_vars[f"{pid}_csv_path"].get().strip()
    config['automation_enabled']       = bool(ui_vars[f"{pid}_automation_enabled"].get())
    config['automation_speed']         = (ui_vars[f"{pid}_automation_speed"].get() or "fast").lower()
    config['eh_manga_manager_path']    = ui_vars[f"{pid}_manga_manager_path"].get().strip()
    return config

# 為了讓主程式能正確找到函式，維持此賦值
get_settings_frame = create_settings_frame