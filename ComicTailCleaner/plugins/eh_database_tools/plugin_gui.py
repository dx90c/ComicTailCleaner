# ======================================================================
# 檔案：plugins/eh_database_tools/plugin_gui.py
# 模組：資料庫更新工具設定介面 (v2.11 - 完整設定持久化與超連結)
# ======================================================================

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import urllib.request
import ssl
import webbrowser  # 用於開啟超連結
from typing import Dict, Any, Optional, List, Tuple, Callable

from config import DATA_DIR

# 定義外掛專屬的根目錄
PLUGIN_BASE_DIR = os.path.join(DATA_DIR, "eh_database_tools")

def get_name() -> str:
    return "資料庫更新工具"

def _normalize(path: str) -> str:
    """統一將路徑分隔符轉換為 / """
    if not path: return ""
    return path.replace('\\', '/')

def _ask_file(parent: ttk.Frame, var: tk.StringVar, title: str,
              filetypes: Optional[List[Tuple[str, str]]] = None) -> None:
    p = filedialog.askopenfilename(
        parent=parent, title=title,
        filetypes=filetypes or [("All files", "*.*")]
    )
    if p: var.set(_normalize(p))

def _ask_dir(parent: ttk.Frame, var: tk.StringVar, title: str, callback: Optional[Callable] = None) -> None:
    p = filedialog.askdirectory(parent=parent, title=title)
    if p:
        norm_p = _normalize(p)
        var.set(norm_p)
        if callback:
            callback(norm_p)

def _set_children_state(widget: tk.Widget, state: str) -> None:
    try:
        widget.configure(state=state)
    except tk.TclError:
        pass
    for child in widget.winfo_children():
        _set_children_state(child, state)

def _download_ehtag_db(parent, path_var):
    url = "https://github.com/EhTagTranslation/Database/releases/latest/download/db.ast.json"
    
    target_dir = path_var.get().strip()
    if not target_dir:
        target_dir = os.path.join(PLUGIN_BASE_DIR, "EhTagTranslation")
    
    if not os.path.exists(target_dir):
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as e:
            messagebox.showerror("錯誤", f"無法建立資料夾：\n{e}", parent=parent)
            return

    target_file = os.path.join(target_dir, "db.ast.json")
    
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        btn_top = tk.Toplevel(parent)
        btn_top.geometry("200x60+300+300")
        tk.Label(btn_top, text="正在下載 EhTag DB...", pady=20).pack()
        btn_top.update()

        with urllib.request.urlopen(url, context=ctx) as response, open(target_file, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
        
        btn_top.destroy()
        
        path_var.set(_normalize(target_dir))
        messagebox.showinfo("成功", f"資料庫已下載至：\n{_normalize(target_file)}\n\n路徑已自動填入。", parent=parent)
        
    except Exception as e:
        messagebox.showerror("下載失敗", f"無法從 GitHub 下載資料庫：\n{e}\n\n請檢查網路連線。", parent=parent)

def create_settings_frame(parent_frame: ttk.Frame,
                          config: Dict[str, Any],
                          ui_vars: Dict[str, tk.Variable]) -> Optional[ttk.Frame]:
    plugin_id = "eh_database_tools"

    frame = ttk.Frame(parent_frame)
    frame.columnconfigure(1, weight=1)

    # --- 初始化變數 ---
    enable_key = f"enable_{plugin_id}"
    enable_var = ui_vars.get(enable_key)
    if not isinstance(enable_var, tk.BooleanVar):
        initial_value = config.get(enable_key, True)
        enable_var = tk.BooleanVar(value=initial_value)
        ui_vars[enable_key] = enable_var
    
    get = config.get
    
    def default_path(key, default_subfolder):
        val = get(key, '')
        if not val:
            return _normalize(os.path.join(PLUGIN_BASE_DIR, default_subfolder))
        return _normalize(val)

    # === 1. 讀取設定 (增加 EMM 和 MMD 的根目錄讀取) ===
    
    # 詳細路徑
    var_data_dir = tk.StringVar(value=_normalize(get('eh_data_directory', '')))
    var_mgr_exe  = tk.StringVar(value=_normalize(get('eh_manga_manager_path', '')))
    var_mmd_json = tk.StringVar(value=_normalize(get('eh_mmd_json_path', '')))
    
    # 根目錄路徑 (優先讀取設定，若無則嘗試推算)
    saved_emm_root = get('eh_emm_root_folder', '')
    if not saved_emm_root:
        # 保底推算：如果沒有存過根目錄，就暫時用資料庫目錄
        saved_emm_root = var_data_dir.get()
    var_emm_root = tk.StringVar(value=_normalize(saved_emm_root))

    saved_mmd_root = get('eh_mmd_root_folder', '')
    if not saved_mmd_root and var_mmd_json.get():
        # 保底推算：往上兩層
        try: saved_mmd_root = os.path.dirname(os.path.dirname(var_mmd_json.get()))
        except: pass
    var_mmd_root = tk.StringVar(value=_normalize(saved_mmd_root))

    # 註冊變數到 ui_vars，以便 save_settings 可以存取
    ui_vars[f"{plugin_id}_data_dir"]           = var_data_dir
    ui_vars[f"{plugin_id}_manga_manager_path"] = var_mgr_exe
    ui_vars[f"{plugin_id}_mmd_json_path"]      = var_mmd_json
    ui_vars[f"{plugin_id}_emm_root_folder"]    = var_emm_root # 新增
    ui_vars[f"{plugin_id}_mmd_root_folder"]    = var_mmd_root # 新增
    
    ui_vars[f"{plugin_id}_backup_dir"]      = tk.StringVar(value=default_path('eh_backup_directory', 'Backups'))
    ui_vars[f"{plugin_id}_quarantine_path"] = tk.StringVar(value=default_path('eh_quarantine_path', 'Quarantine'))
    ui_vars[f"{plugin_id}_syringe_dir"]     = tk.StringVar(value=default_path('eh_syringe_directory', 'EhTagTranslation'))
    ui_vars[f"{plugin_id}_csv_path"]        = tk.StringVar(value=default_path('eh_csv_path', 'tagfailed.csv'))
    ui_vars[f"{plugin_id}_automation_enabled"] = tk.BooleanVar(value=get('automation_enabled', False))
    ui_vars[f"{plugin_id}_automation_speed"]   = tk.StringVar(value=(get('automation_speed', 'fast') or 'fast'))

    # 使用字典來儲存 Entry 引用
    widgets = {}

    # --- 自動偵測回呼函式 ---
    def on_emm_root_selected(path):
        var_emm_root.set(path) # 更新顯示
        var_data_dir.set(path) # 資料庫通常在根目錄
        exe_path = os.path.join(path, "exhentai-manga-manager.exe")
        
        if os.path.exists(exe_path):
            var_mgr_exe.set(_normalize(exe_path))
            if 'ent_mgr_auto' in widgets:
                widgets['ent_mgr_auto'].configure(state='readonly')
        else:
            messagebox.showinfo("自動偵測失敗", f"在該資料夾下找不到執行檔。\n請手動瀏覽選擇 'exhentai-manga-manager.exe'。", parent=frame)
            var_mgr_exe.set("")
            if 'ent_mgr_auto' in widgets:
                widgets['ent_mgr_auto'].configure(state='normal')

    def on_mmd_root_selected(path):
        var_mmd_root.set(path) # 更新顯示
        json_path = os.path.join(path, "User_Data", "mmd_List1.json")
        if os.path.exists(json_path):
            var_mmd_json.set(_normalize(json_path))
            if 'ent_json_auto' in widgets:
                widgets['ent_json_auto'].configure(state='readonly')
        else:
            messagebox.showinfo("自動偵測失敗", f"在該資料夾下找不到 'User_Data/mmd_List1.json'。\n請手動瀏覽選擇。", parent=frame)
            var_mmd_json.set("")
            if 'ent_json_auto' in widgets:
                widgets['ent_json_auto'].configure(state='normal')

    # --- UI 建構 ---
    chk_enable = ttk.Checkbutton(frame, text="啟用 ExHentai Manga Manager 資料庫整合外掛", variable=enable_var)
    chk_enable.grid(row=0, column=0, columnspan=3, sticky="w", padx=2, pady=(2, 4))

    box = ttk.Frame(frame)
    box.grid(row=1, column=0, columnspan=3, sticky="ew")
    box.columnconfigure(1, weight=1)

    # --- 文字說明區塊 ---
    # 1. 功能說明
    ttk.Label(
        box,
        text="功能說明：若使用 MahoMangaDownloader，此外掛可讀取其預設下載列表 (mmd_List1.json)\n並寫入資料庫 (database.sqlite)，能顯著加快 ExHentai Manga Manager 的更新速度。",
        justify="left",
        foreground="#107C10"
    ).grid(row=1, column=0, columnspan=3, sticky="w", padx=4, pady=(4, 2))

    # 2. 超連結 (MahoMangaDownloader)
    url = "https://project.zmcx16.moe/?page=mahomangadownloader"
    link_label = ttk.Label(
        box,
        text=f"MahoMangaDownloader : {url}",
        foreground="#107C10",  # 保持綠色風格
        cursor="hand2"         # 滑鼠移上去變成手指形狀
    )
    link_label.grid(row=2, column=0, columnspan=3, sticky="w", padx=4, pady=(0, 2))
    # 綁定左鍵點擊事件 -> 開啟網頁
    link_label.bind("<Button-1>", lambda e: webbrowser.open(url))

    # 3. 操作提示
    ttk.Label(
        box,
        text="請設定以下兩個核心程式的資料夾，其他選項會自動偵測。",
        foreground="blue"
    ).grid(row=3, column=0, columnspan=3, sticky="w", padx=4, pady=(15, 8))

    current_row = 4
    
    def add_section_label(text):
        nonlocal current_row
        ttk.Label(box, text=text, font=("Microsoft JhengHei", 9, "bold")).grid(row=current_row, column=0, columnspan=3, sticky="w", padx=4, pady=(10, 2))
        current_row += 1

    def add_row(label, var, browse_type="dir", callback=None, widget_key=None, read_only=False, extra_btn=None) -> ttk.Entry:
        nonlocal current_row
        ttk.Label(box, text=label).grid(row=current_row, column=0, sticky="w", padx=4, pady=2)
        
        # 這裡根據傳入的 read_only 決定初始狀態
        state = 'readonly' if read_only else 'normal'
        ent = ttk.Entry(box, textvariable=var, state=state)
        ent.grid(row=current_row, column=1, sticky="ew", padx=4, pady=2)
        
        if widget_key:
            widgets[widget_key] = ent

        btn_frame = ttk.Frame(box)
        btn_frame.grid(row=current_row, column=2, sticky="w", padx=4)
        
        if browse_type == "dir":
            cmd = lambda: _ask_dir(box, var, f"選擇 {label}", callback)
        else:
            cmd = lambda: _ask_file(box, var, f"選擇 {label}")

        ttk.Button(btn_frame, text="瀏覽...", command=cmd).pack(side="left")
        
        if extra_btn:
            ttk.Button(btn_frame, text=extra_btn[0], command=extra_btn[1]).pack(side="left", padx=(2, 0))
            
        current_row += 1
        return ent

    # === 區域 1: 核心程式設定 (必填) ===
    add_section_label("1. 核心程式路徑 (由此自動設定)")
    add_row("Exhentai Manga Manager 資料夾：", var_emm_root, browse_type="dir", callback=on_emm_root_selected)
    add_row("MahoMangaDownloader 資料夾：", var_mmd_root, browse_type="dir", callback=on_mmd_root_selected)

    # === 區域 2: 功能設定 ===
    add_section_label("2. 功能設定")
    
    auto_frame = ttk.Frame(box)
    auto_frame.grid(row=current_row, column=0, columnspan=3, sticky="w", padx=4, pady=2)
    ttk.Checkbutton(auto_frame, text="啟用 UI 自動化 (更新 EHM 元數據)", variable=ui_vars[f"{plugin_id}_automation_enabled"]).pack(side="left")
    ttk.Label(auto_frame, text="   速度：").pack(side="left")
    ttk.Combobox(auto_frame, width=8, state="readonly", values=["fast", "normal", "safe"], textvariable=ui_vars[f"{plugin_id}_automation_speed"]).pack(side="left")
    current_row += 1

    add_row("EhTag DB 資料夾：", ui_vars[f"{plugin_id}_syringe_dir"], browse_type="dir", 
            extra_btn=("下載 DB", lambda: _download_ehtag_db(frame, ui_vars[f"{plugin_id}_syringe_dir"])))

    # === 區域 3: 詳細路徑 ===
    add_section_label("3. 詳細路徑 (自動填入，必要時可手動修改)")
    
    # 根據內容判斷是否唯讀
    is_exe_locked = True if var_mgr_exe.get() and os.path.exists(var_mgr_exe.get()) else False
    is_json_locked = True if var_mmd_json.get() and os.path.exists(var_mmd_json.get()) else False

    add_row("EHM 執行檔路徑：", var_mgr_exe, browse_type="file", widget_key='ent_mgr_auto', read_only=is_exe_locked)
    add_row("MMD JSON 路徑：", var_mmd_json, browse_type="file", widget_key='ent_json_auto', read_only=is_json_locked)
    
    add_row("備份儲存路徑：", ui_vars[f"{plugin_id}_backup_dir"], browse_type="dir")
    add_row("CSV 輸出路徑：", ui_vars[f"{plugin_id}_csv_path"], browse_type="file")

    # --- 狀態連動 ---
    def _toggle_enable(*_):
        is_enabled = enable_var.get()
        _set_children_state(box, tk.NORMAL if is_enabled else tk.DISABLED)
        
        # 重新檢查那兩個自動欄位，如果啟用且有值，就要鎖起來
        if is_enabled:
            if var_mgr_exe.get() and os.path.exists(var_mgr_exe.get()):
                if 'ent_mgr_auto' in widgets: widgets['ent_mgr_auto'].configure(state='readonly')
            if var_mmd_json.get() and os.path.exists(var_mmd_json.get()):
                if 'ent_json_auto' in widgets: widgets['ent_json_auto'].configure(state='readonly')

        # 文字與超連結永遠保持可見/可用
        try: 
            for child in box.winfo_children():
                if isinstance(child, ttk.Label):
                    child.configure(state=tk.NORMAL)
        except tk.TclError: pass
        
    enable_var.trace_add("write", _toggle_enable)
    frame.after(10, _toggle_enable)

    frame.pack(fill="x", expand=True)
    return frame

def save_settings(config: Dict[str, Any], ui_vars: Dict[str, tk.Variable]) -> Dict[str, Any]:
    pid = "eh_database_tools"
    enable_key = f"enable_{pid}"
    if enable_key in ui_vars:
        config[enable_key] = bool(ui_vars[enable_key].get())
    
    # 儲存功能路徑
    config['eh_data_directory']        = ui_vars[f"{pid}_data_dir"].get().strip()
    config['eh_backup_directory']      = ui_vars[f"{pid}_backup_dir"].get().strip()
    config['eh_quarantine_path']       = ui_vars[f"{pid}_quarantine_path"].get().strip()
    config['eh_syringe_directory']     = ui_vars[f"{pid}_syringe_dir"].get().strip()
    config['eh_mmd_json_path']         = ui_vars[f"{pid}_mmd_json_path"].get().strip()
    config['eh_csv_path']              = ui_vars[f"{pid}_csv_path"].get().strip()
    config['automation_enabled']       = bool(ui_vars[f"{pid}_automation_enabled"].get())
    config['automation_speed']         = (ui_vars[f"{pid}_automation_speed"].get() or "fast").lower()
    config['eh_manga_manager_path']    = ui_vars[f"{pid}_manga_manager_path"].get().strip()
    
    # 儲存介面上的根目錄設定 (確保下次打開不會重置)
    config['eh_emm_root_folder']       = ui_vars[f"{pid}_emm_root_folder"].get().strip()
    config['eh_mmd_root_folder']       = ui_vars[f"{pid}_mmd_root_folder"].get().strip()
    
    return config

get_settings_frame = create_settings_frame