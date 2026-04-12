# ======================================================================
# 檔案：plugins/maintenance_workflow/processor.py
# 目的：自動化維護套餐 (檢查資料庫 -> 同步啟動 EMM & 掃描廣告)
# 版本：1.6.0 (邏輯修正：防止 start_scan 觸發二次前置處理，避免 EMM 被誤關)
# ======================================================================

import sys
import os
import datetime
import subprocess
import time
from tkinter import messagebox
from plugins.base_plugin import BasePlugin
from utils import log_info, log_warning, save_config

# 嘗試導入 psutil 用於檢查進程
try:
    import psutil
except ImportError:
    psutil = None

class MaintenanceWorkflowPlugin(BasePlugin):
    def get_id(self) -> str: return "maintenance_workflow"
    def get_name(self) -> str: return "自動維護套餐腳本"
    def get_description(self) -> str: return "配合 --maintenance 參數，執行：資料庫同步 -> (若有新書) 廣告掃描 -> 開啟 EMM。"
    
    def get_plugin_type(self) -> str: return 'script'
    
    def on_app_ready(self, app):
        # 檢查啟動參數是否有 --maintenance
        if "--maintenance" in sys.argv:
            log_info("[自動維護] 偵測到維護參數，準備執行套餐...")
            # 延遲執行，確保 UI 已經完全載入
            app.after(1000, lambda: self.run_maintenance_sequence(app))

    def run_maintenance_sequence(self, app):
        app.status_label.config(text="🤖 [自動維護] 套餐執行中...")
        
        # 1. 呼叫 eh_database_tools 外掛
        eh_plugin = app.plugin_manager.get('eh_database_tools')
        if not eh_plugin:
            messagebox.showerror("錯誤", "找不到 eh_database_tools 外掛，無法執行維護套餐。")
            return

        log_info("[自動維護] 步驟 1/3: 執行資料庫同步...")
        
        # 執行同步
        summary = eh_plugin.run(app.config, app.scan_queue, {'cancel': app.cancel_event, 'pause': app.pause_event})
        
        added_count = 0
        ui_automated = False
        if summary:
            if hasattr(summary, 'added'): added_count = summary.added
            if hasattr(summary, 'tasks_processed'): ui_automated = (summary.tasks_processed > 0)
            log_info(f"[自動維護] 資料庫同步完成。新增本子數: {added_count}, 是否已執行UI自動化: {ui_automated}")
        
        # 2. 判斷分支
        if added_count == 0:
            log_info("[自動維護] 沒有新本子，跳過廣告掃描。")
            if not ui_automated:
                self._launch_emm(app)
            else:
                log_info("[自動維護] EMM 已由資料庫工具開啟。")
            app.after(1000, app.destroy)
        else:
            log_info("[自動維護] 發現新本子，步驟 2/3: 準備廣告掃描...")
            self._setup_and_start_ad_scan(app, already_launched=ui_automated)

    def _is_emm_running(self, emm_path):
        """檢查 EMM 是否已經在執行中"""
        if not psutil or not emm_path: return False
        target_name = os.path.basename(emm_path).lower()
        try:
            for proc in psutil.process_iter(['name']):
                try:
                    if proc.info['name'] and proc.info['name'].lower() == target_name:
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception:
            pass
        return False

    def _launch_emm(self, app):
        """以「獨立進程」方式啟動 EMM"""
        emm_path = app.config.get('eh_manga_manager_path')
        
        if not emm_path or not os.path.exists(emm_path):
            log_warning(f"找不到 EMM 路徑 ({emm_path})，無法自動啟動。請檢查設定。")
            return

        # 防止雙重啟動
        if self._is_emm_running(emm_path):
            log_info(f"[自動維護] EMM 已經在執行中，跳過啟動步驟。")
            return

        log_info(f"[自動維護] 正在獨立啟動 EMM: {emm_path}")
        try:
            DETACHED_PROCESS = 0x00000008
            subprocess.Popen(
                [emm_path],
                cwd=os.path.dirname(emm_path),
                creationflags=DETACHED_PROCESS,
                close_fds=True,
                shell=False
            )
        except Exception as e:
            log_warning(f"啟動 EMM 失敗: {e}")

    def _setup_and_start_ad_scan(self, app, already_launched=False):
        # 3. 設定時間與掃描
        
        today = datetime.date.today()
        today_str = today.strftime("%Y-%m-%d")

        # 智慧時間窗口：兩個月前的 1 號
        target_month = today.month - 2
        target_year = today.year
        if target_month < 1:
            target_month += 12
            target_year -= 1
            
        try:
            start_date_obj = datetime.date(target_year, target_month, 1)
            start_date_str = start_date_obj.strftime("%Y-%m-%d")
        except ValueError:
            start_date_str = "2025-10-01"

        log_info(f"[自動維護] 設定掃描區間: {start_date_str} ~ {today_str} (近兩個月)")

        app.config['comparison_mode'] = 'ad_comparison'
        app.config['enable_time_filter'] = True
        app.config['start_date_filter'] = start_date_str
        app.config['end_date_filter'] = today_str
        
        # --- 關鍵修正：暫時禁用前置處理器 ---
        # 因為我們剛剛已經手動跑過 eh_database_tools 了
        # 如果不設為 False，start_scan 會再跑一次，導致 EMM 被關閉重開
        app.config['enable_eh_database_tools'] = False
        log_info("[自動維護] 已暫時禁用主程式的前置處理觸發 (避免重複執行)。")
        # ----------------------------------
        
        # 啟動掃描 (非同步)
        app.start_scan()
        
        # 處理 EMM 啟動
        if not already_launched:
            log_info("[自動維護] 掃描已啟動，確認 EMM 狀態...")
            self._launch_emm(app)
        else:
            log_info("[自動維護] EMM 已在背景運行，跳過重啟步驟。")

    def run(self, *args, **kwargs):
        return None