import tkinter as tk
from typing import Union

class Tooltip:
    """
    通用的滑鼠懸停提示工具 (Tooltip)
    """
    def __init__(self, widget: tk.Widget, text: str):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.id = None
        self.x = self.y = 0
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)

    def enter(self, event: Union[tk.Event, None] = None) -> None:
        self.schedule()

    def leave(self, event: Union[tk.Event, None] = None) -> None:
        self.unschedule()
        self.hidetip()

    def schedule(self) -> None:
        self.unschedule()
        self.id = self.widget.after(500, self.showtip)

    def unschedule(self) -> None:
        id_ = self.id
        self.id = None
        if id_:
            self.widget.after_cancel(id_)

    def showtip(self) -> None:
        if self.tooltip_window:
            return
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 20
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left',
                         background="#ffffe0", relief='solid', borderwidth=1,
                         font=("tahoma", "8", "normal"))
        label.pack(ipadx=1)

    def hidetip(self) -> None:
        tw = self.tooltip_window
        self.tooltip_window = None
        if tw:
            tw.destroy()