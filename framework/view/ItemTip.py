import tkinter as tk
from tkinter import ttk
from model import Statistics
import os
import logging
from utils import logging_config

logger = logging_config.setup_logging(log_level=logging.INFO)

def hide_when_false(func):
    def wrapper(self, *args, **kwargs):
        if func(self, *args, **kwargs) is False:
            self.hide()
    return wrapper

# 工具提示类，用于显示鼠标悬停时的提示信息
class ItemTip:
    def __init__(self, master):
        """初始化工具提示类（单Toplevel实例）"""
        self.master = master
        self.itemtipview = None
        self.label = None
        self.visible = False
        self.delay = 500  # 显示延迟(毫秒)
        self.hide_id = None
        self.current_item_name = None  # 当前显示的项目名称
        
    def _create_ItemTipView(self):
        """创建工具提示窗口（只执行一次）"""
        if self.itemtipview is None:
            self.itemtipview = tk.Toplevel(self.master)
            self.itemtipview.wm_overrideredirect(True)
            self.itemtipview.withdraw()  # 初始隐藏
            
            # 创建内容标签
            self.label = ttk.Label(
                self.itemtipview, 
                background='#FFFFE0', 
                relief='solid', 
                borderwidth=1,
                padding=(5, 3)
            )
            self.label.pack()
            
            # 绑定事件
            self.itemtipview.bind('<Leave>', self._on_leave)
            self.itemtipview.bind('<Enter>', self._on_enter)
    
    @hide_when_false
    def show(self, item_name, auto_hide:bool=False, x=None, y=None)->bool:
        """显示工具提示"""
        if not self.itemtipview:
            self._create_ItemTipView()

        logger.debug(f"Show itemtipview, item_name: {item_name}, auto_hide: {auto_hide}")  # 调试信息，确保显示被调用
        content = None
        # 更新内容
        if item_name.startswith("backup_") or item_name.endswith(".zip"):
            #content = Statistics.get_backup_status(item_name)
            return False
        elif item_name.startswith("blobs"):
            content = Statistics.get_blob(os.path.basename(item_name))
        elif item_name.startswith("manifests"):
            #content = Statistics.get_manifest(item_name)
            return False
        elif item_name:  # 模型名称，显示模型的status信息
            content = Statistics.get_model(item_name)
            #return False
        
        if content is None:  # 内容为空，不显示
            logger.debug(f"ItemTip: {item_name} content is None")
            return False
        # 取消任何待执行的隐藏操作
        if self.hide_id:
            self.master.after_cancel(self.hide_id)
            self.hide_id = None        
        self.label.config(text=content)
        self.current_item_name = item_name
        
        # 更新位置
        if x is None or y is None:
            x = self.master.winfo_pointerx() + 10
            y = self.master.winfo_pointery() + 10
        self.itemtipview.wm_geometry(f"+{x}+{y}")
        
        logger.debug(f"self.visible: {self.visible}")
        # 显示窗口
        if not self.visible:
            logger.debug("ReShow itemtipview")  # 调试信息，确保显示被调用
            self.itemtipview.deiconify()
            self.visible = True
        
        # 设置自动隐藏
        if auto_hide:
            self._schedule_hide()
        
        return True
    
    def _on_enter(self, event=None):
        """鼠标进入工具提示时取消自动隐藏"""
        if self.hide_id:
            self.master.after_cancel(self.hide_id)
            self.hide_id = None
    
    def _on_leave(self, event=None):
        """鼠标离开工具提示时隐藏"""
        self.hide()
    
    def _schedule_hide(self):
        """安排自动隐藏"""
        self.hide_id = self.master.after(3000, self.hide)  # 3秒后自动隐藏
    
    def hide(self, event=None):
        """隐藏工具提示"""
        if self.visible and self.itemtipview:
            if self.hide_id:
                self.master.after_cancel(self.hide_id)
                self.hide_id = None
            logger.debug("Hide itemtipview")  # 调试信息，确保隐藏被调用
            logger.debug(f"Window exists: {self.itemtipview.winfo_exists()}")
            #state = self.itemtipview.state()
            #viewable = self.itemtipview.winfo_viewable()
            #logger.debug(f"Window state: {state}, viewable: {viewable}")

            self.itemtipview.wm_attributes("-topmost", False)
            self.itemtipview.withdraw()
            #self.itemtipview.lower()  # 降低到最底层
            #self.itemtipview.update()  # 强制立即更新界面
            self.visible = False
            self.current_item_name = None
    
    def destroy(self):
        """销毁工具提示"""
        try:
            if self.itemtipview:
                self.hide()
                self.itemtipview.destroy()
                self.itemtipview = None
        except:
            pass