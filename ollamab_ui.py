import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import logging
from utils.logging_config import setup_logging
import os
import threading
import traceback
from ollamab_controller import BackupController
from models import (
    ModelBackupStatus, 
    ModelData, 
    ModelObserver,
    LLMModel,
    ProcessStatus,
    ProcessEvent
)
from theme import Theme, StyleConfigurator
import queue
from threading import Thread
from pathlib import Path


# 初始化日志配置
logger = setup_logging(log_level=logging.INFO)

from ctypes import windll
windll.shcore.SetProcessDpiAwareness(1)  # 解决高DPI缩放问题

class BackupApp:
    def __init__(self, master):
        self.master = master        
        master.title("Ollama模型备份工具")
        master.geometry("1200x800")

        # 状态符号配置
        self.CHECKED_SYMBOL = '[Y]'
        self.UNCHECKED_SYMBOL = '[ ]'
        self.BACKUPED_SYMBOL = '[已备份]'
        self.CHECKING_SYMBOL = '[校验中]'
        self.BACKUPED_ERROR_SYMBOL = '[异常]'

        self.default_backup_path = r"F:\llm_models\ollama_modes_backup"
        self.default_model_path = r"F:\llm_models\ollama_modes"
        # 环境变量检测
        self.model_path = os.getenv("OLLAMA_MODELS")
        if not self.model_path or not os.path.exists(self.model_path):
            self.model_path = self.default_model_path
        self.backup_path = os.getenv("OLLAMA_BACKUP_PATH")
        if not self.backup_path:
            self.backup_path = self.default_backup_path
        
        self.controller = BackupController(self.model_path, self.backup_path)
        # 初始化数据模型和观察者
        self.model_data = ModelData()
        self.item_count = 0
        self.uiHandler = UIUpdateHandler(self)
        self.observer = Obeserver(self.uiHandler)
        self.model_data.add_observer(self.observer)

        # 初始化缓存锁和模型缓存
        self.cache_lock = threading.Lock()
        self.model_cache = {}

        # 初始化UI组件
        self.create_widgets()
        #self.configure_style_warm()
        StyleConfigurator.configure_style(self, Theme.WARM)
        # 初始化数据内容
        self.controller.start_async_loading()    

    def create_widgets(self)->None:
        # 主框架
        main_frame = ttk.Frame(self.master)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 控制面板
        control_frame = ttk.Frame(main_frame, style='TFrame')
        control_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

        # 模型路径控件组（第一行）
        model_path_frame = ttk.Frame(control_frame)
        model_path_frame.pack(fill=tk.X, pady=(0,5))

        ttk.Label(model_path_frame, text="模型路径:").pack(side=tk.LEFT, padx=(0,5))
        self.model_path_var = tk.StringVar(value=self.model_path)
        self.model_path_var.trace_add('write', lambda *_: self._update_model_path())
        ttk.Entry(model_path_frame, 
                textvariable=self.model_path_var,
                style='TEntry').pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0,5))

        ttk.Button(model_path_frame, 
                text="选择路径",
                command=self.choose_model_dir,
                style='TButton').pack(side=tk.LEFT)

        # 备份路径控件组（第二行）
        backup_path_frame = ttk.Frame(control_frame)
        backup_path_frame.pack(fill=tk.X)

        ttk.Label(backup_path_frame, text="备份路径:").pack(side=tk.LEFT, padx=(0,5))
        self.backup_path_var = tk.StringVar(value=self.default_backup_path)
        self.backup_path_var.trace_add('write', lambda *_: self._update_backup_path())
        ttk.Entry(backup_path_frame, 
                textvariable=self.backup_path_var,
                style='TEntry').pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0,5))

        self.backup_btn = ttk.Button(backup_path_frame, 
                                text="开始备份", 
                                command=self.start_backup,
                                style='Accent.TButton')
        self.backup_btn.pack(side=tk.RIGHT)

        # 添加分隔线增强视觉区分
        #ttk.Separator(control_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)

        # 信息面板容器
        info_panel = ttk.PanedWindow(main_frame, orient=tk.VERTICAL, style='TPanedwindow')
        info_panel.pack(fill=tk.BOTH, expand=True)

        # 模型树形面板
        tree_frame = ttk.Frame(info_panel)
        self.tree = ttk.Treeview(tree_frame, columns=('selected', '_padding'), show='tree headings', 
                                selectmode='extended', style="Treeview")
        # 隐藏多余的列（避免显示填充列）
        self.tree['displaycolumns'] = ('selected',)

        self.tree.heading('#0', text='模型名称', anchor=tk.W)
        self.tree.column('#0', width=670, anchor=tk.W, stretch=True)
        self.tree.heading('selected', text='备份', anchor=tk.E)
        self.tree.column('selected', width=80, anchor=tk.E, stretch=False)

        # 添加填充列配置（确保右对齐列能固定在右侧）
        self.tree.column('_padding', width=0, stretch=True, minwidth=0)

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview, style='Vertical.TScrollbar')
        self.tree.configure(yscroll=scrollbar.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH,  expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        info_panel.add(tree_frame)

        # 绑定事件
        self.tree.bind('<Button-1>', self.toggle_checkbox)
        # 添加选中项变更事件
        # self.tree.bind('<<TreeviewOpen>>', self.update_node_status)

        # 添加状态栏，状态栏绑定变量
        self.status_var = tk.StringVar(value="加载中")
        #self.status_var.trace_add('write', lambda *_: self._update_status_bar())
        self.status_bar = ttk.Label(main_frame, textvariable=self.status_var, anchor=tk.W, style='TLabel')
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)        

    def toggle_checkbox(self, event:any)->None:
        # 获取点击位置的列ID
        region = self.tree.identify("region", event.x, event.y)
        column = self.tree.identify_column(event.x)

        #logger.debug(f"点击位置: {region}, 列: {column}")  # 调试日志，确保正确获取点击位置和列ID
        
        # 仅在第一列（复选框列）响应点击
        if region == 'cell' and column == '#1':
            item = self.tree.identify_row(event.y)
            current = self.tree.item(item, 'values')
            logger.debug(f"当前状态: {current}")  # 调试日志，确保正确获取当前状态
            new_state = self.CHECKED_SYMBOL if current[0] == self.UNCHECKED_SYMBOL else self.UNCHECKED_SYMBOL
            self.tree.item(item, values=(new_state,))
            
            model_name = self.tree.item(item, 'text')
            logger.debug(f"复选框状态更新：{model_name} -> {new_state}")
    
    def _get_backup_value(self, status: ModelBackupStatus)->str:
        if not status:
            return self.CHECKING_SYMBOL

        backuped = status.backup_status
        zip_file = status.zip_file
        if backuped:
            if zip_file:
                return self.BACKUPED_SYMBOL
        elif zip_file:
                return self.BACKUPED_ERROR_SYMBOL
        else:
            return self.UNCHECKED_SYMBOL

    def add_model(self, model: LLMModel)->None:
        logger.debug(f"添加模型: {model.name}")  # 调试日志，确保正确获取模型名称
        value = self.CHECKING_SYMBOL
        item = self.tree.insert('', 'end', text=model.name, values=(value,),
                        tags=('oddrow' if (self.item_count % 2) == 0 else 'evenrow'))
        self.tree.insert(item, 'end', values=('',), 
                        text=model.manifest,
                        tags=('childrow'))
        for digest in model.blobs:
            self.tree.insert(item, 'end', values=('',), text=os.path.join('blobs', digest),
                            tags=('childrow'))
        self.item_count += 1

    def delete_model(self, model: LLMModel)->None:
        found = False
        for item in self.tree.get_children():
            if self.tree.item(item, 'text') == model.name:
                self.tree.delete(item)
                found = True
                break
        if found:
            self.item_count -= 1

    def update_model(self, model: LLMModel)->None:
        found = False
        for item in self.tree.get_children():
            if self.tree.item(item, 'text') == model.name:
                value = self._get_backup_value(model.bk_status)
                self.tree.item(item, values=(value,))
                found = True
                break

    def update_backup_status(self, status: ModelBackupStatus)->None:
        for item in self.tree.get_children():
            if self.tree.item(item, 'text') == status.model_name:
                value = self._get_backup_value(status)
                self.tree.item(item, values=(value,))
                break

    def start_backup(self):
        if not self.backup_path_var.get():
            self.choose_backup_dir()
            if not self.backup_path_var.get():
                return

        selected_models = [
            self.tree.item(item, 'text')
            for item in self.tree.get_children()
            if self.tree.item(item, 'values')[0] == self.CHECKED_SYMBOL
        ]
        logger.debug(f"选中的模型: {selected_models}")  # 调试日志，确保正确获取选中的模型

        if not selected_models:
            messagebox.showwarning("警告", "请选择要备份的模型")
            return

        self.backup_btn.config(state=tk.DISABLED)
        threading.Thread(target=self.run_backup, args=(selected_models,)).start()

    def run_backup(self, models):
        try:
            self.controller.run_backup(models)
        except Exception as e:
            logger.error(f"备份过程中发生错误: \n{traceback.format_exc()}")
            self.thread_safe_messagebox("备份错误", f"备份失败！", "error")
        finally:
            try:
                if self.master and self.master.winfo_exists():
                    self.master.after(0, lambda: self.backup_btn.config(state=tk.NORMAL))
            except:
                pass

    def _update_model_path(self):
        self.model_path = self.model_path_var.get()
        self.controller.chdir_path(self.model_path, self.backup_path)
        self.controller.start_async_loading()

    def _update_backup_path(self):
        self.backup_path = self.backup_path_var.get()
        self.controller.chdir_path(self.model_path, self.backup_path)
        self.controller.async_recheck_backup_status()

    def set_initialized(self, initialized: bool)->None:
        self.initialized = initialized
        if initialized:
            self.status_var.set("就绪")
        else:
            self.status_var.set("加载中...")
    
    def show_process_status(self, status: ProcessStatus)->None:
        if status.event == ProcessEvent.WINDOW_INFO:
            self.thread_safe_messagebox("提示", status.message, "info")
        elif status.event == ProcessEvent.WINDOW_ERR:
            self.thread_safe_messagebox("错误", status.message, "error")
        elif status.event == ProcessEvent.WINDOW_WAR:
            self.thread_safe_messagebox("警告", status.message, "warning")
        else:
            self.status_var.set(status.message)

    def choose_backup_dir(self):
        path = filedialog.askdirectory(title="选择备份模型目录")
        if path:
            path = str(Path(path))
            self.backup_path_var.set(path)
            self.backup_path = path  # 更新实例变量
    
    def choose_model_dir(self):
        """选择模型路径"""
        path = filedialog.askdirectory(title="选择模型存储目录")
        if path:
            path = str(Path(path))
            self.model_path_var.set(path)
            self.model_path = path  # 更新实例变量

    def update_treeview(self):
        self.tree.delete(*self.tree.get_children())
        for i, model in enumerate(self.model_data.models):
            model_name = f"{model['name']}:{model['version']}"
            status = self.model_data.get_backup_status(model_name)
            item = self.tree.insert('', 'end', text=model_name, values=(status,),
                            tags=('oddrow' if (i % 2) == 0 else 'evenrow'))
            self.tree.insert(item, 'end', values=('',), 
                            text=os.path.join('manifests', 'registry.ollama.ai', 'library', model['name'], model['version']),
                            tags=('childrow'))
            for digest in model.get('digests', []):
                self.tree.insert(item, 'end', values=('',), text=os.path.join('blobs', digest),
                                tags=('childrow'))

    def load_models(self):
        # 注册进度更新观察者
        #self.model_data.add_observer(lambda: self.update_progress(self.model_data.loading_progress))
        # 启动异步加载
        self.controller.chdir_path(self.model_path, self.backup_path)
        self.controller.start_async_loading()

    def thread_safe_messagebox(self, title, message, message_type="info"):
        """线程安全的消息框显示"""
        try:
            if self.master and self.master.winfo_exists():
                if message_type == 'info':
                    self.master.after(0, lambda: messagebox.showinfo(title, message))
                elif message_type == 'error':
                    self.master.after(0, lambda: messagebox.showerror(title, message))
                else:
                    self.master.after(0, lambda: messagebox.showwarning(title, message))
        except:
            pass

class UIUpdateHandler:
    def __init__(self, backup_app):
        self.queue = queue.Queue()
        self.backup_app = backup_app
        self.running = True
        Thread(target=self.process_queue, daemon=True).start()
    
    def process_queue(self):
        while self.running:
            try:
                action, payload = self.queue.get(block=True, timeout=1)
                method = getattr(self.backup_app, action)
                if callable(method):
                    method(payload)
            except AttributeError:
                logger.error(f"无效的UI方法: {action}")
            except (queue.Empty, ValueError):
                continue
            except Exception as e:
                logger.error(f"执行 {action} 失败: {e}")
    
class Obeserver(ModelObserver):
    def __init__(self, handler: UIUpdateHandler):
        self.handler = handler
    def notify_add_model(self, model: LLMModel) -> None:
        logger.debug(f"通知添加模型: {model}")
        self.handler.queue.put(("add_model", model))
    def notify_delete_model(self, model: LLMModel) -> None:
        self.handler.queue.put(("delete_model", model))
    def notify_update_model(self, model: LLMModel) -> None:
        self.handler.queue.put(("update_model", model))
    def notify_update_backup_status(self, status: ModelBackupStatus) -> None:
        logger.debug(f"通知更新备份状态: {status}")
        self.handler.queue.put(("update_backup_status", status))
    def notify_initialized(self, initialized: bool) -> None:
        self.handler.queue.put(("set_initialized", initialized))
    def notify_loading_progress(self, progress_status: ProcessStatus) -> None:
        self.handler.queue.put(("show_process_status", progress_status))

if __name__ == "__main__":
    root = tk.Tk()
    app = BackupApp(root)
    root.mainloop()