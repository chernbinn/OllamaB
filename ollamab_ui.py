import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import logging
import os
import threading
from typing import override
from ollamab_controller import BackupController
from models import (
    ModelBackupStatus, 
    ModelData, 
    ModelObserver,
    LLMModel,
    ProcessStatus,
    ProcessEvent,
    Blob
)
from theme import Theme, StyleConfigurator
import queue
from threading import Thread
from pathlib import Path
from utils import (
    logging_config,
    AsyncExecutor,
    MultiKeyDict,
)

# 初始化日志配置
logger = logging_config.setup_logging(log_level=logging.DEBUG, b_log_file=False)

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
        self.BACKUPED_ERROR_SYMBOL = '[备份异常]'
        self.BACKUPED_FAILED_SYMBOL = '[备份失败]'
        self.BACKUPING_SYMBOL = '[备份中]'

        self.default_backup_path = r"F:\llm_models\ollama_modes_backup"
        self.default_model_path = r"F:\llm_models\ollama_modes"
        # 环境变量检测
        self.model_path = os.getenv("OLLAMA_MODELS")
        if not self.model_path or not os.path.exists(self.model_path):
            self.model_path = self.default_model_path
        self.backup_path = os.getenv("OLLAMA_BACKUP_PATH")
        if not self.backup_path:
            self.backup_path = self.default_backup_path

        # 初始化异步执行器
        self.async_executor = AsyncExecutor()

        # 初始化UI组件
        self.create_widgets()
        StyleConfigurator.configure_style(self, Theme.WARM)
        
        # 绑定窗口关闭事件
        master.protocol("WM_DELETE_WINDOW", self.on_close)
        self.master.after(300, self.on_window_ready)  # 0表示尽快执行，但在渲染完成后

    def on_window_ready(self):
        self.async_executor.execute_async("ui_init", self.init, is_long_task=False)
    
    def init(self):
        """ 异步初始化，不可以初始化UI组件。不可以直接刷新UI """
        logger.info("开始初始化...")
        self.tree_items: MultiKeyDict = MultiKeyDict()
        self.data_lock = threading.Lock()
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
        
        # 初始化数据内容
        self.controller.start_async_loading()

    def _release(self):
        self.master.destroy()
        self.uiHandler.running = False
        self.controller.destroy(True)

    def on_close(self):
        """处理窗口关闭事件，释放资源"""  
        process_count = self.controller.get_backupping_count()
        queue_count = self.controller.get_queued_count()
        if (process_count == 0 and queue_count == 0) or not self.model_data.initialized:
            logger.info("没有正在进行的备份或排队备份，直接关闭应用程序。")
            self._release()
        else:
            logger.info(f"请求确认退出应用程序？")
            b_destroy = messagebox.askyesno(
                "确认退出", 
                "是否确认退出应用程序？"
                f"""注意：\n
                    有{process_count}个模型正在备份，
                    有{queue_count}个模型正在排队备份。\n
                    如果您选择退出，所有正在进行的操作将被取消。"""
            )
            if b_destroy:
                logger.info("用户确认退出应用程序。")
                self._release()
            else:
                logger.info("用户取消退出应用程序。")

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
        self.tree = ttk.Treeview(tree_frame, 
                    columns=('size', 'selected', '_padding'), 
                    show='tree headings', 
                    selectmode='extended', style="Treeview")
        # 隐藏多余的列（避免显示填充列）
        self.tree['displaycolumns'] = ('size','selected',)

        self.tree.heading('#0', text='模型名称', anchor=tk.W)
        self.tree.column('#0', width=670, anchor=tk.W, stretch=True)
        self.tree.heading('size', text='占用空间', anchor=tk.E)
        self.tree.column('size', width=100, anchor=tk.E, stretch=True)
        self.tree.heading('selected', text='备份', anchor=tk.E)
        self.tree.column('selected', width=120, anchor=tk.E, stretch=False)

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
        column_id = self.tree.identify_column(event.x)
        column = self._tree_column_name(column_id)

        logger.debug(f"点击位置: {region}, 列: {column_id} {column}")  # 调试日志，确保正确获取点击位置和列ID
        
        # 仅在第一列（复选框列）响应点击
        if region == 'cell' and column == 'selected':
            item = self.tree.identify_row(event.y)    
            old_state = self.tree.set(item, 'selected')
            logger.debug(f"当前状态: {old_state}")  # 调试日志，确保正确获取当前状态
            new_state = self.CHECKED_SYMBOL if old_state == self.UNCHECKED_SYMBOL else self.UNCHECKED_SYMBOL

            if any([
                old_state == self.BACKUPED_ERROR_SYMBOL,
                old_state == self.BACKUPED_SYMBOL,
                old_state == self.CHECKING_SYMBOL,
            ]):
                return

            model_name = self.tree.item(item, 'text')
            if any([
                old_state == self.BACKUPING_SYMBOL,
                old_state == self.CHECKED_SYMBOL,
            ]):
                new_state = self.UNCHECKED_SYMBOL
                if not self.cancle_backup(model_name, old_state):
                    return

            self.tree.set(item, 'selected', value=new_state)            
            logger.debug(f"复选框状态更新：{model_name} -> {new_state}")
    
    def _tree_column_name(self, column_id:str)->str:
        display_columns = self.tree['displaycolumns']
        columns = self.tree['columns']
        try:
            if display_columns and display_columns[0] != '#0':
                # 如果有自定义显示的列
                col_index = int(column_id[1:]) - 1  # '#1' -> 0
                column_name = display_columns[col_index]  if col_index > 0 else '#0'
            else:
                # 标准列顺序
                col_index = int(column_id[1:])  # '#1' -> 1
                column_name = columns[col_index]
        except (IndexError, ValueError) as e:
            logger.error(f"无效的列ID: {column_id}, exception: {e}", exc_info=True)  # 错误日志，确保正确处理无效的列ID
            return None
        return column_name
    
    def _get_backup_value(self, status: ModelBackupStatus)->str:
        if not status:
            return self.CHECKING_SYMBOL

        backuped = status.backup_status
        zip_file = status.zip_file
        if backuped:
            if zip_file:
                return self.BACKUPED_SYMBOL
            else:
                return self.BACKUPING_SYMBOL
        elif zip_file:
            if status.zip_md5:
                return self.BACKUPED_ERROR_SYMBOL                
            elif status.zip_md5:
                return self.BACKUPED_FAILED_SYMBOL
        else:
            return self.UNCHECKED_SYMBOL

    def _add_model(self, model: LLMModel)->None:        
        with self.data_lock:
            logger.debug(f"添加模型: {model.name}")  # 调试日志，确保正确获取模型名称
            value = self.CHECKING_SYMBOL if not model.bk_status else self._get_backup_value(model.bk_status)
            item = self.tree.insert('', 'end', text=model.name, values=("", value,),
                            tags=('oddrow' if (self.item_count % 2) == 0 else 'evenrow'))
            
            manifest_size = os.path.getsize(os.path.join(model.model_path, model.manifest))
            humansize = self.model_data._human_readable_size(manifest_size)
            manifest_item = self.tree.insert(item, 'end', values=(humansize, '',), 
                            text=model.manifest,
                            tags=('childrow'))
            blobs_size = 0
            for digest in model.blobs:
                blobs_size += self.model_data.get_blob_size(digest)
                humansize = self.model_data.get_blob_size(digest, True)
                blob_item = self.tree.insert(item, 'end', values=(humansize, '',), text=os.path.join('blobs', digest),
                                tags=('childrow'))
                
                self.tree_items[digest] = blob_item
            
            model_size = blobs_size + manifest_size
            humansize = self.model_data._human_readable_size(model_size)
            #self.tree.item(item, values=(humansize, value,)) # 更新两列值
            self.tree.set(item, column='size', value=humansize) # 只更新size列的值
            self.tree_items[model.name] = item
            self.tree_items[model.manifest] = manifest_item
            self.item_count += 1

    def delete_model(self, model: LLMModel)->None:
        logger.debug(f"删除模型: {model.name}")  # 调试日志，确保正确获取模型名称
        with self.data_lock:
            item = self.tree_items.pop(model.name, None)
            if item:
                self.tree.delete(item)
                self.item_count -= 1

    def _update_model(self, model: LLMModel)->None:
        logger.debug(f"更新模型: {model.name}")  # 调试日志，确保正确获取模型名称
        with self.data_lock:
            item = self.tree_items.get(model.name, None)
            if not item:
                logger.warning(f"模型 {model.name} 不存在，无法更新")  # 警告日志，确保正确获取模型名称
                return
            # 获取主项目ID
            item = self.tree_items[model.name]
            
            # 更新备份状态
            backup_value = self.CHECKING_SYMBOL if not model.bk_status else self._get_backup_value(model.bk_status)
            self.tree.set(item, column='selected', value=backup_value)
            
            # 计算并更新总大小
            manifest_path = os.path.join(model.model_path, model.manifest)
            manifest_size = os.path.getsize(manifest_path) if os.path.exists(manifest_path) else 0
            blobs_size = sum(self.model_data.get_blob_size(digest) or 0 for digest in model.blobs)
            model_size = manifest_size + blobs_size
            
            self.tree.set(item, column='size', value=self.model_data._human_readable_size(model_size))
            
            # 更新manifest子项（如果manifest变化）
            if model.manifest in self.tree_items:
                manifest_item = self.tree_items[model.manifest]
                self.tree.set(manifest_item, column='size', 
                            value=self.model_data._human_readable_size(manifest_size))
            
            # 更新blob子项
            existing_blobs = set()
            for child in self.tree.get_children(item):
                item_text = self.tree.item(child, 'text')
                if item_text.startswith('blobs'):
                    digest = os.path.basename(item_text)
                    existing_blobs.add(digest)
                    
                    # 更新已有blob大小
                    if digest in model.blobs:
                        blob_size = self.model_data.get_blob_size(digest) or 0
                        self.tree.set(child, column='size',
                                    value=self.model_data._human_readable_size(blob_size))
            
            # 添加新增的blob
            for digest in model.blobs:
                if digest not in existing_blobs:
                    blob_size = self.model_data.get_blob_size(digest) or 0
                    humansize = self.model_data._human_readable_size(blob_size)
                    blob_item = self.tree.insert(
                        item, 'end',
                        text=os.path.join('blobs', digest),
                        values=(humansize, "", ""),
                        tags=('childrow')
                    )
                    self.tree_items[digest] = blob_item
            
            # 删除不存在的blob
            for digest in existing_blobs - set(model.blobs):
                if digest in self.tree_items:
                    self.tree.delete(self.tree_items[digest])
                    del self.tree_items[digest]            

    def set_model(self, model: LLMModel)->None:
        with self.data_lock:
            exist = (model.name in self.tree_items)
        if exist:
            self._update_model(model)
        else:
            self._add_model(model)
    
    def set_blob(self, blob: Blob)->None:
        logger.debug(f"更新blob: {blob.name}")  # 调试日志，确保正确获取模型名称
        with self.data_lock:
            if not blob.name in self.tree_items:
                return
            exist_parent = False
            for item in self.tree_items.get_all(blob.name):
                humansize = self.model_data._human_readable_size(blob.size)
                self.tree.set(item, column='size', value=humansize)
                parent_item = self.tree.parent(item)
                parent_item_name = self.tree.item(parent_item, 'text')
                if parent_item_name in self.tree_items:
                    exist_parent = True
                    parent_humansize = self.tree.set(parent_item, column='size')
                    logger.debug(f"{blob.name[:15]}--parent_humansize: {parent_humansize}")
                    parent_size = self.model_data._humansize_to_bytes(parent_humansize)
                    logger.debug(f"{blob.name[:15]}--parent_size: {parent_size}")
                    logger.debug(f"{blob.name[:15]}--blob.size: {blob.size}")
                    humansize = self.model_data._human_readable_size(parent_size+blob.size)
                    logger.debug(f"{blob.name[:15]}--humansize: {humansize}")
                    self.tree.set(parent_item, column='size', value=humansize)

            if not exist_parent:
                humansize = self.model_data._human_readable_size(blob.size)
                self.item_count += 1
                item = self.tree.insert('', 'end', text=os.path.join('blobs', blob.name), values=(humansize, "", ""),
                                tags=('oddrow' if (self.item_count % 2)==0 else 'evenrow'))
                self.tree_items[blob.name] = item

    def set_backup_status(self, status: ModelBackupStatus)->None:
        logger.debug(f"更新备份状态: {status.model_name}")  # 调试日志，确保正确获取模型名称
        with self.data_lock:
            if not status.model_name in self.tree_items:
                logger.warning(f"模型 {status.model_name} 不存在，无法更新备份状态")  # 警告日志，确保正确获取模型名称
                return
        
            item = self.tree_items[status.model_name]
            backuped_value = self._get_backup_value(status)
            self.tree.set(item, 'selected', value=backuped_value)

    def start_backup(self):
        if not self.backup_path_var.get():
            self.choose_backup_dir()
            if not self.backup_path_var.get():
                return

        selected_models = [
            self.tree.item(item, 'text')
            for item in self.tree.get_children()
            if self.tree.set(item, 'selected') == self.CHECKED_SYMBOL
        ]
        logger.debug(f"选中的模型: {selected_models}")  # 调试日志，确保正确获取选中的模型

        if not selected_models:
            messagebox.showwarning("警告", "请选择要备份的模型")
            return

        self.controller.run_backup(selected_models)
        # threading.Thread(target=self.run_backup, args=(selected_models,)).start()
    
    def cancle_backup(self, model_name: str, status: str)->bool:
        if not self.controller.is_backupping(model_name):
            return True
        if status == self.BACKUPING_SYMBOL:
            msg_info = "正在备份中，确定取消备份吗？"
        else:
            msg_info = "排队备份中，确定取消备份吗？"
        if not messagebox.askyesno("确认", msg_info):
            return False
        self.controller.cancle_backup(model_name)
        return True

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
    
    def choose_model_dir(self):
        """选择模型路径"""
        path = filedialog.askdirectory(title="选择模型存储目录")
        if path:
            path = str(Path(path))
            self.model_path_var.set(path)

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
                logger.error(f"无效的UI方法: {action}", exc_info=True)
            except (queue.Empty, ValueError):
                continue
            except Exception as e:
                logger.error(f"执行 {action} 失败: {e}", exc_info=True)
    
class Obeserver(ModelObserver):
    def __init__(self, handler: UIUpdateHandler):
        self.handler = handler
    def notify_set_model(self, model: LLMModel) -> None:
        logger.debug(f"通知添加模型: {model}")
        self.handler.queue.put(("set_model", model))
    def notify_delete_model(self, model: LLMModel) -> None:
        self.handler.queue.put(("delete_model", model))
    def notify_set_blob(self, blob: Blob) -> None:
        self.handler.queue.put(("set_blob", blob))
    def notify_set_backup_status(self, status: ModelBackupStatus) -> None:
        logger.debug(f"通知更新备份状态: {status}")
        self.handler.queue.put(("set_backup_status", status))
    def notify_initialized(self, initialized: bool) -> None:
        self.handler.queue.put(("set_initialized", initialized))
    def notify_loading_progress(self, progress_status: ProcessStatus) -> None:
        self.handler.queue.put(("show_process_status", progress_status))

if __name__ == "__main__":
    root = tk.Tk()
    app = BackupApp(root)
    root.mainloop()