import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import logging
from logging_config import setup_logging
import os
import threading
import traceback
from ollamab_controller import BackupController
from theme import Theme, StyleConfigurator

# 初始化日志配置
logger = setup_logging(log_level=logging.DEBUG)

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

        self.default_backup_path = r"F:\llm_models\ollama_modes_backup"        
        # 环境变量检测
        self.model_path = os.getenv("OLLAMA_MODELS")
        if not self.model_path or not os.path.exists(self.model_path):
            self.prompt_model_path()
        
        self.controller = BackupController(self.model_path, self.default_backup_path)        

        # 初始化缓存锁和模型缓存
        self.cache_lock = threading.Lock()
        self.model_cache = {}

        # 初始化UI组件
        self.create_widgets()
        #self.configure_style_warm()
        StyleConfigurator.configure_style(self, Theme.WARM)
        # 初始化数据内容
        self.load_models()    

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

    def toggle_checkbox(self, event:any)->None:
        # 获取点击位置的列ID
        region = self.tree.identify("region", event.x, event.y)
        column = self.tree.identify_column(event.x)

        logger.debug(f"点击位置: {region}, 列: {column}")  # 调试日志，确保正确获取点击位置和列ID
        
        # 仅在第一列（复选框列）响应点击
        if region == 'cell' and column == '#1':
            item = self.tree.identify_row(event.y)
            current = self.tree.item(item, 'values')
            logger.debug(f"当前状态: {current}")  # 调试日志，确保正确获取当前状态
            new_state = self.CHECKED_SYMBOL if current[0] == self.UNCHECKED_SYMBOL else self.UNCHECKED_SYMBOL
            self.tree.item(item, values=(new_state,))
            
            model_name = self.tree.item(item, 'text')
            logger.debug(f"复选框状态更新：{model_name} -> {new_state}")

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

    def prompt_model_path(self):
        path = filedialog.askdirectory(title="选择模型根目录")
        if path:
            self.model_path = path
            self.load_models()

    def choose_backup_dir(self):
        path = filedialog.askdirectory(title="选择备份模型目录")
        if path:
            self.backup_path_var.set(path)
    
    def choose_model_dir(self):
        """选择模型路径"""
        path = filedialog.askdirectory(title="选择模型存储目录")
        if path:
            self.model_path_var.set(path)
            self.model_path = path  # 更新实例变量
            self.load_models()  # 重新加载模型    
    
    def update_backup_status(self, model_name, backup_status: str)->None:
        def update_backup_status_ui(model_name, backup_status):
            for item in self.tree.get_children():
                if self.tree.item(item, 'text') == model_name:
                    current_values = list(self.tree.item(item, 'values'))
                    current_values[0] = backup_status
                    self.tree.item(item,
                                    values=tuple(current_values),
                                    tags=self.tree.item(item, 'tags'))
                    logger.debug(f"更新状态: {model_name} -> {backup_status}")
                    return
            logger.error(f"未找到模型: {model_name}")
        # 在UI线程中更新状态
        try:
            self.master.after(0, lambda: update_backup_status_ui(model_name, backup_status))
        except:
            pass

    def load_models(self):
        manifests_path = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library')
        if not os.path.exists(manifests_path):
            logger.error(f"模型根目录结构异常: {manifests_path}")
            messagebox.showwarning("路径错误", "模型存储目录结构不完整，请重新选择正确路径")
            return

        i = 0
        for model in os.listdir(manifests_path):
            model_versions = os.path.join(manifests_path, model)
            if os.path.isdir(model_versions):
                for version in os.listdir(model_versions): 
                    # 添加文件子节点
                    model_file = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library', model, version)
                    model_dict = self.controller.get_model_detail_file(f"{model}:{version}", model_file)
                    #logger.debug(f"模型文件: {json.dumps(model_dict, indent=2)}")
                    if not model_dict:
                        continue
                    value = self.CHECKING_SYMBOL #self.BACKUPED_SYMBOL if self.check_backup_status(f"backup_{model}_{version}.zip") else self.UNCHECKED_SYMBOL
                    item = self.tree.insert('', 'end', text=f"{model}:{version}", values=(value,),
                            tags=('oddrow' if (i % 2) == 0 else 'evenrow'))
                    logger.debug(f"已加载模型: {model}:{version}")
                    self.tree.insert(item, 'end', values=('',), 
                                    text=os.path.join('manifests', 'registry.ollama.ai', 'library', model, version),
                                    tags=('childrow'))
                    for digest in model_dict.get('digests', []):
                        self.tree.insert(item, 'end', values=('',), text=os.path.join('blobs', digest),
                                        tags=('childrow'))
                    i += 1  

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

if __name__ == "__main__":
    root = tk.Tk()
    app = BackupApp(root)
    root.mainloop()