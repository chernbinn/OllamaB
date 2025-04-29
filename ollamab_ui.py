import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import threading
import traceback
from ollamab import parse_model_file, copy_and_zip_model, backup_zip, zip_model, paq_zip_model
import logging
from logging_config import setup_logging
import json

# 初始化日志配置
logger = setup_logging(log_level=logging.DEBUG)

class BackupApp:
    def __init__(self, master):
        self.master = master
        master.title("Ollama模型备份工具")
        master.geometry("800x600")

        # 状态符号配置
        self.CHECKED_SYMBOL = '[Y]'
        self.UNCHECKED_SYMBOL = '[ ]'
        
        # 环境变量检测
        self.model_path = "D:\\ollama_model" # os.getenv("OLLAMA_MODELS")
        if not self.model_path or not os.path.exists(self.model_path):
            self.prompt_model_path()

        # 初始化缓存锁和模型缓存
        self.cache_lock = threading.Lock()
        self.model_cache = {}

        # 初始化UI组件
        self.create_widgets()
        self.load_models()

    def create_widgets(self)->None:
        # 主框架
        main_frame = ttk.Frame(self.master)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 控制面板
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

        self.backup_path = tk.StringVar()
        ttk.Entry(control_frame, textvariable=self.backup_path).pack(side=tk.LEFT, expand=True, fill=tk.X)
        ttk.Button(control_frame, text="选择路径", command=self.choose_backup_dir).pack(side=tk.LEFT, padx=5)
        self.backup_btn = ttk.Button(control_frame, text="开始备份", command=self.start_backup)
        self.backup_btn.pack(side=tk.RIGHT)

        # 信息面板容器
        info_panel = ttk.PanedWindow(main_frame, orient=tk.VERTICAL)
        info_panel.pack(fill=tk.BOTH, expand=True)

        # 模型树形面板
        tree_frame = ttk.Frame(info_panel)
        self.tree = ttk.Treeview(tree_frame, columns=('selected'), show='tree headings')
        self.tree.heading('#0', text='模型名称', anchor=tk.W)
        self.tree.column('#0', width=550, anchor=tk.W)
        self.tree.heading('selected', text='备份', anchor=tk.W)
        self.tree.column('selected', width=60, anchor=tk.W)

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        info_panel.add(tree_frame)

        # 绑定事件
        self.tree.bind('<Button-1>', self.toggle_checkbox)
        # 添加选中项变更事件
        self.tree.bind('<<TreeviewOpen>>', self.update_node_status)

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
        if not self.backup_path.get():
            self.choose_backup_dir()
            if not self.backup_path.get():
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
        logger.info(f"开始备份模型: {models}")
        try:
            for model in models:
                logger.info(f"备份模型: {model}")
                model_dict = self.get_model_detail_file(model)
                if not model_dict:
                    # 新增错误处理流程
                    model_file = os.path.join(self.model_path,'manifests','registry.ollama.ai', 'library', *model.split(':', 1))
                    logger.error(f"模型{model}的文件{model_file}缺失")
                    self.thread_safe_messagebox("文件缺失", f"模型{model}的文件{model_file}不存在！", "error")
                    # 删除树节点model
                    self.tree.delete(model)
                    continue

                backup_dir = self.backup_path.get()
                seps = model_dict["model_file_path"].split(os.sep)
                zip_name = "backup_" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zip"
                # zip_name = "backup_" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zpaq"
                logger.debug(f"zip_name: {zip_name}")
                dest_path = os.path.join(backup_dir, zip_name)
                if os.path.exists(dest_path):
                    logger.warning(f"备份文件已存在: {dest_path}")
                    self.thread_safe_messagebox("文件存在", f"备份文件{dest_path}已存在，不再备份！", "warning")
                    continue
                # 开始备份
                #zip_path = copy_and_zip_model(self.model_path, model_dict, zip_name)
                zip_path = zip_model(self.model_path, model_dict, zip_name)
                #zip_path = paq_zip_model(self.model_path, model_dict, zip_name)
                
                if zip_path:
                    zip_path = backup_zip(zip_path, backup_dir)
                    self.thread_safe_messagebox("备份完成", f"{model} 备份完成：{zip_path}")
        except Exception as e:
            logger.error(f"备份过程中发生错误: \n{traceback.format_exc()}")
            self.thread_safe_messagebox("备份错误", f"备份失败！", "error")
        finally:
            self.thread_safe_messagebox("备份完成", f"所有模型备份完成！", "info")
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
        else:
            messagebox.showerror("错误", "必须指定模型路径")

    def choose_backup_dir(self):
        path = filedialog.askdirectory()
        if path:
            self.backup_path.set(path)

    def update_node_status(self, event):
        """处理树形节点展开事件，暂时保留空实现"""
        pass

    def update_node_status(self, event):
        """处理树形节点展开事件，暂时保留空实现"""
        pass

    def load_models(self):
        manifests_path = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library')
        if not os.path.exists(manifests_path):
            logger.error(f"模型根目录结构异常: {manifests_path}")
            messagebox.showwarning("路径错误", "模型存储目录结构不完整，请重新选择正确路径")
            return

        for model in os.listdir(manifests_path):
            model_versions = os.path.join(manifests_path, model)
            if os.path.isdir(model_versions):
                for version in os.listdir(model_versions):                    
                    # 添加文件子节点
                    model_file = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library', model, version)
                    model_dict = self.get_model_detail_file(f"{model}:{version}", model_file)
                    #logger.debug(f"模型文件: {json.dumps(model_dict, indent=2)}")
                    if not model_dict:
                        continue
                    item = self.tree.insert('', 'end', text=f"{model}:{version}", values=(self.UNCHECKED_SYMBOL,))
                    logger.debug(f"已加载模型: {model}:{version}")
                    self.tree.insert(item, 'end', values=('',), text=os.path.join('manifests', 'registry.ollama.ai', 'library', model, version))
                    for digest in model_dict.get('digests', []):
                        self.tree.insert(item, 'end', values=('',), text=os.path.join('blobs', digest))
                    #self.tree.tag_bind(item, '<Button-1>', self.toggle_checkbox)

    def get_model_detail_file(self, model_name, model_file=None):
        # 从缓存中获取模型信息
        logger.debug(f"获取模型信息: {model_name} {model_file}")  # 调试日志，确保正确获取模型名称
        with self.cache_lock:
            model_dict = self.model_cache.get(model_name)
        if model_dict:
            return model_dict

        # 解析模型文件
        if not model_file:
            model_parts = model_name.lsplit(':', 1)
            model_file = os.path.join(self.model_path,'manifests','registry.ollama.ai', 'library', *model_parts)
        logger.debug(f"解析模型文件: {model_file}")  # 调试日志，确保正确获取模型文件路径
        if not os.path.exists(model_file):
            # 深度遍历library目录结构寻找匹配路径
            found = False
            library_path = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library')
            for root, dirs, files in os.walk(library_path):
                # 跳过非末级目录
                if dirs:
                    continue
                
                # 逆向构建模型名称：父目录名/当前目录名
                path_parts = os.path.relpath(root, library_path).split(os.sep)
                if len(path_parts) == 1:
                    current_model = path_parts[0]
                else:
                    current_model = f"{path_parts[-2]}:{path_parts[-1]}"

                if current_model == model_name:
                    model_file = root
                    found = True
                    break

            if not found:
                logger.error(f"未找到匹配的模型路径: {model_name}")
                messagebox.showerror("错误", f"未找到匹配的模型路径: {model_name}")
                return None

        model_dict = parse_model_file(model_file)
        if model_dict:
            # 缓存模型信息
            with self.cache_lock:
                self.model_cache[model_name] = model_dict
        return model_dict

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
        except Exception as e:
            logger.error(f"消息框显示异常: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = BackupApp(root)
    root.mainloop()