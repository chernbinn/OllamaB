import logging
from logging_config import setup_logging
from ollamab import (
    clean_temp_files,
    parse_model_file,     
    backup_zip, 
    copy_and_zip_model,  # 拷贝到临时文件在进行普通zip压缩
    zip_model,  # 直接对文件压缩，采用ZIP_LZMA t9高压缩算法
    paq_zip_model, # zpaq压缩算法，t5效果最好，但是太耗内存，大文件无法执行压缩
    check_zip_file_integrity # 检查zip压缩文件完整性
)
import os
import threading
import traceback
import json

# 初始化日志配置
logger = setup_logging(log_level=logging.INFO, log_tag="ollamab_controller")

class BackupController:
    def __init__(self, model_path, default_backup_path):
        self.model_path = model_path
        self.default_backup_path = default_backup_path
        self.cache_lock = threading.Lock()
        self.model_cache = {}

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
                    # 这里需要与视图层交互，可通过回调函数实现
                    continue

                backup_dir = self.default_backup_path
                seps = model_dict["model_file_path"].split(os.sep)
                zip_name = "backup_" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zip"
                logger.debug(f"zip_name: {zip_name}")
                if self.check_backup_status(zip_name):
                    logger.info(f"备份文件已存在")
                    continue
                zip_path = zip_model(self.model_path, model_dict, zip_name)
                if zip_path:
                    zip_path = backup_zip(zip_path, backup_dir)
                    logger.info(f"备份完成: {zip_path}")
            logger.info("所有模型备份完成！")
        except Exception as e:
            logger.error(f"备份过程中发生错误: \n{traceback.format_exc()}")
            clean_temp_files(self.model_path, self.model_cache)

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

    def check_backup_status(self, backup_file: str)->bool:
        backup_dir = self.backup_path_var.get()
        if not backup_dir or not os.path.exists(backup_dir):
            return False
        dest_path = os.path.join(backup_dir, backup_file)
        backupde, zip_file = check_zip_file_integrity(dest_path)
        if backupde and zip_file:
            return True
        elif not backupde and zip_file:
            #thread_safe_messagebox("文件损坏", f"备份文件{zip_file}校验失败，手动检查！", "warning")
            return False
        else:
            return False