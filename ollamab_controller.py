from queue import LifoQueue
from tokenize import triple_quoted
from typing import List, Dict
import logging
from utils.logging_config import setup_logging
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
from models import ModelData, LLMModel, ModelBackupStatus
from queue import Queue, Empty
from pydantic import BaseModel

# 初始化日志配置
logger = setup_logging(log_level=logging.DEBUG, log_tag="ollamab_controller")

class ModelDatialFile(BaseModel):
    model_file_path: str
    digests: List[str]

class BackupController:
    def __init__(self, model_path: str, backup_path: str):
        self.model_path = model_path
        self.backup_path = backup_path
        self.model_data = ModelData()

        self.cache_lock = threading.Lock()
        self.model_cache = {}
        self.isLoading = False
    
    def chdir_path(self, model_path: str, backup_path: str) -> None:
        """切换工作目录"""
        if model_path:
            self.model_path = model_path
        if backup_path:
            self.backup_path = backup_path

    def start_async_loading(self) -> bool:
        """启动异步数据加载"""        
        return AsyncLoad.load_models(self.model_path, self.backup_path)
    
    def async_recheck_backup_status(self) -> bool:
        """重新检查备份状态"""
        return AsyncLoad.check_backup_status(self.model_path, self.backup_path)

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

                backup_dir = self.backup_path
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
            clean_temp_files(self.model_path, self.model_path)

    def _get_model_detail_file(self, model_name, model_file=None)->ModelDatialFile|None:
        llmmodel = self.model_data.get_model(model_name)
        if llmmodel:
            return ModelDatialFile(**{
                'model_file_path': os.path.join(self.model_path, llmmodel.manifest),
                'digests': llmmodel.blobs,
            })
        return AyncLoad.get_model_detail_file(model_name, model_file, self.model_path)

    def _get_all_models(self) -> List[Dict]:
        """
        获取所有模型信息
        Returns:
            List[Dict]: 模型信息列表
        """
        models = []
        manifests_path = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library')
        if not os.path.exists(manifests_path):
            logger.error(f"模型根目录结构异常: {manifests_path}")
            return models

        for model in os.listdir(manifests_path):
            model_versions = os.path.join(manifests_path, model)
            if os.path.isdir(model_versions):
                for version in os.listdir(model_versions):
                    model_file = os.path.join(self.model_path, 'manifests', 'registry.ollama.ai', 'library', model, version)
                    model_dict = self._get_model_detail_file(f"{model}:{version}", model_file)
                    if model_dict:
                        models.append({
                            'name': model,
                            'version': version,
                            'digests': model_dict.get('digests', [])
                        })
        return models

    def check_backup_status(self, backup_file: str)->bool:
        backup_dir = self.backup_path
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

class AsyncLoad:
    model_cache = {}
    cache_lock = threading.Lock()
    model_data = ModelData()
    _stop_event = threading.Event()
    _data_ready_event = threading.Event()
    model_queue = Queue()

    model_path = None
    backup_path = None
    isLoading = False
    initialized = False
    
    @classmethod
    def init(cls, model_path: str, backup_path: str):
        cls.model_path = model_path
        cls.backup_path = backup_path        

        cls.isLoading = False
        cls.isChecking = False
        while not cls.model_queue.empty():
            cls.model_queue.get()
        cls._stop_event.clear()
        cls._data_ready_event.clear()
        cls.initialized = True

    @classmethod
    def load_models(cls, model_path: str, backup_path: str):
        if cls.isLoading:
            logger.warning("数据加载任务正在进行中，跳过启动")
            return
        cls.isLoading = True
        cls.model_data.initialized = False
        cls.init(model_path, backup_path)
        # 启动阶段一任务（模型信息加载）
        init_thread = threading.Thread(
            target=cls._init_models_task,
            args=(cls.model_queue,),
            daemon=True
        )
        init_thread.start()
        # 启动阶段二任务（备份状态检查）
        check_thread = threading.Thread(
            target=cls._check_backup_task,
            args=(cls.model_queue,),
            daemon=True
        )
        check_thread.start()
    
    @classmethod
    def check_backup_status(cls, model_path:str, backup_path: str)->bool:
        if not backup_dir or not os.path.exists(backup_dir):
            return True
        if cls.isLoading:
            logger.warning("备份状态检查任务正在进行中，跳过启动")
            return False
        cls.isLoading = True
        self.model_data.process_event = ProcessStatus(event=None, message="检查备份状态中...")
        cls.init(model_path, backup_path)
        init_thread = threading.Thread(
            target=cls._get_models_task,
            args=(model_queue,),
            daemon=True
        )
        init_thread.start()
        # 启动阶段二任务（备份状态检查）
        check_thread = threading.Thread(
            target=cls._check_backup_task,
            args=(model_queue,),
            daemon=True
        )
        check_thread.start()
    
    @classmethod
    def _get_models_task(cls, model_queue: Queue):
        try:
            models = cls.model_data.models
            _data_ready_event.set()
            for model in models:
                model_queue.put([model.name, f"backup_{model.llm}_{model.version}.zip"])
        finally:
            cls._stop_event.set() 

    @classmethod
    def _init_models_task(cls, model_queue: Queue):
        logger.info("第一阶段：开始初始化模型信息")
        try:
            manifests_path = os.path.join(cls.model_path, 'manifests', 'registry.ollama.ai', 'library')
            if not os.path.exists(manifests_path):
                logger.error(f"模型根目录结构异常: {manifests_path}")
                cls._stop_event.set()
                return

            cls._data_ready_event.set()
            for model in os.listdir(manifests_path):
                model_versions = os.path.join(manifests_path, model)
                if os.path.isdir(model_versions):
                    for version in os.listdir(model_versions):
                        try:
                            model_file = os.path.join(cls.model_path, 'manifests', 'registry.ollama.ai', 'library', model, version)
                            model_dict = cls._get_model_detail_file(f"{model}:{version}", model_file)
                            logger.debug(f"模型信息: {model_file}")
                            if model_dict:
                                cls.model_data.add_model(LLMModel(**{
                                    'model_path': cls.model_path,
                                    'name': f"{model}:{version}",
                                    'description': f"{model}:{version}",
                                    'llm': model,
                                    'version': version,
                                    'manifest': os.path.relpath(model_dict.get('model_file_path', ""), cls.model_path),
                                    'blobs': model_dict.get('digests', []),
                                    'bk_status': None,
                                    }))
                                model_queue.put([f"{model}:{version}", f"backup_{model}_{version}.zip"])
                        except Exception as e:
                            logger.error(f"初始化模型信息时出错: {e}")
                            logger.error(traceback.format_exc())
                            continue
        finally:
            cls._stop_event.set()            
            logger.info("第一阶段：模型信息初始化完成")

    @classmethod
    def _check_backup_task(cls, model_queue: Queue) -> None:
        """阶段二：并行执行备份检查"""
        try:
            logger.info("第二阶段：开始执行备份状态检查")
            # 等待阶段一数据准备就绪或超时
            if not cls._data_ready_event.wait(timeout=10):
                logger.warning("等待阶段一数据超时，退出检查任务")
                return
            logger.debug(f"model_queue: {model_queue.qsize()} cls._stop_event.is_set(): {cls._stop_event.is_set()}")
            while not (cls._stop_event.is_set() and model_queue.empty()):
                try:
                    model_name, zip_file = model_queue.get(block=True, timeout=1)
                    if model_name is None:
                        continue
                    dest_path = os.path.join(cls.backup_path, zip_file)
                    backuped, zip_file = check_zip_file_integrity(dest_path)
                    cls.model_data.update_backup_status(ModelBackupStatus(**{
                        'model_name': model_name,
                        'backup_path': os.path.dirname(zip_file) if zip_file else None,
                        'backup_status': backuped,
                        'zip_file': os.path.basename(zip_file) if zip_file else None,
                        'zip_md5': None,
                    }))
                except Empty:
                    if cls._stop_event.is_set():  # 检查是否应该退出
                        break
                    continue
                except Exception as e:
                    logger.error(f"检查备份状态时出错: {e}")
                    logger.error(traceback.format_exc())
                    continue
        finally:
            # 确保在退出时关闭线程池
            cls.isLoading = False
            logger.info("第二阶段：备份状态检查完成")
            cls.model_data.initialized = True

    @classmethod
    def _get_model_detail_file(cls, model_name, model_file:str=None, model_path:str=None)->ModelDatialFile|None:
        # 从缓存中获取模型信息
        logger.debug(f"获取模型信息: {model_name} {model_file}")  # 调试日志，确保正确获取模型名称
        with cls.cache_lock:
            model_dict = cls.model_cache.get(model_name)
        if model_dict:
            return model_dict

        if not model_path and cls.model_path:
            model_path = cls.model_path
        else:
            logger.error(f"未提供有效的模型路径: {model_path}")
            return None

        # 解析模型文件
        if not model_file:
            model_parts = model_name.lsplit(':', 1)
            model_file = os.path.join(cls.model_path,'manifests','registry.ollama.ai', 'library', *model_parts)
        logger.debug(f"解析模型文件: {model_file}")  # 调试日志，确保正确获取模型文件路径
        if not os.path.exists(model_file):
            # 深度遍历library目录结构寻找匹配路径
            found = False
            library_path = os.path.join(cls.model_path, 'manifests', 'registry.ollama.ai', 'library')
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
                return None

        model_dict = parse_model_file(model_file)
        if model_dict:
            # 缓存模型信息
            with cls.cache_lock:
                cls.model_cache[model_name] = model_dict
        return model_dict
