
from functools import partial
import threading
from queue import Queue, Empty
import hashlib
import logging, os
from typing import List, Optional
from utils.UniqueQueue import UniqueQueue
from utils.AsyncExecutor import AsyncExecutor
from utils.logging_config import setup_logging
from core import ollamab
from model import ModelData, LLMModel, ModelBackupStatus, Blob
from control.ModelDatialFile import ModelDatialFile

logger = setup_logging(log_level=logging.DEBUG, log_tag="AsyncLoad")

class AsyncLoad:
    # 外部实例对象
    async_executor = None
    model_data = ModelData()
    # 外部参数
    model_path = None
    backup_path = None
    # 内部参数
    _lock = threading.Lock()  
    _isLoading = False    
    _data_stop_event = threading.Event()
    _data_ready_event = threading.Event()
    _model_queue = UniqueQueue() #Queue()
    _task_list = set()
    
    @classmethod
    def init(cls, model_path: str, backup_path: str):
        with cls._lock:
            cls.model_path = model_path
            cls.backup_path = backup_path        

            # UI数据全部加载后，通知UI数据加载完成
            cls._isLoading = False

            # 初始化模型数据变量，不包含备份情况和blobs文件大小
            while not cls._model_queue.empty():
                cls._model_queue.get()
            cls._data_stop_event.clear()
            cls._data_ready_event.clear() 
            cls._task_list = set()

            cls.async_executor = AsyncExecutor()
            cls.async_executor.set_concurrency(3, 1)

    @classmethod
    def load_models(cls, model_path: str, backup_path: str):
        with cls._lock:
            if cls._isLoading:
                logger.warning("数据加载任务正在进行中，跳过启动")
                return
       
        cls.init(model_path, backup_path)
        with cls._lock:
            cls._isLoading = True
            cls.model_data.initialized = False

        cls._init_models_thread()
        cls._check_dirbackup_thread()

        with cls._lock:
            cls._task_list.add("load_blobs")
        cls.async_executor.execute_async(
            "load_blobs",
            cls._iter_blobs_task,
            is_long_task=False,
            callback=partial(cls._async_loading_task_done, "load_blobs")
        )
    
    @classmethod
    def check_backup_status(cls, model_path:str, backup_path: str)->bool:
        if not backup_dir or not os.path.exists(backup_dir):
            return True

        with cls._lock:
            if cls._isLoading:
                logger.warning("数据加载任务正在进行中，跳过启动")
                return False

        self.model_data.process_event = ProcessStatus(event=None, message="检查备份状态中...")
        cls.init(model_path, backup_path)

        with cls._lock:
            cls._isLoading = True
            cls.model_data.initialized = False
        
        cls._init_models_thread()
        cls._check_dirbackup_thread()
    
    @classmethod
    def _init_models_thread(cls):
        with cls._lock:
            cls._task_list.add("load_models")
        cls.async_executor.execute_async(
            "load_models",
            cls._init_models_task if not cls.model_data.initialized else cls._get_models_task,
            cls._model_queue,
            is_long_task=False,
            callback=partial(cls._async_loading_task_done, "load_models")
        )
    
    @classmethod
    def _check_dirbackup_thread(cls):
        with cls._lock:
            cls._task_list.add("check_allbackup")
        cls.async_executor.execute_async(
            "check_allbackup",
            cls._check_backup_task,
            cls._model_queue,
            is_long_task=False,
            callback=partial(cls._async_loading_task_done, "check_allbackup")
        )

    @classmethod
    def _init_models_task(cls, model_queue: Queue):
        #logger.info("第一阶段：开始初始化模型信息")
        try:
            manifests_path = os.path.join(cls.model_path, 'manifests', 'registry.ollama.ai', 'library')
            if not os.path.exists(manifests_path):
                logger.error(f"模型根目录结构异常: {manifests_path}")
                return

            cls._data_ready_event.set()
            for llm in os.listdir(manifests_path):
                llm_path = os.path.join(manifests_path, llm)
                for version in os.listdir(llm_path):
                    logger.debug(f"----llm: {llm} version: {version}")
                    cls._iter_model_task(llm, version, model_queue)
            cls._get_backuped_models(model_queue)
        finally:            
            logger.info("第一阶段：模型信息初始化完成")
            cls._data_stop_event.set()
    
    @classmethod
    def _get_models_task(cls, model_queue: Queue):
        try:
            models = cls.model_data.models
            cls._data_ready_event.set()
            for model in models:
                model_queue.put((model.name, f"backup_{model.llm}_{model.version}.zip"))
            cls._get_backuped_models(model_queue)
        finally:
            cls._data_stop_event.set()

    @classmethod
    def _get_backuped_models(cls, queue: Queue=None)->List[str]:
        """获取已备份的模型列表"""
        backuped_models = []
        for file in os.listdir(cls.backup_path):
            if file.endswith('.zip'):
                seps = file.split('_')
                model_name = f"{seps[1]}:{seps[2]}"
                zip_name = f"backup_{seps[1]}_{seps[2]}.zip"
                cls.model_data.set_backup_status(ModelBackupStatus(
                    model_name=model_name,
                    backup_path=None,
                    backup_status=True,
                    zip_file=zip_name,
                    zip_md5=None
                ))
                if queue:
                    queue.put([model_name, zip_name])
                backuped_models.append(model_name)

        for file in os.listdir(cls.model_path):
            if file.endswith('.zip'):
                seps = file.split('_')
                model_name = f"{seps[1]}:{seps[2]}"
                zip_name = f"backup_{seps[1]}_{seps[2]}.zip"
                if queue:
                    queue.put([model_name, zip_name])
                backuped_models.append(model_name)
        return backuped_models
    
    @classmethod
    def _check_backup_task(cls, model_queue: Queue) -> None:
        """阶段二：并行执行备份检查"""
        try:
            logger.info("第二阶段：开始执行备份状态检查")
            # 等待阶段一数据准备就绪或超时
            if not cls._data_ready_event.wait(timeout=10) and model_queue.empty():
                logger.warning("等待阶段一数据超时且没有待校验的备份模型，退出检查任务")
                return
            logger.debug(f"model_queue: {model_queue.qsize()} cls._data_stop_event.is_set(): {cls._data_stop_event.is_set()}")
            while not (cls._data_stop_event.is_set() and model_queue.empty()):
                try:
                    model_name, zip_name = model_queue.get(block=True, timeout=1)
                    if model_name is None:
                        continue
                    dest_path = os.path.join(cls.backup_path, zip_name)                    
                    with cls._lock:
                        cls._task_list.add(f"loadcheck_{model_name}")
                        logger.debug(f"loadcheck_{model_name}")
                    cls.async_executor.execute_async(
                        f"loadcheck_{model_name}",
                        cls.check_model_backup_status,
                        model_name,
                        dest_path,
                        is_long_task=False,
                        callback=partial(cls._async_loading_task_done, f"loadcheck_{model_name}")
                    )
                except Empty:
                    if cls._data_stop_event.is_set():  # 检查是否应该退出
                        break
                    continue
                except Exception as e:
                    logger.error(f"检查备份状态时出错: {e}", exc_info=True)
                    continue            
        finally:
            logger.info("第二阶段：备份状态检查完成")
            """
            cls._isLoading = False
            cls.model_data.initialized = True
            """
    
    @classmethod
    def get_model_detail_file(cls, model_name, model_file:str=None, model_path:str=None) -> Optional[ModelDatialFile]:
        # 从缓存中获取模型信息
        logger.debug(f"获取模型信息: {model_name} {model_file}")  # 调试日志，确保正确获取模型名称
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
        model_dict = ollamab.parse_model_file(model_file)
        return model_dict

    @classmethod
    def check_model_backup_status(cls, model_name: str, zip_file: str = None) -> bool:
        """检查模型备份状态"""
        dest_path = zip_file
        if zip_file == None:
            backup_dir = cls.backup_path
            if not backup_dir or not os.path.exists(backup_dir):
                return False
            dest_path = os.path.join(backup_dir, backup_file)

        backupde, zip_file = ollamab.check_zip_file_integrity(dest_path)
        if backupde or zip_file:
            cls.model_data.set_backup_status(ModelBackupStatus(
                model_name=model_name,
                backup_path=cls.backup_path if zip_file else None,
                backup_status=True if zip_file else False,
                zip_file=zip_file,
                zip_md5='invalidmd5' if zip_file and not backupde else None
            ))
        elif os.path.exists(os.path.join(cls.model_path, os.path.basename(dest_path))):
            cls.model_data.set_backup_status(ModelBackupStatus(
                model_name=model_name,
                backup_path=None,
                backup_status=False,
                zip_file=os.path.join(cls.model_path, os.path.basename(dest_path)),
                zip_md5='invalidmd5'
            ))
        else:
            cls.model_data.set_backup_status(ModelBackupStatus(
                model_name=model_name,
                backup_path=None,
                backup_status=False,
                zip_file=None,
                zip_md5=None,
            ))

        if backupde and zip_file:
            return True
        elif not backupde and zip_file:
            return False
        else:
            return False    
    
    @classmethod
    def _iter_model_task(cls, llm: str, version: str, model_queue: Queue)->any:
        logger.info(f"开始加载: {llm}:{version}")
        try:
            model_file = os.path.join(cls.model_path, 'manifests', 'registry.ollama.ai', 'library', llm, version)
            logger.debug(f"模型信息: {model_file}")
            model_dict = cls.get_model_detail_file(f"{llm}:{version}", model_file)
            
            cls.model_data.set_model(LLMModel(**{
                'model_path': cls.model_path,
                'name': f"{llm}:{version}",
                'description': f"{llm}:{version}",
                'llm': llm,
                'version': version,
                'manifest': os.path.relpath(model_dict.get('model_file_path', ""), cls.model_path),
                'blobs': model_dict.get('digests', []),
                'bk_status': None,
                }))
            #dest_path = os.path.join(cls.backup_path, f"backup_{llm}_{version}.zip")
            #cls._check_model_backup_status(f"{llm}:{version}", dest_path)
            model_queue.put((f"{llm}:{version}", f"backup_{llm}_{version}.zip"))
        except Exception as e:
            logger.error(f"初始化模型信息时出错: {e}", exc_info=True)
            return e
        return True
    
    @classmethod
    def _iter_blobs_task(cls):
        """迭代所有blobs"""
        logger.info("第三阶段：开始遍历所有blobs并统计信息")
        # 遍历blobs目录
        blobs_path = os.path.join(cls.model_path,'blobs')
        if not os.path.exists(blobs_path):
            logger.warning(f"blobs目录不存在: {blobs_path}")
            return False
        for blob in os.listdir(blobs_path):
            blob_path = os.path.join(blobs_path, blob)
            if os.path.isfile(blob_path):
                try:
                    """
                    blob = cls.model_data.get_blob(blob)
                    if  blob and blob.path and blob.size:
                        continue
                    """
                    with cls._lock:
                        cls._task_list.add(f"loadblob_{blob}")
                    cls.async_executor.execute_async(
                        f"loadblob_{blob}",
                        lambda cls_ref, blob, blob_path: cls_ref.model_data.set_blob(Blob(**{
                            'name': blob,
                            'size': os.path.getsize(blob_path),
                            'md5': None, # hashlib.md5(open(blob_path, 'rb').read()).hexdigest(),
                            'path': blob_path,
                        })),
                        cls, blob, blob_path,
                        is_long_task=False,
                        callback=partial(cls._async_loading_task_done, f"loadblob_{blob}")
                    )
                except Exception as e:
                    logger.error(f"计算文件MD5时出错: {e}", exc_info=True)
                    continue
        return True

    @classmethod
    def _async_loading_task_done(cls, task_id: str, result: any)->None:
        if not isinstance(result, Exception):
            with cls._lock:
                if task_id in cls._task_list:
                    cls._task_list.remove(task_id)

                logger.debug(f"pending task: {cls._task_list} exist task: {cls.async_executor.has_tasks()}")
                if len(cls._task_list) == 0 or cls.async_executor.has_tasks() == 0:
                    cls._isLoading = False
                    cls.model_data.initialized = True
        else:
            logger.error(f"检查备份状态时出错: {task_id}")

        