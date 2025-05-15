import hashlib
from typing import List, Dict
import logging
from core import ollamab
import os
import threading
from model import ModelData, LLMModel, ModelBackupStatus, Blob
from queue import Queue, Empty
from pydantic import BaseModel
from utils.logging_config import setup_logging
from utils.AsyncExecutor import AsyncExecutor
from functools import partial
from utils.UniqueQueue import UniqueQueue

# 初始化日志配置
logger = setup_logging(log_level=logging.DEBUG, log_tag="ollamab_controller")

logger.debug(f"ollamab_controller ModelData: {id(ModelData)}")

class ModelDatialFile(BaseModel):
    model_file_path: str
    digests: List[str]

class BackupController:
    def __init__(self, model_path: str, backup_path: str):
        self.model_path = model_path
        self.backup_path = backup_path
        self.model_data = ModelData()
        self.asyncExcutor = AsyncExecutor()
        self.cancle_backup_models = []

        self.asyncExcutor.set_notify_processing(self._process_async_task_status)
        self.asyncExcutor.set_concurrency(7, 1)
        self.b_shutdown = False
    
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

    @staticmethod
    def _backup_one_model(model_path: str, backup_dir: str, model_dict: dict, zip_name: str) -> str:
        """备份单个模型"""
        zip_path = ollamab.zip_model(model_path, model_dict, zip_name)
        if zip_path:
            zip_path = ollamab.backup_zip(zip_path, backup_dir)
            logger.info(f"备份完成: {zip_path}")
            return zip_path
        return None
    
    def _process_async_task_status(self, task_id: str) -> None:
        """处理异步任务状态"""
        if task_id.startswith("backup_"):
            self.model_data.set_backup_status(ModelBackupStatus(
                model_name=task_id[len("backup_"):],
                backup_path=self.backup_path,
                backup_status=True,
                zip_file=None
            ))    

    def _backup_terminated(self, model_name: str, zip_name, result: any) -> None:
        """备份完成回调"""
        if isinstance(result, str) and os.path.exists(result):
            logger.info(f"{model_name}备份完成: {result}")
            self.model_data.set_backup_status(ModelBackupStatus(
                model_name=model_name,
                backup_path=self.backup_path,
                backup_status=True,
                zip_file=result,
                zip_md5=result.split('_')[-1].split('.')[0]
            ))
        elif model_name in self.cancle_backup_models:
            logger.info(f"{model_name}取消备份成功")
            ollamab.clean_temp_files(self.model_path, self.model_path, zip_name)
            self.model_data.set_backup_status(ModelBackupStatus(
                model_name=model_name,
                backup_path=self.backup_path,
                backup_status=False,
                zip_file=zip_name,
                zip_md5=None
            ))
            """
            for taskid in self.asyncExcutor.get_task_status().get("processes").keys():
                model_name = taskid.split("_")[-1]
                logger.info(f"开始备份模型: {model_name}")
                self.model_data.set_backup_status(ModelBackupStatus(
                    model_name=model,
                    backup_path=self.backup_path,
                    backup_status=True,
                    zip_file=None
                ))
            """
        else:
            ollamab.clean_temp_files(self.model_path, self.model_path, zip_name)
            if not self.b_shutdown:
                logger.error(f"备份失败: {model_name}")
                self.model_data.set_backup_status(ModelBackupStatus(
                    model_name=model_name,
                    backup_path=None,
                    backup_status=False,
                    zip_file=os.path.join(self.model_path, zip_name),
                    zip_md5='invalidmd5'
                ))

    def _get_zip_name(self, model_name: str) -> str:
        """获取zip文件名"""
        model_dict = self._get_model_detail_file(model_name)
        seps = model_dict.model_file_path.split(os.sep)
        zip_name = "backup_" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zip"
        return zip_name  

    def check_model_backup_status(self, model_name: str) -> bool:
        """检查备份状态"""        
        zip_name = self._get_zip_name(model_name)
        res = self.asyncExcutor.execute_async(
            f"zipcheck_{model_name}", # task_id
            AsyncLoad.check_model_backup_status, # task_func
            model_name,  # func_args
            os.path.join(self.backup_path, zip_name), # func_args
            is_long_task=False) # is_long_task
        if not res:
            logger.error(f"提交异步检查{model_name}备份失败后的文件状态的任务失败！")
        
    def run_backup(self, models):
        logger.info(f"开始备份模型: {models}")
        try:
            for model in models:
                logger.info(f"备份模型: {model}")
                if model in self.cancle_backup_models:
                    self.cancle_backup_models.remove(model)
                model_dict = self._get_model_detail_file(model)
                seps = model_dict.model_file_path.split(os.sep)
                zip_name = "backup_" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zip"
                logger.debug(f"zip_name: {zip_name}")
                res = self.asyncExcutor.execute_async(
                        f"backup_{model}", 
                        self._backup_one_model,
                        self.model_path, self.backup_path, model_dict.model_dump(), zip_name,
                        is_long_task=True, 
                        callback=partial(self._backup_terminated, model, zip_name)
                )
                if not res:
                    logger.error(f"提交异步备份模型失败: {model}")
                    continue
                """
                if not self.asyncExcutor.is_queued(f"backup_{model}"):
                    logger.info(f"模型{model}提交备份任务成功，并且开始执行！")
                    self.model_data.set_backup_status(ModelBackupStatus(
                        model_name=model,
                        backup_path=self.backup_path,
                        backup_status=True,
                        zip_file=None
                    )
                else:
                """
                if self.asyncExcutor.is_queued(f"backup_{model}"):
                    logger.info(f"模型{model}提交备份任务成功，但是还未开始执行，排队中！")
                    self.model_data.set_backup_status(ModelBackupStatus(
                        model_name=model,
                        backup_path=None,
                        backup_status=True,
                        zip_file=None
                    ))

        except Exception as e:
            logger.error(f"备份过程中发生错误: {e}", exc_info=True)
    
    def cancle_backup(self, model_name: str) -> None:
        """取消备份"""
        logger.debug(f"取消备份: {model_name}")
        if self.is_backupping(model_name):
            logger.info(f"取消正在进行的备份: {model_name}")
            self.cancle_backup_models.append(model_name)
            self.model_data.set_backup_status(ModelBackupStatus(
                model_name=model_name,
                backup_path=None,
                backup_status=False,
                zip_file=self._get_zip_name(model_name),
                zip_md5=None
            ))
        self.asyncExcutor.cancel_task(f"backup_{model_name}")

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
    
    def get_backupping_count(self):
        return self.asyncExcutor.get_running_process_count()

    def is_backupping(self, model_name: str) -> bool:
        """检查模型是否正在备份"""
        return all([self.asyncExcutor.is_task_active(f"backup_{model_name}"),
                    not self.asyncExcutor.is_queued(f"backup_{model_name}")
        ])
    
    def get_queued_count(self):
        return self.asyncExcutor.get_queued_task_count()
    
    def clean_temp_files(self):
        for file in os.listdir(self.model_path):
            if file.endswith(".zip") and file.startswith("backup_"):
                count = 0
                while True:
                    os.remove(os.path.join(self.model_path, zip))
                    time.sleep(300)
                    if not os.path.exists(os.path.join(self.model_path, zip)):
                        break
                    count += 1
                    if count > 3:
                        logger.error(f"删除{zip}文件失败，尝试3次后仍然存在！")
                        break

    def destroy(self, force: bool=False)->bool:
        if force:
            self.b_shutdown = True
            self.asyncExcutor.shutdown()
        elif self.asyncExcutor.is_all_tasks_done():
            self.b_shutdown = True
            self.asyncExcutor.shutdown()
        else:
            return False
        self.clean_temp_files()
        return True


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
    def get_model_detail_file(cls, model_name, model_file:str=None, model_path:str=None)->ModelDatialFile|None:
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

        