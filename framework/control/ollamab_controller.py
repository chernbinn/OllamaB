
from typing import List, Dict, Optional
import logging
import os
from functools import partial
from utils import logging_config
from utils.AsyncExecutor import AsyncExecutor
from core import ollamab
from model import ModelData, LLMModel, ModelBackupStatus, Blob
from control.AsyncLoad import AsyncLoad
from control.ModelDatialFile import ModelDatialFile

# 初始化日志配置
logger = logging_config.setup_logging(log_level=logging.DEBUG, log_tag="ollamab_controller")

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

    def start_async_loading(self) -> None:
        """启动异步数据加载"""        
        AsyncLoad.load_models(self.model_path, self.backup_path)
    
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

    def _get_model_detail_file(self, model_name, model_file=None)->Optional[ModelDatialFile]:
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


