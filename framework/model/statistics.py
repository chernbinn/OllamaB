from model.models import ModelData
from model.models import LLMModel
import logging, os
from utils import logging_config

logger = logging_config.setup_logging(logging.INFO, "statistics")

class Statistics:
    model_data = ModelData()

    @classmethod
    def get_blob(cls, blob_name):
        blob = cls.model_data.get_blob(blob_name)
        content = f"名称：{blob_name}\n"
        if not blob or not blob.path:
            return (content + "\n加载中。。。\n")
        content += f"\n大小：{cls.model_data.get_blob_size(blob_name, True)}\n"
        content += f"路径：{os.path.dirname(blob.path)}\n"
        content += f"相关模型：\n"
        for model in blob.models:
            content += f"        {model}\n"
        
        return content

    @classmethod
    def get_manifest(cls, manifest_name):
        return manifest_name

    @classmethod
    def get_backup_status(cls, backup_name):
        return backup_name

    @classmethod
    def get_model(cls, model_name):
        logger.debug(f"get_model：{model_name}")  # 调试信息，确保模型名称正确
        seps = model_name.split(":")
        llm = seps[0]
        version = seps[1]
        llm_model:LLMModel = cls.model_data.get_model(model_name)
        logger.debug(f"get_model：{model_name}")  # 调试信息，确保模型名称正确
        if not llm_model:
            return None
        content = f"名称：{model_name}\n"
        content += f"类型：{llm}\n"
        content += f"版本：{version}\n"
        if not llm_model.model_path:
            return content

        content += f"路径：{os.path.join(llm_model.model_path, 'blobs')}\n"
        content += f"manifest：{llm_model.manifest}\n\n"
        content += f"blobs：\n"
        for blob in llm_model.blobs:
            content += f"    {blob}\n"
            content += f"    大小：{cls.model_data.get_blob_size(blob, True)}\n"
            content += f"    相关模型：\n"
            for model in cls.model_data.get_blob(blob).models:
                content += f"        {model}\n"
            content += "\n"

        return content