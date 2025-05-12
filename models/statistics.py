from models import (
    ModelBackupStatus, 
    ModelData, 
    ModelObserver,
    LLMModel,
    ProcessStatus,
    ProcessEvent,
    Blob
)

class Statistics:
    model_data = ModelData()

    @classmethod
    def get_blob(cls, blob_name):
        blob = cls.model_data.get_blob(blob_name)
        content = f"名称：{blob_name}\n"
        if not blob:
            return (content + "\n加载中...\n")
        content += f"相关模型：\n"
        count = 0
        for model in blob.models:
            content += f"    {model} "
            count += 1
            if count % 3 == 0:  # 每行显示3个模型
                content += "\n"
        content += f"\n大小：{cls.model_data.get_blob_size(blob_name, True)}\n"
        content += f"路径：{blob.path}\n"
        return content

    @classmethod
    def get_manifest(cls, manifest_name):
        return manifest_name

    @classmethod
    def get_backup_status(cls, backup_name):
        return backup_name

    @classmethod
    def get_model(cls, model_name):
        return model_name