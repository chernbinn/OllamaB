import logging
import os
from pathlib import Path
import shutil
import zipfile
import json
import hashlib

from logging_config import setup_logging

# 初始化日志配置
logger = setup_logging(log_level=logging.DEBUG, log_file="ollama_backup.log")


def parse_model_file(model_file_path: str)->dict|None:
    """
    此函数用于解析模型文件路径，读取模型清单文件（manifest），并处理其中的digest值。
    它会检查传入的模型文件路径是否存在，若存在则读取清单文件，提取其中config和layers部分的digest值，
    并将冒号替换为连字符，最后返回一个包含处理后的digest列表和原始模型文件路径的字典。
    如果传入的路径为空、路径不存在或在解析过程中发生异常，函数将返回None。

    :param model_file_path: 模型文件路径，用于定位模型清单文件，类型为字符串。
    :return: 若解析成功，返回一个字典，包含键 'model_file_path'（原始模型文件路径）和 'digests'（处理后的digest值列表）；
             若解析失败，返回None。
    """
    if not model_file_path:
        return None

    try: 
        if not os.path.exists(model_file_path):
            logger.warning(f"{model_name}不存在: {model_file_path}")
            return None
        with open(model_file_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)

        # 统一处理config和layers的digest
        digests = []
        
        if 'config' in manifest and 'digest' in manifest['config']:
            digests.append(manifest['config']['digest'].replace(':', '-'))
        
        digests.extend(
            layer['digest'].replace(':', '-')
            for layer in manifest.get('layers', [])
            if 'digest' in layer
        )

        return {
            'model_file_path': model_file_path,
            'digests': digests
        }

    except Exception as e:
        logger.error(f"解析模型文件时发生错误: {e}")
        return None

def copy_and_zip_model(model_path: str, model_dict: dict, temp_dir: str, 
                            zip_name:str|None=None)->str|None:
    """
    拷贝模型文件到临时目录并压缩
    :param model_path: 模型基础路径
    :param model_dict: 包含digests的模型字典
    :param temp_dir: 临时目录路径
    :param zip_name: 压缩文件名
    :return: 压缩文件路径
    """
    logger.info(f"开始处理模型文件: {model_path}")

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    pre_checksums = {}
    checksums = {}

    def copy_with_retry(src, dest, max_retries=3):
        """带重试机制的文件拷贝，包含MD5校验"""
        retries = 0
        src_md5 = hashlib.md5(open(src, 'rb').read()).hexdigest()
        pre_checksums[os.path.relpath(src, model_path)] = src_md5

        if os.path.exists(dest):
            dest_md5 = hashlib.md5(open(dest, 'rb').read()).hexdigest()
            if dest_md5 == src_md5:
                logger.info(f"文件已存在且MD5校验通过: {src} -> {dest}")
                return True
            else:
                logger.warning(f"文件已存在但MD5校验失败: {src} -> {dest}")
                os.remove(dest)

        while retries < max_retries:
            try:
                logger.info(f"开始拷贝文件: {src} -> {dest}")
                shutil.copy2(src, dest)
                dest_md5 = hashlib.md5(open(dest, 'rb').read()).hexdigest()
                
                if dest_md5 != src_md5:
                    raise ValueError(f"MD5校验失败: 源文件 {src_md5} != 目标文件 {dest_md5}")
                
                logger.info(f"校验通过: {src} -> {dest}")
                return True
            except Exception as e:
                logger.warning(f"第{retries+1}次拷贝失败: {e}")
                if os.path.exists(dest):
                    os.remove(dest)
                retries += 1
        return False

    # 处理清单文件
    manifest_src = model_dict['model_file_path']
    relative_manifest_path = os.path.relpath(manifest_src, model_path)
    manifest_dest = os.path.join(temp_dir, relative_manifest_path)
    os.makedirs(os.path.dirname(manifest_dest), exist_ok=True)
    
    if not copy_with_retry(manifest_src, manifest_dest):
        raise RuntimeError(f"文件拷贝失败: {manifest_src}")
    
    checksums[relative_manifest_path] = pre_checksums[relative_manifest_path]

    # 处理blob文件
    for digest in model_dict.get('digests', []):
        src_path = os.path.join(model_path, 'blobs', digest)
        dest_path = os.path.join(temp_dir, 'blobs', digest)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        if not copy_with_retry(src_path, dest_path):
            raise RuntimeError(f"文件拷贝失败: {src_path}")
        
        relative_path = os.path.relpath(dest_path, temp_dir)
        checksums[relative_path] = pre_checksums[os.path.relpath(src_path, model_path)]

    # 生成校验文件（记录原始文件MD5）
    checksum_path = os.path.join(temp_dir, 'checksum.json')
    if os.path.exists(checksum_path):
        os.remove(checksum_path)
    with open(checksum_path, 'w') as f:
        json.dump({
            'source_checksums': pre_checksums,
            'target_checksums': checksums
        }, f, indent=2)
    
    logger.info(f"已生成双重校验文件: {checksum_path}")

    # 创建压缩文件
    if not zip_name:
        seps = relative_manifest_path.split(os.sep)
        logger.debug(f"seps: {seps}")
        zip_name = ((seps[-2]+"_") if seps[-2] else '') + seps[-1]
        logger.debug(f"zip_name: {zip_name}")
    zip_path = os.path.join(temp_dir, f"{zip_name}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        logger.info(f"开始压缩临时目录: {temp_dir}")
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, temp_dir)
                logger.debug(f"正在压缩文件: {file_path} -> {arcname}")
                zipf.write(file_path, arcname=arcname)

    logger.info(f"压缩完成: {zip_path}")
    return zip_path

def backup_zip(zip_path: str, backup_dir: str)->str|None:
    """
    备份压缩文件
    :param zip_path: 压缩文件路径
    :param backup_dir: 备份目录路径
    :return: 备份文件路径
    """
    logger.info(f"开始备份压缩文件: {zip_path} 到目录: {backup_dir}")
    
    if not os.path.exists(backup_dir):
        logger.info(f"创建备份目录: {backup_dir}")
        os.makedirs(backup_dir)
    
    zip_file = Path(zip_path)
    backup_path = os.path.join(backup_dir, zip_file.name)
    logger.debug(f"备份文件从 {zip_path} 到 {backup_path}")
    shutil.copy2(zip_path, backup_path)
    
    logger.info(f"备份完成，备份文件路径: {backup_path}")
    # 删除临时目录及源压缩文件
    temp_dir = os.path.dirname(zip_path)
    if os.path.exists(temp_dir):
        logger.info(f"删除临时目录: {temp_dir}")
        shutil.rmtree(temp_dir)  # 删除临时目录及其内容，包括blobs和manifest文件，以及checksum.json文件，但是不删除zip文件，因为zip文件是ne
    return backup_path

def extract_digests_from_manifest(manifest_path):
    """
    从模型清单文件中提取所有digest值
    :param manifest_path: 清单文件路径
    :return: digest值列表
    """
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        digests = []
        # 添加config中的digest
        if 'config' in data and 'digest' in data['config']:
            digests.append(data['config']['digest'])
        
        # 添加layers中的所有digest
        if 'layers' in data:
            for layer in data['layers']:
                if 'digest' in layer:
                    digests.append(layer['digest'])
        
        return digests
    except Exception as e:
        logger.error(f"解析清单文件失败: {e}")
        return None


if __name__ == "__main__":
    # 示例用法
    model_path = os.getenv("OLLAMA_MODELS")
    if model_path is None or not os.path.exists(model_path):
        logger.warning(f"model_path: {model_path}, 路径不存在或未设置OLLAMA_MODELS环境变量.")
        model_path = None
    logger.info(f"ollama模型目录: {model_path}")
    try:
        if model_path is None:
            model_path = input("请输入模型文件路径: ")

        """
        model_path的目录结构如下：
        ├─blobs
        └─manifests
            └─registry.ollama.ai
                └─library
                    ├─bge-m3
                    ├─deepseek-r1
                    └─llama3.2
        """
        model_name = "deepseek-r1:1.5b"
        model = model_name.split(":")[0]
        model_category = model_name.split(":")[1]
        model_file_path = os.path.join(model_path, "manifests", "registry.ollama.ai", "library", model, model_category)
        logger.info(f"model_file_path: {model_file_path}")
        model_dict = parse_model_file(model_file_path)
        
        temp_dir = "temp_models"
        # zip_name = model_name.replace(":", "_")
        zip_name = None
        zip_path = copy_and_zip_model(model_path, model_dict, temp_dir, zip_name)
        
        backup_dir = "backup"
        backup_path = backup_zip(zip_path, backup_dir)
        
        logger.info(f"模型已成功备份到: {backup_path}")
        print(f"模型已成功备份到: {backup_path}")
    except Exception as e:
        logger.error(f"发生错误: {e}")
        print(f"发生错误: {e}")