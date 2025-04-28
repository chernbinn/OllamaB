import logging
import os
from pathlib import Path
import shutil
import traceback
import zipfile
import json
import hashlib
import subprocess

from logging_config import setup_logging

# 初始化日志配置
logger = setup_logging(log_level=logging.INFO, log_tag="ollamab")


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
        logger.error(f"堆栈信息: {traceback.format_exc()}")
        return None

def copy_and_zip_model(model_path: str, model_dict: dict, zip_name:str|None=None,
                        temp_dir: str="temp_models",)->str|None:
    """
    拷贝模型文件到临时目录并压缩
    :param model_path: 模型基础路径
    :param model_dict: 包含digests的模型字典
    :param temp_dir: 临时目录路径
    :param zip_name: 压缩文件名
    :return: 压缩文件路径
    """
    logger.info(f"开始备份模型 {model_dict['model_file_path']}")

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
                logger.error(f"堆栈信息: {traceback.format_exc()}")
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
            'checksums': pre_checksums
        }, f, indent=2)
    
    logger.info(f"已生成校验文件: {checksum_path}")

    # 创建压缩文件
    if not zip_name:
        seps = relative_manifest_path.split(os.sep)
        logger.debug(f"seps: {seps}")
        zip_name = ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zip"
        logger.debug(f"zip_name: {zip_name}")
    zip_path = os.path.join(temp_dir, f"{zip_name}")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        logger.info(f"开始压缩临时目录: {temp_dir}")
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # 新增排除压缩文件自身的判断
                if os.path.realpath(file_path) == os.path.realpath(zip_path):
                    logger.debug(f"跳过压缩文件自身: {file_path}")
                    continue
                arcname = os.path.relpath(file_path, temp_dir)
                logger.info(f"正在压缩文件: {file_path} -> {arcname}")
                zipf.write(file_path, arcname=arcname)

    logger.info(f"压缩完成: {zip_path}")
    return zip_path

def zip_model(model_path: str, model_dict: dict, zip_name:str|None=None)->str|None:
    """
    直接压缩源文件到ZIP包
    :param model_path: 模型基础路径
    :param model_dict: 包含digests的模型字典
    :param zip_name: 压缩文件名
    :return: 压缩文件路径
    """
    logger.info(f"开始直接压缩模型 {model_dict['model_file_path']}")

    checksums = {}

    # 生成压缩文件名
    if not zip_name:
        manifest_relpath = os.path.relpath(model_dict['model_file_path'], model_path)
        seps = manifest_relpath.split(os.sep)
        zip_name = "backup" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zip"
    
    zip_path = os.path.join(model_path, f"{zip_name}")
    try:
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_LZMA, compresslevel=9) as zipf:
            # 添加清单文件
            manifest_src = model_dict['model_file_path']
            manifest_relpath = os.path.relpath(manifest_src, model_path)
            zipf.write(manifest_src, arcname=manifest_relpath)
            
            # 计算并记录MD5
            with open(manifest_src, 'rb') as f:
                checksums[manifest_relpath] = hashlib.md5(f.read()).hexdigest()

            # 添加blob文件
            for digest in model_dict.get('digests', []):
                blob_path = os.path.join(model_path, 'blobs', digest)
                blob_relpath = os.path.join('blobs', digest)
                zipf.write(blob_path, arcname=blob_relpath)
                
                # 计算并记录MD5
                with open(blob_path, 'rb') as f:
                    checksums[blob_relpath] = hashlib.md5(f.read()).hexdigest()

            # 在内存中生成校验文件
            checksum_data = json.dumps({'checksums': checksums}, indent=2)
            zipf.writestr('checksum.json', checksum_data)

        logger.info(f"直接压缩完成: {zip_path}")
        return zip_path

    except Exception as e:
        logger.error(f"压缩过程中发生错误: {e}")
        logger.error(f"{traceback.format_exc()}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return None

def paq_zip_model(model_path: str, model_dict: dict, zip_name:str|None=None)->str|None:
    """
    使用PAQ算法直接压缩源文件
    :param model_path: 模型基础路径
    :param model_dict: 包含digests的模型字典
    :param zip_name: 压缩文件名
    :return: 压缩文件路径
    """
    logger.info(f"开始PAQ压缩模型 {model_dict['model_file_path']}")

    try:       
        if not zip_name:
            # 生成压缩文件名
            manifest_relpath = os.path.relpath(model_dict['model_file_path'], model_path)
            seps = manifest_relpath.split(os.sep)
            zip_name = "backup_" + ((seps[-2]+"_") if seps[-2] else '') + seps[-1] + ".zpaq"

        # 构建要压缩的文件列表
        file_list = [
            model_dict['model_file_path'],
            *[os.path.join(model_path, 'blobs', d) for d in model_dict.get('digests', [])]
        ]

        # 生成内存校验文件
        checksums = {}
        for f in file_list:
            rel_path = os.path.relpath(f, model_path)
            with open(f, 'rb') as fp:
                checksums[rel_path] = hashlib.md5(fp.read()).hexdigest()
        
        # 生成带时间戳的校验文件
        checksum_path = os.path.join(model_path, f'checksum.json')
        with open(checksum_path, 'w') as f:
            json.dump({'checksums': checksums}, f, indent=2)
        # 将校验文件加入压缩列表
        file_list.append(checksum_path)
        
        logger.info(f"开始ZPAQ压缩文件: {file_list}")
        # 调用PAQ命令行工具（需预先安装PAQ）
        paq_path = os.path.join(model_path, zip_name)
        cmd = [
            'zpaq',
            'a',            
            paq_path,
            *file_list,
            '-m4'          
        ]
        
        # 执行压缩命令
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            cwd=model_path
        )
        
        # 压缩完成后删除临时校验文件
        if os.path.exists(checksum_path):
            os.remove(checksum_path)

        logger.debug(f"PAQ压缩输出: {result.stdout}")
        logger.info(f"PAQ压缩完成: {paq_path}")        

        return paq_path

    except subprocess.CalledProcessError as e:
        logger.error(f"PAQ压缩失败: {e.stderr}")
        logger.error(f"完整命令: {' '.join(e.cmd)}")
        logger.error(f"错误码: {e.returncode}")
        logger.error(f"堆栈追踪: {traceback.format_exc()}")
        return None
    except FileNotFoundError:
        logger.error("未找到zpaq命令行工具，请先安装zpaq压缩工具")
        return None
    except Exception as e:
        logger.error(f"PAQ压缩异常: {str(e)}")
        logger.error(traceback.format_exc())
        return None

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
    try:
        zip_file = Path(zip_path)
        backup_path = os.path.join(backup_dir, zip_file.name)
        logger.debug(f"备份文件从 {zip_path} 到 {backup_path}")
        shutil.move(zip_path, backup_path)
    except Exception as e:
        logger.error(f"备份文件时发生错误: {e}")
        logger.error(f"{traceback.format_exc()}")
        return None
    
    logger.info(f"备份完成，备份文件路径: {backup_path}")
    return backup_path

def clean_temp_files(zip_dir: str, model_path:str)->None:
    """
    清理临时目录
    :param zip_dir: 压缩文件路径
    :param model_path: 模型基础路径
    :return: default return None
    """
    logger.info(f"开始清理临时目录: {zip_dir}")
    if os.path.exists(zip_dir):
        if zip_dir != model_path:
            logger.info(f"删除临时目录: {zip_dir}")
            shutil.rmtree(zip_dir)

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
        zip_path = copy_and_zip_model(model_path, model_dict, zip_name, temp_dir)
        
        backup_dir = "backup"
        backup_path = backup_zip(zip_path, backup_dir)
        clean_temp_files(temp_dir, model_path)
        
        logger.info(f"模型已成功备份到: {backup_path}")
        print(f"模型已成功备份到: {backup_path}")
    except Exception as e:
        logger.error(f"发生错误: {e}")
        print(f"发生错误: {e}")
