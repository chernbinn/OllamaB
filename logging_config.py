import logging
import logging.handlers
import inspect
from pathlib import Path

# 模块级日志级别配置
_module_log_levels = {}

def set_module_log_level(level, module_name=None):
    """
    设置模块级日志级别
    :param level: 日志级别
    :param module_name: 模块名称(可选)，如未提供则自动获取调用模块名
    """
    if module_name is None:
        frame = inspect.currentframe().f_back
        module_name = frame.f_globals.get('__name__', '__main__')
    _module_log_levels[module_name] = level

def get_module_log_level(module_name):
    """
    获取模块级日志级别
    :param module_name: 模块名称
    :return: 日志级别，如未设置返回None
    """
    return _module_log_levels.get(module_name)

def setup_logging(log_level=logging.INFO, log_file=None, max_bytes=10485760, backup_count=0):
    # 参数说明：
    # max_bytes=10485760 (10MB) 单个日志文件最大尺寸
    # backup_count=5 保留5个备份文件
    # 使用RotatingFileHandler实现自动滚动
    """
    配置日志系统
    :param log_level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
    :param log_file: 日志文件路径，None表示不输出到文件
    :param max_bytes: 单个日志文件最大字节数
    :param backup_count: 保留的备份日志文件数量
    """
    # 获取调用模块名称
    frame = inspect.currentframe().f_back
    module_name = frame.f_globals.get('__name__', '__main__')
    
    # 创建模块级logger
    logger = logging.getLogger(module_name)
    
    # 优先使用模块级日志级别，如未设置则使用全局级别
    module_level = get_module_log_level(module_name)
    logger.setLevel(module_level if module_level is not None else log_level)
    
    # 清除现有handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(pathname)s - [%(name)s:%(lineno)d] - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 文件handler
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(pathname)s - [%(name)s:%(lineno)d] - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger