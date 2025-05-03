import logging
import logging.handlers
import inspect
import sys
import os

golbal_log_level = logging.DEBUG
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
        module_name = os.path.basename(frame.f_globals.get('__file__', 'unknown')).split('.')[0]
    _module_log_levels[module_name] = level

def get_module_log_level(module_name):
    """
    获取模块级日志级别
    :param module_name: 模块名称
    :return: 日志级别，如未设置返回None
    """
    return _module_log_levels.get(module_name)

class ModuleFilter(logging.Filter):
    def __init__(self, module_name):
        super().__init__()
        self.module_name = module_name

    def filter(self, record):
        return record.name == self.module_name


def setup_logging(log_level=logging.INFO, log_tag=None, b_log_file:bool=False, max_bytes=10485760, backup_count=0):
    # 参数说明：
    # max_bytes=10485760 (10MB) 单个日志文件最大尺寸
    # backup_count=5 保留5个备份文件
    # 使用RotatingFileHandler实现自动滚动
    """
    配置日志系统
    :param log_level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
    :param log_tag: 日志客制化标记，None表示使用默认值
    :param b_log_file: 是否输出到专有的日志文件，True表示输出到日志文件，False表示只输出到控制台
    :param max_bytes: 单个日志文件最大字节数
    :param backup_count: 保留的备份日志文件数量
    """
    # 获取调用模块名称
    frame = inspect.currentframe().f_back
    module_name = os.path.basename(frame.f_globals.get('__file__', 'unknown')).split('.')[0]
    log_tag = log_tag or module_name  # 如果没有提供log_tag，则使用模块名称作为log_tag
    
    # 创建模块级logger
    logger = logging.getLogger(module_name)
    adapter = logging.LoggerAdapter(logger, {'log_tag': log_tag})
    
    # module_level = get_module_log_level(module_name)
    logger.setLevel(log_level)
    
    # 清除现有handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 控制台handler
    # 在控制台格式中增加log_tag占位符
    console_formatter = logging.Formatter(
        '%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(log_tag)s]%(message)s')    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    #console_handler.setLevel(log_level)
    console_handler.encoding = 'utf-8'
    logger.addHandler(console_handler)
    
    # 全局日志文件（记录所有模块）
    global_handler = logging.handlers.RotatingFileHandler(
        "ollama_backup.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8',
        errors='replace'
    )
    #global_handler.setLevel(golbal_log_level)
    global_handler.setFormatter(logging.Formatter(
        '%(asctime)s-%(pathname)s:%(funcName)s:%(lineno)d-%(levelname)s-[%(log_tag)s]%(message)s'
    ))
    logger.addHandler(global_handler)

    if b_log_file:
        module_handler = logging.handlers.RotatingFileHandler(
            f"ollama_backup_{log_tag}.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
            errors='replace'
        )
        #module_handler.setLevel(log_level)
        module_handler.addFilter(ModuleFilter(module_name))
        # 确保所有handler使用相同格式
        module_handler.setFormatter(logging.Formatter(
            '%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(log_tag)s]%(message)s'
        ))
        logger.addHandler(module_handler)

    return adapter