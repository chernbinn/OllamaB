import logging
import logging.handlers
import inspect
import sys
import os
import threading

_release = False
_release_log_level = logging.INFO
_golbal_log_level = logging.DEBUG

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
    if _release:
        log_level = _release_log_level
        golbal_log_level = _release_log_level
    # 获取调用模块名称
    frame = inspect.currentframe().f_back
    module_name = os.path.basename(frame.f_globals.get('__file__', 'unknown')).split('.')[0]
    log_tag = log_tag or module_name  # 如果没有提供log_tag，则使用模块名称作为log_tag
    
    # 创建模块级logger
    logger = logging.getLogger(module_name)
    adapter = logging.LoggerAdapter(logger, {'log_tag': log_tag})
    
    logger.setLevel(_golbal_log_level)
    # 清除现有handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 控制台handler
    # 在控制台格式中增加log_tag占位符
    console_formatter = logging.Formatter(
        '%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(log_tag)s]%(message)s')    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    console_handler.encoding = 'utf-8'
    logger.addHandler(console_handler)
    
    # 全局日志文件（记录所有模块）
    global_handler = logging.handlers.RotatingFileHandler(
        "ollama_backup.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8',
        # errors='replace'
    )
    global_handler.setLevel(_golbal_log_level)
    global_handler.setFormatter(logging.Formatter(
        '%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d-%(levelname)s-[%(log_tag)s]%(message)s'
    ))
    logger.addHandler(global_handler)

    if b_log_file:
        module_handler = logging.handlers.RotatingFileHandler(
            f"ollama_backup_{log_tag}.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
            #errors='replace'
        )
        module_handler.setLevel(log_level)
        module_handler.addFilter(ModuleFilter(module_name))
        # 确保所有handler使用相同格式
        module_handler.setFormatter(logging.Formatter(
            '%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(log_tag)s]%(message)s'
        ))
        logger.addHandler(module_handler)
    
    def thread_excepthook(args):
        adapter.error(
            f"线程 {args.thread.name} 发生未捕获异常:",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback)
        )
    
    def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
        """
        全局捕获未处理的异常，并记录到日志
        """
        if issubclass(exc_type, KeyboardInterrupt):
            # 如果是 Ctrl+C 触发的 KeyboardInterrupt，不记录
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        adapter.error(
            "未捕获的异常:",
            exc_info=(exc_type, exc_value, exc_traceback)
        )

    # 设置主线程的异常钩子
    sys.excepthook = handle_uncaught_exception
    # 设置子线程的异常钩子（Python 3.8+）
    threading.excepthook = thread_excepthook

    return adapter