import logging
import logging.handlers
import inspect
import sys, os
import threading, asyncio

_release = False
_release_log_level = logging.INFO
_golbal_log_level = logging.DEBUG
_app_name = "ollama_backup"

"""
临时单个脚本使用的日志配置，此处做备份，方便拷贝使用
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # 输出到控制台
    ]
)
logger = logging.getLogger(__name__)
"""

def basic_setup(log_level=logging.DEBUG, log_tag=None):
    global _release_log_level, _golbal_log_level, _release, _app_name
    module_name = log_tag
    if not module_name:
        frame = inspect.currentframe().f_back
        module_name = os.path.basename(frame.f_globals.get('__file__', 'unknown')).split('.')[0]

    if _release:
        log_level = _release_log_level
        _golbal_log_level = _release_log_level    
        
    console_handler = logging.StreamHandler(sys.stdout)
    #console_formatter = logging.Formatter('%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s')
    #console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    console_handler.encoding = 'utf-8'
    # addHandler和basicConfig有根本上的区别，basicConfig是根log，获取的log比较全面；使用addHandler需要更多的配置
    #logger.addHandler(console_handler)

    logging.basicConfig(
    level=_golbal_log_level,
    format='%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s',
        handlers=[
            logging.FileHandler(f"{_app_name}.log", encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # 输出到控制台
        ]
    )

    # 创建模块级logger
    logger = logging.getLogger(module_name)
    logger.setLevel(_golbal_log_level)

    return logger

class ModuleFilter(logging.Filter):
    def __init__(self, module_name):
        super().__init__()
        self.module_name = module_name

    def filter(self, record):
        # 可能依然存在一些需要的日志被过滤掉，后续根据需求再调整
        #print(f"---------- {record.name} ++ {record.exc_info}")
        return record.name == self.module_name or not record.name or record.exc_info

def setup_logging(log_level=logging.DEBUG, log_tag=None, b_log_file:bool=False, max_bytes=10485760, backup_count=0):
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
    global _release_log_level, _golbal_log_level, _release
    if _release:
        log_level = _release_log_level
        _golbal_log_level = _release_log_level
    # 获取调用模块名称
    module_name = log_tag
    if not module_name:
        frame = inspect.currentframe().f_back
        module_name = os.path.basename(frame.f_globals.get('__file__', 'unknown')).split('.')[0]
    # 使用module_name获取日志，实际上module_name传递给了formatter中的%(name)s变量
    # 因此不需要绑定log_tag
    """
    frame = inspect.currentframe().f_back
    module_name = os.path.basename(frame.f_globals.get('__file__', 'unknown')).split('.')[0]
    log_tag = log_tag or module_name  # 如果没有提供log_tag，则使用模块名称作为log_tag
    adapter = logging.LoggerAdapter(logger, {'log_tag': log_tag})
    """
    # 控制台handler    
    console_handler = logging.StreamHandler(sys.stdout)
    #console_formatter = logging.Formatter('%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s')    
    #console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    console_handler.encoding = 'utf-8'
    #logger.addHandler(console_handler)
    
    # 全局日志文件（记录所有模块）
    global_handler = logging.handlers.RotatingFileHandler(
        f"{_app_name}.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8',
        # errors='replace'
    )
    global_handler.setLevel(_golbal_log_level)
    #global_handler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s'))
    #logger.addHandler(global_handler)

    if b_log_file:
        module_handler = logging.handlers.RotatingFileHandler(
            f"{_app_name}_{module_name}.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
            #errors='replace'
        )
        module_handler.setLevel(log_level)
        module_handler.addFilter(ModuleFilter(module_name))
        # 确保所有handler使用相同格式
        #module_handler.setFormatter(logging.Formatter(
        #    '%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s'
        #))
        #logger.addHandler(module_handler)
        logging.basicConfig(
            level=_golbal_log_level,
            format='%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s',
            handlers=[
                #logging.FileHandler("ollama_backup.log", encoding='utf-8'),
                #logging.StreamHandler(sys.stdout)  # 输出到控制台
                console_handler,
                global_handler,
                module_handler # 存在log不全的情况，作为辅助debug使用，根据需求优化调整
            ]
        )
    else:
        logging.basicConfig(
            level=_golbal_log_level,
            format='%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s',
            handlers=[
                #logging.FileHandler("ollama_backup.log", encoding='utf-8'),
                #logging.StreamHandler(sys.stdout)  # 输出到控制台
                console_handler,
                global_handler,
            ]
        )
    
    # 创建模块级logger
    logger = logging.getLogger(module_name)
    
    #logger.setLevel(_golbal_log_level)
    # 清除现有handler
    #for handler in logger.handlers[:]:
    #    logger.removeHandler(handler)
    
    """
    def thread_excepthook(args):
        logger.error(
            f"线程 {args.thread.name} 发生未捕获异常:",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback)
        )
    
    def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # 如果是 Ctrl+C 触发的 KeyboardInterrupt，不记录
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error(
            "未捕获的异常:",
            exc_info=(exc_type, exc_value, exc_traceback)
        )

    def handle_unraisable(unraisable):
        if hasattr(unraisable, 'exc_value') and unraisable.exc_value:
            exc_info = (type(unraisable.exc_value), unraisable.exc_value, unraisable.exc_traceback)
        else:
            exc_info = None
        logger.error(
            f"Unraisable exception in {getattr(unraisable, 'object', 'unknown')}: {getattr(unraisable, 'exc_value', 'no exception')}",
            exc_info=exc_info
        )
    sys.unraisablehook = handle_unraisable

    # 设置主线程的异常钩子
    sys.excepthook = handle_uncaught_exception
    # 设置子线程的异常钩子（Python 3.8+）
    threading.excepthook = thread_excepthook

    # 1. 设置 asyncio 全局异常处理器
    def handle_asyncio_exception(loop, context):
        exc = context.get('exception', None)
        if exc:
            logger.error("Unhandled asyncio exception", exc_info=exc)
        else:
            logger.error(f"Asyncio error: {context.get('message')}")

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(handle_asyncio_exception)
    """

    return logger