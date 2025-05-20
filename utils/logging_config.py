# version 2.0

import logging
import inspect
import sys
import os, json
from pathlib import Path
import threading
from typing import TextIO, Dict
from threading import Lock
import atexit
from logging.handlers import RotatingFileHandler

# 需要开发者主动配置,可以是相对路径，也可以是绝对路径（相对于当前文件logging_config.py的路径）
_config_path="..\\logging.json"
""" json demo content:
{
    "app_name": "Ollamab",
    "release": true,
    "release_log_level": "INFO",
    "log_format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    "file_logging": {
      "enabled": true,
      "max_bytes": 10485760,
      "backup_count": 0
    }
  }
"""

_DEFAULT_CONFIG = {
    "app_name": "app",
    "release": False,
    "release_log_level": logging.INFO,  # release版本默认使用INFO级别，开发版本各模块使用各自的级别
    "file_logging": {
      "enabled": True,
      "max_bytes": 10485760,
      "backup_count": 0
    }
}
# 由于__init__.py的存在，在main.py中配置日志都是晚的。最好的方式是在第一次加载该模块时，主动从配置文件中读取配置
def _load_config():
    """从配置文件加载配置"""
    global _current_config, _config_path
    
    # 确定配置文件路径 (logging.json的路径)
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        #print(f"base_dir: {base_dir}")
        # 规范化路径格式(统一分隔符)
        normalized_path = os.path.normpath(_config_path)
        # 转换为绝对路径
        abs_path = normalized_path
        if not os.path.isabs(normalized_path):
            # 相对路径，结合基准目录
            abs_path = os.path.normpath(os.path.join(base_dir, normalized_path))
        config_path = str(Path(abs_path).resolve())
        #print(f"config_path: {config_path}")

        _config_path = config_path
        # 如果找到配置文件则加载，否则使用默认配置
        if os.path.exists(config_path):
            print(f"-------- load logging config from: {config_path} --------")
            with open(config_path, 'r') as f:
                _current_config = {**_DEFAULT_CONFIG, **json.load(f)}
            return True
    except Exception as e:
        print(f"加载日志配置文件失败: {e}, 使用默认配置")

    print(f"""\033[93m
    未找到日志配置文件，使用默认配置。如果需要自定义配置，请在项目根目录下创建logging.json文件,
    且在logging_config.py中初始化logging.json路径。
    \033[0m""")
    
    _current_config = _DEFAULT_CONFIG.copy()
    return False

class FileManager:
    """全局文件管理器（单例模式）"""
    _instance = None
    _lock = Lock()
    _open_regular_files: Dict[str, TextIO] = {}
    _open_rotating_handlers: Dict[str, RotatingFileHandler] = {}
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_regular_file(self, path: str, mode: str = "a") -> TextIO:
        """获取普通文件对象"""
        with self._lock:
            if path not in self._open_regular_files:
                self._prepare_directory(path)
                try:
                    print(f"-------- open regular file: {path} --------")
                    self._open_regular_files[path] = open(path, mode, encoding="utf-8")
                except (IOError, OSError) as e:
                    sys.stderr.write(f"Failed to open file {path}: {str(e)}\n")
                    raise
            return self._open_regular_files[path]

    def get_rotating_handler(
        self,
        path: str,
        max_bytes: int = 10*1024*1024,
        backup_count: int = 5,
        mode: str = "a"
    ) -> RotatingFileHandler:
        """获取RotatingFileHandler"""
        with self._lock:            
            if path not in self._open_rotating_handlers:
                print(f"-------- open rotation file: {path}")
                self._prepare_directory(path)
                handler = RotatingFileHandler(
                    filename=path,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding="utf-8",
                    mode=mode
                )
                self._open_rotating_handlers[path] = handler
            return self._open_rotating_handlers[path]
    
    def _prepare_directory(self, path: str):
        """确保目录存在"""
        dirname = os.path.dirname(path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
    
    def close_all(self):
        print("-------- close all log files --------")
        with self._lock:
            for path, file in self._open_regular_files.items():
                try:
                    file.close()
                except:
                    pass
            self._open_regular_files.clear()
            # 关闭RotatingHandler
            for path, handler in self._open_rotating_handlers.items():
                try:
                    handler.close()
                except:
                    pass
            self._open_rotating_handlers.clear()

_file_manager = FileManager()
atexit.register(_file_manager.close_all)

"""
root logger无法区分模块
自定义的logger只负责输出自己模块的日志
root logger和自定义logger配置相同的处理器情况下，抓取的日志相同。
lastResort 不受 propagate=False 影响，只要是无处理器的日志，都会输出到lastResort处理器。
logger抓取的日志都是显性调用了logger方法的。一些在控制台打印的log，是由python内部的其他函数输出的，比如未捕获的异常
全面捕获日志：
1.明确logger的可输出log空间，比如线程、协程、进程等
2.代码中完善的日志抓取
3.使用logging模块的lastResort处理器，将未被捕获的异常输出到控制台或文件中

考虑到日志无法全面通过logger输出，采用：logger输出到控制台，使用Tee同时输出到文件
"""
class Tee:
    """同时写入文件和终端"""
    def __init__(self, file_paths: list, original_stream=sys.stdout, mode="a", rotating_config: dict = None):
        """
        :param rotating_config: {
            'max_bytes': 10*1024*1024,
            'backup_count': 5
        }
        """
        self.original_stream = original_stream
        self.file_manager = FileManager()
        self.handlers = []

        #print(f"---------tee init : {file_paths}")
        for path in file_paths:
            try:
                if rotating_config:
                    handler = self.file_manager.get_rotating_handler(
                        path,
                        max_bytes=rotating_config.get('max_bytes', 10*1024*1024),
                        backup_count=rotating_config.get('backup_count', 5),
                        mode=mode
                    )
                else:
                    handler = self.file_manager.get_regular_file(path, mode)
                self.handlers.append(handler)
            except (IOError, OSError):
                continue

    def write(self, data: str) -> None:
        self.original_stream.write(data)
        for handler in self.handlers:
            try:
                if isinstance(handler, RotatingFileHandler):
                    # 需要先获取stream再写入
                    handler.stream.write(data)
                else:
                    handler.write(data)
            except (IOError, OSError):
                continue

    def flush(self):
        for handler in self.handlers:
            try:
                if isinstance(handler, RotatingFileHandler) and handler.stream is not None:
                    handler.stream.flush()
                else:
                    handler.flush()
            except (IOError, OSError):
                continue

    def close(self) -> None:
        pass # FileManager会自动关闭文件
        """
        for file in self.files:
            try:
                file.close()
            except (IOError, OSError):
                continue
        """

class ModuleFilter(logging.Filter):
    def __init__(self, logger_name):
        super().__init__()
        self.logger_name = logger_name

    def filter(self, record):
        return record.name == self.logger_name or record.exc_info is not None

def _get_caller_module() -> str:
    """获取调用者模块名"""
    frame = inspect.currentframe()
    try:
        # 回溯两层：当前函数 -> 调用者
        if frame is not None and frame.f_back is not None:
            caller_frame = frame.f_back.f_back
            if caller_frame is not None:
                module_path = caller_frame.f_globals.get("__file__", "unknown")
                return os.path.splitext(os.path.basename(module_path))[0]
    finally:
        del frame  # 避免循环引用
    return "unknown"

# 预留，如果使用stdout、stderr重定向，可以不使用该方法
def _setup_exception_handling(logger: logging.Logger) -> None:
    """配置异常处理"""
    def handle_thread_exception(args) -> None:
        logger.error(
            "Uncaught exception in thread %s",
            args.thread.name,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback)
        )

    def handle_uncaught_exception(exc_type, exc_value, exc_traceback) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        logger.error(
            "Uncaught exception",
            exc_info=(exc_type, exc_value, exc_traceback)
        )

    sys.excepthook = handle_uncaught_exception
    threading.excepthook = handle_thread_exception

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
    effective_log_level = _current_config["release_log_level"] if _current_config["release"] else log_level
    global_log_level = _current_config["release_log_level"] if _current_config["release"] else logging.DEBUG
    app_name = f"{_current_config['app_name']}_{'release' if _current_config['release'] else 'debug'}"
    log_path = os.path.join(os.path.dirname(_config_path), "logs")

    # 获取调用模块名称
    module_name = log_tag
    if not log_tag:
        module_name = _get_caller_module()
    
    # 创建模块级logger
    logger = logging.getLogger(module_name)
    # adapter = logging.LoggerAdapter(logger, {'log_tag': log_tag}) # 可以不需要绑定log_tag，log_tag可以传递给name,直接使用%(name)s    
    logger.setLevel(global_log_level)  # 过滤日志的第一个步骤，设置全局日志级别

    # 清除现有handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    formatter = logging.Formatter(
        _current_config["log_format"] if _current_config["log_format"] else
        '%(asctime)s-%(funcName)s:%(lineno)d-%(levelname)s-[%(name)s]%(message)s'
    )
    
    enable_file = _current_config["file_logging"]["enabled"] if _current_config.get("file_logging", {}).get("enabled") else False
    #print(f"---------- enable log file: {enable_file}")
    log_files = []
    if enable_file and (not isinstance(sys.stdout, Tee) or not isinstance(sys.stderr, Tee)):
        # 配置完全log文件，包括stdout、stderr
        # 程序运行的目录可能存在多变，使用绝对路径。否则正式部署后，log在不同目录下会导致日志文件无法找到，比如用户根目录
        log_files.append(os.path.join(log_path, f"{app_name}.log"))
        global_config = {
            'max_bytes': _current_config["file_logging"]["max_bytes"] if _current_config.get("file_logging", {}).get("max_bytes") else 10*1024*1024,
            'backup_count': _current_config["file_logging"]["backup_count"] if _current_config.get("file_logging", {}).get("backup_count") else 5
        }
        sys.stdout = Tee(log_files, sys.stdout, "a", global_config)
        sys.stderr = Tee(log_files, sys.stderr, "a", global_config)

    if not _current_config['release'] and b_log_file:
        # 配置模块级log文件，不包括stdout，文件内容使用logging模块过滤所得及stderr重定向内容
        log_files.append(os.path.join(log_path, f"{app_name}_{module_name}.log"))
        moudle_config = {
            'max_bytes': max_bytes,
            'backup_count': backup_count
        }
        sys.stderr = Tee(log_files, sys.stderr, "a", moudle_config)

    # 控制台handler
    # 在控制台格式中增加log_tag占位符
    console_formatter = formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(effective_log_level)   # 处理器的日志级别
    console_handler.encoding = 'utf-8'
    logger.addHandler(console_handler)

    if b_log_file and not _current_config["release"]:
        module_handler = _file_manager.get_rotating_handler(
            os.path.join(log_path, f"{app_name}_{module_name}.log"),
            max_bytes=max_bytes,
            backup_count=backup_count,
            mode="a"
        )
        module_handler.setLevel(effective_log_level)
        module_handler.addFilter(ModuleFilter(module_name))
        # 确保所有handler使用相同格式
        module_handler.setFormatter(formatter)
        logger.addHandler(module_handler)    

    # 配置异常处理, 使用了stdout、stderr重定向，不需要使用该方法
    #_setup_exception_handling(logger)

    return logger

_load_config()