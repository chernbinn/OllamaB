import os, sys, signal

class ProcessTerminator:
    @staticmethod
    def terminate(pid: int):
        if sys.platform == 'win32':
            return ProcessTerminator._windows_terminate(pid)
        else:
            return ProcessTerminator._posix_terminate(pid)

    @staticmethod
    def _windows_terminate(pid: int):
        try:
            import ctypes
            PROCESS_TERMINATE = 1
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, 0, pid)
            if not handle:
                raise ctypes.WinError()
            ctypes.windll.kernel32.TerminateProcess(handle, -1)
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        except Exception as e:
            logger.error(f"Windows terminate failed: {e}")
            return False

    @staticmethod
    def _posix_terminate(pid: int):
        try:
            os.kill(pid, signal.SIGKILL)
            return True
        except ProcessLookupError:
            return True  # 进程已退出
        except Exception as e:
            logger.error(f"POSIX terminate failed: {e}")
            return False