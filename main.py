import os, sys
"""
只在当前 Python 进程中有效（即运行时有效）
以下导入的路径在项目中都可用，不需要在其他文件中再次配置
以下导入的路径，路径的最后一个目录作为包名，导入时不能显式使用该包名，除非包名之外还有指定的根包名，否则会报错找不到包名
作为根包名，有两种方式：
如果包目录下没有通过__init__.py导出具体的内容，在引用包中内容时，有以下几种方式导入：
1. 模块名，例如：import ollamab_ui
2. 模块名+具体内容，例如：from theme import Theme
3. 根包名.包名.模块名[.具体内容]
注意：即导入具体内容时，模块名不能省略，不支持包名+具体内容的导入方式
如果在包目录下使用__init__.py导出具体的内容，在引用包中内容时，有以下几种方式导入：
1. 模块名，例如：import ollamab_ui
2. 模块名+具体内容，例如：from theme import Theme
3. 根包名.包名[.模块名][.具体内容]
注意：作为根包名，使用根包名后面必须要跟包名，否则会报错找不到包名
作为非根包名，新增以下几种方式导入：
1. 包名.模块名，
2. 包名.模块名+具体内容: 有无__init__.py都可以使用
3. 包名.具体内容：依赖__init__.py导出具体的内容，否则报错找不到包名
4. 只要不是根包名，包名前可以叠加多个包名，例如：from framework.view import ollamab_ui
注意：
1.单例模式不同的导入方式，会导致单例失效，导入方式必须一致，否则会创建新的实例
2.使用不同的方式导入单例模式，会导入多个单例类，进而产生多个实例，导致单例失效
"""
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.extend([
    project_root,
    os.path.join(project_root, 'framework'),
    os.path.join(project_root, 'utils'),
    os.path.join(project_root, 'core'),
])

from view import ollamab_ui
import signal

def handle_ctrl_c(signum, frame):
    print("接收到 Ctrl+C，不响应程序终止，从窗口关闭程序。")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_ctrl_c)
    print(f"Current PID: {os.getpid()}, Parent PID: {os.getppid()}")
    ollamab_ui.run()