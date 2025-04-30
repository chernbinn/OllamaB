import logging
from tkinter import ttk
from enum import Enum

logger = logging.getLogger(__name__)

class Theme(Enum):
    DEFAULT = "default"
    WARM = "warm"

class StyleConfigurator:
    @staticmethod
    def configure_style(app, theme_name=Theme.DEFAULT):
        """
        配置应用程序样式主题
        
        参数:
            app: 应用程序实例
            theme_name: 主题名称 ('default' 或 'warm')
        """
        style = ttk.Style()
        
        # 强制使用 'clam' 主题（支持完整自定义）
        style.theme_use("clam")

        logger.debug(f"可用主题: {style.theme_names()}")
        logger.debug(f"当前主题: {style.theme_use()}")
        logger.debug(f"应用主题: {theme_name}")

        if theme_name.value == Theme.WARM.value:
            StyleConfigurator._configure_warm_theme(style, app)
        else:
            StyleConfigurator._configure_default_theme(style, app)

    @staticmethod
    def _configure_default_theme(style, widget):
        """配置默认主题样式"""
        # 基础Treeview样式
        style.configure('Treeview',
                    background='white',
                    rowheight=25,
                    fieldbackground='white',
                    bordercolor='#e0e0e0',
                    borderwidth=1)
        # 表头样式
        style.configure('Treeview.Heading',
                    background='#f0f0f0',
                    foreground='black',
                    font=('Arial', 10, 'bold'),
                    relief='raised',
                    padding=5)

        # 行样式（必须使用.tag_configure方式）
        if hasattr(widget, 'tree'):
            widget.tree.tag_configure('oddrow', background='white')
            widget.tree.tag_configure('evenrow', background='#f5f5f5')
            widget.tree.tag_configure('childrow', background='#e9e9e9')

        # 选中状态映射
        style.map("Treeview",
                background=[('selected', '#0078d7')],
                foreground=[('selected', 'white')])

    @staticmethod
    def _configure_warm_theme(style, widget):
        """配置温暖主题样式"""
        # 全局字体设置
        default_font = ('Microsoft YaHei', 10)  # 可根据系统调整
        
        # 基础框架样式（浅米色背景）
        style.configure('TFrame', 
                    background='#FFF5E6',  # 浅米色
                    borderwidth=2,
                    relief='groove',
                    bordercolor='#FFD6A8')
        
        # 按钮样式（橙色系）
        style.configure('TButton',
                    background='#FFB347',  # 阳光橙
                    foreground='white',
                    font=default_font,
                    padding=5,
                    relief='raised',
                    bordercolor='#FF9500')
        style.map('TButton',
                background=[('active', '#FF9500'),  # 按下时变深
                            ('disabled', '#FFD699')])  # 禁用时变浅
        
        # 输入框样式
        style.configure('TEntry',
                    fieldbackground='white',
                    foreground='#5A4A3A',  # 咖啡色文字
                    bordercolor='#FFD6A8',
                    insertcolor='#FF9500',  # 光标橙色
                    padding=5)
        
        # 标签样式
        style.configure('TLabel',
                    background='#FFF5E6',
                    foreground='#5A4A3A',
                    font=default_font,
                    padding=(8, 5, 8, 5))
        
        # 滚动条样式
        style.configure('Vertical.TScrollbar',
                    background='#FFA726',
                    troughcolor='#FFD6A8',
                    gripcount=1,
                    arrowsize=12)
        
        # 树形视图样式（温暖风格）
        style.configure('Treeview',
                    background='#FFF9F0',  # 奶油白
                    foreground='#5A4A3A',
                    rowheight=28,
                    fieldbackground='#FFF9F0',
                    bordercolor='#FFD6A8',
                    font=default_font,
                    borderwidth=0,
                    highlightthickness=0,
                    padding=(8, 10))
        
        # 树形视图表头
        style.configure('Treeview.Heading',
                    background='#FFB347',
                    foreground='white',
                    font=('Microsoft YaHei', 10, 'bold'),
                    relief='flat',
                    padding=(8, 5, 8, 5))                    
        style.map('Treeview.Heading',
             background=[('active', '#FFB347'),  # 悬停状态
                        ('!active', '#FFB347')], # 正常状态
             relief=[('active', 'flat'),
                    ('!active', 'flat')])
        
        # 树形视图行样式
        if hasattr(widget, 'tree'):
            widget.tree.tag_configure('oddrow', background='#FFF9F0')  # 奶油白
            widget.tree.tag_configure('evenrow', background='#FFE8D6') # 淡珊瑚
            widget.tree.tag_configure('childrow', background='#FFD6A8') # 浅橙
        
        # 选中状态
        style.map('Treeview',
                background=[('selected', '#E67E22')],  # 南瓜橙
                foreground=[('selected', 'white')])
        
        # PanedWindow分隔线样式
        style.configure('TPanedwindow', 
                    background='#FFD6A8',
                    sashwidth=8,
                    sashrelief='flat')