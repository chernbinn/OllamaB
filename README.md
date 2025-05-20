一个简单的备份ollama平台下载的大模型应用，使用python实现。在windows下运行，跨平台性未测试。备份文件使用zip LZMA算法level 9压缩。

### 初始化运行环境
1.配置环境变量OLLAMA_MODELS，该环境变量ollama应用用来存储下载的大模型存放路径
```
OLLAMA_MODELS=F:\llm_models\ollama_modes
```
2.配置备份大模型路径的环境变量OLLAMA_BACKUP_PATH
```
OLLAMA_BACKUP_PATH=F:\llm_models\ollama_modes_backup
```
3.安装python依赖
```
pip install -r requirements.txt
```

### 运行：
python main.py
