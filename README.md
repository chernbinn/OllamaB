
# ollamab

一个简单的备份ollama平台下载的大模型Python应用。在Windows下运行，跨平台性未测试。使用zip LZMA算法level 9压缩备份大模型文件。

## 搭建环境

1.配置环境变量
<<<<<<< HEAD
配置环境变量OLLAMA_MODELS，该环境变量ollama应用用来存储下载的大模型存放路径，安装ollama应用时需要配置，如果不配置就是默认路径，默认路径是C:\Users\用户名\\.ollama
=======
配置环境变量OLLAMA_MODELS，该环境变量ollama应用用来存储下载的大模型存放路径，安装ollama应用时需要配置，如果不配置就是默认路径，默认路径是C:\Users\用户名\.ollama\models
>>>>>>> 8836c414c3d136419d62d12b1b4c6c0a19bf2ec3

```shell
OLLAMA_MODELS=F:\llm_models\ollama_modes
```

2.配置备份大模型路径的环境变量OLLAMA_BACKUP_PATH

```shell
OLLAMA_BACKUP_PATH=F:\llm_models\ollama_modes_backup
```

3.安装python依赖
3.1 可选：使用虚拟环境

创建虚拟环境

```shell
python -m venv pyollamab
```

激活虚拟环境

```shell
pyollamab\Scripts\activate
```

3.2 安装python依赖

```shell
pip install -r requirements.txt
```

## 运行

```shell
python main.py
```

## 附加

### 1. 封装为命令行命令

脚本封装为命令行命令，在任意路径下执行

1. 在代码根目录创建虚拟环境

```shell
python -m venv pyollamab
```

2. 激活虚拟环境

```shell
pyollamab\Scripts\activate
```

3. 安装python依赖

```shell
pip install -r requirements.txt
```

4. ollamab.bat添加到环境变量PATH

    4.1 使用命令或者图形界面添加ollamab.bat到环境变量PATH

    4.2 添加完成后，重启终端，确保最新环境变量PATH在终端生效

5. 终端命令行执行命令

```
ollamab
```
