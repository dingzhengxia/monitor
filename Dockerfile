# 使用官方轻量级 Python 3.11 镜像
FROM python:3.11-slim

# 设置容器内的工作目录
WORKDIR /usr/src/app

# 复制依赖文件并安装，这可以利用 Docker 的缓存机制
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 复制你的程序文件
# 由于你的代码入口在 app/main.py，我们把整个项目都复制进去
COPY . .

# 启动程序
# 这是容器启动时执行的命令
CMD ["python", "app/main.py"]