# 将基础镜像从 3.11 升级到 3.12，以支持最新的依赖库
FROM python:3.12-slim

# 设置工作目录
WORKDIR /usr/src/app

# 复制依赖文件到容器中
COPY requirements.txt ./

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制整个应用目录到容器中
COPY . .

# 容器启动时运行的命令 (已修正)
CMD ["python", "main.py"]