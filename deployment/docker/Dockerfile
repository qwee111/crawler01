# 使用官方Python运行时作为父镜像
FROM python:3.11-slim-buster

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制整个项目到容器中
COPY . /app

# 设置环境变量
ENV PYTHONUNBUFFERED 1

# 暴露Scrapy的默认端口 (如果需要，例如Scrapy Web UI)
# EXPOSE 6800

# 定义容器启动时运行的命令
# CMD ["scrapy", "crawl", "your_spider_name"]
