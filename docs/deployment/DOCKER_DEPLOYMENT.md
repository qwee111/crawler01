# Docker 部署指南

本文档将指导您如何在 Linux 服务器上使用 Docker 和 Docker Compose 部署 `crawler01` 项目。

## 1. 环境准备

### 1.1 安装 Docker

在您的 Linux 服务器上安装 Docker。以下是针对不同 Linux 发行版的安装命令：

**Ubuntu/Debian:**

```bash
# 更新apt包索引
sudo apt update
# 安装必要的包，允许apt通过HTTPS使用仓库
sudo apt install -y ca-certificates curl gnupg
# 添加Docker的官方GPG密钥
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
# 设置仓库
echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
# 再次更新apt包索引
sudo apt update
# 安装Docker Engine、Containerd和Docker Compose
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

**CentOS/RHEL:**

```bash
# 卸载旧版本
sudo yum remove docker \
                  docker-client \
                  docker-client-latest \
                  docker-common \
                  docker-latest \
                  docker-latest-logrotate \
                  docker-logrotate \
                  docker-engine
# 安装yum-utils
sudo yum install -y yum-utils
# 添加Docker仓库
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
# 安装Docker Engine、Containerd和Docker Compose
sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
# 启动Docker
sudo systemctl start docker
# 设置Docker开机自启
sudo systemctl enable docker
```

**验证 Docker 安装：**

```bash
sudo docker run hello-world
```

如果看到 "Hello from Docker!" 的消息，则表示 Docker 安装成功。

### 1.2 安装 Docker Compose (如果未通过 `docker-compose-plugin` 安装)

较新版本的 Docker 已经将 Docker Compose 作为 `docker-compose-plugin` 包含在内，可以通过 `docker compose` 命令直接使用。如果您使用的是旧版本 Docker 或未安装 `docker-compose-plugin`，可以手动安装：

```bash
# 下载最新稳定版Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
# 赋予执行权限
sudo chmod +x /usr/local/bin/docker-compose
# 验证安装
docker-compose --version
```

## 2. 项目部署

### 2.1 复制项目代码

将 `crawler01` 项目的整个代码库复制到您的 Linux 服务器上。您可以使用 `git clone` 命令：

```bash
git clone https://github.com/qwee111/crawler01.git
cd crawler01
```

### 2.2 配置环境变量

项目使用 `.env` 文件来管理环境变量。请在项目根目录创建 `.env` 文件，并根据您的实际需求配置数据库凭据、Redis 密码等。您可以参考 `deployment/docker/.env.example` 文件。

```bash
# 示例 .env 文件内容 (请根据实际情况修改)
REDIS_PASSWORD=your_redis_password
MONGODB_ROOT_USERNAME=admin
MONGODB_ROOT_PASSWORD=password
MONGODB_DATABASE=crawler_db
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
PGADMIN_EMAIL=admin@crawler.com
PGADMIN_PASSWORD=admin123
MONGO_EXPRESS_USER=admin
MONGO_EXPRESS_PASSWORD=pass
```

### 2.3 构建 Docker 镜像

在项目根目录（包含 `deployment/docker` 目录的上一级目录）执行以下命令来构建 `crawler` 服务的 Docker 镜像：

```bash
docker compose build crawler
```
或者，如果您想构建所有服务（包括管理界面和 Selenium Grid），可以使用：
```bash
docker compose build
```

### 2.4 启动项目服务

在项目根目录执行以下命令，使用 Docker Compose 启动所有服务：

```bash
docker compose up -d
```

这将会在后台启动所有在 `docker-compose.yml` 中定义的服务。

如果您只想启动核心服务（不包括管理界面和 Selenium Grid），可以使用 `profiles`：

```bash
docker compose --profile tools --profile selenium up -d
```
这将启动所有服务，包括 `tools` 和 `selenium` 配置文件中的服务。

### 2.5 停止项目服务

要停止所有运行中的服务，请在项目根目录执行：

```bash
docker compose down
```

## 3. 验证项目运行

项目启动后，您可以通过以下方式验证服务是否正常运行：

1.  **查看容器状态：**
    ```bash
    docker compose ps
    ```
    确保所有服务的状态都是 `running` 或 `healthy`。

2.  **查看服务日志：**
    ```bash
    docker compose logs -f [服务名称]
    ```
    例如，查看爬虫服务的日志：
    ```bash
    docker compose logs -f crawler
    ```
    检查日志输出，确保没有明显的错误信息。

3.  **访问管理界面 (如果启动了 `tools` profile):**
    *   **Redis Commander:** `http://your_server_ip:8081`
    *   **Mongo Express:** `http://your_server_ip:8082`
    *   **MinIO Console:** `http://your_server_ip:9001`
    *   **Selenium Grid Hub:** `http://your_server_ip:4444`

    使用您在 `.env` 文件中配置的凭据登录。

4.  **验证爬虫功能：**
    *   如果您的爬虫有 Web 界面或 API，尝试访问并触发爬取任务。
    *   检查数据库（MongoDB/PostgreSQL）中是否有新抓取的数据。
    *   检查项目 `logs` 目录（挂载到容器的 `/app/logs`）中的日志文件。

## 4. 常见问题与故障排除

*   **端口冲突：** 如果您的服务器上已有服务占用了 Docker Compose 中定义的端口（例如 6379, 27017, 5432, 8081, 8082, 8083, 4444），您需要在 `docker-compose.yml` 中修改端口映射，或者停止占用端口的服务。
*   **权限问题：** 确保 Docker 用户对项目目录有读写权限。
*   **网络问题：** 检查服务器防火墙设置，确保所需的端口是开放的。
*   **内存不足：** 如果服务器内存不足，部分服务可能无法启动。考虑增加服务器内存或优化 Docker 配置。
*   **日志分析：** 遇到问题时，始终优先查看 `docker compose logs -f [服务名称]` 的输出，这通常能提供解决问题的关键信息。
