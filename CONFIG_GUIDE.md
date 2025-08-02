# 🔧 配置指南

本指南将帮助您快速配置企业级分布式爬虫系统的环境变量和数据库设置。

## 📋 配置方式

我们提供了三种配置方式，您可以根据需要选择：

### 🚀 方式一：快速配置（推荐新手）

适合快速开始开发，使用安全的默认配置：

```bash
python quick_setup.py
```

**特点：**
- ✅ 一键生成所有配置
- ✅ 自动生成安全密码
- ✅ 使用开发环境默认值
- ✅ 无需手动输入

### 🎯 方式二：向导配置（推荐生产环境）

交互式配置向导，可以自定义所有设置：

```bash
python setup_config.py
```

**特点：**
- ✅ 交互式配置界面
- ✅ 可自定义所有参数
- ✅ 支持邮件/Slack告警配置
- ✅ 适合生产环境

### ✏️ 方式三：手动配置

手动编辑配置文件：

```bash
# 复制示例文件
cp deployment/docker/.env.example .env

# 编辑配置文件
nano .env
```

## 🔍 配置验证

配置完成后，使用验证脚本检查配置是否正确：

```bash
python validate_config.py
```

验证脚本会检查：
- 📁 配置文件是否存在
- 🔑 必需的环境变量
- 🔐 密码强度
- 🗄️ 数据库配置
- 🕷️ 爬虫参数
- 📧 通知设置

## 📊 配置项说明

### 🗄️ 数据库配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `POSTGRES_PASSWORD` | PostgreSQL密码 | 自动生成 |
| `MONGODB_ROOT_PASSWORD` | MongoDB管理员密码 | 自动生成 |
| `REDIS_PASSWORD` | Redis密码（可选） | 自动生成 |

### 🔐 安全配置

| 配置项 | 说明 | 长度 |
|--------|------|------|
| `SECRET_KEY` | 应用密钥 | 64位 |
| `API_TOKEN` | API访问令牌 | 32位 |
| `JWT_SECRET` | JWT签名密钥 | 64位 |

### 🕷️ 爬虫配置

| 配置项 | 说明 | 推荐值 |
|--------|------|--------|
| `CONCURRENT_REQUESTS` | 并发请求数 | 16 |
| `DOWNLOAD_DELAY` | 下载延迟(秒) | 1 |
| `PROXY_POOL_SIZE` | 代理池大小 | 100 |
| `RETRY_TIMES` | 重试次数 | 3 |

### 📧 通知配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `SMTP_USERNAME` | 邮箱用户名 | your-email@gmail.com |
| `SMTP_PASSWORD` | 邮箱密码/应用密码 | - |
| `SLACK_WEBHOOK_URL` | Slack Webhook | https://hooks.slack.com/... |

## 🚀 启动系统

配置完成后，按以下步骤启动系统：

### 1. 验证配置
```bash
python validate_config.py
```

### 2. 设置开发环境
```bash
python setup_dev_env.py
```

### 3. 启动存储服务
```bash
python deployment/scripts/start_storage.py start --with-tools
```

### 4. 运行爬虫
```bash
# 激活虚拟环境
source .venv/bin/activate  # Linux/Mac
# 或
.venv\Scripts\activate     # Windows

# 运行爬虫
scrapy crawl nhc
```

## 🌐 管理界面

启动服务后，可以访问以下管理界面：

| 服务 | 地址 | 用途 |
|------|------|------|
| MongoDB Express | http://localhost:8082 | MongoDB数据管理 |
| pgAdmin | http://localhost:8083 | PostgreSQL数据管理 |
| Redis Commander | http://localhost:8081 | Redis数据管理 |
| MinIO Console | http://localhost:9001 | 对象存储管理 |

## 🔐 登录信息

### MongoDB Express
- 用户名: `admin`
- 密码: 配置文件中的 `MONGO_EXPRESS_PASSWORD`

### pgAdmin
- 邮箱: 配置文件中的 `PGADMIN_EMAIL`
- 密码: 配置文件中的 `PGADMIN_PASSWORD`

### MinIO Console
- 用户名: `minioadmin`
- 密码: 配置文件中的 `MINIO_ROOT_PASSWORD`

## 🛠️ 常见问题

### Q: 忘记了生成的密码怎么办？
A: 密码保存在 `.env` 文件中，可以直接查看：
```bash
grep PASSWORD .env
```

### Q: 如何修改配置？
A: 有三种方式：
1. 重新运行配置脚本
2. 直接编辑 `.env` 文件
3. 使用 `validate_config.py` 检查修改后的配置

### Q: 生产环境需要注意什么？
A: 
- ✅ 使用强密码
- ✅ 定期更换密钥
- ✅ 启用SSL/TLS
- ✅ 配置防火墙
- ✅ 定期备份数据

### Q: 如何重置所有配置？
A: 删除配置文件后重新生成：
```bash
rm .env deployment/docker/.env
python quick_setup.py
```

### Q: 配置文件在哪里？
A: 主要配置文件位置：
- `.env` - 主配置文件
- `deployment/docker/.env` - Docker配置文件
- `config/` - 网站和服务配置目录

## 📞 获取帮助

如果遇到问题，可以：

1. **查看日志**：
   ```bash
   python deployment/scripts/start_storage.py logs
   ```

2. **检查服务状态**：
   ```bash
   python deployment/scripts/start_storage.py status
   ```

3. **重启服务**：
   ```bash
   python deployment/scripts/start_storage.py restart
   ```

## 🔄 配置更新

当需要更新配置时：

1. 停止服务
2. 修改配置文件
3. 验证配置
4. 重启服务

```bash
# 停止服务
python deployment/scripts/start_storage.py stop

# 修改配置
nano .env

# 验证配置
python validate_config.py

# 重启服务
python deployment/scripts/start_storage.py start --with-tools
```

---

🎉 **配置完成后，您的爬虫系统就可以开始工作了！**
