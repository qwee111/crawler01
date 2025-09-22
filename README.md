# 分布式疫情信息爬虫系统

一个基于Scrapy-Redis的分布式爬虫系统，专注于收集和处理全国各级疾控中心和卫生部门的疫情信息。

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](#)
[![Scrapy](https://img.shields.io/badge/scrapy-2.13.3-red)](https://scrapy.org/)

## 核心功能与特性

- **分布式架构**：基于Scrapy-Redis构建，支持横向扩展
- **多源数据采集**：覆盖全国各级疾控中心、卫健委等官方网站
- **智能反爬机制**：集成Selenium Grid、代理池、User-Agent轮换等反爬策略
- **灵活配置**：支持针对不同网站的定制化采集规则
- **数据质量保障**：内置数据验证、清洗和标准化流程
- **实时监控**：集成Prometheus和Grafana监控系统
- **容器化部署**：支持Docker和Kubernetes部署

## 环境要求

- Python 3.9+
- Redis 5.0+
- MongoDB 4.0+
- PostgreSQL 12+
- Docker (可选，用于Selenium Grid)
- Windows 10/11, Linux 或 macOS

## 安装步骤

### 使用uv安装（推荐）

```bash
# 克隆项目
git clone https://github.com/your-org/crawler-system.git
cd crawler-system

# 安装uv（如果尚未安装）
pip install uv

# 安装依赖
uv sync

# 安装项目
pip install -e .
```

### 传统方式安装

```bash
# 克隆项目
git clone https://github.com/your-org/crawler-system.git
cd crawler-system

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/macOS
# venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 安装项目
pip install -e .
```

## 快速开始

### 1. 启动基础服务

```bash
# 启动Redis
redis-server

# 启动MongoDB
mongod
```

### 2. 配置项目

```bash
# 设置基础配置
python setup_config.py
```

### 3. 运行爬虫

```bash
# 运行特定网站的爬虫
scrapy crawl adaptive -a site=nhc

# 使用Selenium模式运行（适用于动态页面）
python start_phase2.py
```

### 4. 查看结果

爬取的数据将存储在MongoDB中，默认数据库为`crawler`，集合为`epidemic_data`。

## Docker部署

项目支持完整的Docker容器化部署，包括所有依赖服务和爬虫应用本身。

### 启动所有服务

```bash
# 进入Docker部署目录
cd deployment/docker

# 启动所有基础服务（Redis、MongoDB、PostgreSQL）
docker-compose up -d

# 启动包括Selenium Grid在内的所有服务
docker-compose --profile selenium up -d

# 启动包括管理工具在内的所有服务
docker-compose --profile selenium --profile tools up -d
```

### 启动爬虫应用

```bash
# 启动爬虫调度器
docker-compose up -d crawler

# 或者直接运行爬虫
docker-compose run --rm crawler scrapy crawl adaptive -a site=nhc
```

### 访问管理界面

当使用`--profile tools`启动时，可以通过以下地址访问管理界面：
- Redis Commander: http://localhost:8081
- Mongo Express: http://localhost:8082
- pgAdmin: http://localhost:8083

## 详细文档

- [系统设计文档](系统设计.md) - 系统架构和设计思路
- [实施指南](实施指南.md) - 项目实施详细步骤
- [第一阶段实施指南](第一阶段实施指南.md) - 基础框架搭建指南
- [第二阶段使用指南](第二阶段使用指南.md) - 反爬机制使用说明
- [配置指南](CONFIG_GUIDE.md) - 配置文件详细说明

## 配置说明

系统主要配置文件位于`config/`目录下：

- `config/sites/` - 各网站的采集规则配置
- `config/database.yaml` - 数据库连接配置
- `config/proxy.yaml` - 代理配置
- `config/nhc_firefox_config.yaml` - NHC网站Firefox配置

## 开发与贡献

### 开发环境搭建

```bash
# 安装开发依赖
pip install -e .[dev]

# 运行测试
pytest tests/

# 代码格式化
black .
```

### 贡献流程

1. Fork 本项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 项目结构

```
crawler_system/
├── config/                 # 配置文件
│   ├── sites/              # 各网站配置
│   ├── database.yaml       # 数据库配置
│   └── proxy.yaml          # 代理配置
├── crawler/                # 爬虫核心
│   ├── spiders/            # 爬虫实现
│   ├── middlewares/        # 中间件
│   ├── pipelines/          # 数据管道
│   └── items.py            # 数据模型
├── scheduler/              # 任务调度
├── proxy_pool/             # 代理池
├── anti_crawl/             # 反爬模块
├── data_processing/        # 数据处理
├── monitoring/             # 监控模块
└── deployment/             # 部署配置
```

## 联系方式

项目维护团队 - team@crawler.com

项目链接: [https://github.com/your-org/crawler-system](https://github.com/your-org/crawler-system)