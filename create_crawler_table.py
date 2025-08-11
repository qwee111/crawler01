#!/usr/bin/env python3
"""
创建crawler_data表的脚本
"""

import os

import psycopg2
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# PostgreSQL连接配置
postgres_host = os.getenv("POSTGRES_HOST", "localhost")
# 如果是Docker容器名，转换为localhost
if postgres_host in ["postgresql", "postgres"]:
    postgres_host = "localhost"

POSTGRES_CONFIG = {
    "host": postgres_host,
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "crawler_db"),
    "user": os.getenv("POSTGRES_USER", "crawler_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "crawler_pass"),
    "connect_timeout": 10,
}

# 创建表的SQL
CREATE_TABLE_SQL = """
-- 删除现有表（如果存在）
DROP TABLE IF EXISTS crawler_data;

-- 创建新表
CREATE TABLE crawler_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url VARCHAR(1000) NOT NULL,
    title TEXT,
    content TEXT,

    -- 基本信息
    spider_name VARCHAR(100),
    spider_version VARCHAR(20),
    site_name VARCHAR(200),
    page_type VARCHAR(100),
    content_type VARCHAR(200),
    status_code INTEGER,

    -- 列表页特有字段
    items TEXT,  -- JSON格式存储列表项

    -- 时间信息
    extraction_time TIMESTAMP WITH TIME ZONE,
    extraction_timestamp TIMESTAMP WITH TIME ZONE,
    crawl_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- 元数据（使用TEXT存储JSON字符串）
    page_analysis TEXT,
    response_meta TEXT,
    extraction_metadata TEXT,
    processing_metadata TEXT,
    validation_metadata TEXT,
    quality_metadata TEXT,

    -- 其他字段
    content_length INTEGER,
    content_fingerprint VARCHAR(64),
    chinese_char_count INTEGER,
    extraction_config VARCHAR(200),

    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_crawler_data_url ON crawler_data(url);
CREATE INDEX IF NOT EXISTS idx_crawler_data_spider ON crawler_data(spider_name);
CREATE INDEX IF NOT EXISTS idx_crawler_data_site ON crawler_data(site);
CREATE INDEX IF NOT EXISTS idx_crawler_data_crawl_time ON crawler_data(crawl_timestamp);
"""


def create_table():
    """创建crawler_data表"""
    try:
        print(
            f"连接PostgreSQL数据库: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        )
        print(f"用户: {POSTGRES_CONFIG['user']}")

        # 连接数据库
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        print("✅ 数据库连接成功")

        # 执行创建表的SQL
        print("创建crawler_data表...")
        cursor.execute(CREATE_TABLE_SQL)

        # 提交更改
        conn.commit()
        print("✅ crawler_data表创建成功")

        # 检查表是否存在
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'crawler_data'
            ORDER BY ordinal_position;
        """
        )

        columns = cursor.fetchall()
        print(f"✅ 表结构验证成功，共有 {len(columns)} 个字段:")
        for col_name, col_type in columns:
            print(f"  - {col_name}: {col_type}")

        # 关闭连接
        cursor.close()
        conn.close()
        print("✅ 数据库连接已关闭")

        return True

    except Exception as e:
        print(f"❌ 创建表失败: {e}")
        return False


if __name__ == "__main__":
    print("🚀 开始创建crawler_data表...")
    success = create_table()

    if success:
        print("🎉 表创建完成！")
    else:
        print("💥 表创建失败！")
