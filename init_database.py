#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化PostgreSQL数据库表
"""

import os

import psycopg2
from psycopg2 import sql

# 数据库连接配置
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "172.20.0.5"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "crawler_db"),
    "user": os.getenv("POSTGRES_USER", "crawler_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "crawler_pass123"),
    "client_encoding": "utf8",
}

# 创建表的SQL语句
CREATE_TABLES_SQL = {
    "news_data": """
        CREATE TABLE IF NOT EXISTS news_data (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            publish_date DATE,
            author TEXT,
            source TEXT,
            category TEXT,
            tags TEXT[],
            view_count INTEGER DEFAULT 0,
            comment_count INTEGER DEFAULT 0,
            crawl_time TIMESTAMP,
            spider_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    "epidemic_data": """
        CREATE TABLE IF NOT EXISTS epidemic_data (
            id SERIAL PRIMARY KEY,
            source_url TEXT UNIQUE NOT NULL,
            source_name TEXT,
            title TEXT NOT NULL,
            content TEXT,
            region TEXT,
            confirmed_cases INTEGER DEFAULT 0,
            death_cases INTEGER DEFAULT 0,
            recovered_cases INTEGER DEFAULT 0,
            active_cases INTEGER DEFAULT 0,
            report_date DATE,
            update_time TEXT,
            data_quality_score FLOAT,
            validation_status TEXT,
            crawl_time TIMESTAMP,
            spider_name TEXT,
            crawl_timestamp FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    "policy_data": """
        CREATE TABLE IF NOT EXISTS policy_data (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            policy_number TEXT,
            issue_date DATE,
            effective_date DATE,
            issuing_authority TEXT,
            policy_type TEXT,
            policy_level TEXT,
            keywords TEXT[],
            crawl_time TIMESTAMP,
            spider_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    "statistics_data": """
        CREATE TABLE IF NOT EXISTS statistics_data (
            id SERIAL PRIMARY KEY,
            source_url TEXT UNIQUE NOT NULL,
            region TEXT,
            total_cases INTEGER DEFAULT 0,
            new_cases INTEGER DEFAULT 0,
            total_deaths INTEGER DEFAULT 0,
            new_deaths INTEGER DEFAULT 0,
            total_recovered INTEGER DEFAULT 0,
            new_recovered INTEGER DEFAULT 0,
            statistics_date DATE,
            crawl_time TIMESTAMP,
            spider_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
}


def test_connection():
    """测试数据库连接"""
    try:
        print(f"测试连接: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ PostgreSQL版本: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except psycopg2.OperationalError as e:
        print(f"❌ 连接失败: {e}")
        print("请检查:")
        print("1. PostgreSQL服务是否运行")
        print("2. 连接参数是否正确")
        print("3. 用户权限是否足够")
        return False
    except Exception as e:
        print(f"❌ 其他错误: {e}")
        return False


def init_database():
    """初始化数据库表"""
    try:
        # 先测试连接
        if not test_connection():
            return False

        # 连接数据库
        print(
            f"\n连接PostgreSQL数据库: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        )
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        print("✅ 数据库连接成功")

        # 创建表
        for table_name, create_sql in CREATE_TABLES_SQL.items():
            print(f"创建表: {table_name}")
            cursor.execute(create_sql)
            print(f"✅ 表 {table_name} 创建成功")

        # 提交更改
        conn.commit()
        print("✅ 所有表创建完成")

        # 检查表是否存在
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('news_data', 'epidemic_data', 'policy_data', 'statistics_data')
            ORDER BY table_name;
        """
        )

        tables = cursor.fetchall()
        print(f"\n📊 数据库中的表:")
        for table in tables:
            print(f"  - {table[0]}")

        # 关闭连接
        cursor.close()
        conn.close()
        print("\n🎉 数据库初始化完成!")

    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        return False

    return True


if __name__ == "__main__":
    print("🚀 开始初始化PostgreSQL数据库...")
    success = init_database()

    if success:
        print("\n✅ 数据库已准备就绪，可以开始爬取数据!")
    else:
        print("\n❌ 数据库初始化失败，请检查配置和连接!")
