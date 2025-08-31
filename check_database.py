#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库连接状态
"""

import os


def check_psycopg2():
    """检查psycopg2是否安装"""
    try:
        import psycopg2

        print("✅ psycopg2已安装")
        print(f"   版本: {psycopg2.__version__}")
        return True
    except ImportError:
        print("❌ psycopg2未安装")
        print("   请运行: pip install psycopg2-binary")
        return False


def check_mongodb():
    """检查MongoDB连接"""
    try:
        import pymongo

        print("✅ pymongo已安装")

        # 尝试连接MongoDB
        client = pymongo.MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=2000
        )
        client.server_info()  # 触发连接
        print("✅ MongoDB连接成功")

        # 检查数据库
        db = client["crawler_db"]
        collections = db.list_collection_names()
        print("   数据库: crawler_db")
        print(f"   集合数量: {len(collections)}")
        if collections:
            print(f"   集合: {collections}")

        client.close()
        return True

    except ImportError:
        print("❌ pymongo未安装")
        return False
    except Exception as e:
        print(f"❌ MongoDB连接失败: {e}")
        return False


def check_postgresql():
    """检查PostgreSQL连接"""
    if not check_psycopg2():
        return False

    try:
        import psycopg2

        config = {
            "host": "localhost",
            "port": 5432,
            "database": "crawler_db",
            "user": "postgres",
            "password": "password",
            "client_encoding": "utf8",
        }

        print(f"尝试连接PostgreSQL: {config['host']}:{config['port']}/{config['database']}")

        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("✅ PostgreSQL连接成功")
        print(f"   版本: {version[0]}")

        # 检查表
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """
        )
        tables = cursor.fetchall()
        print(f"   表数量: {len(tables)}")
        if tables:
            print(f"   表: {[t[0] for t in tables]}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ PostgreSQL连接失败: {e}")
        return False


def main():
    print("🔍 检查数据库连接状态...\n")

    print("=== MongoDB 检查 ===")
    mongodb_ok = check_mongodb()

    print("\n=== PostgreSQL 检查 ===")
    postgresql_ok = check_postgresql()

    print("\n=== 总结 ===")
    if mongodb_ok:
        print("✅ MongoDB: 可用")
    else:
        print("❌ MongoDB: 不可用")

    if postgresql_ok:
        print("✅ PostgreSQL: 可用")
    else:
        print("❌ PostgreSQL: 不可用")

    if mongodb_ok or postgresql_ok:
        print("\n🎉 至少有一个数据库可用，爬虫可以运行!")
    else:
        print("\n⚠️  没有可用的数据库，请检查数据库服务!")


if __name__ == "__main__":
    main()
