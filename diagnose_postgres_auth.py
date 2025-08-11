#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断PostgreSQL认证问题
"""

import psycopg2
import psycopg2.extensions


def test_connection_with_credentials(host, port, database, user, password):
    """测试特定凭据的连接"""
    try:
        print(f"\n🔍 测试连接: {user}@{host}:{port}/{database}")

        # 设置编码
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

        config = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "client_encoding": "utf8",
            "connect_timeout": 5,
        }

        conn = psycopg2.connect(**config)
        conn.set_client_encoding("UTF8")
        cursor = conn.cursor()

        cursor.execute("SELECT current_user, current_database();")
        result = cursor.fetchone()
        print(f"✅ 连接成功! 用户: {result[0]}, 数据库: {result[1]}")

        cursor.close()
        conn.close()
        return True

    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "authentication failed" in error_msg.lower():
            print(f"❌ 认证失败: 用户名或密码错误")
        elif "does not exist" in error_msg.lower():
            print(f"❌ 用户或数据库不存在")
        elif "connection refused" in error_msg.lower():
            print(f"❌ 连接被拒绝: PostgreSQL服务可能未运行")
        elif "timeout" in error_msg.lower():
            print(f"❌ 连接超时")
        else:
            print(f"❌ 操作错误: {error_msg}")
        return False

    except UnicodeDecodeError as e:
        print(f"❌ UTF-8解码错误: {e}")
        print("这通常表示PostgreSQL返回了包含非UTF-8字符的错误消息")
        print("可能的原因: 用户名/密码错误，或PostgreSQL配置问题")
        return False

    except Exception as e:
        print(f"❌ 其他错误: {e}")
        return False


def main():
    print("🔍 PostgreSQL认证诊断工具\n")

    # 测试配置
    host = "localhost"
    port = 5432
    database = "crawler_db"

    # 常见的用户名/密码组合
    credentials_to_test = [
        # 从.env文件中的配置
        ("crawler_user", "crawler_pass123"),
        # 常见的默认配置
        ("postgres", "password"),
        ("postgres", "postgres"),
        ("postgres", ""),
        # Docker初始化脚本中的配置
        ("crawler_user", "crawler_pass"),
        # 其他可能的配置
        ("admin", "admin"),
        ("root", "root"),
    ]

    print(f"目标服务器: {host}:{port}")
    print(f"目标数据库: {database}")
    print(f"将测试 {len(credentials_to_test)} 种用户名/密码组合...\n")

    successful_credentials = []

    for user, password in credentials_to_test:
        if test_connection_with_credentials(host, port, database, user, password):
            successful_credentials.append((user, password))

    print(f"\n{'='*50}")
    print("📊 测试结果:")

    if successful_credentials:
        print(f"✅ 找到 {len(successful_credentials)} 个有效的凭据:")
        for user, password in successful_credentials:
            masked_password = "*" * len(password) if password else "(空密码)"
            print(f"  - 用户: {user}, 密码: {masked_password}")

        print(f"\n💡 建议:")
        user, password = successful_credentials[0]
        print(f"更新.env文件中的PostgreSQL配置:")
        print(f"POSTGRES_USER={user}")
        print(f"POSTGRES_PASSWORD={password}")

    else:
        print("❌ 没有找到有效的凭据")
        print("\n🔧 可能的解决方案:")
        print("1. 检查PostgreSQL服务是否运行:")
        print("   docker-compose ps")
        print("2. 检查Docker容器日志:")
        print("   docker-compose logs postgresql")
        print("3. 重新创建PostgreSQL容器:")
        print("   docker-compose down postgresql")
        print("   docker-compose up -d postgresql")
        print("4. 手动连接到PostgreSQL容器:")
        print("   docker exec -it crawler_postgresql psql -U postgres -d crawler_db")


if __name__ == "__main__":
    main()
