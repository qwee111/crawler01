#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复PostgreSQL权限问题
"""

import psycopg2
import psycopg2.extensions


def fix_permissions():
    """修复PostgreSQL权限"""
    try:
        # 使用postgres超级用户连接
        admin_config = {
            "host": "localhost",
            "port": 5432,
            "database": "crawler_db",
            "user": "postgres",
            "password": "123456",  # 使用postgres默认密码
            "client_encoding": "utf8",
        }

        print("🔧 使用postgres超级用户连接...")
        conn = psycopg2.connect(**admin_config)
        conn.autocommit = True  # 自动提交
        cursor = conn.cursor()

        # 检查当前用户
        cursor.execute("SELECT current_user;")
        current_user = cursor.fetchone()[0]
        print(f"✅ 当前用户: {current_user}")

        # 创建crawler_user用户（如果不存在）
        print("👤 创建/更新crawler_user用户...")
        cursor.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'crawler_user') THEN
                    CREATE ROLE crawler_user WITH LOGIN PASSWORD 'crawler_pass123';
                    RAISE NOTICE 'User crawler_user created';
                ELSE
                    ALTER ROLE crawler_user WITH PASSWORD 'crawler_pass123';
                    RAISE NOTICE 'User crawler_user password updated';
                END IF;
            END
            $$;
        """
        )

        # 授予数据库权限
        print("🔑 授予数据库权限...")
        cursor.execute("GRANT ALL PRIVILEGES ON DATABASE crawler_db TO crawler_user;")
        cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA public TO crawler_user;")

        # 授予表权限
        print("📊 授予表权限...")
        tables = [
            "epidemic_data",
            "news_data",
            "policy_data",
            "statistics_data",
            "data_quality_reports",
            "crawler_statistics",
            "proxy_statistics",
            "crawler_tasks",
        ]

        for table in tables:
            try:
                cursor.execute(
                    f"GRANT ALL PRIVILEGES ON TABLE {table} TO crawler_user;"
                )
                print(f"  ✅ {table}")
            except psycopg2.Error as e:
                print(f"  ⚠️  {table}: {e}")

        # 授予序列权限
        print("🔢 授予序列权限...")
        cursor.execute(
            """
            DO $$
            DECLARE
                seq_name TEXT;
            BEGIN
                FOR seq_name IN
                    SELECT sequence_name
                    FROM information_schema.sequences
                    WHERE sequence_schema = 'public'
                LOOP
                    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || seq_name || ' TO crawler_user';
                END LOOP;
            END
            $$;
        """
        )

        # 设置默认权限
        print("🛡️  设置默认权限...")
        cursor.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO crawler_user;"
        )
        cursor.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO crawler_user;"
        )
        cursor.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO crawler_user;"
        )

        # 授予视图权限
        print("👁️  授予视图权限...")
        cursor.execute("GRANT SELECT ON epidemic_summary TO crawler_user;")

        # 验证权限
        print("\n🔍 验证权限...")
        cursor.execute(
            """
            SELECT table_name, privilege_type
            FROM information_schema.table_privileges
            WHERE grantee = 'crawler_user'
            AND table_schema = 'public'
            ORDER BY table_name, privilege_type;
        """
        )

        privileges = cursor.fetchall()
        if privileges:
            print("✅ crawler_user的表权限:")
            current_table = None
            for table_name, privilege_type in privileges:
                if table_name != current_table:
                    print(f"  📊 {table_name}:")
                    current_table = table_name
                print(f"    - {privilege_type}")
        else:
            print("⚠️  未找到crawler_user的权限")

        cursor.close()
        conn.close()

        print("\n🎉 权限修复完成!")
        return True

    except Exception as e:
        print(f"❌ 权限修复失败: {e}")
        import traceback

        print(f"详细错误: {traceback.format_exc()}")
        return False


def test_crawler_user_connection():
    """测试crawler_user连接"""
    try:
        print("\n🧪 测试crawler_user连接...")

        user_config = {
            "host": "localhost",
            "port": 5432,
            "database": "crawler_db",
            "user": "crawler_user",
            "password": "crawler_pass123",
            "client_encoding": "utf8",
        }

        conn = psycopg2.connect(**user_config)
        cursor = conn.cursor()

        # 测试查询权限
        cursor.execute("SELECT current_user, current_database();")
        result = cursor.fetchone()
        print(f"✅ 连接成功: {result[0]}@{result[1]}")

        # 测试插入权限
        cursor.execute(
            """
            INSERT INTO news_data (url, title, content, source, spider_name)
            VALUES ('http://test-permissions.com', '权限测试', '测试内容', '测试来源', 'permission_test')
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """
        )

        result = cursor.fetchone()
        conn.commit()
        print(f"✅ 插入测试成功，ID: {result[0]}")

        cursor.close()
        conn.close()

        print("🎉 crawler_user权限测试通过!")
        return True

    except Exception as e:
        print(f"❌ crawler_user连接测试失败: {e}")
        return False


def main():
    print("🔧 PostgreSQL权限修复工具\n")

    # 1. 修复权限
    if fix_permissions():
        # 2. 测试权限
        test_crawler_user_connection()
    else:
        print("\n💡 如果postgres用户密码不正确，请尝试:")
        print("1. 检查Docker容器日志: docker-compose logs postgresql")
        print(
            "2. 进入容器手动修复: docker exec -it crawler_postgresql psql -U postgres -d crawler_db"
        )
        print(
            "3. 重置PostgreSQL容器: docker-compose down postgresql && docker-compose up -d postgresql"
        )


if __name__ == "__main__":
    main()
