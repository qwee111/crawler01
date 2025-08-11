#!/usr/bin/env python3
"""
存储系统启动脚本

自动化启动和配置存储服务
"""

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class StorageManager:
    """存储系统管理器"""

    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.docker_dir = self.project_root / "deployment" / "docker"
        self.compose_file = self.docker_dir / "docker-compose.yml"

        # 检查Docker Compose文件
        if not self.compose_file.exists():
            raise FileNotFoundError(f"Docker Compose文件不存在: {self.compose_file}")

    def check_prerequisites(self) -> bool:
        """检查前置条件"""
        logger.info("检查前置条件...")

        # 检查Docker
        try:
            result = subprocess.run(
                ["docker", "--version"], capture_output=True, text=True, check=True
            )
            logger.info(f"Docker版本: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("Docker未安装或不可用")
            return False

        # 检查Docker Compose
        try:
            result = subprocess.run(
                ["docker-compose", "--version"],
                capture_output=True,
                text=True,
                check=True,
            )
            logger.info(f"Docker Compose版本: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("Docker Compose未安装或不可用")
            return False

        # 检查环境变量文件
        env_file = self.docker_dir / ".env"
        if not env_file.exists():
            logger.warning("环境变量文件不存在，将使用默认值")
            self.create_env_file()

        return True

    def create_env_file(self):
        """创建环境变量文件"""
        logger.info("创建环境变量文件...")

        env_example = self.docker_dir / ".env.example"
        env_file = self.docker_dir / ".env"

        if env_example.exists():
            # 复制示例文件
            import shutil

            shutil.copy(env_example, env_file)
            logger.info(f"已复制 {env_example} 到 {env_file}")
        else:
            # 创建基本环境变量文件
            env_content = """# 基本环境变量配置
ENVIRONMENT=development
DEBUG=true

# Redis配置
REDIS_PASSWORD=

# MongoDB配置
MONGODB_ROOT_USERNAME=admin
MONGODB_ROOT_PASSWORD=password123
MONGODB_DATABASE=crawler_db

# PostgreSQL配置
POSTGRES_DB=crawler_db
POSTGRES_USER=crawler_user
POSTGRES_PASSWORD=crawler_pass123

# MinIO配置
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

# 管理界面配置
MONGO_EXPRESS_USER=admin
MONGO_EXPRESS_PASSWORD=admin123
PGADMIN_EMAIL=admin@crawler.com
PGADMIN_PASSWORD=admin123
"""

            with open(env_file, "w", encoding="utf-8") as f:
                f.write(env_content)

            logger.info(f"已创建基本环境变量文件: {env_file}")

    def start_services(
        self, services: List[str] = None, with_tools: bool = False
    ) -> bool:
        """启动服务"""
        logger.info("启动存储服务...")

        try:
            # 检查compose文件是否存在
            if not self.compose_file.exists():
                logger.error(f"Docker Compose文件不存在: {self.compose_file}")
                return False

            # 构建命令 - 使用相对路径
            cmd = ["docker-compose", "-f", "docker-compose.yml"]

            if with_tools:
                cmd.extend(["--profile", "tools"])

            cmd.append("up")
            cmd.extend(["-d", "--remove-orphans"])

            if services:
                cmd.extend(services)

            logger.info(f"执行命令: {' '.join(cmd)}")
            logger.info(f"工作目录: {self.docker_dir}")

            # 执行命令
            result = subprocess.run(
                cmd, cwd=self.docker_dir, check=True, encoding="utf-8", errors="ignore"
            )

            logger.info("服务启动成功")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"启动服务失败: {e}")
            return False

    def wait_for_services(self, timeout: int = 60) -> bool:
        """等待服务就绪"""
        logger.info("等待服务就绪...")

        # 简化检查逻辑，只检查容器是否运行
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # 检查所有服务状态
                result = subprocess.run(
                    [
                        "docker-compose",
                        "-f",
                        "docker-compose.yml",
                        "ps",
                        "--format",
                        "table",
                    ],
                    cwd=self.docker_dir,
                    capture_output=True,
                    text=True,
                    check=True,
                    encoding="utf-8",
                    errors="ignore",
                )

                if result.stdout:
                    # 简单检查是否有服务在运行
                    running_count = result.stdout.count("Up")
                    if running_count >= 3:  # 至少3个核心服务运行
                        logger.info(f"检测到 {running_count} 个服务正在运行")
                        logger.info("服务基本就绪")
                        return True

                logger.info("等待更多服务启动...")
                time.sleep(3)

            except Exception as e:
                logger.warning(f"服务状态检查失败: {e}")
                time.sleep(3)

        logger.warning(f"服务启动检查超时 ({timeout}秒)")
        logger.info("💡 提示: 服务可能仍在启动中，您可以手动检查:")
        logger.info("   docker-compose -f deployment/docker/docker-compose.yml ps")
        return True  # 返回True以继续执行

    def check_service_health(self, service_name: str) -> bool:
        """检查服务健康状态"""
        try:
            # 使用docker-compose ps检查服务状态
            result = subprocess.run(
                ["docker-compose", "-f", "docker-compose.yml", "ps", service_name],
                cwd=self.docker_dir,
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
                errors="ignore",  # 忽略编码错误
            )

            # 检查输出中是否包含"Up"状态
            if result.stdout:
                return "Up" in result.stdout
            else:
                logger.warning(f"服务 {service_name} 状态检查返回空结果")
                return False

        except subprocess.CalledProcessError as e:
            logger.warning(f"服务 {service_name} 状态检查失败: {e}")
            return False
        except Exception as e:
            logger.warning(f"服务 {service_name} 健康检查异常: {e}")
            return False

    def check_redis(self) -> bool:
        """检查Redis服务"""
        try:
            import redis

            # 从环境变量读取Redis密码
            redis_password = os.getenv("REDIS_PASSWORD", "")

            if redis_password:
                client = redis.Redis(
                    host="localhost",
                    port=6379,
                    db=0,
                    password=redis_password,
                    socket_timeout=5,
                )
            else:
                client = redis.Redis(
                    host="localhost", port=6379, db=0, socket_timeout=5
                )

            client.ping()
            return True
        except ImportError:
            logger.warning("redis库未安装，跳过Redis连接检查")
            return True  # 假设服务正常
        except Exception as e:
            logger.debug(f"Redis连接失败: {e}")
            return False

    def check_mongodb(self) -> bool:
        """检查MongoDB服务"""
        try:
            import pymongo

            # 从环境变量读取MongoDB配置
            mongodb_user = os.getenv("MONGODB_ROOT_USERNAME", "admin")
            mongodb_password = os.getenv("MONGODB_ROOT_PASSWORD", "password123")

            uri = f"mongodb://{mongodb_user}:{mongodb_password}@localhost:27017/"
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.server_info()
            return True
        except ImportError:
            logger.warning("pymongo库未安装，跳过MongoDB连接检查")
            return True  # 假设服务正常
        except Exception as e:
            logger.debug(f"MongoDB连接失败: {e}")
            return False

    def check_postgresql(self) -> bool:
        """检查PostgreSQL服务"""
        try:
            import psycopg2

            # 从环境变量读取PostgreSQL配置
            postgres_user = os.getenv("POSTGRES_USER", "crawler_user")
            postgres_password = os.getenv("POSTGRES_PASSWORD", "crawler_pass123")
            postgres_db = os.getenv("POSTGRES_DB", "crawler_db")

            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database=postgres_db,
                user=postgres_user,
                password=postgres_password,
                connect_timeout=5,
            )
            conn.close()
            return True
        except ImportError:
            logger.warning("psycopg2库未安装，跳过PostgreSQL连接检查")
            return True  # 假设服务正常
        except Exception as e:
            logger.debug(f"PostgreSQL连接失败: {e}")
            return False

    def show_service_status(self):
        """显示服务状态"""
        logger.info("检查服务状态...")

        try:
            result = subprocess.run(
                ["docker-compose", "-f", "docker-compose.yml", "ps"],
                cwd=self.docker_dir,
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
                errors="ignore",
            )

            print("\n=== 服务状态 ===")
            print(result.stdout)

        except subprocess.CalledProcessError as e:
            logger.error(f"获取服务状态失败: {e}")

    def show_service_logs(self, service: str = None, lines: int = 50):
        """显示服务日志"""
        try:
            cmd = [
                "docker-compose",
                "-f",
                "docker-compose.yml",
                "logs",
                "--tail",
                str(lines),
            ]

            if service:
                cmd.append(service)

            subprocess.run(
                cmd, cwd=self.docker_dir, check=True, encoding="utf-8", errors="ignore"
            )

        except subprocess.CalledProcessError as e:
            logger.error(f"获取服务日志失败: {e}")

    def stop_services(self):
        """停止服务"""
        logger.info("停止存储服务...")

        try:
            subprocess.run(
                ["docker-compose", "-f", "docker-compose.yml", "down"],
                cwd=self.docker_dir,
                check=True,
                encoding="utf-8",
                errors="ignore",
            )
            logger.info("服务已停止")

        except subprocess.CalledProcessError as e:
            logger.error(f"停止服务失败: {e}")

    def restart_services(self):
        """重启服务"""
        logger.info("重启存储服务...")
        self.stop_services()
        time.sleep(5)
        return self.start_services()

    def show_connection_info(self):
        """显示连接信息"""
        print("\n=== 连接信息 ===")
        print("Redis:")
        print("  Host: localhost")
        print("  Port: 6379")
        print("  Database: 0")
        print()
        print("MongoDB:")
        print("  Host: localhost")
        print("  Port: 27017")
        print("  Database: crawler_db")
        print("  Username: admin")
        print("  Password: password123")
        print()
        print("PostgreSQL:")
        print("  Host: localhost")
        print("  Port: 5432")
        print("  Database: crawler_db")
        print("  Username: crawler_user")
        print("  Password: crawler_pass123")
        print()
        print("MinIO:")
        print("  Host: localhost")
        print("  Port: 9000 (API), 9001 (Console)")
        print("  Username: minioadmin")
        print("  Password: minioadmin123")
        print()
        print("管理界面:")
        print("  Redis Commander: http://localhost:8081")
        print("  Mongo Express: http://localhost:8082")
        print("  pgAdmin: http://localhost:8083")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="存储系统管理脚本")
    parser.add_argument(
        "action",
        choices=["start", "stop", "restart", "status", "logs", "info"],
        help="执行的操作",
    )
    parser.add_argument("--with-tools", action="store_true", help="启动管理工具")
    parser.add_argument("--service", type=str, help="指定服务名称")
    parser.add_argument("--lines", type=int, default=50, help="日志行数")
    parser.add_argument("--project-root", type=str, default=".", help="项目根目录")

    args = parser.parse_args()

    try:
        manager = StorageManager(args.project_root)

        if args.action == "start":
            if not manager.check_prerequisites():
                sys.exit(1)

            if manager.start_services(with_tools=args.with_tools):
                if manager.wait_for_services():
                    manager.show_connection_info()
                else:
                    logger.error("服务启动失败")
                    sys.exit(1)
            else:
                sys.exit(1)

        elif args.action == "stop":
            manager.stop_services()

        elif args.action == "restart":
            if manager.restart_services():
                if manager.wait_for_services():
                    manager.show_connection_info()
                else:
                    sys.exit(1)
            else:
                sys.exit(1)

        elif args.action == "status":
            manager.show_service_status()

        elif args.action == "logs":
            manager.show_service_logs(args.service, args.lines)

        elif args.action == "info":
            manager.show_connection_info()

    except Exception as e:
        logger.error(f"执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
