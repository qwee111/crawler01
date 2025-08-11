#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置热更新管理器

实现配置文件的热更新、版本管理和分发
"""

import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event, Thread
from typing import Callable, Dict, List, Optional

import yaml

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ConfigVersion:
    """配置版本信息"""

    config_name: str
    version: str
    checksum: str
    updated_at: float
    content: Dict

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "ConfigVersion":
        return cls(**data)


class ConfigFileHandler(FileSystemEventHandler):
    """配置文件变更处理器"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.debounce_time = 1.0  # 防抖时间
        self.last_modified = {}

    def on_modified(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        current_time = time.time()

        # 防抖处理
        if file_path in self.last_modified:
            if current_time - self.last_modified[file_path] < self.debounce_time:
                return

        self.last_modified[file_path] = current_time

        # 检查是否是配置文件
        if self.is_config_file(file_path):
            logger.info(f"检测到配置文件变更: {file_path}")
            self.config_manager.reload_config_file(Path(file_path))

    def is_config_file(self, file_path: str) -> bool:
        """检查是否是配置文件"""
        config_extensions = [".yaml", ".yml", ".json"]
        return any(file_path.endswith(ext) for ext in config_extensions)


class ConfigManager:
    """配置热更新管理器"""

    def __init__(self, config_dirs: List[str], redis_url="redis://localhost:6379/0"):
        self.config_dirs = [Path(d) for d in config_dirs]
        self.redis_url = redis_url
        self.redis = None

        # Redis键名
        self.config_versions_key = "crawler:config_versions"
        self.config_content_key = "crawler:config_content"
        self.config_subscribers_key = "crawler:config_subscribers"

        # 配置缓存
        self.config_cache = {}
        self.config_callbacks = {}

        # 文件监控
        self.observer = None
        self.monitoring = False

        # 线程控制
        self.stop_event = Event()

        # 初始化
        self.connect_redis()
        self.load_all_configs()

        logger.info("配置管理器初始化完成")

    def connect_redis(self) -> bool:
        """连接Redis"""
        if not REDIS_AVAILABLE:
            logger.warning("Redis不可用，配置热更新功能受限")
            return False

        try:
            self.redis = redis.from_url(self.redis_url)
            self.redis.ping()
            logger.info("Redis连接成功")
            return True
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            return False

    def load_all_configs(self):
        """加载所有配置文件"""
        logger.info("加载所有配置文件...")

        for config_dir in self.config_dirs:
            if not config_dir.exists():
                logger.warning(f"配置目录不存在: {config_dir}")
                continue

            # 递归查找配置文件
            for config_file in config_dir.rglob("*.yaml"):
                self.load_config_file(config_file)

            for config_file in config_dir.rglob("*.yml"):
                self.load_config_file(config_file)

            for config_file in config_dir.rglob("*.json"):
                self.load_config_file(config_file)

    def load_config_file(self, config_path: Path) -> bool:
        """加载单个配置文件"""
        try:
            if not config_path.exists():
                logger.error(f"配置文件不存在: {config_path}")
                return False

            # 读取文件内容
            with open(config_path, "r", encoding="utf-8") as f:
                content = f.read()

            # 解析配置
            if config_path.suffix.lower() in [".yaml", ".yml"]:
                config_data = yaml.safe_load(content)
            elif config_path.suffix.lower() == ".json":
                config_data = json.loads(content)
            else:
                logger.warning(f"不支持的配置文件格式: {config_path}")
                return False

            # 生成配置名称和版本信息
            config_name = self.get_config_name(config_path)
            checksum = self.calculate_checksum(content)

            # 检查是否有变更
            if config_name in self.config_cache:
                if self.config_cache[config_name]["checksum"] == checksum:
                    logger.debug(f"配置文件无变更: {config_name}")
                    return True

            # 创建配置版本
            version = str(int(time.time()))
            config_version = ConfigVersion(
                config_name=config_name,
                version=version,
                checksum=checksum,
                updated_at=time.time(),
                content=config_data,
            )

            # 更新缓存
            self.config_cache[config_name] = config_version.to_dict()

            # 保存到Redis
            if self.redis:
                self.save_config_to_redis(config_version)

            # 触发回调
            self.trigger_config_callbacks(config_name, config_data)

            logger.info(f"配置文件加载成功: {config_name} (版本: {version})")
            return True

        except Exception as e:
            logger.error(f"加载配置文件失败 {config_path}: {e}")
            return False

    def reload_config_file(self, config_path: Path):
        """重新加载配置文件"""
        logger.info(f"重新加载配置文件: {config_path}")
        self.load_config_file(config_path)

    def get_config_name(self, config_path: Path) -> str:
        """获取配置名称"""
        # 使用相对路径作为配置名称
        for config_dir in self.config_dirs:
            try:
                relative_path = config_path.relative_to(config_dir)
                return (
                    str(relative_path)
                    .replace("\\", "/")
                    .replace(".yaml", "")
                    .replace(".yml", "")
                    .replace(".json", "")
                )
            except ValueError:
                continue

        # 如果不在配置目录中，使用文件名
        return config_path.stem

    def calculate_checksum(self, content: str) -> str:
        """计算内容校验和"""
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def save_config_to_redis(self, config_version: ConfigVersion):
        """保存配置到Redis"""
        if not self.redis:
            return

        try:
            # 保存版本信息
            self.redis.hset(
                self.config_versions_key,
                config_version.config_name,
                json.dumps(config_version.to_dict()),
            )

            # 保存配置内容
            content_key = f"{self.config_content_key}:{config_version.config_name}:{config_version.version}"
            self.redis.set(content_key, json.dumps(config_version.content))
            self.redis.expire(content_key, 30 * 24 * 3600)  # 保留30天

            # 发布配置更新通知
            self.publish_config_update(
                config_version.config_name, config_version.version
            )

        except Exception as e:
            logger.error(f"保存配置到Redis失败: {e}")

    def publish_config_update(self, config_name: str, version: str):
        """发布配置更新通知"""
        if not self.redis:
            return

        try:
            update_message = {
                "config_name": config_name,
                "version": version,
                "timestamp": time.time(),
            }

            # 发布到Redis频道
            channel = f"config_update:{config_name}"
            self.redis.publish(channel, json.dumps(update_message))

            logger.info(f"发布配置更新通知: {config_name} v{version}")

        except Exception as e:
            logger.error(f"发布配置更新失败: {e}")

    def get_config(self, config_name: str, version: str = None) -> Optional[Dict]:
        """获取配置"""
        try:
            # 从缓存获取
            if config_name in self.config_cache:
                cached_config = self.config_cache[config_name]
                if version is None or cached_config["version"] == version:
                    return cached_config["content"]

            # 从Redis获取
            if self.redis:
                if version:
                    content_key = f"{self.config_content_key}:{config_name}:{version}"
                    content_data = self.redis.get(content_key)
                    if content_data:
                        return json.loads(content_data)
                else:
                    # 获取最新版本
                    version_data = self.redis.hget(
                        self.config_versions_key, config_name
                    )
                    if version_data:
                        version_info = json.loads(version_data)
                        return version_info["content"]

            logger.warning(f"配置不存在: {config_name} (版本: {version})")
            return None

        except Exception as e:
            logger.error(f"获取配置失败: {e}")
            return None

    def register_config_callback(
        self, config_name: str, callback: Callable[[Dict], None]
    ):
        """注册配置变更回调"""
        if config_name not in self.config_callbacks:
            self.config_callbacks[config_name] = []

        self.config_callbacks[config_name].append(callback)
        logger.info(f"注册配置回调: {config_name}")

    def trigger_config_callbacks(self, config_name: str, config_data: Dict):
        """触发配置变更回调"""
        if config_name in self.config_callbacks:
            for callback in self.config_callbacks[config_name]:
                try:
                    callback(config_data)
                except Exception as e:
                    logger.error(f"配置回调执行失败: {e}")

    def start_file_monitoring(self) -> bool:
        """开始文件监控"""
        if not WATCHDOG_AVAILABLE:
            logger.warning("watchdog不可用，无法启动文件监控")
            return False

        if self.monitoring:
            logger.warning("文件监控已在运行")
            return True

        try:
            self.observer = Observer()
            handler = ConfigFileHandler(self)

            # 监控所有配置目录
            for config_dir in self.config_dirs:
                if config_dir.exists():
                    self.observer.schedule(handler, str(config_dir), recursive=True)
                    logger.info(f"开始监控配置目录: {config_dir}")

            self.observer.start()
            self.monitoring = True
            logger.info("文件监控启动成功")
            return True

        except Exception as e:
            logger.error(f"启动文件监控失败: {e}")
            return False

    def stop_file_monitoring(self):
        """停止文件监控"""
        if self.observer and self.monitoring:
            self.observer.stop()
            self.observer.join()
            self.monitoring = False
            logger.info("文件监控已停止")

    def subscribe_config_updates(self, config_names: List[str] = None):
        """订阅配置更新"""
        if not self.redis:
            logger.warning("Redis不可用，无法订阅配置更新")
            return

        def subscription_worker():
            try:
                pubsub = self.redis.pubsub()

                # 订阅指定配置或所有配置
                if config_names:
                    for config_name in config_names:
                        channel = f"config_update:{config_name}"
                        pubsub.subscribe(channel)
                else:
                    pubsub.psubscribe("config_update:*")

                logger.info("开始订阅配置更新")

                for message in pubsub.listen():
                    if self.stop_event.is_set():
                        break

                    if message["type"] in ["message", "pmessage"]:
                        try:
                            update_data = json.loads(message["data"])
                            config_name = update_data["config_name"]
                            version = update_data["version"]

                            logger.info(f"收到配置更新通知: {config_name} v{version}")

                            # 从Redis获取最新配置
                            latest_config = self.get_config(config_name, version)
                            if latest_config:
                                # 更新本地缓存
                                if config_name in self.config_cache:
                                    self.config_cache[config_name][
                                        "content"
                                    ] = latest_config
                                    self.config_cache[config_name]["version"] = version

                                # 触发回调
                                self.trigger_config_callbacks(
                                    config_name, latest_config
                                )

                        except Exception as e:
                            logger.error(f"处理配置更新消息失败: {e}")

            except Exception as e:
                logger.error(f"配置订阅失败: {e}")

        # 启动订阅线程
        subscription_thread = Thread(target=subscription_worker, daemon=True)
        subscription_thread.start()

    def get_config_versions(self) -> Dict[str, Dict]:
        """获取所有配置版本信息"""
        versions = {}

        # 从缓存获取
        for config_name, config_data in self.config_cache.items():
            versions[config_name] = {
                "version": config_data["version"],
                "checksum": config_data["checksum"],
                "updated_at": config_data["updated_at"],
            }

        # 从Redis获取（如果有的话）
        if self.redis:
            try:
                redis_versions = self.redis.hgetall(self.config_versions_key)
                for config_name, version_data in redis_versions.items():
                    config_name_str = (
                        config_name.decode()
                        if isinstance(config_name, bytes)
                        else config_name
                    )
                    version_info = json.loads(version_data)

                    if config_name_str not in versions:
                        versions[config_name_str] = {
                            "version": version_info["version"],
                            "checksum": version_info["checksum"],
                            "updated_at": version_info["updated_at"],
                        }

            except Exception as e:
                logger.error(f"从Redis获取配置版本失败: {e}")

        return versions

    def cleanup_old_versions(self, keep_versions: int = 5):
        """清理旧版本配置"""
        if not self.redis:
            return

        try:
            # 获取所有配置的版本信息
            all_versions = self.redis.hgetall(self.config_versions_key)

            for config_name, version_data in all_versions.items():
                config_name_str = (
                    config_name.decode()
                    if isinstance(config_name, bytes)
                    else config_name
                )

                # 获取该配置的所有版本
                pattern = f"{self.config_content_key}:{config_name_str}:*"
                version_keys = self.redis.keys(pattern)

                if len(version_keys) > keep_versions:
                    # 按版本号排序，删除旧版本
                    version_keys.sort()
                    old_keys = version_keys[:-keep_versions]

                    for old_key in old_keys:
                        self.redis.delete(old_key)

                    logger.info(f"清理配置 {config_name_str} 的 {len(old_keys)} 个旧版本")

        except Exception as e:
            logger.error(f"清理旧版本配置失败: {e}")

    def stop(self):
        """停止配置管理器"""
        self.stop_event.set()
        self.stop_file_monitoring()
        logger.info("配置管理器已停止")


def main():
    """主函数 - 测试配置管理器"""
    print("⚙️ 配置热更新管理器测试")
    print("=" * 60)

    # 创建测试配置目录
    test_config_dir = Path("test_configs")
    test_config_dir.mkdir(exist_ok=True)

    # 创建测试配置文件
    test_config = {
        "spider_settings": {
            "download_delay": 3,
            "concurrent_requests": 16,
            "retry_times": 3,
        },
        "site_config": {
            "bjcdc": {"base_url": "https://www.bjcdc.org", "enabled": True}
        },
    }

    config_file = test_config_dir / "test_config.yaml"
    with open(config_file, "w", encoding="utf-8") as f:
        yaml.dump(test_config, f, default_flow_style=False, allow_unicode=True)

    # 创建配置管理器
    config_manager = ConfigManager([str(test_config_dir)])

    # 注册配置变更回调
    def config_callback(config_data):
        print(f"📢 配置已更新: {config_data}")

    config_manager.register_config_callback("test_config", config_callback)

    # 获取配置
    print(f"\n📖 获取配置:")
    config = config_manager.get_config("test_config")
    if config:
        print(f"   下载延迟: {config['spider_settings']['download_delay']}")
        print(f"   并发请求: {config['spider_settings']['concurrent_requests']}")

    # 获取配置版本信息
    print(f"\n📋 配置版本信息:")
    versions = config_manager.get_config_versions()
    for name, info in versions.items():
        print(f"   {name}: v{info['version']} (校验和: {info['checksum'][:8]}...)")

    # 启动文件监控
    if WATCHDOG_AVAILABLE:
        print(f"\n👁️ 启动文件监控...")
        monitor_success = config_manager.start_file_monitoring()
        print(f"   文件监控: {'✅' if monitor_success else '❌'}")

        if monitor_success:
            print(f"   提示: 修改 {config_file} 文件来测试热更新")
            print(f"   等待5秒...")
            time.sleep(5)

    # 清理测试文件
    try:
        config_file.unlink()
        test_config_dir.rmdir()
    except:
        pass

    # 停止配置管理器
    config_manager.stop()

    print(f"\n✅ 配置管理器测试完成")
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
