#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能预警系统

基于数据分析的疾病监测预警系统
"""

import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from analysis.data_analyzer import DiseaseDataAnalyzer

    ANALYZER_AVAILABLE = True
except ImportError:
    ANALYZER_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DiseaseAlertSystem:
    """疾病预警系统"""

    def __init__(self, config_file="monitoring/alert_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.analyzer = DiseaseDataAnalyzer() if ANALYZER_AVAILABLE else None

        # 预警规则
        self.alert_rules = {
            "daily_spike": {
                "threshold_multiplier": 2.0,  # 超过平均值2倍
                "severity": "medium",
                "description": "每日文章数异常增长",
            },
            "keyword_surge": {
                "threshold_multiplier": 3.0,  # 超过平均值3倍
                "severity": "high",
                "description": "关键词频次异常增长",
            },
            "new_disease_keyword": {
                "threshold": 5,  # 新关键词出现5次以上
                "severity": "high",
                "description": "发现新的疾病关键词",
            },
            "regional_concentration": {
                "threshold": 0.7,  # 70%的提及集中在单一地区
                "severity": "medium",
                "description": "地区提及过度集中",
            },
        }

        logger.info("疾病预警系统初始化完成")

    def load_config(self) -> Dict:
        """加载配置"""
        default_config = {
            "email": {
                "enabled": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "",
                "password": "",
                "recipients": [],
            },
            "webhook": {"enabled": False, "url": "", "headers": {}},
            "alert_frequency": 3600,  # 1小时检查一次
            "alert_history_days": 7,  # 保留7天的预警历史
        }

        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r", encoding="utf-8") as f:
                    config = json.load(f)
                # 合并默认配置
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                return config
            else:
                # 创建默认配置文件
                os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
                with open(self.config_file, "w", encoding="utf-8") as f:
                    json.dump(default_config, f, indent=2, ensure_ascii=False)
                return default_config
        except Exception as e:
            logger.error(f"加载配置失败: {e}")
            return default_config

    def save_config(self):
        """保存配置"""
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存配置失败: {e}")

    def check_daily_spike(self, trends: Dict) -> List[Dict]:
        """检查每日数据异常"""
        alerts = []

        daily_counts = list(trends.get("daily_counts", {}).values())
        if len(daily_counts) < 3:
            return alerts

        # 计算基线（排除最新一天）
        baseline = daily_counts[:-1]
        mean_count = sum(baseline) / len(baseline)
        latest_count = daily_counts[-1]

        threshold = mean_count * self.alert_rules["daily_spike"]["threshold_multiplier"]

        if latest_count > threshold:
            alerts.append(
                {
                    "type": "daily_spike",
                    "severity": self.alert_rules["daily_spike"]["severity"],
                    "message": f"今日文章数({latest_count})超过正常水平({mean_count:.1f})的{self.alert_rules['daily_spike']['threshold_multiplier']}倍",
                    "value": latest_count,
                    "threshold": threshold,
                    "timestamp": datetime.now().isoformat(),
                }
            )

        return alerts

    def check_keyword_surge(
        self, trends: Dict, historical_keywords: Dict = None
    ) -> List[Dict]:
        """检查关键词异常"""
        alerts = []

        current_keywords = trends.get("keyword_frequency", {})
        if not current_keywords:
            return alerts

        # 如果有历史数据，比较变化
        if historical_keywords:
            for keyword, current_freq in current_keywords.items():
                historical_freq = historical_keywords.get(keyword, 0)
                if historical_freq > 0:
                    growth_ratio = current_freq / historical_freq
                    if (
                        growth_ratio
                        > self.alert_rules["keyword_surge"]["threshold_multiplier"]
                    ):
                        alerts.append(
                            {
                                "type": "keyword_surge",
                                "severity": self.alert_rules["keyword_surge"][
                                    "severity"
                                ],
                                "message": f"关键词'{keyword}'频次异常增长({historical_freq} -> {current_freq})",
                                "keyword": keyword,
                                "growth_ratio": growth_ratio,
                                "timestamp": datetime.now().isoformat(),
                            }
                        )
                elif (
                    current_freq >= self.alert_rules["new_disease_keyword"]["threshold"]
                ):
                    # 新出现的关键词
                    alerts.append(
                        {
                            "type": "new_disease_keyword",
                            "severity": self.alert_rules["new_disease_keyword"][
                                "severity"
                            ],
                            "message": f"发现新的高频关键词'{keyword}'(出现{current_freq}次)",
                            "keyword": keyword,
                            "frequency": current_freq,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

        return alerts

    def check_regional_concentration(self, distribution: Dict) -> List[Dict]:
        """检查地区集中度异常"""
        alerts = []

        region_counts = distribution.get("region_counts", {})
        if not region_counts:
            return alerts

        total_mentions = sum(region_counts.values())
        if total_mentions == 0:
            return alerts

        # 检查是否有地区占比过高
        for region, count in region_counts.items():
            ratio = count / total_mentions
            if ratio > self.alert_rules["regional_concentration"]["threshold"]:
                alerts.append(
                    {
                        "type": "regional_concentration",
                        "severity": self.alert_rules["regional_concentration"][
                            "severity"
                        ],
                        "message": f"地区'{region}'的提及占比过高({ratio:.1%})",
                        "region": region,
                        "ratio": ratio,
                        "count": count,
                        "total": total_mentions,
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        return alerts

    def run_alert_check(self, days_back=7) -> List[Dict]:
        """运行预警检查"""
        logger.info("开始运行预警检查...")

        if not self.analyzer:
            logger.error("分析器不可用")
            return []

        try:
            # 获取当前数据
            current_results = self.analyzer.run_full_analysis(days_back=days_back)
            if "error" in current_results:
                logger.error(f"分析失败: {current_results['error']}")
                return []

            # 获取历史数据用于比较
            historical_results = self.analyzer.run_full_analysis(
                days_back=days_back * 2
            )

            all_alerts = []

            # 检查各种异常
            all_alerts.extend(self.check_daily_spike(current_results.get("trends", {})))

            historical_keywords = (
                historical_results.get("trends", {}).get("keyword_frequency", {})
                if "error" not in historical_results
                else {}
            )
            all_alerts.extend(
                self.check_keyword_surge(
                    current_results.get("trends", {}), historical_keywords
                )
            )

            all_alerts.extend(
                self.check_regional_concentration(
                    current_results.get("distribution", {})
                )
            )

            # 保存预警记录
            if all_alerts:
                self.save_alert_history(all_alerts)

            logger.info(f"预警检查完成，发现 {len(all_alerts)} 个预警")
            return all_alerts

        except Exception as e:
            logger.error(f"预警检查失败: {e}")
            return []

    def save_alert_history(self, alerts: List[Dict]):
        """保存预警历史"""
        try:
            history_file = "monitoring/alert_history.json"
            os.makedirs(os.path.dirname(history_file), exist_ok=True)

            # 加载现有历史
            history = []
            if os.path.exists(history_file):
                with open(history_file, "r", encoding="utf-8") as f:
                    history = json.load(f)

            # 添加新预警
            history.extend(alerts)

            # 清理过期记录
            cutoff_date = datetime.now() - timedelta(
                days=self.config["alert_history_days"]
            )
            history = [
                alert
                for alert in history
                if datetime.fromisoformat(alert["timestamp"]) > cutoff_date
            ]

            # 保存历史
            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2, ensure_ascii=False)

            logger.info(f"预警历史已保存，共 {len(history)} 条记录")

        except Exception as e:
            logger.error(f"保存预警历史失败: {e}")

    def send_email_alert(self, alerts: List[Dict]) -> bool:
        """发送邮件预警"""
        if not self.config["email"]["enabled"] or not alerts:
            return False

        try:
            # 创建邮件内容
            subject = f"疾病监测预警 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

            body = "检测到以下疾病监测预警：\n\n"
            for i, alert in enumerate(alerts, 1):
                body += f"{i}. [{alert['severity'].upper()}] {alert['message']}\n"
                body += f"   时间: {alert['timestamp']}\n\n"

            body += "\n请及时关注相关情况。\n\n"
            body += "此邮件由疾病监测系统自动发送。"

            # 发送邮件
            msg = MIMEMultipart()
            msg["From"] = self.config["email"]["username"]
            msg["To"] = ", ".join(self.config["email"]["recipients"])
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain", "utf-8"))

            server = smtplib.SMTP(
                self.config["email"]["smtp_server"], self.config["email"]["smtp_port"]
            )
            server.starttls()
            server.login(
                self.config["email"]["username"], self.config["email"]["password"]
            )

            text = msg.as_string()
            server.sendmail(
                self.config["email"]["username"],
                self.config["email"]["recipients"],
                text,
            )
            server.quit()

            logger.info(f"邮件预警发送成功，收件人: {len(self.config['email']['recipients'])} 人")
            return True

        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False

    def send_webhook_alert(self, alerts: List[Dict]) -> bool:
        """发送Webhook预警"""
        if not self.config["webhook"]["enabled"] or not alerts:
            return False

        try:
            import requests

            payload = {
                "timestamp": datetime.now().isoformat(),
                "alert_count": len(alerts),
                "alerts": alerts,
            }

            headers = self.config["webhook"].get("headers", {})
            headers["Content-Type"] = "application/json"

            response = requests.post(
                self.config["webhook"]["url"], json=payload, headers=headers, timeout=10
            )

            if response.status_code == 200:
                logger.info("Webhook预警发送成功")
                return True
            else:
                logger.error(f"Webhook发送失败，状态码: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Webhook发送失败: {e}")
            return False

    def run_monitoring_cycle(self):
        """运行监控周期"""
        logger.info("开始监控周期...")

        # 运行预警检查
        alerts = self.run_alert_check()

        if alerts:
            logger.warning(f"发现 {len(alerts)} 个预警")

            # 发送预警通知
            email_sent = self.send_email_alert(alerts)
            webhook_sent = self.send_webhook_alert(alerts)

            # 输出预警信息
            print(f"\n🚨 疾病监测预警 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            print("=" * 60)

            for i, alert in enumerate(alerts, 1):
                severity_icon = "🔴" if alert["severity"] == "high" else "🟡"
                print(f"{severity_icon} {i}. {alert['message']}")

            print(f"\n📧 邮件通知: {'✅ 已发送' if email_sent else '❌ 未配置或失败'}")
            print(f"🔗 Webhook通知: {'✅ 已发送' if webhook_sent else '❌ 未配置或失败'}")

        else:
            logger.info("未发现异常，系统正常")
            print(f"✅ 监控检查完成 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) - 系统正常")

        return alerts


def main():
    """主函数"""
    print("🚨 疾病监测预警系统")
    print("=" * 60)

    if not ANALYZER_AVAILABLE:
        print("❌ 数据分析器不可用，无法运行预警系统")
        return False

    alert_system = DiseaseAlertSystem()

    print("🔍 运行预警检查...")
    alerts = alert_system.run_monitoring_cycle()

    print("\n📊 检查结果:")
    print(f"   预警数量: {len(alerts)}")
    print(f"   系统状态: {'⚠️ 需要关注' if alerts else '✅ 正常'}")

    if alerts:
        print("\n💡 建议:")
        print("   1. 查看详细的预警信息")
        print("   2. 分析相关数据趋势")
        print("   3. 必要时采取应对措施")
        print("   4. 配置邮件/Webhook通知")

    return len(alerts) == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
