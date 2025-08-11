# -*- coding: utf-8 -*-
"""
数据质量评估模块

提供数据质量评分、完整性检查、一致性验证等功能
"""

import json
import logging
import re
import statistics
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DataQualityAssessor:
    """数据质量评估器"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}

        # 质量评估维度权重
        self.quality_weights = {
            "completeness": 0.25,  # 完整性
            "accuracy": 0.25,  # 准确性
            "consistency": 0.20,  # 一致性
            "timeliness": 0.15,  # 时效性
            "validity": 0.15,  # 有效性
        }

        # 字段重要性权重
        self.field_weights = {
            "title": 0.3,
            "content": 0.4,
            "url": 0.1,
            "date": 0.1,
            "author": 0.05,
            "source": 0.05,
        }

        logger.info("数据质量评估器初始化完成")

    def assess_quality(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """评估数据质量"""
        if not isinstance(data, dict):
            return self._create_quality_report(0.0, ["数据格式错误"])

        # 各维度评分
        completeness_score = self.assess_completeness(data)
        accuracy_score = self.assess_accuracy(data)
        consistency_score = self.assess_consistency(data)
        timeliness_score = self.assess_timeliness(data)
        validity_score = self.assess_validity(data)

        # 计算综合评分
        overall_score = (
            completeness_score * self.quality_weights["completeness"]
            + accuracy_score * self.quality_weights["accuracy"]
            + consistency_score * self.quality_weights["consistency"]
            + timeliness_score * self.quality_weights["timeliness"]
            + validity_score * self.quality_weights["validity"]
        )

        # 生成质量报告
        quality_report = {
            "overall_score": round(overall_score, 3),
            "dimension_scores": {
                "completeness": round(completeness_score, 3),
                "accuracy": round(accuracy_score, 3),
                "consistency": round(consistency_score, 3),
                "timeliness": round(timeliness_score, 3),
                "validity": round(validity_score, 3),
            },
            "quality_level": self.get_quality_level(overall_score),
            "issues": self.identify_issues(data),
            "recommendations": self.generate_recommendations(data),
            "assessment_time": datetime.now().isoformat(),
        }

        return quality_report

    def assess_completeness(self, data: Dict[str, Any]) -> float:
        """评估数据完整性"""
        total_weight = 0
        completed_weight = 0

        for field, weight in self.field_weights.items():
            total_weight += weight

            if field in data and data[field] is not None:
                value = data[field]

                # 检查值是否有意义
                if isinstance(value, str):
                    if value.strip():  # 非空字符串
                        completed_weight += weight
                elif isinstance(value, (list, dict)):
                    if value:  # 非空列表或字典
                        completed_weight += weight
                else:
                    completed_weight += weight  # 其他类型的非None值

        return completed_weight / total_weight if total_weight > 0 else 0.0

    def assess_accuracy(self, data: Dict[str, Any]) -> float:
        """评估数据准确性"""
        accuracy_score = 1.0
        penalties = []

        # URL格式检查
        if "url" in data and data["url"]:
            url = str(data["url"])
            if not re.match(r'https?://[^\s<>"{}|\\^`\[\]]+', url):
                penalties.append(0.1)

        # 日期格式检查
        if "date" in data and data["date"]:
            date_str = str(data["date"])
            date_patterns = [
                r"\d{4}-\d{2}-\d{2}",
                r"\d{4}/\d{2}/\d{2}",
                r"\d{4}\.\d{2}\.\d{2}",
            ]
            if not any(re.search(pattern, date_str) for pattern in date_patterns):
                penalties.append(0.1)

        # 内容长度合理性检查
        if "content" in data and data["content"]:
            content = str(data["content"])
            if len(content) < 50:  # 内容过短
                penalties.append(0.2)
            elif len(content) > 100000:  # 内容过长
                penalties.append(0.1)

        # 标题长度合理性检查
        if "title" in data and data["title"]:
            title = str(data["title"])
            if len(title) < 5:  # 标题过短
                penalties.append(0.1)
            elif len(title) > 200:  # 标题过长
                penalties.append(0.1)

        # 应用惩罚
        for penalty in penalties:
            accuracy_score -= penalty

        return max(0.0, accuracy_score)

    def assess_consistency(self, data: Dict[str, Any]) -> float:
        """评估数据一致性"""
        consistency_score = 1.0

        # 检查字段命名一致性
        field_names = list(data.keys())
        naming_patterns = {
            "snake_case": sum(
                1 for name in field_names if "_" in name and name.islower()
            ),
            "camelCase": sum(
                1 for name in field_names if any(c.isupper() for c in name[1:])
            ),
            "PascalCase": sum(1 for name in field_names if name[0].isupper()),
        }

        # 如果命名风格不一致，扣分
        max_pattern_count = (
            max(naming_patterns.values()) if naming_patterns.values() else 0
        )
        total_fields = len(field_names)
        if total_fields > 0 and max_pattern_count / total_fields < 0.8:
            consistency_score -= 0.1

        # 检查数据类型一致性
        if "content" in data and "title" in data:
            content_type = type(data["content"])
            title_type = type(data["title"])
            if content_type != title_type and not (
                isinstance(data["content"], str) and isinstance(data["title"], str)
            ):
                consistency_score -= 0.1

        return max(0.0, consistency_score)

    def assess_timeliness(self, data: Dict[str, Any]) -> float:
        """评估数据时效性"""
        timeliness_score = 1.0

        # 检查爬取时间
        crawl_time = None
        for time_field in ["crawl_time", "extraction_time", "timestamp"]:
            if time_field in data and data[time_field]:
                try:
                    if isinstance(data[time_field], str):
                        crawl_time = datetime.fromisoformat(
                            data[time_field].replace("Z", "+00:00")
                        )
                    break
                except:
                    continue

        if crawl_time:
            # 计算数据新鲜度
            now = datetime.now()
            age_hours = (now - crawl_time.replace(tzinfo=None)).total_seconds() / 3600

            if age_hours > 168:  # 超过一周
                timeliness_score -= 0.3
            elif age_hours > 24:  # 超过一天
                timeliness_score -= 0.1
        else:
            # 没有时间戳，扣分
            timeliness_score -= 0.2

        return max(0.0, timeliness_score)

    def assess_validity(self, data: Dict[str, Any]) -> float:
        """评估数据有效性"""
        validity_score = 1.0

        # 检查必需字段
        required_fields = ["url"]
        for field in required_fields:
            if field not in data or not data[field]:
                validity_score -= 0.3

        # 检查数据格式
        if "url" in data and data["url"]:
            url = str(data["url"])
            if not url.startswith(("http://", "https://")):
                validity_score -= 0.2

        # 检查内容质量
        if "content" in data and data["content"]:
            content = str(data["content"])

            # 检查是否包含错误页面内容
            error_indicators = ["404", "not found", "页面不存在", "访问错误", "error"]
            if any(indicator in content.lower() for indicator in error_indicators):
                validity_score -= 0.4

            # 检查内容是否主要是HTML标签
            html_ratio = len(re.findall(r"<[^>]+>", content)) / max(
                len(content.split()), 1
            )
            if html_ratio > 0.3:
                validity_score -= 0.2

        return max(0.0, validity_score)

    def get_quality_level(self, score: float) -> str:
        """获取质量等级"""
        if score >= 0.9:
            return "优秀"
        elif score >= 0.8:
            return "良好"
        elif score >= 0.7:
            return "中等"
        elif score >= 0.6:
            return "较差"
        else:
            return "很差"

    def identify_issues(self, data: Dict[str, Any]) -> List[str]:
        """识别数据问题"""
        issues = []

        # 检查缺失字段
        important_fields = ["title", "content", "url"]
        for field in important_fields:
            if field not in data or not data[field]:
                issues.append(f"缺少重要字段: {field}")

        # 检查内容质量
        if "content" in data and data["content"]:
            content = str(data["content"])
            if len(content) < 100:
                issues.append("内容过短")
            if content.count("<") > content.count(" ") / 10:
                issues.append("内容包含过多HTML标签")

        # 检查标题质量
        if "title" in data and data["title"]:
            title = str(data["title"])
            if len(title) < 10:
                issues.append("标题过短")
            if title.isupper():
                issues.append("标题全为大写")

        return issues

    def generate_recommendations(self, data: Dict[str, Any]) -> List[str]:
        """生成改进建议"""
        recommendations = []

        # 基于问题生成建议
        issues = self.identify_issues(data)

        if any("缺少" in issue for issue in issues):
            recommendations.append("完善数据提取规则，确保重要字段的提取")

        if any("内容过短" in issue for issue in issues):
            recommendations.append("检查内容提取选择器，可能需要提取更多内容")

        if any("HTML标签" in issue for issue in issues):
            recommendations.append("增强HTML清洗规则，移除多余的标签")

        if any("标题" in issue for issue in issues):
            recommendations.append("优化标题提取规则，确保标题的完整性和格式")

        # 通用建议
        if not recommendations:
            recommendations.append("数据质量良好，建议定期监控和维护")

        return recommendations

    def _create_quality_report(self, score: float, issues: List[str]) -> Dict[str, Any]:
        """创建质量报告"""
        return {
            "overall_score": score,
            "quality_level": self.get_quality_level(score),
            "issues": issues,
            "assessment_time": datetime.now().isoformat(),
        }


class QualityMonitor:
    """数据质量监控器"""

    def __init__(self):
        self.assessor = DataQualityAssessor()
        self.quality_history = []
        self.stats = {
            "total_assessed": 0,
            "average_score": 0.0,
            "quality_distribution": Counter(),
        }

    def monitor_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """监控数据项质量"""
        quality_report = self.assessor.assess_quality(item)

        # 更新统计
        self.stats["total_assessed"] += 1
        self.quality_history.append(quality_report["overall_score"])

        # 计算平均分
        self.stats["average_score"] = statistics.mean(self.quality_history)

        # 更新质量分布
        quality_level = quality_report["quality_level"]
        self.stats["quality_distribution"][quality_level] += 1

        # 添加质量报告到数据项
        item["_quality_report"] = quality_report

        return item

    def get_quality_summary(self) -> Dict[str, Any]:
        """获取质量汇总"""
        if not self.quality_history:
            return {"message": "暂无质量数据"}

        return {
            "total_items": self.stats["total_assessed"],
            "average_score": round(self.stats["average_score"], 3),
            "min_score": min(self.quality_history),
            "max_score": max(self.quality_history),
            "quality_distribution": dict(self.stats["quality_distribution"]),
            "recent_trend": self._calculate_trend(),
        }

    def _calculate_trend(self) -> str:
        """计算质量趋势"""
        if len(self.quality_history) < 10:
            return "数据不足"

        recent_scores = self.quality_history[-10:]
        earlier_scores = (
            self.quality_history[-20:-10]
            if len(self.quality_history) >= 20
            else self.quality_history[:-10]
        )

        if not earlier_scores:
            return "数据不足"

        recent_avg = statistics.mean(recent_scores)
        earlier_avg = statistics.mean(earlier_scores)

        if recent_avg > earlier_avg + 0.05:
            return "上升"
        elif recent_avg < earlier_avg - 0.05:
            return "下降"
        else:
            return "稳定"


class QualityReporter:
    """质量报告生成器"""

    def __init__(self, monitor: QualityMonitor):
        self.monitor = monitor

    def generate_report(self, output_path: str = None) -> Dict[str, Any]:
        """生成质量报告"""
        summary = self.monitor.get_quality_summary()

        report = {
            "report_time": datetime.now().isoformat(),
            "summary": summary,
            "recommendations": self._generate_global_recommendations(summary),
        }

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

        return report

    def _generate_global_recommendations(self, summary: Dict[str, Any]) -> List[str]:
        """生成全局改进建议"""
        recommendations = []

        if "average_score" in summary:
            avg_score = summary["average_score"]

            if avg_score < 0.7:
                recommendations.append("整体数据质量偏低，建议全面检查数据提取和清洗流程")
            elif avg_score < 0.8:
                recommendations.append("数据质量中等，建议优化关键字段的提取规则")
            else:
                recommendations.append("数据质量良好，建议保持现有流程并定期监控")

        if "quality_distribution" in summary:
            distribution = summary["quality_distribution"]
            poor_ratio = (distribution.get("较差", 0) + distribution.get("很差", 0)) / max(
                summary.get("total_items", 1), 1
            )

            if poor_ratio > 0.2:
                recommendations.append("低质量数据比例过高，建议加强数据验证")

        return recommendations
