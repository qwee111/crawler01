#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据分析引擎

对爬取的疾病防控数据进行智能分析
"""

import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict

# import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import pymongo

    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

try:
    import jieba
    import jieba.analyse

    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False

try:
    import matplotlib.pyplot as plt
    import seaborn as sns

    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]  # 支持中文
    plt.rcParams["axes.unicode_minus"] = False
    PLOT_AVAILABLE = True
except ImportError:
    PLOT_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DiseaseDataAnalyzer:
    """疾病数据分析器"""

    def __init__(
        self,
        mongo_uri="mongodb://admin:password123@localhost:27017/",
        db_name="crawler_db",
    ):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None

        # 疾病关键词分类
        self.disease_categories = {
            "呼吸道疾病": ["流感", "新冠", "COVID", "肺炎", "咳嗽", "发热", "感冒", "哮喘"],
            "消化道疾病": ["腹泻", "诺如", "轮状", "食物中毒", "肠炎", "胃炎"],
            "传染病": ["结核", "艾滋", "HIV", "肝炎", "手足口", "麻疹", "水痘", "流脑"],
            "慢性病": ["糖尿病", "高血压", "心脏病", "癌症", "肿瘤", "慢病"],
            "疫苗相关": ["疫苗", "接种", "免疫", "预防接种"],
            "公共卫生": ["监测", "预警", "防控", "消毒", "隔离", "应急", "卫生"],
        }

        # 地区信息
        self.beijing_districts = [
            "朝阳",
            "海淀",
            "丰台",
            "石景山",
            "门头沟",
            "房山",
            "通州",
            "顺义",
            "昌平",
            "大兴",
            "怀柔",
            "平谷",
            "密云",
            "延庆",
            "东城",
            "西城",
        ]

        self.connect_database()

    def connect_database(self):
        """连接数据库"""
        if not MONGO_AVAILABLE:
            logger.warning("pymongo未安装，无法连接MongoDB")
            return False

        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            # 测试连接
            self.client.admin.command("ping")
            logger.info("MongoDB连接成功")
            return True
        except Exception as e:
            logger.error(f"MongoDB连接失败: {e}")
            return False

    def load_data_from_mongodb(self, collection_name="bjcdc_data", limit=None):
        """从MongoDB加载数据"""
        if self.db is None:
            logger.error("数据库未连接")
            return []

        try:
            collection = self.db[collection_name]
            query = {}

            if limit:
                cursor = collection.find(query).limit(limit)
            else:
                cursor = collection.find(query)

            data = list(cursor)
            logger.info(f"从MongoDB加载了 {len(data)} 条数据")
            return data
        except Exception as e:
            logger.error(f"数据加载失败: {e}")
            return []

    def load_data_from_json(self, json_file_path):
        """从JSON文件加载数据"""
        try:
            with open(json_file_path, "r", encoding="utf-8") as f:
                data = []
                for line in f:
                    line = line.strip()
                    if line:
                        data.append(json.loads(line))

            logger.info(f"从JSON文件加载了 {len(data)} 条数据")
            return data
        except Exception as e:
            logger.error(f"JSON文件加载失败: {e}")
            return []

    def extract_disease_keywords(self, text):
        """提取疾病关键词"""
        if not text:
            return []

        found_keywords = []
        text_lower = text.lower()

        for category, keywords in self.disease_categories.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    found_keywords.append({"keyword": keyword, "category": category})

        return found_keywords

    def extract_location_info(self, text):
        """提取地区信息"""
        if not text:
            return []

        found_locations = []
        for district in self.beijing_districts:
            if district in text:
                found_locations.append(district)

        return found_locations

    def analyze_time_trends(self, data):
        """分析时间趋势"""
        logger.info("分析时间趋势...")

        # 按日期统计
        date_counts = defaultdict(int)
        disease_by_date = defaultdict(lambda: defaultdict(int))

        for item in data:
            # 提取日期
            date_str = None
            if "publish_dates" in item and item["publish_dates"]:
                if isinstance(item["publish_dates"], list):
                    date_str = item["publish_dates"][0]
                else:
                    date_str = item["publish_dates"]
            elif "crawl_timestamp" in item:
                date_str = item["crawl_timestamp"][:10]  # 取日期部分

            if date_str:
                # 清理日期格式
                date_str = re.sub(r"[【】\[\]]", "", date_str)
                try:
                    if len(date_str) == 10 and "-" in date_str:
                        date_counts[date_str] += 1

                        # 分析疾病类型
                        text = str(item.get("news_titles", "")) + str(
                            item.get("title", "")
                        )
                        keywords = self.extract_disease_keywords(text)
                        for kw in keywords:
                            disease_by_date[date_str][kw["category"]] += 1
                except:
                    continue

        # 转换为时间序列
        if date_counts:
            dates = sorted(date_counts.keys())
            trend_data = {
                "dates": dates,
                "counts": [date_counts[date] for date in dates],
                "disease_trends": dict(disease_by_date),
            }

            logger.info(f"时间趋势分析完成，覆盖 {len(dates)} 天")
            return trend_data

        return {}

    def analyze_disease_distribution(self, data):
        """分析疾病分布"""
        logger.info("分析疾病分布...")

        category_counts = defaultdict(int)
        keyword_counts = defaultdict(int)

        for item in data:
            # 合并所有文本
            text_fields = ["title", "news_titles", "content"]
            combined_text = ""

            for field in text_fields:
                if field in item and item[field]:
                    if isinstance(item[field], list):
                        combined_text += " ".join(str(x) for x in item[field])
                    else:
                        combined_text += str(item[field])

            # 提取疾病关键词
            keywords = self.extract_disease_keywords(combined_text)
            for kw in keywords:
                category_counts[kw["category"]] += 1
                keyword_counts[kw["keyword"]] += 1

        distribution_data = {
            "categories": dict(category_counts),
            "keywords": dict(keyword_counts),
            "top_categories": dict(Counter(category_counts).most_common(10)),
            "top_keywords": dict(Counter(keyword_counts).most_common(20)),
        }

        logger.info(f"疾病分布分析完成，发现 {len(category_counts)} 个类别")
        return distribution_data

    def analyze_geographic_distribution(self, data):
        """分析地理分布"""
        logger.info("分析地理分布...")

        location_counts = defaultdict(int)
        location_diseases = defaultdict(lambda: defaultdict(int))

        for item in data:
            # 合并所有文本
            text_fields = ["title", "news_titles", "content"]
            combined_text = ""

            for field in text_fields:
                if field in item and item[field]:
                    if isinstance(item[field], list):
                        combined_text += " ".join(str(x) for x in item[field])
                    else:
                        combined_text += str(item[field])

            # 提取地区信息
            locations = self.extract_location_info(combined_text)
            diseases = self.extract_disease_keywords(combined_text)

            for location in locations:
                location_counts[location] += 1
                for disease in diseases:
                    location_diseases[location][disease["category"]] += 1

        geographic_data = {
            "location_counts": dict(location_counts),
            "location_diseases": dict(location_diseases),
            "top_locations": dict(Counter(location_counts).most_common(10)),
        }

        logger.info(f"地理分布分析完成，覆盖 {len(location_counts)} 个地区")
        return geographic_data

    def analyze_content_quality(self, data):
        """分析内容质量"""
        logger.info("分析内容质量...")

        quality_metrics = {
            "total_items": len(data),
            "items_with_title": 0,
            "items_with_content": 0,
            "items_with_date": 0,
            "items_with_disease_keywords": 0,
            "avg_title_length": 0,
            "avg_content_length": 0,
            "disease_relevance_rate": 0,
        }

        title_lengths = []
        content_lengths = []
        disease_relevant_count = 0

        for item in data:
            # 标题质量
            if item.get("title") or (item.get("news_titles") and item["news_titles"]):
                quality_metrics["items_with_title"] += 1
                title = item.get("title", "") or (
                    item.get("news_titles", [""])[0] if item.get("news_titles") else ""
                )
                title_lengths.append(len(str(title)))

            # 内容质量
            if item.get("content") or item.get("news_titles"):
                quality_metrics["items_with_content"] += 1
                content = str(item.get("content", "")) + str(
                    item.get("news_titles", "")
                )
                content_lengths.append(len(content))

            # 日期质量
            if item.get("publish_dates") or item.get("crawl_timestamp"):
                quality_metrics["items_with_date"] += 1

            # 疾病相关性
            text = (
                str(item.get("title", ""))
                + str(item.get("news_titles", ""))
                + str(item.get("content", ""))
            )
            if self.extract_disease_keywords(text):
                quality_metrics["items_with_disease_keywords"] += 1
                disease_relevant_count += 1

        # 计算平均值
        if title_lengths:
            quality_metrics["avg_title_length"] = np.mean(title_lengths)
        if content_lengths:
            quality_metrics["avg_content_length"] = np.mean(content_lengths)
        if quality_metrics["total_items"] > 0:
            quality_metrics["disease_relevance_rate"] = (
                disease_relevant_count / quality_metrics["total_items"]
            )

        logger.info(f"内容质量分析完成，疾病相关性: {quality_metrics['disease_relevance_rate']:.2%}")
        return quality_metrics

    def generate_comprehensive_analysis(self, data_source=None):
        """生成综合分析报告"""
        logger.info("开始生成综合分析报告...")

        # 加载数据
        if data_source is None:
            data = self.load_data_from_mongodb()
        elif isinstance(data_source, str):
            data = self.load_data_from_json(data_source)
        else:
            data = data_source

        if not data:
            logger.error("没有数据可供分析")
            return None

        # 执行各项分析
        analysis_results = {
            "metadata": {
                "analysis_time": datetime.now().isoformat(),
                "data_count": len(data),
                "analyzer_version": "1.0",
            },
            "time_trends": self.analyze_time_trends(data),
            "disease_distribution": self.analyze_disease_distribution(data),
            "geographic_distribution": self.analyze_geographic_distribution(data),
            "content_quality": self.analyze_content_quality(data),
        }

        # 生成摘要
        summary = self.generate_analysis_summary(analysis_results)
        analysis_results["summary"] = summary

        logger.info("综合分析报告生成完成")
        return analysis_results

    def generate_analysis_summary(self, analysis_results):
        """生成分析摘要"""
        summary = {"key_findings": [], "recommendations": [], "data_insights": {}}

        # 数据洞察
        quality = analysis_results.get("content_quality", {})
        disease_dist = analysis_results.get("disease_distribution", {})
        geo_dist = analysis_results.get("geographic_distribution", {})

        summary["data_insights"] = {
            "total_analyzed": quality.get("total_items", 0),
            "disease_relevance": f"{quality.get('disease_relevance_rate', 0):.1%}",
            "top_disease_category": max(
                disease_dist.get("categories", {}).items(), key=lambda x: x[1]
            )[0]
            if disease_dist.get("categories")
            else "N/A",
            "most_active_district": max(
                geo_dist.get("location_counts", {}).items(), key=lambda x: x[1]
            )[0]
            if geo_dist.get("location_counts")
            else "N/A",
        }

        # 关键发现
        if quality.get("disease_relevance_rate", 0) > 0.8:
            summary["key_findings"].append("数据疾病相关性很高，内容质量良好")
        elif quality.get("disease_relevance_rate", 0) > 0.5:
            summary["key_findings"].append("数据疾病相关性中等，需要优化过滤规则")
        else:
            summary["key_findings"].append("数据疾病相关性较低，建议调整爬取策略")

        # 建议
        summary["recommendations"].append("定期监控疾病趋势变化")
        summary["recommendations"].append("重点关注高发地区的疫情动态")
        summary["recommendations"].append("建立疾病预警机制")

        return summary

    def save_analysis_results(self, results, output_file="analysis_results.json"):
        """保存分析结果"""
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"分析结果已保存到: {output_file}")
            return True
        except Exception as e:
            logger.error(f"保存分析结果失败: {e}")
            return False


def main():
    """主函数"""
    print("🔬 疾病数据分析引擎")
    print("=" * 60)

    # 创建分析器
    analyzer = DiseaseDataAnalyzer()

    # 生成综合分析
    results = analyzer.generate_comprehensive_analysis()

    if results:
        # 保存结果
        analyzer.save_analysis_results(results, "reports/disease_analysis_report.json")

        # 显示摘要
        print("\n📊 分析摘要:")
        print("-" * 40)
        summary = results.get("summary", {})
        insights = summary.get("data_insights", {})

        print(f"📈 分析数据量: {insights.get('total_analyzed', 0)} 条")
        print(f"🦠 疾病相关性: {insights.get('disease_relevance', 'N/A')}")
        print(f"🏥 主要疾病类别: {insights.get('top_disease_category', 'N/A')}")
        print(f"🏙️ 最活跃地区: {insights.get('most_active_district', 'N/A')}")

        print("\n💡 关键发现:")
        for finding in summary.get("key_findings", []):
            print(f"   • {finding}")

        print("\n🎯 建议:")
        for rec in summary.get("recommendations", []):
            print(f"   • {rec}")

        print("\n✅ 分析完成！详细报告已保存")
        return True
    else:
        print("❌ 分析失败")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
