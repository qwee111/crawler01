#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬取数据分析脚本

分析当前爬虫系统爬取的所有数据
"""

import glob
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

# import pandas as pd  # 暂时不使用pandas


def analyze_json_files():
    """分析JSON数据文件"""
    print("📊 分析JSON数据文件...")

    data_dir = Path("data")
    if not data_dir.exists():
        print("❌ data目录不存在")
        return {}

    json_files = list(data_dir.glob("*.json"))

    if not json_files:
        print("❌ 未找到JSON数据文件")
        return {}

    print(f"📁 找到 {len(json_files)} 个JSON文件:")

    all_data = []
    file_stats = {}

    for json_file in json_files:
        print(f"   📄 {json_file.name}")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                content = f.read().strip()

                if not content:
                    print(f"      ⚠️ 文件为空")
                    continue

                # 处理多行JSON格式
                items = []
                for line in content.split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            item = json.loads(line)
                            items.append(item)
                        except json.JSONDecodeError:
                            # 尝试解析整个文件作为JSON数组
                            try:
                                items = json.loads(content)
                                break
                            except:
                                continue

                file_stats[json_file.name] = {
                    "items_count": len(items),
                    "file_size": json_file.stat().st_size,
                    "created_time": datetime.fromtimestamp(json_file.stat().st_ctime),
                }

                all_data.extend(items)

                print(f"      ✅ 解析成功，包含 {len(items)} 条数据")

                # 显示数据样例
                if items:
                    sample = items[0]
                    print(f"      📋 数据字段: {list(sample.keys())}")

        except Exception as e:
            print(f"      ❌ 解析失败: {e}")

    return {
        "all_data": all_data,
        "file_stats": file_stats,
        "total_items": len(all_data),
    }


def analyze_data_content(data_analysis):
    """分析数据内容"""
    print("\n🔍 分析数据内容...")

    all_data = data_analysis.get("all_data", [])

    if not all_data:
        print("❌ 没有数据可分析")
        return

    print(f"📊 总数据条数: {len(all_data)}")

    # 分析数据字段
    all_fields = set()
    field_counts = Counter()

    for item in all_data:
        if isinstance(item, dict):
            fields = set(item.keys())
            all_fields.update(fields)
            for field in fields:
                field_counts[field] += 1

    print(f"\n📋 数据字段统计 (共 {len(all_fields)} 个字段):")
    for field, count in field_counts.most_common():
        percentage = (count / len(all_data)) * 100
        print(f"   {field:<20} {count:>6} 条 ({percentage:>5.1f}%)")

    # 分析数据来源
    sources = Counter()
    urls = Counter()

    for item in all_data:
        if isinstance(item, dict):
            # 分析URL来源
            if "url" in item:
                from urllib.parse import urlparse

                parsed = urlparse(item["url"])
                domain = parsed.netloc
                sources[domain] += 1
                urls[item["url"]] += 1

            # 分析网站标识
            if "site" in item:
                sources[f"site:{item['site']}"] += 1

    if sources:
        print("\n🌐 数据来源统计:")
        for source, count in sources.most_common():
            print(f"   {source:<30} {count:>6} 条")

    # 分析数据类型
    data_types = Counter()
    for item in all_data:
        if isinstance(item, dict):
            if "title" in item and item["title"]:
                data_types["有标题"] += 1
            if "content" in item and item["content"]:
                data_types["有内容"] += 1
            if "links" in item and item["links"]:
                data_types["有链接"] += 1
            if "images" in item and item["images"]:
                data_types["有图片"] += 1

    if data_types:
        print("\n📝 数据类型统计:")
        for data_type, count in data_types.most_common():
            percentage = (count / len(all_data)) * 100
            print(f"   {data_type:<15} {count:>6} 条 ({percentage:>5.1f}%)")

    # 显示数据样例
    print("\n📄 数据样例:")
    for i, item in enumerate(all_data[:3]):
        print(f"\n   样例 {i+1}:")
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                elif isinstance(value, list) and len(value) > 3:
                    value = value[:3] + ["..."]
                print(f"     {key}: {value}")
        else:
            print(f"     {item}")


def check_database_data():
    """检查数据库中的数据"""
    print("\n💾 检查数据库数据...")

    # 检查MongoDB
    try:
        import subprocess

        result = subprocess.run(
            [
                "docker",
                "exec",
                "crawler_mongodb",
                "mongosh",
                "--eval",
                'db.adminCommand("listCollections")',
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            print("✅ MongoDB连接成功")
            # 这里可以进一步查询数据
        else:
            print("⚠️ MongoDB连接失败或无数据")

    except Exception as e:
        print(f"⚠️ MongoDB检查失败: {e}")

    # 检查PostgreSQL
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "crawler_postgresql",
                "psql",
                "-U",
                "crawler",
                "-d",
                "crawler",
                "-c",
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            print("✅ PostgreSQL连接成功")
            print(f"   输出: {result.stdout.strip()}")
        else:
            print("⚠️ PostgreSQL连接失败或无数据")

    except Exception as e:
        print(f"⚠️ PostgreSQL检查失败: {e}")


def analyze_crawl_logs():
    """分析爬虫日志"""
    print("\n📋 分析爬虫日志...")

    log_file = Path("logs/scrapy.log")
    if not log_file.exists():
        print("❌ 日志文件不存在")
        return

    try:
        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        lines = content.split("\n")
        print(f"📄 日志文件大小: {len(lines)} 行")

        # 统计爬虫运行次数
        spider_starts = [line for line in lines if "Spider opened" in line]
        spider_closes = [line for line in lines if "Spider closed" in line]

        print(f"🕷️ 爬虫启动次数: {len(spider_starts)}")
        print(f"🏁 爬虫完成次数: {len(spider_closes)}")

        # 统计请求和响应
        requests = [line for line in lines if "Scraped from" in line or "GET" in line]
        responses = [line for line in lines if "response" in line.lower()]

        print(f"📤 请求数量: {len(requests)}")
        print(f"📥 响应数量: {len(responses)}")

        # 统计错误
        errors = [line for line in lines if "ERROR" in line]
        warnings = [line for line in lines if "WARNING" in line]

        print(f"❌ 错误数量: {len(errors)}")
        print(f"⚠️ 警告数量: {len(warnings)}")

        # 统计状态码
        status_codes = Counter()
        for line in lines:
            if "status=" in line:
                try:
                    status_part = line.split("status=")[1].split()[0]
                    status_code = status_part.rstrip(",")
                    status_codes[status_code] += 1
                except:
                    continue

        if status_codes:
            print("\n📊 HTTP状态码统计:")
            for code, count in status_codes.most_common():
                print(f"   {code}: {count} 次")

        # 显示最近的日志
        print("\n📝 最近的日志 (最后10行):")
        for line in lines[-10:]:
            if line.strip():
                print(f"   {line}")

    except Exception as e:
        print(f"❌ 日志分析失败: {e}")


def generate_summary_report(data_analysis):
    """生成汇总报告"""
    print("\n📊 生成汇总报告...")

    file_stats = data_analysis.get("file_stats", {})
    total_items = data_analysis.get("total_items", 0)

    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_files": len(file_stats),
            "total_items": total_items,
            "total_size": sum(stats["file_size"] for stats in file_stats.values()),
        },
        "files": file_stats,
    }

    # 保存报告
    report_file = Path("data_analysis_report.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)

    print(f"✅ 报告已保存到: {report_file}")

    return report


def main():
    """主函数"""
    print("🎯 爬取数据分析报告")
    print("=" * 60)

    # 分析JSON文件
    data_analysis = analyze_json_files()

    # 分析数据内容
    analyze_data_content(data_analysis)

    # 检查数据库
    check_database_data()

    # 分析日志
    analyze_crawl_logs()

    # 生成报告
    report = generate_summary_report(data_analysis)

    # 输出总结
    print("\n" + "=" * 60)
    print("📋 数据爬取总结")
    print("=" * 60)

    summary = report["summary"]
    print(f"📁 数据文件数量: {summary['total_files']}")
    print(f"📊 数据条目总数: {summary['total_items']}")
    print(f"💾 数据总大小: {summary['total_size']:,} 字节")

    if summary["total_items"] > 0:
        print("\n✅ 爬虫系统已成功爬取数据！")
        print("📍 数据保存位置: data/ 目录")
        print("📄 数据格式: JSON")
        print("🔍 详细报告: data_analysis_report.json")
    else:
        print("\n⚠️ 暂无爬取数据")
        print("💡 建议运行爬虫: uv run scrapy crawl adaptive -a site=test_site")

    print("\n🎊 数据分析完成！")


if __name__ == "__main__":
    main()
