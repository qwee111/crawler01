#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告生成器

自动生成疾病监测分析报告
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from analysis.data_analyzer import DiseaseDataAnalyzer

    ANALYZER_AVAILABLE = True
except ImportError:
    ANALYZER_AVAILABLE = False

try:
    import matplotlib.pyplot as plt
    import seaborn as sns

    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
    plt.rcParams["axes.unicode_minus"] = False
    PLOT_AVAILABLE = True
except ImportError:
    PLOT_AVAILABLE = False

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DiseaseReportGenerator:
    """疾病监测报告生成器"""

    def __init__(self):
        self.analyzer = DiseaseDataAnalyzer() if ANALYZER_AVAILABLE else None
        self.output_dir = "reports/generated"
        os.makedirs(self.output_dir, exist_ok=True)

        logger.info("报告生成器初始化完成")

    def generate_html_report(
        self, analysis_results: Dict, report_title: str = "疾病监测分析报告"
    ) -> str:
        """生成HTML报告"""
        logger.info("生成HTML报告...")

        # 提取数据
        summary = analysis_results.get("summary", {})
        trends = analysis_results.get("trends", {})
        distribution = analysis_results.get("distribution", {})
        keywords = analysis_results.get("keywords", {})
        anomalies = analysis_results.get("anomalies", {})

        # 生成HTML内容
        html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{report_title}</title>
    <style>
        body {{ font-family: 'Microsoft YaHei', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; border-bottom: 2px solid #3498db; padding-bottom: 20px; margin-bottom: 30px; }}
        .header h1 {{ color: #2c3e50; margin: 0; }}
        .header p {{ color: #7f8c8d; margin: 10px 0 0 0; }}
        .section {{ margin: 30px 0; }}
        .section h2 {{ color: #2c3e50; border-left: 4px solid #3498db; padding-left: 15px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric {{ background: #ecf0f1; padding: 20px; border-radius: 8px; text-align: center; }}
        .metric-value {{ font-size: 2em; font-weight: bold; color: #3498db; }}
        .metric-label {{ color: #7f8c8d; margin-top: 5px; }}
        .table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .table th, .table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        .table th {{ background: #3498db; color: white; }}
        .table tr:nth-child(even) {{ background: #f9f9f9; }}
        .alert {{ padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .alert-high {{ background: #ffebee; border-left: 4px solid #e74c3c; }}
        .alert-medium {{ background: #fff3e0; border-left: 4px solid #f39c12; }}
        .alert-low {{ background: #e8f5e8; border-left: 4px solid #27ae60; }}
        .insights {{ background: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #17a2b8; }}
        .footer {{ text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #7f8c8d; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🦠 {report_title}</h1>
            <p>生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
        </div>

        <div class="section">
            <h2>📊 数据概览</h2>
            <div class="metrics">
                <div class="metric">
                    <div class="metric-value">{summary.get('summary', {}).get('total_articles', 0)}</div>
                    <div class="metric-label">总文章数</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{len(trends.get('keyword_frequency', {}))}</div>
                    <div class="metric-label">关键词数量</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{len(distribution.get('region_counts', {}))}</div>
                    <div class="metric-label">涉及地区</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{len(anomalies.get('daily_anomalies', [])) + len(anomalies.get('keyword_anomalies', []))}</div>
                    <div class="metric-label">异常数量</div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>📈 趋势分析</h2>
            <table class="table">
                <thead>
                    <tr><th>日期</th><th>文章数量</th></tr>
                </thead>
                <tbody>"""

        # 添加趋势数据
        daily_counts = trends.get("daily_counts", {})
        for date in sorted(daily_counts.keys())[-7:]:  # 最近7天
            html_content += f"<tr><td>{date}</td><td>{daily_counts[date]}</td></tr>"

        html_content += """
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>🔍 热门关键词</h2>
            <table class="table">
                <thead>
                    <tr><th>关键词</th><th>出现次数</th></tr>
                </thead>
                <tbody>"""

        # 添加关键词数据
        keyword_freq = keywords.get("top_keywords", {})
        for keyword, freq in list(keyword_freq.items())[:10]:  # 前10个
            html_content += f"<tr><td>{keyword}</td><td>{freq}</td></tr>"

        html_content += """
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>🗺️ 地区分布</h2>
            <table class="table">
                <thead>
                    <tr><th>地区</th><th>提及次数</th></tr>
                </thead>
                <tbody>"""

        # 添加地区数据
        region_counts = distribution.get("region_counts", {})
        for region, count in sorted(
            region_counts.items(), key=lambda x: x[1], reverse=True
        ):
            html_content += f"<tr><td>{region}</td><td>{count}</td></tr>"

        html_content += """
                </tbody>
            </table>
        </div>"""

        # 添加异常检测
        all_anomalies = anomalies.get("daily_anomalies", []) + anomalies.get(
            "keyword_anomalies", []
        )
        if all_anomalies:
            html_content += """
        <div class="section">
            <h2>⚠️ 异常检测</h2>"""

            for anomaly in all_anomalies:
                severity_class = f"alert-{anomaly.get('severity', 'low')}"
                html_content += f"""
            <div class="alert {severity_class}">
                <strong>{anomaly.get('type', '未知类型').upper()}</strong>: {anomaly.get('message', '无描述')}
            </div>"""

            html_content += "</div>"

        # 添加洞察
        insights = summary.get("insights", [])
        if insights:
            html_content += """
        <div class="section">
            <h2>💡 主要洞察</h2>
            <div class="insights">
                <ul>"""

            for insight in insights:
                html_content += f"<li>{insight}</li>"

            html_content += """
                </ul>
            </div>
        </div>"""

        html_content += """
        <div class="footer">
            <p>本报告由疾病监测系统自动生成 | 数据来源: 北京市疾病预防控制中心</p>
        </div>
    </div>
</body>
</html>"""

        # 保存HTML文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_file = os.path.join(self.output_dir, f"disease_report_{timestamp}.html")

        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"HTML报告已生成: {html_file}")
        return html_file

    def generate_json_report(self, analysis_results: Dict) -> str:
        """生成JSON格式报告"""
        logger.info("生成JSON报告...")

        # 添加报告元数据
        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "report_type": "disease_monitoring",
                "version": "1.0",
                "generator": "DiseaseReportGenerator",
            },
            "analysis_results": analysis_results,
        }

        # 保存JSON文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = os.path.join(self.output_dir, f"disease_report_{timestamp}.json")

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"JSON报告已生成: {json_file}")
        return json_file

    def generate_charts(self, analysis_results: Dict) -> List[str]:
        """生成图表"""
        if not PLOT_AVAILABLE:
            logger.warning("matplotlib不可用，跳过图表生成")
            return []

        logger.info("生成图表...")

        chart_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        trends = analysis_results.get("trends", {})
        distribution = analysis_results.get("distribution", {})
        keywords = analysis_results.get("keywords", {})

        # 1. 每日趋势图
        if trends.get("daily_counts"):
            plt.figure(figsize=(12, 6))
            daily_counts = trends["daily_counts"]
            dates = sorted(daily_counts.keys())
            counts = [daily_counts[date] for date in dates]

            plt.plot(dates, counts, marker="o", linewidth=2, markersize=6)
            plt.title("每日文章数量趋势", fontsize=16, fontweight="bold")
            plt.xlabel("日期", fontsize=12)
            plt.ylabel("文章数量", fontsize=12)
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()

            chart_file = os.path.join(self.output_dir, f"daily_trend_{timestamp}.png")
            plt.savefig(chart_file, dpi=300, bbox_inches="tight")
            plt.close()
            chart_files.append(chart_file)

        # 2. 关键词频次图
        if keywords.get("top_keywords"):
            plt.figure(figsize=(12, 8))
            keyword_freq = keywords["top_keywords"]
            top_keywords = list(keyword_freq.items())[:15]  # 前15个

            words, freqs = zip(*top_keywords)

            plt.barh(range(len(words)), freqs, color="skyblue")
            plt.yticks(range(len(words)), words)
            plt.title("热门关键词频次", fontsize=16, fontweight="bold")
            plt.xlabel("出现次数", fontsize=12)
            plt.gca().invert_yaxis()
            plt.grid(True, alpha=0.3, axis="x")
            plt.tight_layout()

            chart_file = os.path.join(self.output_dir, f"keywords_{timestamp}.png")
            plt.savefig(chart_file, dpi=300, bbox_inches="tight")
            plt.close()
            chart_files.append(chart_file)

        # 3. 地区分布饼图
        if distribution.get("region_counts"):
            plt.figure(figsize=(10, 8))
            region_counts = distribution["region_counts"]

            # 只显示前8个地区，其他合并为"其他"
            sorted_regions = sorted(
                region_counts.items(), key=lambda x: x[1], reverse=True
            )
            if len(sorted_regions) > 8:
                top_regions = sorted_regions[:8]
                other_count = sum(count for _, count in sorted_regions[8:])
                top_regions.append(("其他", other_count))
            else:
                top_regions = sorted_regions

            regions, counts = zip(*top_regions)

            colors = plt.cm.Set3(range(len(regions)))
            plt.pie(
                counts, labels=regions, autopct="%1.1f%%", colors=colors, startangle=90
            )
            plt.title("地区提及分布", fontsize=16, fontweight="bold")
            plt.axis("equal")

            chart_file = os.path.join(self.output_dir, f"regions_{timestamp}.png")
            plt.savefig(chart_file, dpi=300, bbox_inches="tight")
            plt.close()
            chart_files.append(chart_file)

        logger.info(f"图表生成完成，共 {len(chart_files)} 个文件")
        return chart_files

    def generate_summary_text(self, analysis_results: Dict) -> str:
        """生成文本摘要"""
        logger.info("生成文本摘要...")

        summary = analysis_results.get("summary", {})
        trends = analysis_results.get("trends", {})
        distribution = analysis_results.get("distribution", {})
        keywords = analysis_results.get("keywords", {})
        anomalies = analysis_results.get("anomalies", {})

        # 构建摘要文本
        text_summary = f"""
疾病监测分析摘要报告
生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}

一、数据概览
- 分析文章总数: {summary.get('summary', {}).get('total_articles', 0)} 篇
- 时间范围: {summary.get('summary', {}).get('date_range', {}).get('start', 'N/A')} 至 {summary.get('summary', {}).get('date_range', {}).get('end', 'N/A')}
- 涉及关键词: {len(keywords.get('top_keywords', {}))} 个
- 涉及地区: {len(distribution.get('region_counts', {}))} 个

二、主要发现
"""

        # 添加洞察
        insights = summary.get("insights", [])
        if insights:
            for i, insight in enumerate(insights, 1):
                text_summary += f"{i}. {insight}\n"
        else:
            text_summary += "暂无特殊发现。\n"

        # 添加异常情况
        all_anomalies = anomalies.get("daily_anomalies", []) + anomalies.get(
            "keyword_anomalies", []
        )
        if all_anomalies:
            text_summary += f"\n三、异常检测\n发现 {len(all_anomalies)} 个异常情况:\n"
            for i, anomaly in enumerate(all_anomalies, 1):
                text_summary += f"{i}. [{anomaly.get('severity', '未知').upper()}] {anomaly.get('message', '无描述')}\n"
        else:
            text_summary += "\n三、异常检测\n未发现异常情况，系统运行正常。\n"

        # 添加建议
        text_summary += "\n四、建议\n"
        if all_anomalies:
            text_summary += "1. 重点关注检测到的异常情况\n"
            text_summary += "2. 加强相关地区和关键词的监测\n"
            text_summary += "3. 必要时启动应急响应机制\n"
        else:
            text_summary += "1. 继续保持常规监测\n"
            text_summary += "2. 关注数据趋势变化\n"
            text_summary += "3. 定期更新监测规则\n"

        text_summary += "\n本报告由疾病监测系统自动生成。"

        # 保存文本文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        text_file = os.path.join(self.output_dir, f"disease_summary_{timestamp}.txt")

        with open(text_file, "w", encoding="utf-8") as f:
            f.write(text_summary)

        logger.info(f"文本摘要已生成: {text_file}")
        return text_file

    def generate_full_report(self, days_back=30, include_charts=True) -> Dict[str, str]:
        """生成完整报告"""
        logger.info(f"开始生成完整报告，分析最近{days_back}天的数据...")

        if not self.analyzer:
            logger.error("分析器不可用")
            return {"error": "分析器不可用"}

        try:
            # 运行数据分析
            analysis_results = self.analyzer.run_full_analysis(days_back=days_back)

            if "error" in analysis_results:
                return {"error": analysis_results["error"]}

            # 生成各种格式的报告
            report_files = {}

            report_files["html"] = self.generate_html_report(analysis_results)
            report_files["json"] = self.generate_json_report(analysis_results)
            report_files["summary"] = self.generate_summary_text(analysis_results)

            if include_charts:
                chart_files = self.generate_charts(analysis_results)
                report_files["charts"] = chart_files

            logger.info("完整报告生成完成")
            return report_files

        except Exception as e:
            logger.error(f"报告生成失败: {e}")
            return {"error": str(e)}


def main():
    """主函数"""
    print("📋 疾病监测报告生成器")
    print("=" * 60)

    if not ANALYZER_AVAILABLE:
        print("❌ 数据分析器不可用，无法生成报告")
        return False

    generator = DiseaseReportGenerator()

    print("📊 生成分析报告...")
    report_files = generator.generate_full_report(days_back=30, include_charts=True)

    if "error" in report_files:
        print(f"❌ 报告生成失败: {report_files['error']}")
        return False

    print("\n✅ 报告生成完成！")
    print(f"📁 输出目录: {generator.output_dir}")
    print("\n📄 生成的文件:")

    for report_type, file_path in report_files.items():
        if report_type == "charts":
            print(f"   📊 图表文件: {len(file_path)} 个")
            for chart_file in file_path:
                print(f"      - {os.path.basename(chart_file)}")
        else:
            print(f"   📋 {report_type.upper()}: {os.path.basename(file_path)}")

    print("\n💡 建议:")
    print("   1. 查看HTML报告获得最佳阅读体验")
    print("   2. 使用JSON数据进行进一步分析")
    print("   3. 分享文本摘要给相关人员")
    print("   4. 定期生成报告跟踪趋势变化")

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
