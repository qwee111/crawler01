import os
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
import requests
import json
import hashlib
import argparse
import re
from html import escape as html_escape

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AIReportGenerator:
    def __init__(self, mongo_uri=None, db_name="crawler_db", zhipuai_api_key=None, glm_model="glm-4.5-air", mongo_client=None, generate_pdf=True):
        self.mongo_uri = mongo_uri if mongo_uri else os.getenv("MONGODB_URL", "mongodb://localhost:27017/")
        self.db_name = db_name
        self.zhipuai_api_key = zhipuai_api_key if zhipuai_api_key else os.getenv("ZHIPUAI_API_KEY")
        self.glm_model = glm_model
        self.client = mongo_client # 允许注入mock客户端
        self.db = self.client[self.db_name] if self.client else None
        self.generate_pdf = generate_pdf

        if not self.zhipuai_api_key:
            logging.error("ZHIPUAI_API_KEY is not set in environment variables or provided.")
            raise ValueError("ZHIPUAI_API_KEY is required for AI report generation.")

    def _connect_mongo(self):
        """连接MongoDB数据库"""
        if not self.client: # 如果没有注入客户端，则创建新的连接
            try:
                self.client = MongoClient(self.mongo_uri)
                self.db = self.client[self.db_name]
                logging.info("Successfully connected to MongoDB.")
                logging.info(f"Using database: {self.db_name}")
                logging.info(f"MongoDB URI: {self.mongo_uri}")
            except Exception as e:
                logging.error(f"Failed to connect to MongoDB: {e}")
                raise

    def _close_mongo(self):
        """关闭MongoDB连接"""
        # 只有当客户端是由本实例创建时才关闭，避免关闭mock客户端
        if self.client and isinstance(self.client, MongoClient):
            self.client.close()
            logging.info("MongoDB connection closed.")

    def fetch_data_from_mongo(self, site_name, start_date, end_date):
        """
        从MongoDB中提取指定站点和时间范围内的数据。
        数据结构示例:
        {
            "site": "jxcdc",
            "url": "http://www.jxcdc.cn/news/2023/01/01/123.html",
            "title": "江西省疾控中心发布流感预警",
            "content": "详细内容...",
            "publish_date": "2023-01-01",
            "crawl_date": "2023-01-02"
        }
        """
        if self.db is None:
            self._connect_mongo()

        collection = self.db[f"{site_name}_data"]
        query = {
            "publish_date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        try:
            data = list(collection.find(query))
            logging.info(f"Fetched {len(data)} records for site '{site_name}' from {start_date} to {end_date}.")
            return data
        except Exception as e:
            logging.error(f"Error fetching data from MongoDB for site '{site_name}': {e}")
            return []

    def generate_report_with_glm(self, prompt_content):
        """
        调用GLM大模型生成报告。
        """
        headers = {
            "Authorization": f"Bearer {self.zhipuai_api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.glm_model,
            "messages": [
                {"role": "user", "content": prompt_content}
            ]
        }
        try:
            response = requests.post("https://open.bigmodel.cn/api/paas/v4/chat/completions", headers=headers, json=payload)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            result = response.json()
            if result and result.get("choices"):
                report_content = result["choices"][0]["message"]["content"]
                logging.info("Successfully generated report content using GLM model.")
                return report_content
            else:
                logging.warning(f"GLM API returned no choices or empty result: {result}")
                return "未能生成报告内容。"
        except requests.exceptions.RequestException as e:
            logging.error(f"Error calling GLM API: {e}")
            return f"调用GLM API失败: {e}"
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding GLM API response: {e}")
            return f"解析GLM API响应失败: {e}"

    def create_report_prompt(self, site_name, start_date, end_date, data):
        """
        根据提取的数据创建用于GLM模型的Prompt。
        """
        if not data:
            return f"请生成一份关于 {site_name} 传染病态势监测报告。没有可用的数据。"

        # 简单地将数据内容拼接起来，实际应用中可能需要更复杂的摘要或结构化
        data_summary = "\n".join([f"标题: {item.get('title', '无标题')}\n内容摘要: {item.get('content', '无内容')[:800]}\n链接: {item.get('url','无链接')}..." for item in data])
        
        prompt = f"""
        # 角色设定
        你是一名专业的公共卫生分析师，负责撰写权威的传染病态势监测报告。你具备深厚的流行病学知识和数据分析能力。

        # 任务说明
        请根据提供的网络搜索摘要信息，严格按照以下模板和格式要求，撰写一份专业的《传染病态势监测与风险评估报告》。

        # 原始搜索信息
        {data_summary}

        # 报告撰写要求

        ## 1. 专业规范
        - **语言风格**：使用专业、客观、简洁的公共卫生领域术语
        - **数据呈现**：对数据进行合理解读和分析，避免简单罗列，如有详细的地点、时间、事件等，请使用表格形式呈现
        - **风险评估**：基于证据进行风险评估，避免主观臆断
        - **时效性**：重点关注最新、最相关的疫情信息

        ## 2. 报告模板

        # 传染病态势监测与风险评估报告

        **报告周期：** {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}
        **签发单位：** 南湖实验室
        **签发日期：** {datetime.now().strftime('%Y-%m-%d')}

        ---

        ### 一、执行摘要

        **核心提要：**
        [用3-5句话高度概括本期核心态势，供决策者快速阅读]

        **总体态势评估：**
        - 主要关注传染病种类：[基于搜索信息列出]

        - 总体疫情水平：[定性描述，如：总体平稳、呈上升趋势、处于高位流行期等]
        - 关键特征概述：[简要描述主要特点]

        **主要发现与亮点：**
        - 关键事件：[病例数显著变化、聚集性疫情、新发传染病等]
        - 高风险区域/人群：[基于信息识别出的重点]

        **核心风险评估：**
        - 风险等级：[低/中/高]
        - 主要风险点：[列出1-3个核心风险]

        **优先建议：**
        [提出1-3项最紧迫的防控建议]

        ---

        ### 二、监测背景与目的

        **监测背景：**
        [结合当前季节特点、国际疫情形势、大型活动等因素描述背景]

        **报告目的：**
        [阐明本报告旨在及时掌握流行态势，识别风险，为防控策略提供依据]

        ---

        ### 三、数据来源与方法

        **数据来源：**
        基于网络公开信息、官方通报、媒体报道等多元数据源。   

        **分析方法：**
        采用描述性流行病学方法，结合专业研判进行分析。

        ---

        ### 四、监测结果与流行病学特征分析

        #### 4.1 总体疫情概况
        [基于搜索信息描述总体发病情况、重点传染病构成等]

        #### 4.2 重点传染病专题分析

        **1. 传染病名称**
        - **流行强度**：[发病情况、变化趋势]
        - **时间分布**：[流行趋势分析]
        - **地区分布**：[高发地区识别]
        - **人群分布**：[高危人群分析]
        - **病原学特征**：[如涉及相关信息]

        **2. 其他传染病**
        [结构同上，根据实际信息调整]

        #### 4.3 突发公共卫生事件及聚集性疫情
        [描述发现的聚集性疫情或突发事件]

        #### 4.4 国际/境外疫情动态
        [简述相关的国际疫情及输入风险]

        ---

        ### 五、风险评估

        **风险等级：**
        - **总体风险**：[低/中/高]
        - **重点病种风险**：[对各重点传染病分别评估]

        **风险分析：**
        - **可能性分析**：[基于当前态势评估传播风险]
        - **影响程度分析**：[评估潜在健康和社会影响]

        **主要风险点：**
        [具体列出识别出的风险因素]

        ---

        ### 六、结论与建议

        #### 6.1 主要结论
        [总结核心发现，重申总体态势和主要风险]

        #### 6.2 防控建议

        **对政府及决策部门的建议：**
        [具体可行的政策建议]

        **对医疗卫生机构的建议：**
        [专业技术层面的建议]

        **对公众的建议：**
        [健康教育和行为指导]

        **对监测工作的建议：**
        [完善监测体系的建议]

        ---

        ### 七、局限性
        [说明基于网络信息的局限性，如信息完整性、时效性等]

        ---

        ### 八、数据来源
        [有用的新闻来源的标题+URL列表]

        ## 3. 格式要求
        - 使用清晰的标题层级（从一级标题到四级标题）
        - 重要内容使用**加粗**强调
        - 列表项使用规范的符号（- 或 1.）
        - 段落分明，逻辑清晰
        - 避免使用复杂表格（因格式限制）
        - 使用规范的Markdown语法

        ## 4. 注意事项
        - 如某些部分信息不足，请明确标注"信息不足，需进一步监测"
        - 基于可获得的信息进行合理推断，避免过度解读
        - 确保建议具有可操作性和针对性
        - 保持客观中立，不夸大风险
        - 严格按照模板结构组织内容
        - 使用专业的公共卫生术语

        请开始撰写报告：
        """
        return prompt

    def save_report(self, site_name, report_content, report_date):
        """
        将生成的报告保存到文件。
        """
        report_dir = "reports/generated"
        os.makedirs(report_dir, exist_ok=True)
        file_path = self._build_unique_path(report_dir, site_name, report_date, "md", report_content)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(report_content)
            logging.info(f"Report saved to {file_path}")
            # 需要时生成 PDF
            if self.generate_pdf:
                try:
                    pdf_path = self._save_report_pdf(report_dir, site_name, report_date, report_content)
                    if pdf_path:
                        logging.info(f"PDF saved to {pdf_path}")
                        return pdf_path
                    else:
                        logging.warning("PDF 生成失败或未安装可用依赖，已跳过。")
                except Exception as e:
                    logging.warning(f"PDF 生成异常，已跳过: {e}")
        except Exception as e:
            logging.error(f"Error saving report to {file_path}: {e}")
            return None

    def _build_unique_path(self, directory, site_name, report_date, ext, content_str):
        """构建唯一文件路径：site + 时间戳 + 内容哈希（若冲突则递增后缀）。"""
        ts = report_date.strftime("%Y%m%d_%H%M%S")
        short_hash = hashlib.sha1((content_str or "").encode("utf-8")).hexdigest()[:8]
        safe_site = self._sanitize_filename(site_name)
        base = f"{safe_site}_report_{ts}_{short_hash}"
        candidate = os.path.join(directory, f"{base}.{ext}")
        if not os.path.exists(candidate):
            return candidate
        # 罕见冲突时递增版本后缀
        i = 2
        while True:
            candidate = os.path.join(directory, f"{base}_v{i}.{ext}")
            if not os.path.exists(candidate):
                return candidate
            i += 1

    def _sanitize_filename(self, name):
        """将文件名中的特殊字符替换为下划线。"""
        if not name:
            return "report"
        allowed = []
        for ch in name:
            if ch.isalnum() or ch in ("-", "_", "."):
                allowed.append(ch)
            else:
                allowed.append("_")
        return "".join(allowed).strip("._") or "report"

    def _save_report_pdf(self, directory, site_name, report_date, report_md):
        """将 Markdown 报告导出为 PDF。多方案回退：WeasyPrint -> pdfkit -> ReportLab。

        为保证多次生成版式一致：统一 Markdown→HTML 转换与 CSS 模板。
        """
        pdf_path = self._build_unique_path(directory, site_name, report_date, "pdf", report_md)

        # 统一的 HTML 文档
        body_html = self._markdown_to_html(report_md or "")
        # 优化链接显示：为纯文本链接插入 <wbr> 软换行，避免 PDF 中随机位置被截断
        body_html = self._insert_soft_wraps_into_links(body_html)
        document_html = self._build_html_document(
            title=f"{self._sanitize_filename(site_name)} 传染病态势监测与风险评估报告",
            body_html=body_html,
            gen_time=report_date,
        )

        # 1) WeasyPrint (HTML->PDF)
        try:
            from weasyprint import HTML
            HTML(string=document_html).write_pdf(pdf_path)
            return pdf_path
        except Exception:
            pass

        # 2) pdfkit (wkhtmltopdf)
        try:
            import pdfkit
            pdfkit.from_string(document_html, pdf_path)
            return pdf_path
        except Exception:
            pass

        # 3) ReportLab (纯文本渲染)
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.units import mm

            c = canvas.Canvas(pdf_path, pagesize=A4)
            width, height = A4
            left_margin = 15 * mm
            top_margin = height - 15 * mm
            line_height = 6 * mm

            textobject = c.beginText()
            textobject.setTextOrigin(left_margin, top_margin)
            textobject.setFont("Helvetica", 10)

            for line in (report_md or "").splitlines():
                if textobject.getY() <= 15 * mm:
                    c.drawText(textobject)
                    c.showPage()
                    textobject = c.beginText()
                    textobject.setTextOrigin(left_margin, top_margin)
                    textobject.setFont("Helvetica", 10)
                # 简单截断过长行，避免超出页面宽度
                max_chars = 110
                while len(line) > max_chars:
                    textobject.textLine(line[:max_chars])
                    line = line[max_chars:]
                textobject.textLine(line)

            c.drawText(textobject)
            c.showPage()
            c.save()
            return pdf_path
        except Exception:
            return None

    def _markdown_to_html(self, md_text):
        """将 Markdown 文本转换为 HTML，启用扩展以修复列表/换行格式。"""
        try:
            import markdown
            extensions = [
                "extra",       # 包含 tables、fenced_code、abbr 等
                "sane_lists",  # 更符合直觉的列表解析
                "nl2br",       # 单换行转换为 <br/>
            ]
            return markdown.markdown(md_text or "", extensions=extensions, output_format="html5")
        except Exception:
            # 回退：将内容作为纯文本展示
            return f"<pre style='font-family:monospace;white-space:pre-wrap;'>{html_escape(md_text or '')}</pre>"

    def _build_html_document(self, title, body_html, gen_time):
        """构建带统一样式的 HTML 文档，确保 PDF 渲染一致。"""
        safe_title = html_escape(title or "报告")
        gen_time_str = html_escape(gen_time.strftime("%Y-%m-%d %H:%M:%S")) if hasattr(gen_time, "strftime") else ""
        css = """
        <style>
        @page { size: A4; margin: 20mm; }
        body {
          font-family: "Noto Sans CJK SC", "Source Han Sans SC", "Microsoft YaHei", "PingFang SC", "SimSun", sans-serif;
          font-size: 12pt; line-height: 1.6; color: #222;
        }
        h1, h2, h3, h4 { margin: 1.2em 0 0.6em; font-weight: 600; page-break-after: avoid; }
        h1 { font-size: 20pt; }
        h2 { font-size: 16pt; border-bottom: 1px solid #ddd; padding-bottom: 4px; }
        h3 { font-size: 14pt; }
        p { margin: 0.6em 0; }
        ul, ol { margin: 0.6em 0 0.6em 1.8em; padding: 0; }
        li { margin: 0.2em 0; }
        strong { font-weight: 700; }
        em { font-style: italic; }
        code, pre { font-family: "Fira Code", "JetBrains Mono", Consolas, monospace; }
        /* 预格式文本：允许在任意位置换行，避免超长行导致溢出。
           注意：不使用无效的 `word-break: break-word`，改用标准属性。*/
        pre {
          white-space: pre-wrap;
          overflow-wrap: anywhere; /* WeasyPrint 支持，优先 */
          word-break: break-all;   /* 兜底：对连续长串/URL 保证可断行（牺牲部分可读性）*/
          background: #f8f8f8;
          padding: 8px;
          border-radius: 4px;
        }
        /* 链接：保持文本完整，仅允许视觉换行，复制/点击不受影响 */
        a { word-break: normal; overflow-wrap: anywhere; color: #0645ad; text-decoration: none; }
        a:hover { text-decoration: underline; }
        table { border-collapse: collapse; margin: 0.8em 0; width: 100%; }
        th, td { border: 1px solid #ccc; padding: 6px 8px; text-align: left; vertical-align: top; }
        blockquote { border-left: 3px solid #ccc; margin: 0.6em 0; padding: 0.3em 0 0.3em 0.8em; color: #555; }
        .doc-header { margin-bottom: 16px; font-size: 10pt; color: #666; }
        .doc-title { margin: 0 0 6px; font-size: 18pt; font-weight: 700; }
        .doc-meta { margin: 0; }
        </style>
        """
        header = f"""
        <div class=\"doc-header\">
          <div class=\"doc-title\">{safe_title}</div>
          <p class=\"doc-meta\">生成时间：{gen_time_str}</p>
        </div>
        """
        return f"""
        <!DOCTYPE html>
        <html lang=\"zh-CN\">
        <head>
          <meta charset=\"utf-8\"/>
          <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
          <title>{safe_title}</title>
          {css}
        </head>
        <body>

          {body_html}
        </body>
        </html>
        """

    def _insert_soft_wraps_into_links(self, html):
        """在 <a>文本</a> 内插入 <wbr>，优先在 / . ? & - _ = # 处换行，避免 URL 被随机截断。
        仅处理纯文本锚文本（不含内嵌标签）。
        """
        if not html:
            return html

        def _looks_like_url(text):
            t = text.strip().lower()
            return t.startswith("http://") or t.startswith("https://") or ("/" in t and "." in t)

        def _soft_wrap(s):
            # 在指定字符后插入 <wbr>
            return re.sub(r"([/\\.\?\&\-_=#])", r"\1<wbr>", s)

        # 仅替换锚文本为纯文本的 <a>，避免破坏嵌套结构
        pattern = re.compile(r"(<a\b[^>]*>)([^<]+)(</a>)", flags=re.IGNORECASE)

        def repl(m):
            prefix, text, suffix = m.group(1), m.group(2), m.group(3)
            if _looks_like_url(text):
                return f"{prefix}{_soft_wrap(text)}{suffix}"
            return m.group(0)

        try:
            return pattern.sub(repl, html)
        except Exception:
            return html

    def generate_full_report(self, site_name, days_ago=7):
        """
        生成完整的传染病态势监测报告。
        """
        try:
            self._connect_mongo()
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_ago)

            logging.info(f"Starting report generation for site: {site_name}, from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

            data = self.fetch_data_from_mongo(site_name, start_date, end_date)
            if not data:
                logging.warning(f"No data found for {site_name} in the specified period. Skipping report generation.")
                return None

            prompt_content = self.create_report_prompt(site_name, start_date, end_date, data)
            report_content = self.generate_report_with_glm(prompt_content)

            if report_content:
                saved_path = self.save_report(site_name, report_content, end_date)
                return saved_path
            else:
                logging.error("Failed to generate report content from GLM model.")
                return None
        except Exception as e:
            logging.error(f"An error occurred during full report generation for site '{site_name}': {e}")
            return None
        finally:
            self._close_mongo()

def _parse_cli_args():
    parser = argparse.ArgumentParser(description="Generate infectious disease situation report")
    parser.add_argument("--site", required=True, help="Site name (collection prefix)")
    parser.add_argument("--days", type=int, default=7, help="Days to look back (default: 7)")
    parser.add_argument("--no-pdf", action="store_true", help="Disable PDF export")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGODB_URL"), help="MongoDB URI (optional)")
    parser.add_argument("--db", default=os.getenv("MONGODB_DATABASE", "crawler_db"), help="MongoDB database name")
    parser.add_argument("--api-key", default=os.getenv("ZHIPUAI_API_KEY"), help="ZhipuAI API key (optional)")
    parser.add_argument("--model", default=os.getenv("ZHIPUAI_MODEL_NAME", "glm-4.5-air"), help="GLM model name")
    return parser.parse_args()


def _main_cli():
    args = _parse_cli_args()
    try:
        generator = AIReportGenerator(
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            zhipuai_api_key=args.api_key,
            glm_model=args.model,
            generate_pdf=(not args.no_pdf),
        )
        report_path = generator.generate_full_report(args.site, days_ago=args.days)
        if report_path:
            print(f"Report successfully generated and saved to: {report_path}")
            return 0
        else:
            print(f"Failed to generate report for site: {args.site}")
            return 2
    except ValueError as ve:
        print(f"Configuration error: {ve}")
        return 3
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return 4


if __name__ == "__main__":
    raise SystemExit(_main_cli())
