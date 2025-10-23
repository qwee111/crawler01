import argparse
import json
import os
import redis
from dotenv import load_dotenv
from typing import List

# 加载.env文件中的环境变量
load_dotenv()


def default_china_regions() -> List[str]:
    """
    中国省级与直辖市/自治区/特别行政区名称（可按需扩展）。
    """
    return [
        "北京", "上海", "天津", "重庆",
        # "河北", "山西", "辽宁", "吉林", "黑龙江",
        # "江苏", "浙江", "安徽", "福建", "江西", "山东",
        # "河南", "湖北", "湖南", "广东", "广西", "海南",
        # "四川", "贵州", "云南", "西藏", "陕西", "甘肃",
        # "青海", "宁夏", "新疆", "内蒙古",
        # "香港", "澳门", "台湾",
    ]


def default_china_diseases() -> List[str]:
    """
    针对国内关注的重点法定传染病/综合类关键词（可按需扩展）。
    """
    return [
        # 重点示例
        "霍乱", "登革热", "手足口病", "流感", "甲流", "乙脑", "禽流感",
        # "麻疹", "风疹", "百日咳", "腺病毒", "结核", "伤寒", "诺如",
        # "急性胃肠炎", "沙门氏菌", "李斯特菌", "布鲁氏菌病", "炭疽",
        # "狂犬病", "疟疾", "恙虫病", "寨卡",
    ]


def build_china_combination_keywords(
    regions: List[str], diseases: List[str]
) -> List[str]:
    """
    生成面向中国国内的组合关键词，如：
    "{地区} {疾病} 疫情"、"{地区} {疾病} 通报"、"{地区} {疾病} 暴发"、"{地区} {疾病} 聚集"。
    """
    variants = ["疫情", "通报", "暴发", "聚集", "病例", "预警"]
    results: List[str] = []
    for region in regions:
        for disease in diseases:
            # for v in variants:
                results.append(f"{region} {disease}")
    # 去重保持顺序
    seen = set()
    deduped: List[str] = []
    for k in results:
        if k not in seen:
            deduped.append(k)
            seen.add(k)
    return deduped


def default_monitor_keywords() -> List[str]:
    """
    默认的传染病与突发公共卫生事件监测关键词列表。
    说明：覆盖常见法定传染病、症候群、聚集性事件与官方发布用语。
    可根据需要在外部文件中扩展或替换。
    """
    return [
        # 总括类
        "中国 疫情 通报",
        "中国 聚集性 疫情",
        "突发公共卫生事件",
        "公共卫生 预警",
        "公共卫生 风险提示",
        "不明原因 发热",
        "不明原因 肺炎",
        "暴发 疫情",
        "群体性 事件",

        # 呼吸道传染病
        "新冠 疫情",
        "新冠 病例",
        "流感 暴发",
        "甲流 暴发",
        "禽流感 人感染",
        "麻疹 疫情",
        "风疹 疫情",
        "百日咳 增加",
        "腺病毒 暴发",
        "结核病 聚集",

        # 肠道/食品与水相关
        "霍乱 疫情",
        "伤寒 疫情",
        "诺如 病毒 暴发",
        "急性胃肠炎 聚集",
        "食源性 中毒",
        "沙门氏菌 暴发",
        "李斯特菌 感染",

        # 节肢动物/虫媒
        "登革热 疫情",
        "疟疾 输入性",
        "乙脑 病例",
        "基孔肯雅 热",
        "寨卡 病毒",
        "恙虫病 聚集",

        # 人畜共患与其他重点
        "布鲁氏菌病 暴发",
        "炭疽 个案",
        "狂犬病 暴露",
        "手足口病 暴发",

        # 官方渠道关键词
        "疾控中心 通报",
        "卫生健康委 通报",
        "健康应急 办公室 通告",
        "疾控 风险评估",
    ]


def load_keywords_from_file(path: str) -> List[str]:
    """
    从文本文件加载关键词，每行一个；忽略空行与以#开头的注释行。
    """
    keywords: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text or text.startswith("#"):
                    continue
                keywords.append(text)
    except FileNotFoundError:
        print(f"关键词文件未找到: {path}")
    except Exception as e:
        print(f"读取关键词文件失败: {e}")
    return keywords

def push_request_to_redis(request_data: dict):
    """
    将完整的请求数据推送到Redis队列。
    """
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_key = "bochaai:start_urls"  # 与爬虫中定义的redis_key一致

    try:
        r = redis.from_url(redis_url)
        r.ping()
        print(f"成功连接到Redis: {redis_url}")
    except redis.exceptions.ConnectionError as e:
        print(f"无法连接到Redis: {e}")
        print("请确保Redis服务器正在运行，并且REDIS_URL配置正确。")
        return

    # 将请求数据转换为JSON字符串
    data = json.dumps(request_data, ensure_ascii=False)
    r.lpush(redis_key, data)
    print(f"成功将请求推送到Redis队列 '{redis_key}'")
    print(f"推送的数据: {data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="将Bochaai API查询推送到Redis队列。")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--query", type=str, help="单条搜索关键词。")
    group.add_argument("--keywords_file", type=str, help="从文件批量读取关键词（每行一个）。")
    group.add_argument("--use_default_keywords", action="store_true", help="使用内置监测关键词列表进行批量推送。")
    group.add_argument("--china_combo", action="store_true", help="使用国内 省市×疾病×变体 组合关键词进行批量推送。")
    parser.add_argument("--regions_file", type=str, help="自定义地区列表文件（每行一个），与 --china_combo 搭配。")
    parser.add_argument("--diseases_file", type=str, help="自定义疾病列表文件（每行一个），与 --china_combo 搭配。")
    parser.add_argument("--freshness", type=str, default="oneWeek",
                        choices=["oneDay", "oneWeek", "oneMonth", "oneYear"],
                        help="结果的新鲜度 (oneDay, oneWeek, oneMonth, oneYear)。")
    parser.add_argument("--summary", type=bool, default=True, help="是否包含摘要。")
    parser.add_argument("--include", type=str, help="包含特定内容。")
    parser.add_argument("--exclude", type=str, help="排除特定内容。")
    parser.add_argument("--count", type=int, default=50, help="返回结果的数量。")
    parser.add_argument("--api_key", type=str, help="Bochaai API KEY。如果未提供，将从 .env 文件中获取 API_TOKEN。")

    args = parser.parse_args()

    # 获取API KEY
    api_key = args.api_key if args.api_key else os.getenv("API_TOKEN")
    if not api_key or api_key == "your-api-token-change-this-in-production":
        print("错误: API_TOKEN 未在 .env 文件中配置或未通过 --api_key 参数提供。请设置有效的API KEY。")
        exit(1)

    # 计算关键词来源
    keywords: List[str] = []
    if args.keywords_file:
        keywords = load_keywords_from_file(args.keywords_file)
        if not keywords:
            print("从文件未加载到任何关键词，程序退出。")
            exit(1)
    elif args.use_default_keywords:
        keywords = default_monitor_keywords()
    elif args.china_combo:
        regions = load_keywords_from_file(args.regions_file) if args.regions_file else default_china_regions()
        diseases = load_keywords_from_file(args.diseases_file) if args.diseases_file else default_china_diseases()
        if not regions:
            print("地区列表为空，程序退出。")
            exit(1)
        if not diseases:
            print("疾病列表为空，程序退出。")
            exit(1)
        keywords = build_china_combination_keywords(regions, diseases)
    elif args.query:
        keywords = [args.query]
    else:
        print("未提供 --query / --keywords_file / --use_default_keywords / --china_combo 之一，程序退出。")
        exit(1)

    # 构建请求头（复用）
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    total = 0
    for q in keywords:
        request_body = {
            "query": q,
            "freshness": args.freshness,
            "summary": args.summary,
            "count": args.count,
        }
        if args.include:
            request_body["include"] = args.include
        if args.exclude:
            request_body["exclude"] = args.exclude

        full_request_data = {
            "url": "https://api.bochaai.com/v1/web-search",
            "method": "POST",
            "headers": headers,
            "body": json.dumps(request_body),
            "callback": "parse",  # 爬虫中处理响应的方法
            "meta": {"query_params": request_body},  # 将原始查询参数传递给parse方法
        }

        push_request_to_redis(full_request_data)
        total += 1

    print(f"已完成推送，共 {total} 条关键词。")
