# -*- coding: utf-8 -*-
"""
数据验证器

提供数据格式验证、业务规则验证、数据完整性检查等功能
"""

import json
import logging
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ValidationRule:
    """验证规则基类"""

    def __init__(self, field_name: str, rule_name: str, error_message: str = None):
        self.field_name = field_name
        self.rule_name = rule_name
        self.error_message = error_message or f"{field_name} 验证失败"

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        """验证方法，子类需要实现"""
        raise NotImplementedError

    def get_error_message(self, value: Any = None) -> str:
        """获取错误消息"""
        return self.error_message


class RequiredRule(ValidationRule):
    """必需字段验证"""

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        return value is not None and str(value).strip() != ""


class TypeRule(ValidationRule):
    """数据类型验证"""

    def __init__(self, field_name: str, expected_type: type, **kwargs):
        super().__init__(field_name, "type", **kwargs)
        self.expected_type = expected_type

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        if value is None:
            return True  # None值由RequiredRule处理
        return isinstance(value, self.expected_type)


class LengthRule(ValidationRule):
    """长度验证"""

    def __init__(
        self, field_name: str, min_length: int = None, max_length: int = None, **kwargs
    ):
        super().__init__(field_name, "length", **kwargs)
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        if value is None:
            return True

        length = len(str(value))

        if self.min_length is not None and length < self.min_length:
            return False

        if self.max_length is not None and length > self.max_length:
            return False

        return True


class RegexRule(ValidationRule):
    """正则表达式验证"""

    def __init__(self, field_name: str, pattern: str, **kwargs):
        super().__init__(field_name, "regex", **kwargs)
        self.pattern = re.compile(pattern)

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        if value is None:
            return True

        return bool(self.pattern.match(str(value)))


class URLRule(ValidationRule):
    """URL格式验证"""

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        if value is None:
            return True

        try:
            result = urlparse(str(value))
            return all([result.scheme, result.netloc])
        except:
            return False


class EmailRule(ValidationRule):
    """邮箱格式验证"""

    def __init__(self, field_name: str, **kwargs):
        super().__init__(field_name, "email", **kwargs)
        self.pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        if value is None:
            return True

        return bool(self.pattern.match(str(value)))


class RangeRule(ValidationRule):
    """数值范围验证"""

    def __init__(
        self,
        field_name: str,
        min_value: Union[int, float] = None,
        max_value: Union[int, float] = None,
        **kwargs,
    ):
        super().__init__(field_name, "range", **kwargs)
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        if value is None:
            return True

        try:
            num_value = float(value)

            if self.min_value is not None and num_value < self.min_value:
                return False

            if self.max_value is not None and num_value > self.max_value:
                return False

            return True
        except (ValueError, TypeError):
            return False


class CustomRule(ValidationRule):
    """自定义验证规则"""

    def __init__(self, field_name: str, validator_func: Callable, **kwargs):
        super().__init__(field_name, "custom", **kwargs)
        self.validator_func = validator_func

    def validate(self, value: Any, data: Dict[str, Any] = None) -> bool:
        try:
            return self.validator_func(value, data)
        except Exception as e:
            logger.error(f"自定义验证规则执行失败: {e}")
            return False


class DataValidator:
    """数据验证器"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.rules = {}
        self.global_rules = []

        # 加载预定义规则
        self._load_predefined_rules()

        logger.info("数据验证器初始化完成")

    def _load_predefined_rules(self):
        """加载预定义验证规则"""
        # 基础字段验证规则
        self.add_rule("url", RequiredRule("url", "required", "URL是必需字段"))
        self.add_rule("url", URLRule("url", "url", "URL格式不正确"))

        self.add_rule(
            "title",
            LengthRule(
                "title", min_length=1, max_length=500, error_message="标题长度应在1-500字符之间"
            ),
        )

        self.add_rule(
            "content", LengthRule("content", min_length=10, error_message="内容长度至少10个字符")
        )

    def add_rule(self, field_name: str, rule: ValidationRule):
        """添加验证规则"""
        if field_name not in self.rules:
            self.rules[field_name] = []
        self.rules[field_name].append(rule)

    def add_global_rule(self, rule: ValidationRule):
        """添加全局验证规则"""
        self.global_rules.append(rule)

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """验证数据"""
        validation_result = {
            "is_valid": True,
            "errors": {},
            "warnings": {},
            "field_results": {},
            "validation_time": datetime.now().isoformat(),
        }

        # 验证各字段
        for field_name, field_rules in self.rules.items():
            field_value = data.get(field_name)
            field_errors = []

            for rule in field_rules:
                try:
                    if not rule.validate(field_value, data):
                        field_errors.append(rule.get_error_message(field_value))
                except Exception as e:
                    field_errors.append(f"验证规则执行失败: {e}")

            validation_result["field_results"][field_name] = {
                "is_valid": len(field_errors) == 0,
                "errors": field_errors,
            }

            if field_errors:
                validation_result["errors"][field_name] = field_errors
                validation_result["is_valid"] = False

        # 执行全局验证规则
        global_errors = []
        for rule in self.global_rules:
            try:
                if not rule.validate(data, data):
                    global_errors.append(rule.get_error_message())
            except Exception as e:
                global_errors.append(f"全局验证规则执行失败: {e}")

        if global_errors:
            validation_result["errors"]["_global"] = global_errors
            validation_result["is_valid"] = False

        # 执行业务逻辑验证
        business_validation = self._validate_business_logic(data)
        if business_validation["errors"]:
            validation_result["errors"].update(business_validation["errors"])
            validation_result["is_valid"] = False

        if business_validation["warnings"]:
            validation_result["warnings"].update(business_validation["warnings"])

        return validation_result

    def _validate_business_logic(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """业务逻辑验证"""
        errors = {}
        warnings = {}

        # 检查内容和标题的一致性
        if "title" in data and "content" in data:
            title = str(data["title"]).lower()
            content = str(data["content"]).lower()

            # 标题应该在内容中出现
            if title and content and title not in content:
                warnings["consistency"] = ["标题与内容可能不一致"]

        # 检查日期的合理性
        if "date" in data and data["date"]:
            try:
                # 简单的日期格式检查
                date_str = str(data["date"])
                if re.search(r"(\d{4})", date_str):
                    year = int(re.search(r"(\d{4})", date_str).group(1))
                    current_year = datetime.now().year

                    if year > current_year:
                        errors["date"] = ["日期不能是未来时间"]
                    elif year < 1900:
                        errors["date"] = ["日期过于久远"]
            except:
                pass

        # 检查内容质量
        if "content" in data and data["content"]:
            content = str(data["content"])

            # 检查是否包含错误页面标识
            error_indicators = ["404", "not found", "页面不存在", "访问错误"]
            if any(indicator in content.lower() for indicator in error_indicators):
                errors["content"] = ["内容可能是错误页面"]

            # 检查HTML标签比例
            html_tags = len(re.findall(r"<[^>]+>", content))
            total_chars = len(content)
            if total_chars > 0 and html_tags / total_chars > 0.3:
                warnings["content"] = ["内容包含过多HTML标签"]

        return {"errors": errors, "warnings": warnings}

    def validate_batch(self, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量验证数据"""
        batch_result = {
            "total_items": len(data_list),
            "valid_items": 0,
            "invalid_items": 0,
            "validation_results": [],
            "summary": {
                "common_errors": {},
                "field_error_rates": {},
            },
        }

        all_errors = []
        field_error_counts = {}

        for i, data in enumerate(data_list):
            result = self.validate(data)
            batch_result["validation_results"].append(result)

            if result["is_valid"]:
                batch_result["valid_items"] += 1
            else:
                batch_result["invalid_items"] += 1

                # 收集错误统计
                for field, errors in result["errors"].items():
                    if field not in field_error_counts:
                        field_error_counts[field] = 0
                    field_error_counts[field] += 1
                    all_errors.extend(errors)

        # 计算错误率
        for field, count in field_error_counts.items():
            batch_result["summary"]["field_error_rates"][field] = count / len(data_list)

        # 统计常见错误
        from collections import Counter

        error_counter = Counter(all_errors)
        batch_result["summary"]["common_errors"] = dict(error_counter.most_common(10))

        return batch_result


class ValidationPipeline:
    """验证管道"""

    def __init__(self, config: Optional[Dict] = None):
        self.validator = DataValidator(config)
        self.stats = {
            "total_processed": 0,
            "total_valid": 0,
            "total_invalid": 0,
            "error_counts": {},
        }

    def process_item(self, item: Dict[str, Any], spider=None) -> Dict[str, Any]:
        """处理数据项"""
        self.stats["total_processed"] += 1

        # 验证数据
        validation_result = self.validator.validate(item)

        # 添加验证结果到数据项
        item["_validation"] = validation_result

        # 更新统计
        if validation_result["is_valid"]:
            self.stats["total_valid"] += 1
        else:
            self.stats["total_invalid"] += 1

            # 统计错误类型
            for field, errors in validation_result["errors"].items():
                for error in errors:
                    if error not in self.stats["error_counts"]:
                        self.stats["error_counts"][error] = 0
                    self.stats["error_counts"][error] += 1

        return item

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()

        if stats["total_processed"] > 0:
            stats["valid_rate"] = stats["total_valid"] / stats["total_processed"]
            stats["invalid_rate"] = stats["total_invalid"] / stats["total_processed"]
        else:
            stats["valid_rate"] = 0.0
            stats["invalid_rate"] = 0.0

        return stats


class SchemaValidator:
    """模式验证器"""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.validator = DataValidator()
        self._build_rules_from_schema()

    def _build_rules_from_schema(self):
        """从模式构建验证规则"""
        for field_name, field_schema in self.schema.get("fields", {}).items():
            # 必需字段
            if field_schema.get("required", False):
                self.validator.add_rule(
                    field_name, RequiredRule(field_name, "required")
                )

            # 数据类型
            field_type = field_schema.get("type")
            if field_type == "string":
                self.validator.add_rule(field_name, TypeRule(field_name, str))
            elif field_type == "integer":
                self.validator.add_rule(field_name, TypeRule(field_name, int))
            elif field_type == "number":
                self.validator.add_rule(field_name, TypeRule(field_name, (int, float)))

            # 长度限制
            if "min_length" in field_schema or "max_length" in field_schema:
                self.validator.add_rule(
                    field_name,
                    LengthRule(
                        field_name,
                        min_length=field_schema.get("min_length"),
                        max_length=field_schema.get("max_length"),
                    ),
                )

            # 正则表达式
            if "pattern" in field_schema:
                self.validator.add_rule(
                    field_name, RegexRule(field_name, field_schema["pattern"])
                )

            # 数值范围
            if "minimum" in field_schema or "maximum" in field_schema:
                self.validator.add_rule(
                    field_name,
                    RangeRule(
                        field_name,
                        min_value=field_schema.get("minimum"),
                        max_value=field_schema.get("maximum"),
                    ),
                )

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """验证数据"""
        return self.validator.validate(data)
