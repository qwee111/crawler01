# 国家卫健委增强反反爬虫系统使用说明

## 概述

本系统针对国家卫健委网站设计了一套完整的反反爬虫解决方案，参考了`example.py`中的基础策略，并大幅增强了反检测能力。

## 主要特性

### 1. 多层反检测策略
- **WebDriver检测规避**: 使用undetected-chromedriver，隐藏自动化特征
- **浏览器指纹伪装**: 随机化视口、屏幕分辨率、时区等
- **请求头轮换**: 多种User-Agent、Referer、Accept-Language轮换
- **人类行为模拟**: 随机鼠标移动、滚动、打字延迟

### 2. 智能延迟系统
- **人类行为延迟**: 模拟真实用户的访问模式
- **指数退避重试**: 遇到错误时智能增加延迟
- **随机抖动**: 避免固定时间间隔被检测

### 3. 代理管理
- **代理池轮换**: 支持HTTP/SOCKS5代理
- **健康检查**: 自动检测代理可用性
- **失败转移**: 代理失效时自动切换

### 4. 会话管理
- **Cookie持久化**: 保持登录状态
- **会话保持**: 模拟长期用户行为
- **验证码处理**: 支持手动和自动验证码识别

## 安装配置

### 1. 安装依赖
```bash
pip install -r requirements_nhc.txt
```

### 2. 配置文件说明

#### 主配置文件: `config/extraction/nhc_new.yaml`

**关键配置项:**

```yaml
# 请求头轮换
request_settings:
  headers:
    User-Agent:
      - "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
      - "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)..."
    Accept-Language:
      - "zh-CN,zh;q=0.9,en;q=0.8"
      - "zh-CN,zh;q=0.9"

# 反检测配置
anti_detection:
  webdriver_stealth:
    enabled: true
    hide_webdriver: true
  browser_fingerprint:
    randomize_viewport: true
    randomize_screen_resolution: true

# 代理配置
proxy_settings:
  enabled: true
  rotation_strategy: "round_robin"
  proxy_pools:
    - type: "http"
      servers:
        - "http://proxy1.example.com:8080"
```

### 3. 代理配置

**修改代理设置:**
1. 编辑`config/extraction/nhc_new.yaml`
2. 在`proxy_settings.proxy_pools`中添加你的代理服务器
3. 设置认证信息（如需要）

```yaml
proxy_pools:
  - type: "http"
    servers:
      - "http://your-proxy.com:8080"
    auth:
      username: "your_username"
      password: "your_password"
```

## 使用方法

### 1. 基础使用

```python
from nhc_enhanced_crawler import EnhancedNHCCrawler

# 创建爬虫实例
crawler = EnhancedNHCCrawler("config/extraction/nhc_new.yaml")

# 爬取单个页面
url = "http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml"
result = crawler.crawl_page(url)

# 关闭爬虫
crawler.close()
```

### 2. 批量爬取

```python
urls = [
    "http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml",
    "http://www.nhc.gov.cn/xcs/yqfkdt/list_gzbd.shtml"
]

crawler = EnhancedNHCCrawler()
results = []

for url in urls:
    result = crawler.crawl_page(url)
    if result:
        results.append(result)

crawler.close()
```

### 3. 自定义配置

```python
# 修改延迟设置
crawler.config['request_settings']['delay']['base'] = 5.0

# 禁用代理
crawler.config['proxy_settings']['enabled'] = False

# 修改重试次数
crawler.config['request_settings']['retry']['max_attempts'] = 10
```

## 反爬虫对策详解

### 1. 基于example.py的改进

**原有策略:**
- 使用Selenium模拟浏览器
- 基础延迟（time.sleep）
- 无头浏览器模式
- 显式等待

**增强策略:**
- undetected-chromedriver避免WebDriver检测
- 智能延迟算法
- 浏览器指纹随机化
- 多维度反检测

### 2. 具体反检测技术

#### WebDriver检测规避
```javascript
// 隐藏webdriver属性
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
});

// 伪装插件信息
Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
});
```

#### 请求模式伪装
- 随机化请求顺序
- 加载CSS/JS资源
- 模拟真实浏览行为
- 跟随重定向

#### 人类行为模拟
- 随机鼠标移动轨迹
- 自然滚动模式
- 打字速度模拟
- 页面停留时间

## 监控和调试

### 1. 日志系统
```python
import logging

# 查看详细日志
logging.getLogger('nhc_crawler').setLevel(logging.DEBUG)
```

### 2. 成功率监控
```python
# 检查爬取统计
print(f"成功率: {crawler.success_count / crawler.request_count:.2%}")
```

### 3. 错误处理
- 自动重试机制
- 代理切换
- 降级策略
- 错误日志记录

## 最佳实践

### 1. 访问频率控制
- 建议每分钟不超过10个请求
- 使用随机延迟避免规律性
- 根据网站响应调整频率

### 2. 代理使用
- 使用高质量住宅代理
- 定期轮换代理IP
- 监控代理健康状态

### 3. 数据质量保证
- 验证提取的数据完整性
- 实施数据清洗规则
- 建立质量评估机制

### 4. 合规性建议
- 遵守robots.txt规则
- 控制访问频率
- 尊重网站服务条款
- 避免对服务器造成过大负载

## 故障排除

### 1. 常见问题

**问题**: WebDriver检测失败
**解决**: 更新undetected-chromedriver版本

**问题**: 代理连接失败
**解决**: 检查代理配置和网络连接

**问题**: 元素定位失败
**解决**: 更新XPath选择器

### 2. 调试技巧
- 启用详细日志
- 使用非无头模式观察
- 检查网络请求
- 分析页面结构变化

## 注意事项

1. **合法合规**: 确保爬取行为符合法律法规
2. **频率控制**: 避免对目标网站造成过大压力
3. **数据使用**: 合理使用爬取的数据
4. **定期维护**: 及时更新配置以应对网站变化
5. **监控告警**: 建立监控机制及时发现问题

## 技术支持

如遇到问题，请检查：
1. 配置文件格式是否正确
2. 依赖包是否完整安装
3. 网络连接是否正常
4. 目标网站是否有变化
