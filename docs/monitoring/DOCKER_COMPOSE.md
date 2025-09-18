# 一键启动监控栈 (Prometheus + Grafana + Alertmanager)

## 前置条件
- 爬虫进程内已启动 Exporter（或独立进程），默认暴露在 9108 端口
- Windows/Mac 使用 host.docker.internal 访问宿主机
- Linux 可将 exporter 以容器方式运行并加入同一网络

## 启动
```bash
# 进入仓库根目录
cd deployment/monitoring

# 启动服务
docker compose -f docker-compose.monitoring.yml up -d

# 查看状态
docker compose -f docker-compose.monitoring.yml ps
```

## 访问
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- Alertmanager: http://localhost:9093

## 配置
- Prometheus 抓取目标已配置为 host.docker.internal:9108
- 如在 Linux 上运行，请将 exporter 启动为容器并在 prometheus.yml 中指向 `exporter:9108`
- 告警规则可通过 Prometheus `--web.enable-lifecycle` 支持热加载（或容器重启）

## 导入仪表板
- 登录 Grafana → Import → 选择 `deployment/monitoring/grafana-dashboard.json`

## 自定义告警
- 编辑 `deployment/monitoring/alert_rules.yml`
- 在 Prometheus Web UI 执行 `POST /-/reload` 以热加载配置

## 常见问题
- 看不到数据：确认 Exporter 端口与主机名是否正确；Windows/Mac 用 host.docker.internal
- 指标标签 env/instance：在爬虫运行环境设置 `ENV=prod` `INSTANCE=$(hostname)`
