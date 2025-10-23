#!/bin/bash

# =====================================
# 系统健康检查脚本（适配 deployment/docker/docker-compose.yml）
# =====================================

set -e

# 项目目录（默认取脚本上级目录的上级目录）
PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
COMPOSE_DIR="$PROJECT_DIR/deployment/docker"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"

# 选择 docker compose 命令（兼容 docker compose 与 docker-compose）
if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    echo "docker compose/ docker-compose 未安装"
    exit 1
fi

# 颜色与标识
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'
CHECK_MARK="${GREEN}✓${NC}"
CROSS_MARK="${RED}✗${NC}"
WARNING_MARK="${YELLOW}⚠${NC}"

# 统计
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
WARNING_CHECKS=0

# 加载 .env（如存在）
if [[ -f "$COMPOSE_DIR/.env" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$COMPOSE_DIR/.env"
    set +a
fi

check_item() {
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    local name=$1
    local command=$2
    if eval "$command" >/dev/null 2>&1; then
        echo -e "$CHECK_MARK $name"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "$CROSS_MARK $name"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        return 1
    fi
}

check_warning() {
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    local name=$1
    local current=$2
    local threshold=$3
    if [[ $current -lt $threshold ]]; then
        echo -e "$CHECK_MARK $name: ${current}% (阈值: ${threshold}%)"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "$WARNING_MARK $name: ${current}% (超过阈值: ${threshold}%)"
        WARNING_CHECKS=$((WARNING_CHECKS + 1))
        return 1
    fi
}

echo "=========================================="
echo "系统健康检查"
echo "项目目录: $PROJECT_DIR"
echo "Compose 文件: $COMPOSE_FILE"
echo "检查时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

echo -e "${BLUE}[Docker服务检查]${NC}"
check_item "Docker 服务可用" "docker info"
check_item "Compose 可用 ($COMPOSE)" "$COMPOSE version"
echo ""

echo -e "${BLUE}[容器状态检查]${NC}"
# 当前 compose 中定义的核心容器名（container_name）
containers=(
  "redis:crawler_redis"
  "mongodb:crawler_mongodb"
  "minio:crawler_minio"
  "redis-commander:crawler_redis_commander"
  "mongo-express:crawler_mongo_express"
  "selenium-hub:crawler_selenium_hub"
)
for container_info in "${containers[@]}"; do
    IFS=':' read -r service container <<<"$container_info"
    check_item "$service 容器运行" "docker ps --format '{{.Names}}' | grep -q '^${container}$'"
done
echo ""

echo -e "${BLUE}[服务健康检查]${NC}"
# Redis
if [[ -n "$REDIS_PASSWORD" ]]; then
    check_item "Redis PING" "docker exec crawler_redis redis-cli -a '$REDIS_PASSWORD' ping | grep -q PONG"
else
    check_item "Redis PING" "docker exec crawler_redis redis-cli ping | grep -q PONG"
fi
# MongoDB
MONGO_USER="${MONGODB_ROOT_USERNAME:-admin}"
MONGO_PASS="${MONGODB_ROOT_PASSWORD:-password}"
check_item "MongoDB PING" "docker exec crawler_mongodb mongosh --username '$MONGO_USER' --password '$MONGO_PASS' --authenticationDatabase admin --eval 'db.runCommand("ping")' --quiet | grep -q ok"
# MinIO
check_item "MinIO live" "curl -fsS http://localhost:9000/minio/health/live"
echo ""

echo -e "${BLUE}[系统资源检查]${NC}"
cpu_usage=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print int(100 - $1)}')
check_warning "CPU使用率" "$cpu_usage" 80
memory_usage=$(free | awk '/Mem:/ {printf "%d", ($3/$2)*100}')
check_warning "内存使用率" "$memory_usage" 85
disk_usage=$(df / | tail -1 | awk '{print int($5)}')
check_warning "磁盘使用率" "$disk_usage" 90
io_wait=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* wa.*/\1/" | awk '{print int($1)}')
check_warning "IO等待" "$io_wait" 30
echo ""

echo -e "${BLUE}[网络检查]${NC}"
check_item "外网连通" "ping -c 1 8.8.8.8"
check_item "DNS解析" "getent hosts google.com"
echo ""

echo -e "${BLUE}[端口监听检查]${NC}"
ports=(6379 27017 9000 9001 8081 8082 4444)
for port in "${ports[@]}"; do
    if command -v ss >/dev/null 2>&1; then
        check_item "端口 $port 监听" "ss -tuln | grep -q :$port"
    else
        check_item "端口 $port 监听" "netstat -tuln | grep -q :$port"
    fi
done
echo ""

echo -e "${BLUE}[Docker日志检查]${NC}"
services_to_check=(redis mongodb)
for svc in "${services_to_check[@]}"; do
    error_lines=$($COMPOSE -f "$COMPOSE_FILE" logs --tail=200 "$svc" 2>/dev/null | grep -Ei "error|exception|fatal" | wc -l)
    if [[ $error_lines -gt 0 ]]; then
        echo -e "$WARNING_MARK $svc: 发现 $error_lines 条错误"
    else
        echo -e "$CHECK_MARK $svc: 无明显错误"
    fi
done
echo ""

echo -e "${BLUE}[磁盘/内存详情]${NC}"
df -h | grep -E "Filesystem|/dev/"
echo ""
free -h
echo ""

echo -e "${BLUE}[容器资源使用]${NC}"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"
echo ""

echo "=========================================="
echo -e "${BLUE}健康检查总结${NC}"
echo "=========================================="
echo "总检查项: $TOTAL_CHECKS"
echo -e "${GREEN}通过: $PASSED_CHECKS${NC}"
echo -e "${RED}失败: $FAILED_CHECKS${NC}"
echo -e "${YELLOW}警告: $WARNING_CHECKS${NC}"
if [[ $TOTAL_CHECKS -gt 0 ]]; then
    health_score=$(( (PASSED_CHECKS * 100) / TOTAL_CHECKS ))
    echo ""
    echo -e "健康分数: ${BLUE}${health_score}%${NC}"
fi
echo "=========================================="

if [[ $FAILED_CHECKS -gt 0 ]]; then
    exit 1
else
    exit 0
fi

















