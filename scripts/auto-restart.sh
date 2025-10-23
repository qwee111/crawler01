#!/bin/bash

# =====================================
# 服务自动重启脚本（适配 deployment/docker/docker-compose.yml）
# =====================================

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
COMPOSE_DIR="$PROJECT_DIR/deployment/docker"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
LOG_FILE="/var/log/crawler-auto-restart.log"
MAX_RESTART_ATTEMPTS=3

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    echo "docker compose/ docker-compose 未安装" | tee -a "$LOG_FILE"
    exit 1
fi

# 加载 .env（如存在）
if [[ -f "$COMPOSE_DIR/.env" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$COMPOSE_DIR/.env"
    set +a
fi

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_and_restart() {
    local service_name=$1
    local health_check_cmd=$2
    local restart_count_file="/tmp/crawler_restart_${service_name}.count"

    if eval "$health_check_cmd" >/dev/null 2>&1; then
        rm -f "$restart_count_file"
        return 0
    fi

    log "ERROR: $service_name 健康检查失败"

    local restart_count=0
    [[ -f "$restart_count_file" ]] && restart_count=$(cat "$restart_count_file")

    if [[ $restart_count -ge $MAX_RESTART_ATTEMPTS ]]; then
        log "CRITICAL: $service_name 已重启${restart_count}次，仍然失败，需要人工介入"
        send_alert "$service_name 服务持续异常，已尝试重启${restart_count}次"
        return 1
    fi

    log "INFO: 尝试重启 $service_name (第$((restart_count + 1))次)"
    if $COMPOSE -f "$COMPOSE_FILE" restart "$service_name"; then
        log "INFO: $service_name 重启命令执行成功"
        sleep 30
        if eval "$health_check_cmd" >/dev/null 2>&1; then
            log "SUCCESS: $service_name 重启后恢复正常"
            rm -f "$restart_count_file"
            send_notification "$service_name 服务已自动重启并恢复正常"
            return 0
        else
            restart_count=$((restart_count + 1))
            echo "$restart_count" > "$restart_count_file"
            log "ERROR: $service_name 重启后仍然异常"
            return 1
        fi
    else
        log "ERROR: $service_name 重启命令执行失败"
        return 1
    fi
}

send_alert() {
    local message=$1
    log "ALERT: $message"
    # 预留：邮件/Webhook 通知
}

send_notification() {
    local message=$1
    log "NOTIFICATION: $message"
}

check_system_resources() {
    local disk_usage=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')
    [[ $disk_usage -gt 90 ]] && send_alert "磁盘空间不足：当前使用率${disk_usage}%"

    local memory_usage=$(free | awk '/Mem:/ {printf "%d", ($3/$2)*100}')
    [[ $memory_usage -gt 90 ]] && send_alert "内存使用率过高：当前${memory_usage}%"

    local cpu_load=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | sed 's/,//')
    local cpu_cores=$(nproc)
    local cpu_load_int=$(printf '%.0f' "$cpu_load")
    [[ $cpu_load_int -gt $((cpu_cores * 2)) ]] && send_alert "CPU负载过高：当前${cpu_load}，CPU核心数${cpu_cores}"
}

main() {
    log "========== 开始健康检查 =========="

    # Redis 健康检查
    if [[ -n "$REDIS_PASSWORD" ]]; then
        check_and_restart "redis" "docker exec $(basename $(docker ps --filter name=crawler_redis --format '{{.Names}}')) redis-cli -a '$REDIS_PASSWORD' ping | grep -q PONG"
    else
        check_and_restart "redis" "docker exec $(basename $(docker ps --filter name=crawler_redis --format '{{.Names}}')) redis-cli ping | grep -q PONG"
    fi

    # MongoDB 健康检查
    local mu="${MONGODB_ROOT_USERNAME:-admin}"
    local mp="${MONGODB_ROOT_PASSWORD:-password}"
    check_and_restart "mongodb" "docker exec $(basename $(docker ps --filter name=crawler_mongodb --format '{{.Names}}')) mongosh --username '$mu' --password '$mp' --authenticationDatabase admin --eval 'db.runCommand(\"ping\")' --quiet | grep -q ok"

    check_system_resources
    log "========== 健康检查完成 =========="
}

main
















