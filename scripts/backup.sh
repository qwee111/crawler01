#!/bin/bash

# =====================================
# 数据备份脚本（适配 deployment/docker/docker-compose.yml）
# =====================================

set -e

BACKUP_DIR="${BACKUP_DIR:-/opt/crawler/backups}"
PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
COMPOSE_DIR="$PROJECT_DIR/deployment/docker"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=${RETENTION_DAYS:-7}

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    echo "docker compose/ docker-compose 未安装"
    exit 1
fi

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${GREEN}[INFO]${NC} $1"; }
log_warning() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${RED}[ERROR]${NC} $1"; }

create_backup_dirs() {
    log_info "创建备份目录..."
    mkdir -p "$BACKUP_DIR/mongodb" "$BACKUP_DIR/redis" "$BACKUP_DIR/files"
}

load_env() {
    if [[ -f "$COMPOSE_DIR/.env" ]]; then
        set -a
        # shellcheck disable=SC1090
        source "$COMPOSE_DIR/.env"
        set +a
        log_info "环境变量加载成功: $COMPOSE_DIR/.env"
    else
        log_warning "未找到环境变量文件: $COMPOSE_DIR/.env，使用默认值"
    fi
}

backup_mongodb() {
    log_info "备份 MongoDB..."
    if $COMPOSE -f "$COMPOSE_FILE" ps | grep -q mongodb; then
        local mu="${MONGODB_ROOT_USERNAME:-admin}"
        local mp="${MONGODB_ROOT_PASSWORD:-password}"
        $COMPOSE -f "$COMPOSE_FILE" exec -T mongodb \
            mongodump \
            --username="$mu" \
            --password="$mp" \
            --authenticationDatabase=admin \
            --archive \
            | gzip > "$BACKUP_DIR/mongodb/backup_${DATE}.archive.gz"
        local size=$(du -h "$BACKUP_DIR/mongodb/backup_${DATE}.archive.gz" | cut -f1)
        log_info "MongoDB备份完成，大小: $size"
    else
        log_warning "MongoDB 容器未运行，跳过备份"
    fi
}

backup_redis() {
    log_info "备份 Redis..."
    if $COMPOSE -f "$COMPOSE_FILE" ps | grep -q redis; then
        local redis_cli_cmd="redis-cli"
        local pass_opt=""
        [[ -n "$REDIS_PASSWORD" ]] && pass_opt="-a $REDIS_PASSWORD"
        $COMPOSE -f "$COMPOSE_FILE" exec -T redis $redis_cli_cmd $pass_opt save || true

        # 使用容器ID避免依赖 container_name
        local redis_cid=$($COMPOSE -f "$COMPOSE_FILE" ps -q redis)
        if [[ -n "$redis_cid" ]]; then
            docker cp "$redis_cid:/data/dump.rdb" "$BACKUP_DIR/redis/dump_${DATE}.rdb" 2>/dev/null || true
        fi

        if [[ -f "$BACKUP_DIR/redis/dump_${DATE}.rdb" ]]; then
            gzip "$BACKUP_DIR/redis/dump_${DATE}.rdb"
            local size=$(du -h "$BACKUP_DIR/redis/dump_${DATE}.rdb.gz" | cut -f1)
            log_info "Redis备份完成，大小: $size"
        else
            log_warning "未找到 Redis RDB 文件"
        fi
    else
        log_warning "Redis 容器未运行，跳过备份"
    fi
}

backup_files() {
    log_info "备份配置与存储文件..."
    tar -czf "$BACKUP_DIR/files/config_${DATE}.tar.gz" \
        -C "$PROJECT_DIR" \
        config/ \
        -C "$COMPOSE_DIR" .env \
        -C "$COMPOSE_DIR" docker-compose.yml \
        2>/dev/null || true

    local storage_size=$(du -sm "$PROJECT_DIR/storage" 2>/dev/null | cut -f1)
    if [[ -n "$storage_size" && "$storage_size" -lt 1000 ]]; then
        tar -czf "$BACKUP_DIR/files/storage_${DATE}.tar.gz" -C "$PROJECT_DIR" storage/ 2>/dev/null || true
        log_info "存储文件备份完成"
    else
        log_warning "存储文件不存在或过大，跳过备份"
    fi
}

cleanup_old_backups() {
    log_info "清理超过 ${RETENTION_DAYS} 天的旧备份..."
    find "$BACKUP_DIR" -type f -mtime +"$RETENTION_DAYS" -delete || true
}

upload_to_cloud() {
    if [[ "$ALIYUN_OSS_ENABLED" == "true" ]]; then
        log_info "上传到阿里云 OSS（未实现，占位）"
    fi
    if [[ "$AWS_S3_ENABLED" == "true" ]]; then
        log_info "上传到 AWS S3（未实现，占位）"
    fi
}

generate_report() {
    log_info "生成备份报告..."
    local report_file="$BACKUP_DIR/backup_report_${DATE}.txt"
    cat > "$report_file" << EOF
=====================================
备份报告
=====================================
备份时间: $(date '+%Y-%m-%d %H:%M:%S')
备份目录: $BACKUP_DIR

MongoDB备份:
$(ls -lh "$BACKUP_DIR/mongodb/backup_${DATE}.archive.gz" 2>/dev/null || echo "未备份")

Redis备份:
$(ls -lh "$BACKUP_DIR/redis/dump_${DATE}.rdb.gz" 2>/dev/null || echo "未备份")

应用文件备份:
$(ls -lh $BACKUP_DIR/files/*_${DATE}.tar.gz 2>/dev/null || echo "未备份")

磁盘使用情况:
$(df -h "$BACKUP_DIR")

备份目录大小:
$(du -sh "$BACKUP_DIR")

备份文件数量:
$(find "$BACKUP_DIR" -type f | wc -l)
=====================================
EOF
    cat "$report_file"
}

send_notification() {
    if [[ "$SMTP_ENABLED" == "true" ]]; then
        log_info "发送备份通知（未实现，占位）"
    fi
}

main() {
    log_info "=========================================="
    log_info "开始执行备份任务"
    log_info "=========================================="
    create_backup_dirs
    load_env
    backup_mongodb
    backup_redis
    backup_files
    cleanup_old_backups
    upload_to_cloud
    generate_report
    send_notification
    log_info "=========================================="
    log_info "备份任务完成"
    log_info "=========================================="
}

main "$@"

















