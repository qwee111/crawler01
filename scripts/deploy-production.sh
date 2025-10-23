#!/bin/bash

# =====================================
# 生产环境一键部署（适配 deployment/docker/docker-compose.yml）
# =====================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "此脚本需要root权限运行，使用: sudo $0"
        exit 1
    fi
}

check_system() {
    log_info "检查系统环境..."
    . /etc/os-release || true
    log_info "操作系统: ${NAME:-unknown} ${VERSION_ID:-}"
    local total_mem=$(free -g | awk '/^Mem:/{print $2}')
    [[ -n "$total_mem" && $total_mem -lt 7 ]] && log_warning "系统内存不足8GB，可能影响性能"
    local total_disk=$(df -BG / | awk 'NR==2 {print $4}' | sed 's/G//')
    [[ -n "$total_disk" && $total_disk -lt 50 ]] && log_warning "磁盘空间不足50GB，可能影响运行"
}

update_system() {
    log_info "更新系统软件包..."
    apt update -y && apt upgrade -y
    log_success "系统更新完成"
}

install_basic_tools() {
    log_info "安装基础工具..."
    apt install -y curl wget git vim htop net-tools ca-certificates gnupg lsb-release
    log_success "基础工具安装完成"
}

install_docker() {
    if command -v docker &>/dev/null; then
        log_warning "Docker已安装，跳过"
        docker --version
        return
    fi
    log_info "安装Docker..."
    mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list
    apt update -y
    apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    systemctl enable --now docker
    log_success "Docker安装完成"
}

install_docker_compose_legacy() {
    if command -v docker-compose &>/dev/null; then
        log_warning "docker-compose 已安装，跳过"
        return
    fi
    log_info "安装兼容的 docker-compose（二进制）..."
    local v="v2.24.0"
    curl -L "https://github.com/docker/compose/releases/download/${v}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
}

configure_firewall() {
    log_info "配置防火墙..."
    apt install -y ufw
    ufw --force reset
    ufw default deny incoming
    ufw default allow outgoing
    ufw allow 22/tcp comment 'SSH'
    # 管理工具端口（按需开放，生产环境建议限制来源）
    ufw allow 8081/tcp comment 'Redis Commander'
    ufw allow 8082/tcp comment 'Mongo Express'
    ufw allow 9001/tcp comment 'MinIO Console'
    ufw allow 4444/tcp comment 'Selenium Hub'
    ufw --force enable
    log_success "防火墙配置完成"
}

create_directories() {
    log_info "创建工作目录..."
    PROJECT_BASE="/opt/crawler"
    mkdir -p "$PROJECT_BASE" && cd "$PROJECT_BASE"
    mkdir -p logs storage/{files,images} backups
    chmod -R 755 storage logs backups
    log_success "目录创建完成: $PROJECT_BASE"
}

clone_project() {
    log_info "克隆项目代码..."
    read -p "请输入Git仓库URL: " REPO_URL
    if [[ -z "$REPO_URL" ]]; then
        log_warning "未提供Git仓库URL，跳过克隆"
        return
    fi
    cd /opt/crawler
    [[ -d crawler01 ]] && { read -p "目录已存在，删除并重克隆？(y/n): " r; [[ "$r" == "y" ]] && rm -rf crawler01; }
    git clone "$REPO_URL" crawler01
    cd crawler01
    log_success "项目代码克隆完成"
}

generate_env_config() {
    log_info "生成环境配置文件（deployment/docker/.env）..."
    cd /opt/crawler/crawler01
    mkdir -p deployment/docker
    gen_pwd() { openssl rand -base64 32 | tr -d '=+/' | cut -c1-25; }
    MONGODB_PASSWORD=$(gen_pwd)
    REDIS_PASSWORD=$(gen_pwd)
    MINIO_PASSWORD=$(gen_pwd)
    cat > deployment/docker/.env << EOF
ENVIRONMENT=production
MONGODB_ROOT_USERNAME=admin
MONGODB_ROOT_PASSWORD=$MONGODB_PASSWORD
MONGODB_DATABASE=crawler_db
REDIS_PASSWORD=$REDIS_PASSWORD
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=$MINIO_PASSWORD
LOG_LEVEL=INFO
EOF
    log_success "已生成: deployment/docker/.env"
}

start_services() {
    log_info "启动服务（使用 deployment/docker/docker-compose.yml）..."
    cd /opt/crawler/crawler01
    local compose_file="deployment/docker/docker-compose.yml"
    # 尝试 docker compose，否则使用 docker-compose
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$compose_file" build crawler
        docker compose -f "$compose_file" --profile selenium --profile tools up -d
    else
        docker-compose -f "$compose_file" build crawler
        docker-compose -f "$compose_file" up -d
    fi
    sleep 10
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$compose_file" ps
    else
        docker-compose -f "$compose_file" ps
    fi
    log_success "服务启动完成"
}

configure_backup() {
    log_info "配置自动备份（每日02:00）..."
    chmod +x /opt/crawler/crawler01/scripts/backup.sh || true
    (crontab -l 2>/dev/null; echo "0 2 * * * BACKUP_DIR=/opt/crawler/backups PROJECT_DIR=/opt/crawler/crawler01 bash /opt/crawler/crawler01/scripts/backup.sh >> /var/log/crawler-backup.log 2>&1") | crontab -
    log_success "自动备份已配置"
}

show_deployment_info() {
    local ip=$(curl -s ifconfig.me || echo "<server-ip>")
    log_success "=========================================="
    log_success "部署完成！"
    log_success "=========================================="
    echo ""
    log_info "管理工具（如已启用 profiles tools/selenium）:"
    echo "  Redis Commander: http://$ip:8081"
    echo "  Mongo Express:   http://$ip:8082"
    echo "  MinIO Console:   http://$ip:9001"
    echo "  Selenium Hub:    http://$ip:4444"
    echo ""
    log_info "路径信息:"
    echo "  项目: /opt/crawler/crawler01"
    echo "  Compose: /opt/crawler/crawler01/deployment/docker/docker-compose.yml"
    echo "  环境: /opt/crawler/crawler01/deployment/docker/.env"
    echo "  备份: /opt/crawler/backups"
    echo ""
    log_info "常用命令:"
    echo "  查看状态:   cd /opt/crawler/crawler01 && docker compose -f deployment/docker/docker-compose.yml ps"
    echo "  查看日志:   cd /opt/crawler/crawler01 && docker compose -f deployment/docker/docker-compose.yml logs -f crawler"
    echo "  重启服务:   cd /opt/crawler/crawler01 && docker compose -f deployment/docker/docker-compose.yml restart crawler"
    echo "  停止所有:   cd /opt/crawler/crawler01 && docker compose -f deployment/docker/docker-compose.yml down"
    log_success "=========================================="
}

main() {
    echo "=========================================="
    echo "生产环境一键部署"
    echo "=========================================="
    check_root
    check_system
    read -p "是否继续部署？ (y/n): " go
    [[ "$go" != "y" ]] && { log_info "部署已取消"; exit 0; }
    update_system
    install_basic_tools
    install_docker
    install_docker_compose_legacy
    configure_firewall
    create_directories
    clone_project
    generate_env_config
    start_services
    configure_backup
    show_deployment_info
}

main
















