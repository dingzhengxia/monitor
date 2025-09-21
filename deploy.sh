#!/bin/bash

# ==============================================================================
#  Crypto Monitor - 自动化部署脚本
# ==============================================================================
#
# 功能:
# 1. 从 Git 拉取最新代码。
# 2. 使用 docker-compose 重新构建并以分离模式启动服务。
# 3. 清理无用的 Docker 镜像。
#
# 使用方法:
# 1. 将此脚本放置在 docker-compose.yml 文件所在的目录。
# 2. 授予执行权限: chmod +x deploy.sh
# 3. 运行脚本: ./deploy.sh
#
# ==============================================================================

# 设置颜色变量，方便输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 如果任何命令执行失败，则立即退出脚本
set -e

# --- 步骤 1: 拉取 Git 最新代码 ---
echo -e "\n${YELLOW}Step 1/3: 正在从 Git 拉取最新代码...${NC}"
git pull
echo -e "${GREEN}Git 拉取完成。${NC}\n"


# --- 步骤 2: 重构并重启 Docker 服务 ---
echo -e "${YELLOW}Step 2/3: 正在重新构建 Docker 镜像并重启服务...${NC}"
# 这个命令会智能地停止旧容器，构建新镜像，然后以后台模式启动新容器。
# --remove-orphans 会移除在 docker-compose.yml 中已不存在的服务的容器。
docker compose up --build -d --remove-orphans
echo -e "${GREEN}服务已成功构建并重启。${NC}\n"


# --- 步骤 3: 清理旧的 Docker 镜像 ---
echo -e "${YELLOW}Step 3/3: 正在清理旧的、未使用的 Docker 镜像...${NC}"
docker image prune -f
echo -e "${GREEN}清理完成。${NC}\n"


# --- 完成 ---
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}✅ 自动化部署流程执行完毕！${NC}"
echo -e "${YELLOW}您可以使用 'docker compose logs -f' 来查看程序运行日志。${NC}"
echo -e "${GREEN}=========================================${NC}"