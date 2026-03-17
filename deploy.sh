#!/bin/bash

# ==============================================================================
#  Crypto Monitor - 自动化部署脚本 (已修复 Docker 挂载 Bug)
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

# --- 步骤 1.5: 环境准备 (修复 Docker 自动创建目录的问题) ---
echo -e "${YELLOW}Step 1.5/3: 正在准备持久化状态文件...${NC}"
# 1. 如果发现是一个文件夹，删除它
if [ -d "cooldown_status.json" ]; then
    echo -e "${RED}⚠️ 发现被 Docker 错误创建的 cooldown_status.json 文件夹，正在删除...${NC}"
    rm -rf cooldown_status.json
fi

# 2. 如果文件不存在，主动创建一个合法的空 JSON 文件
if [ ! -f "cooldown_status.json" ]; then
    echo -e "${GREEN}✨ 初始化空的 cooldown_status.json 文件...${NC}"
    echo "{}" > cooldown_status.json
fi
echo -e "${GREEN}状态文件准备完毕。${NC}\n"

# --- 步骤 2: 重构并重启 Docker 服务 ---
echo -e "${YELLOW}Step 2/3: 正在重新构建 Docker 镜像并重启服务...${NC}"
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