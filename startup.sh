#!/bin/bash

set -e

echo "=== Resilio Sync with Persistence Startup ==="

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "Starting Resilio Sync with Persistence support..."

# 检查是否配置了持久化
if [[ -n "$HF_TOKEN" && -n "$DATASET_ID" ]]; then
    log "Persistence configuration detected"
    log "HF_TOKEN: ${HF_TOKEN:0:10}..."
    log "DATASET_ID: $DATASET_ID"
    
    # 启动持久化守护进程（仅守护进程模式，不启动应用）
    log "Starting persistence daemon in background..."
    /home/user/persistence.sh daemon &
    PERSISTENCE_PID=$!
    log "Persistence daemon started with PID: $PERSISTENCE_PID"
    
    # 尝试恢复最新备份
    log "Attempting to restore latest backup..."
    if /home/user/persistence.sh restore latest 2>&1; then
        log "Backup restored successfully"
    else
        log "No backup found or restore failed, continuing with fresh start"
    fi
else
    log "No persistence configuration found (HF_TOKEN or DATASET_ID missing)"
    log "Running without persistence support"
fi

# 执行主程序
log "Execute the main program..."
# Write the execution code here