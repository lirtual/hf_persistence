#!/bin/bash

set -euo pipefail  # 严格错误处理

# 默认配置文件路径
DEFAULT_CONFIG_FILE="${CONFIG_FILE:-./persistence.conf}"

# 日志函数
log() {
    local level="$1"
    shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*" | tee -a "${LOG_FILE:-/tmp/persistence.log}"
}

log_info() { log "INFO" "$@"; }
log_warn() { log "WARN" "$@"; }
log_error() { log "ERROR" "$@"; }

# 加载配置文件
load_configuration() {
    local config_file="${1:-$DEFAULT_CONFIG_FILE}"

    if [[ ! -f "$config_file" ]]; then
        log_warn "配置文件不存在: $config_file，使用默认配置"
        return 0
    fi

    log_info "加载配置文件: $config_file"

    # 读取 Shell 变量格式配置文件
    source "$config_file"
}

# 设置默认配置
set_default_configuration() {
    # 核心配置
    export HF_TOKEN="${HF_TOKEN:-}"
    export DATASET_ID="${DATASET_ID:-}"
    export ARCHIVE_PATHS="${ARCHIVE_PATHS:-/home/user/sync,/home/user/config}"
    export RESTORE_PATH="${RESTORE_PATH:-./}"

    # 同步配置
    export SYNC_INTERVAL="${SYNC_INTERVAL:-7200}"  # 2小时
    export MAX_ARCHIVES="${MAX_ARCHIVES:-5}"
    export COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-6}"

    # 文件配置
    export ARCHIVE_PREFIX="${ARCHIVE_PREFIX:-resilio_backup}"
    export ARCHIVE_EXTENSION="${ARCHIVE_EXTENSION:-tar.gz}"
    export EXCLUDE_PATTERNS="${EXCLUDE_PATTERNS:-*.log,*.tmp,__pycache__,.git}"

    # 应用配置
    export APP_COMMAND="${APP_COMMAND:-python main.py}"
    export ENABLE_AUTO_RESTORE="${ENABLE_AUTO_RESTORE:-true}"
    export ENABLE_AUTO_SYNC="${ENABLE_AUTO_SYNC:-true}"

    # 日志配置
    export LOG_FILE="${LOG_FILE:-/tmp/persistence.log}"
    export LOG_LEVEL="${LOG_LEVEL:-INFO}"
}

# 验证必需的环境变量
validate_configuration() {
    local errors=0

    if [[ -z "$HF_TOKEN" ]]; then
        log_error "缺少必需的环境变量: HF_TOKEN"
        ((errors++))
    fi

    if [[ -z "$DATASET_ID" ]]; then
        log_error "缺少必需的环境变量: DATASET_ID"
        ((errors++))
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "配置验证失败，将在无持久化模式下启动应用"
        return 1
    fi

    # 设置 Hugging Face 认证
    export HUGGING_FACE_HUB_TOKEN="$HF_TOKEN"

    log_info "配置验证成功"
    return 0
}

# 创建归档文件
create_archive() {
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local archive_file="${ARCHIVE_PREFIX}_${timestamp}.${ARCHIVE_EXTENSION}"
    local temp_archive="/tmp/${archive_file}"

    log_info "开始创建归档: $archive_file" >&2

    # 构建排除参数
    local exclude_args=""
    if [[ -n "$EXCLUDE_PATTERNS" ]]; then
        IFS=',' read -ra patterns <<< "$EXCLUDE_PATTERNS"
        for pattern in "${patterns[@]}"; do
            exclude_args+=" --exclude='${pattern// /}'"
        done
    fi

    # 创建归档
    local archive_paths_array
    IFS=',' read -ra archive_paths_array <<< "$ARCHIVE_PATHS"

    local tar_cmd="tar -czf '$temp_archive' $exclude_args"
    local valid_paths=()

    for path in "${archive_paths_array[@]}"; do
        path="${path// /}"  # 移除空格
        if [[ -e "$path" ]]; then
            # 检查目录是否为空
            if [[ -d "$path" ]] && [[ -z "$(ls -A "$path" 2>/dev/null)" ]]; then
                log_warn "目录为空，创建占位文件: $path" >&2
                echo "# Placeholder file for persistence backup" > "$path/.persistence_placeholder"
            fi
            tar_cmd+=" '$path'"
            valid_paths+=("$path")
        else
            log_warn "归档路径不存在，跳过: $path" >&2
        fi
    done

    # 检查是否有有效路径
    if [[ ${#valid_paths[@]} -eq 0 ]]; then
        log_error "没有找到任何有效的归档路径" >&2
        return 1
    fi

    log_info "执行归档命令: $tar_cmd" >&2
    if eval "$tar_cmd" >&2; then
        log_info "归档文件创建成功: $temp_archive" >&2
        echo "$temp_archive"
        return 0
    else
        log_error "归档文件创建失败" >&2
        return 1
    fi
}

# 内嵌 Python 上传处理器
run_upload_handler() {
    local archive_file="$1"
    local filename="$2"
    local dataset_id="$3"
    local backup_prefix="$4"
    local backup_extension="$5"
    local max_backups="$6"
    local token="$7"

    python3 - <<EOF
import sys
import os
import traceback
from huggingface_hub import HfApi

def upload_archive(api, local_path, remote_path, repo_id):
    """上传归档文件到 Hugging Face Dataset"""
    try:
        api.upload_file(
            path_or_fileobj=local_path,
            path_in_repo=remote_path,
            repo_id=repo_id,
            repo_type='dataset'
        )
        print(f'✓ 归档上传成功: {remote_path}')
        return True
    except Exception as e:
        print(f'✗ 归档上传失败: {str(e)}')
        traceback.print_exc()
        return False

def manage_archives(api, repo_id, archive_prefix, archive_extension, max_files):
    """管理归档文件数量，删除超出限制的旧归档"""
    try:
        files = api.list_repo_files(repo_id=repo_id, repo_type='dataset')
        archive_files = [f for f in files if f.startswith(archive_prefix) and f.endswith(f'.{archive_extension}')]
        archive_files.sort()

        if len(archive_files) >= max_files:
            files_to_delete = archive_files[:(len(archive_files) - max_files + 1)]
            for file_to_delete in files_to_delete:
                try:
                    api.delete_file(path_in_repo=file_to_delete, repo_id=repo_id, repo_type='dataset')
                    print(f'✓ 删除旧归档: {file_to_delete}')
                except Exception as e:
                    print(f'✗ 删除失败 {file_to_delete}: {str(e)}')

        print(f'✓ 归档管理完成，当前保留 {min(len(archive_files), max_files)} 个归档')
        return True
    except Exception as e:
        print(f'✗ 归档管理失败: {str(e)}')
        return False

# 设置认证
os.environ['HUGGING_FACE_HUB_TOKEN'] = '$token'

try:
    api = HfApi()
    
    # 上传文件
    if upload_archive(api, '$archive_file', '$filename', '$dataset_id'):
        # 管理归档数量
        if manage_archives(api, '$dataset_id', '$backup_prefix', '$backup_extension', $max_backups):
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        sys.exit(1)

except Exception as e:
    print(f'✗ 上传过程出错: {str(e)}')
    traceback.print_exc()
    sys.exit(1)
EOF
}

# 上传归档到 Hugging Face
upload_archive() {
    local archive_file="$1"
    local filename=$(basename "$archive_file")

    log_info "开始上传归档: $filename"

    # 调用内嵌的Python处理器
    if run_upload_handler "$archive_file" "$filename" "$DATASET_ID" "$ARCHIVE_PREFIX" "$ARCHIVE_EXTENSION" "$MAX_ARCHIVES" "$HF_TOKEN"; then
        log_info "归档上传完成"
        return 0
    else
        log_error "归档上传失败"
        return 1
    fi
}

# 执行一次归档
perform_archive() {
    log_info "开始执行归档操作"

    local archive_file
    if archive_file=$(create_archive); then
        # 检查是否为测试模式（HF_TOKEN 为 test_token）
        if [[ "$HF_TOKEN" == "test_token" ]]; then
            log_info "测试模式：归档创建成功，跳过上传"
            log_info "归档文件: $archive_file"
            ls -la "$archive_file"
            log_info "测试模式：保留归档文件用于检查"
        else
            if upload_archive "$archive_file"; then
                log_info "归档操作成功完成"
            else
                log_error "归档上传失败"
            fi
            # 清理临时文件
            rm -f "$archive_file"
        fi
    else
        log_error "归档创建失败"
        return 1
    fi
}

# 同步守护进程
sync_daemon() {
    log_info "启动同步守护进程，间隔: ${SYNC_INTERVAL}秒"

    while true; do
        perform_archive

        log_info "下次同步将在 ${SYNC_INTERVAL} 秒后执行"
        sleep "$SYNC_INTERVAL"
    done
}

# 内嵌 Python 归档列表器
run_archive_lister() {
    local dataset_id="$1"
    local backup_prefix="$2"
    local backup_extension="$3"
    local token="$4"

    python3 - <<EOF
import sys
import os
import traceback
from huggingface_hub import HfApi

def list_available_archives(api, repo_id, archive_prefix, archive_extension):
    """列出可用的归档文件"""
    try:
        files = api.list_repo_files(repo_id, repo_type='dataset')
        archive_files = [f for f in files if f.startswith(archive_prefix) and f.endswith(f'.{archive_extension}')]
        archive_files.sort(reverse=True)

        if archive_files:
            print('可用归档列表:')
            for i, archive in enumerate(archive_files, 1):
                print(f'  {i}. {archive}')

            # 返回最新归档文件名
            print(f'LATEST_BACKUP:{archive_files[0]}')
            return True
        else:
            print('未找到任何归档文件')
            return False

    except Exception as e:
        print(f'获取归档列表失败: {str(e)}')
        traceback.print_exc()
        return False

# 设置认证
os.environ['HUGGING_FACE_HUB_TOKEN'] = '$token'

try:
    api = HfApi()

    if list_available_archives(api, '$dataset_id', '$backup_prefix', '$backup_extension'):
        sys.exit(0)
    else:
        sys.exit(1)

except Exception as e:
    print(f'列表获取过程出错: {str(e)}')
    traceback.print_exc()
    sys.exit(1)
EOF
}

# 列出可用归档
list_archives() {
    log_info "获取可用归档列表"

    # 调用内嵌的Python处理器
    run_archive_lister "$DATASET_ID" "$ARCHIVE_PREFIX" "$ARCHIVE_EXTENSION" "$HF_TOKEN"
}

# 内嵌 Python 下载处理器
run_download_handler() {
    local backup_name="$1"
    local dataset_id="$2"
    local restore_path="$3"
    local token="$4"

    python3 - <<EOF
import sys
import os
import traceback
from huggingface_hub import HfApi

def restore_from_archive(api, repo_id, archive_name, restore_path):
    """从 Hugging Face Dataset 恢复归档"""
    try:
        # 下载归档文件
        print(f'正在下载归档: {archive_name}')
        local_path = api.hf_hub_download(
            repo_id=repo_id,
            filename=archive_name,
            repo_type='dataset',
            local_dir='/tmp'
        )

        # 解压归档
        print(f'正在解压归档到: {restore_path}')
        extract_cmd = f'tar -xzf {local_path} -C {restore_path}'
        result = os.system(extract_cmd)

        if result == 0:
            print(f'✓ 归档恢复成功: {archive_name}')

            # 清理临时文件
            os.remove(local_path)
            return True
        else:
            print(f'✗ 归档解压失败')
            return False

    except Exception as e:
        print(f'✗ 归档恢复失败: {str(e)}')
        traceback.print_exc()
        return False

# 设置认证
os.environ['HUGGING_FACE_HUB_TOKEN'] = '$token'

try:
    api = HfApi()

    if restore_from_archive(api, '$dataset_id', '$backup_name', '$restore_path'):
        sys.exit(0)
    else:
        sys.exit(1)

except Exception as e:
    print(f'恢复过程出错: {str(e)}')
    traceback.print_exc()
    sys.exit(1)
EOF
}

# 恢复指定归档
restore_archive() {
    local archive_name="${1:-latest}"

    log_info "开始恢复归档: $archive_name"

    # 如果是 latest，先获取最新归档名称
    if [[ "$archive_name" == "latest" ]]; then
        local archive_list_output
        if archive_list_output=$(list_archives 2>&1); then
            archive_name=$(echo "$archive_list_output" | grep "LATEST_BACKUP:" | cut -d: -f2)
            if [[ -z "$archive_name" ]]; then
                log_info "未找到任何归档文件，这可能是首次运行"
                return 1
            fi
        else
            # 检查输出中是否包含"未找到任何归档文件"
            if echo "$archive_list_output" | grep -q "未找到任何归档文件"; then
                log_info "未找到任何归档文件，这可能是首次运行"
                return 1
            else
                log_error "获取归档列表失败: $archive_list_output"
                return 1
            fi
        fi
    fi

    log_info "恢复归档文件: $archive_name"

    # 调用内嵌的Python处理器
    if run_download_handler "$archive_name" "$DATASET_ID" "$RESTORE_PATH" "$HF_TOKEN"; then
        log_info "归档恢复完成"
        return 0
    else
        log_error "归档恢复失败"
        return 1
    fi
}



# 主程序入口
main() {
    local command="start"
    local config_file="$DEFAULT_CONFIG_FILE"
    local verbose=false
    local no_restore=false
    local no_sync=false
    local restore_target=""

    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--config)
                config_file="$2"
                shift 2
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            --no-restore)
                no_restore=true
                shift
                ;;
            --no-sync)
                no_sync=true
                shift
                ;;
            archive|restore|list|daemon|start)
                command="$1"
                shift
                ;;
            *)
                if [[ "$command" == "restore" && -z "$restore_target" ]]; then
                    # restore 命令的归档名称参数
                    restore_target="$1"
                    shift
                else
                    log_error "未知参数: $1"
                    exit 1
                fi
                ;;
        esac
    done

    # 加载配置
    load_configuration "$config_file"
    set_default_configuration

    # 设置日志级别
    if [[ "$verbose" == "true" ]]; then
        export LOG_LEVEL="DEBUG"
    fi

    log_info "=== 数据持久化单文件脚本启动 ==="
    log_info "版本: 4.0"
    log_info "命令: $command"
    log_info "配置文件: $config_file"

    # 根据命令执行相应操作
    case $command in
        archive)
            if validate_configuration; then
                perform_archive
            else
                exit 1
            fi
            ;;
        restore)
            if validate_configuration; then
                restore_archive "${restore_target:-latest}"
            else
                exit 1
            fi
            ;;
        list)
            if validate_configuration; then
                list_archives
            else
                exit 1
            fi
            ;;
        daemon)
            if validate_configuration; then
                sync_daemon
            else
                exit 1
            fi
            ;;
        start)
            # 启动应用模式
            if validate_configuration; then
                # 自动恢复
                if [[ "$ENABLE_AUTO_RESTORE" == "true" && "$no_restore" == "false" ]]; then
                    log_info "执行自动恢复"
                    restore_archive "latest" || log_warn "自动恢复失败，继续启动应用"
                fi

                # 启动同步守护进程
                if [[ "$ENABLE_AUTO_SYNC" == "true" && "$no_sync" == "false" ]]; then
                    log_info "启动同步守护进程"
                    sync_daemon &
                    sync_pid=$!
                    log_info "同步守护进程PID: $sync_pid"
                fi
            else
                log_warn "配置验证失败，在无持久化模式下启动应用"
            fi

            # 启动主应用
            log_info "启动主应用: $APP_COMMAND"
            exec $APP_COMMAND
            ;;
        *)
            log_error "未知命令: $command"
            exit 1
            ;;
    esac
}

# 脚本入口点
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
