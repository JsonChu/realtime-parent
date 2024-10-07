#!/bin/bash

# 设置配置文件目录
config_dir="/data/telegraf/config/vcenter"

# 设置日志文件目录
log_dir="/var/log/telegraf"

# 设置 Telegraf 可执行文件路径
telegraf_bin="/data/telegraf/usr/bin/telegraf"

# 检查参数
if [ $# -ne 1 ]; then
    echo "Usage: $0 [start|stop|status]"
    exit 1
fi

# 创建日志文件目录（如果不存在）
mkdir -p "$log_dir"

# 启动 Telegraf
start_telegraf() {
    # 检查目录是否存在
    if [ ! -d "$config_dir" ]; then
        echo "配置文件目录不存在: $config_dir"
        exit 1
    fi

    # 检查 Telegraf 可执行文件是否存在
    if [ ! -x "$telegraf_bin" ]; then
        echo "Telegraf 可执行文件不存在: $telegraf_bin"
        exit 1
    fi

    # 迭代处理每个以 "telegraf" 开头并以 ".conf" 结尾的文件
    for config_file in "$config_dir"/telegraf*.conf; do
        if [ -f "$config_file" ]; then
            # 根据配置文件名生成日志文件名
            log_file="${log_dir}/$(basename "$config_file" .conf).log"
            echo "启动配置文件: $config_file，日志文件: $log_file"
            # 启动 Telegraf，并将日志输出到对应的日志文件
            "$telegraf_bin" --watch-config notify --config "$config_file" > "$log_file" 2>&1 &
        else
            echo "找不到配置文件: $config_file"
        fi
    done
}

# 停止 Telegraf
stop_telegraf() {
    # 查找正在运行的 Telegraf 进程并杀死对应的进程
    echo "查找并停止正在运行的 Telegraf 进程"
    ps -ef | grep "$telegraf_bin" | grep -E "telegraf.*\.conf" | grep -v grep | awk '{print $2}' | xargs -r kill -9
}

# 检查 Telegraf 进程状态
status_telegraf() {
    echo "检查 Telegraf 进程状态"
    ps -ef | grep "$telegraf_bin" | grep -E "telegraf.*\.conf" | grep -v grep
}

# 根据参数调用对应的函数
case "$1" in
    start)
        start_telegraf
        ;;
    stop)
        stop_telegraf
        ;;
    status)
        status_telegraf
        ;;
    *)
        echo "错误的参数: $1"
        echo "Usage: $0 [start|stop|status]"
        exit 1
        ;;
esac

