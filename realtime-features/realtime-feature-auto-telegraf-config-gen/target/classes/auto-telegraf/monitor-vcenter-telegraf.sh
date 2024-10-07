#!/bin/bash

# 设置配置文件目录
config_dir="/data/telegraf/config/vcenter"

# 设置日志文件目录
log_dir="/var/log/telegraf"

# 设置 Telegraf 可执行文件路径
telegraf_bin="/data/telegraf/usr/bin/telegraf"

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

# 创建日志文件目录（如果不存在）
mkdir -p "$log_dir"

# 获取物理内存总量（以MB为单位）
total_mem=$(free -m | awk '/Mem/{print $2}')

# 获取所有以 "telegraf" 开头并以 ".conf" 结尾的配置文件列表
config_files=("$config_dir"/telegraf*.conf)

# 遍历配置文件列表
for config_file in "${config_files[@]}"; do
    if [ -f "$config_file" ]; then
        # 根据配置文件名生成日志文件名
        log_file="${log_dir}/$(basename "$config_file" .conf).log"

        # 解析文件名获取相关信息
        filename=$(basename "$config_file" .conf)

        # 使用IFS变量将-设为分隔符
        IFS='-' read -ra parts <<< "$filename"

        # 提取相关信息
        vcenter="${parts[1]}"
        type="${parts[2]}"
        max_file_num="${parts[-2]}"
        current_file_num="${parts[-1]}"

        # 检查是否有对应的 Telegraf 进程在运行
        if ! ps aux | grep -q "[t]elegraf.*--config $config_file"; then
            cpu_usage="0"
            mem_usage="0"
            echo "monitor_telegraf,vcenter=$vcenter,type=$type,maxFileNum=$max_file_num,currentFileNum=$current_file_num,status=\"starting\",cpu=$cpu_usage%,totalMem=$total_mem,memUsage=0"
            # 启动 Telegraf，并将日志输出到对应的日志文件
             "$telegraf_bin" --watch-config notify --config "$config_file" > "$log_file" 2>&1 &
        else
            # 获取进程的 CPU 使用率和内存占用
            cpu_usage=$(ps aux | grep "[t]elegraf.*--config $config_file" | awk '{print $3}')
            mem_usage=$(ps aux | grep "[t]elegraf.*--config $config_file" | awk '{print $4}')

            # 计算实际内存使用量（以MB为单位）
            actual_mem_usage=$(echo "scale=2; $total_mem * $mem_usage / 100" | bc)

            echo "monitor_telegraf,vcenter=$vcenter,type=$type,maxFileNum=$max_file_num,currentFileNum=$current_file_num,status=\"running\",cpu=$cpu_usage,totalMem=$total_mem,memUsage=$actual_mem_usage"
        fi
    else
        echo "找不到配置文件: $config_file"
    fi
done