#!/bin/bash

# 设置 Java 应用程序的命令
JAR_FILE="data-process-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.moonpac.task.AutoTelegrafConfigGen"
JAVA_COMMAND="java -cp $JAR_FILE $MAIN_CLASS"
JAVA_COMMAND_WITH_ARGS="$JAVA_COMMAND ./auto-telegraf.properties"

# 定义启动、停止、重启和状态检查函数
start() {
    # 启动 Java 应用程序并捕获错误信息
    $JAVA_COMMAND_WITH_ARGS > /dev/null 2>&1 &
    
    # 等待一段时间
    sleep 5
    
    # 检查程序是否成功启动
    PID=$(ps aux | grep "$JAVA_COMMAND" | grep -v "grep" | awk '{print $2}')
    if [ -n "$PID" ]; then
        # 获取 Java 进程的启动命令
        COMMAND=$(ps -p $PID -o cmd --no-headers)
        # 从启动命令中提取类名
        CLASS_NAME=$(echo "$COMMAND" | awk '{print $(NF-1)}')
        echo "Java 应用程序已启动 (PID: $PID, 类名: $CLASS_NAME)"
    else
        echo "启动失败，请检查日志文件"
    fi
}



stop() {
    # 查找并终止 Java 进程
    PID=$(ps aux | grep "$JAVA_COMMAND" | grep -v "grep" | awk '{print $2}')
    if [ -n "$PID" ]; then
        kill $PID
        # 获取 Java 进程的启动命令
        COMMAND=$(ps -p $PID -o cmd --no-headers)
        # 从启动命令中提取类名
        CLASS_NAME=$(echo "$COMMAND" | awk '{print $(NF-1)}')
        echo "Java 应用程序已停止 (PID: $PID, 类名: $CLASS_NAME)"
    else
        echo "Java 应用程序未运行"
    fi
}


restart() {
    stop
    sleep 3
    start
}

status() {
    # 检查 Java 进程是否在运行
    PID=$(ps aux | grep "$JAVA_COMMAND" | grep -v "grep" | awk '{print $2}')
    if [ -n "$PID" ]; then
        # 获取 Java 进程的启动命令
        COMMAND=$(ps -p $PID -o cmd --no-headers)
        # 从启动命令中提取类名
        CLASS_NAME=$(echo "$COMMAND" | awk '{print $(NF-1)}')
        echo "Java 应用程序正在运行 (PID: $PID, 类名: $CLASS_NAME)"
    else
        echo "Java 应用程序未运行"
    fi
}


# 根据传入参数调用相应的函数
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0

