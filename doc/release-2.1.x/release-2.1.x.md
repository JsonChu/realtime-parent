# release-2.1.x 版本特性

## release-2.1.0版本特性
- 1、新增realtime-dwd-sqlserver-vdi-latest-status模块
  - 读取cs_event_result_topic的数据，统计最新的vdi的状态，并写入dwd_vdi_latest_status_topic
## release-2.1.1版本特性
- 1、修复了datastore-datastore数据合并的时候， 表达式为null的异常
## release-2.1.2版本特性
- 1、修复了海力士现场某些vcenter没有horizon获取不到主机的内存大小和cpu核心数等信息，使用python采集主机的信息，写入doris维度表
## release-2.1.3版本特性
- 1、vdi对应的登录和登出事件，"AGENT_DISCONNECTED", "AGENT_ENDED", "AGENT_SHUTDOWN","AGENT_CONNECTED", "AGENT_RECONNECTED", "AGENT_STARTUP"去除掉了AGENT_STARTUP
  - 1.1、登出：AGENT_DISCONNECTED  AGENT_ENDED AGENT_SHUTDOWN 
  - 1.2、登录：AGENT_CONNECTED AGENT_RECONNECTED
