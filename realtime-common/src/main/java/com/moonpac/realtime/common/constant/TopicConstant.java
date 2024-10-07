package com.moonpac.realtime.common.constant;

public class TopicConstant {
    public static final String TOPIC_ODS_TELEGRAF = "ods_vcenter_topic_1";
    public static final String TOPIC_DIM_VM = "dim_vm_topic";

    public static final String TOPIC_DIM_HOST = "dim_host_topic";
    public static final String TOPIC_DIM_DATASTORE = "dim_vm_datastore_topic";
    public static final String TOPIC_DWD_WIDEN = "dwd_widen_topic";

    public static final String TOPIC_DWS_MERGE = "dwd_merge_topic"; // 之前topic主题命名不规范
    public static final String TOPIC_DWS_AGG = "dwd_agg_topic"; // 之前topic主题命名不规范

    public static final String TOPIC_DWD_VM_LIFECYCLE = "dwd_vm_lifecycle_topic";

    public static final String TOPIC_METRIC_RULE = "metric_rule_topic"; // 指标引擎的规则
    public static final String TOPIC_METRIC_RESULT = "metric_result_topic";  // 指标引擎的结果

    public static final String TOPIC_CS_EVENT = "cs_event_result_topic";  // sqlserver event 对应的topic

    public static final String TOPIC_VDI_LATEST_STATUS = "dwd_vdi_latest_status_topic";  // vdi 最新状态 对应的topic



}
