package com.moonpac.realtime.common.bean.metric;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MetricRuleInfo {

    @JSONField(name = "METRIC_ID", alternateNames = {"metricId"})
    private String metricId;

    @JSONField(name = "METRIC_NAME", alternateNames = {"metricName"})
    private String metricName;

    @JSONField(name = "OBJECT_TYPE_ID", alternateNames = {"objectTypeId"})
    private String objectTypeId;

    @JSONField(name = "METRIC_TYPE", alternateNames = {"metricType"})
    private int metricType;

    @JSONField(name = "METRIC_DATA_TYPE", alternateNames = {"metricDataType"})
    private int metricDataType;

    @JSONField(name = "METRIC_STATUS", alternateNames = {"metricStatus"})
    private int metricStatus;

    @JSONField(name = "METRIC_DESC", alternateNames = {"metricDesc"})
    private String metricDesc;

    @JSONField(name = "FIELD_NAME", alternateNames = {"fieldName"})
    private String fieldName;

    @JSONField(name = "DEL_FLAG", alternateNames = {"delFlag"})
    private int delFlag;

    @JSONField(name = "CREATE_BY", alternateNames = {"createBy"})
    private String createBy;

    @JSONField(name = "CREATE_TIME", alternateNames = {"createTime"})
    private Date createTime;

    @JSONField(name = "UPDATE_BY", alternateNames = {"updateBy"})
    private String updateBy;

    @JSONField(name = "METRIC_COUNT_FORMULA", alternateNames = {"metricCountFormula"})
    private String metricCountFormula;
    @JSONField(name = "UPDATE_TIME", alternateNames = {"updateTime"})
    private Long updateTime;

    // +I +u
    private String op;

}
