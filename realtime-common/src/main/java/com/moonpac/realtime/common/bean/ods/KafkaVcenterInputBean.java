package com.moonpac.realtime.common.bean.ods;

import com.alibaba.fastjson.annotation.JSONField;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import java.util.Map;
import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class KafkaVcenterInputBean {

    @JSONField(name = "event_name")
    private String eventName;

    @JSONField(name = "objectType")
    private String objectType;

    @JSONField(name = "vcenter_region")
    private String vcenterRegion;

    @JSONField(name = "event_ts")
    private long eventTimestamp;

    @JSONField(name = "event_fields")
    private Map<String, String> eventFields;

    @JSONField(name = "event_tags")
    private Map<String, String> eventTags;

    // 后期 动态 新增的字段 用于去重
    @JSONField(name = "unique_key")
    private String uniqueKey;
    // 后期 动态 新增的字段 用于分组
    @JSONField(name = "event_partition_key")
    private String eventPartitionKey;
    // 维度
    @JSONField(name = "vcenter_dim_info")
    private VcenterInfo vcenterInfo;

    // ognl配置文件可以配置的函数
    public double doubleValue(String str) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
    /*
        开机时长计算逻辑 ognl 配置文件中可以引用的的函数
     */
    public double getUptimeSeconds(){

        VcenterInfo vcenterInfo = this.vcenterInfo;
        if (vcenterInfo == null) return 0;

        VmInfo vmInfo = vcenterInfo.getVmInfo();
        if (vmInfo == null) return 0;

        Long bootTime = vmInfo.getPythonBootTime();
        if (bootTime == null) return 0;

        return Math.round(((double)System.currentTimeMillis() - bootTime) / 1000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaVcenterInputBean that = (KafkaVcenterInputBean) o;
        return  Objects.equals(uniqueKey, that.uniqueKey);
    }
    @Override
    public int hashCode() {
        return Objects.hash(uniqueKey);
    }

}
