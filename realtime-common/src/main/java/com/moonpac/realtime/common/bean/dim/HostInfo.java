package com.moonpac.realtime.common.bean.dim;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class HostInfo {

    private String objectType;
    private String vcenter;
    private String region; // vcenter 对应的区域
    private String datacenterName;
    private String datacenterId;
    private String clusterId;
    private String clusterName;
    private String clusterDrsEnabled;
    private String clusterHaEnabled;
    private String hostId;
    private String hostName; //ip
    private Integer hostVMSize;//主机对应的vm个数
    private String hostConnectionState; // CONNECTED
    private String hostPowerState; // POWERED_ON
    private Long eventDate;
    private String eventPartitionKey;
    @JSONField(alternateNames = {"cpuCoreCount", "csCpuCoreCount"}, name = "cpuCoreCount")
    private Integer csCpuCoreCount; //物理核数
    @JSONField(alternateNames = {"memorySizeMb", "csMemorySizemb"}, name = "memorySizeMb")
    private Integer csMemorySizemb; // 内存大小
    @JSONField(alternateNames = {"cpuMhz", "csCpuMhz"}, name = "cpuMhz")
    private Integer csCpuMhz;//cpu mhz

}
