package com.moonpac.realtime.common.bean.dws;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AggBean {
    private String vcenter;
    private String region; // vcenter 对应的区域
    private String datacenterName;
    private String datacenterId;
    private String clusterId;
    private String clusterName;
    private Long eventdate;
    private String objectType;
    private String datastoreId;

    private Integer vmsize;
    private Integer hostsize ;
    private Double cpuusedmhz;
    private Double cputotalmhz;
    private Double memusedgb;
    private Double memtotalgb;
    private Double datastoreusedgb;
    private Double datastoretotalgb;

    // 新增datastore的读写 2024-4-12
    private Double dsnumberreadaveraged;
    private Double dsnumberwriteaeraged;

    /*
        集群峰值指标
        cpu使用容量   cpuusedmhz
        内存使用容量   memusedgb
        网络ipos   host_net hostnetbytesrx+hostnetbytestx
        磁盘kbps   host_disk   hostdiskwrite+hostdiskread
     */
    private Double maxcpuusedmhz;
    private Double avgcpuusedmhz;
    private Double maxmemusedgb;
    private Double avgmemusedgb;

    private Double netused; // hostnetbytesrx+hostnetbytestx
    private Double avgnetused;
    private Double maxnetused;

    private Double diskused; // hostdiskwrite+hostdiskread
    private Double avgdiskused;
    private Double maxdiskused;

    private Double maxdatastoreusedgb;
    private Double avgdatastoreusedgb;

    // 新增datastore的读写 2024-4-12
    private Double sumdsnumberreadaveraged;
    private Double sumdsnumberwriteaeraged;


    @JSONField(name = "unique_key")
    private String uniqueKey;
    // 后期 动态 新增的字段 用于分组
    @JSONField(name = "event_partition_key")
    private String eventPartitionKey;

}
