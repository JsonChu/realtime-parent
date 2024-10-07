package com.moonpac.realtime.common.bean.dim;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VcenterInfo {
    private String region; // vcenter 对应的区域
    private Long eventDate;
    private String objectType;
    private String eventPartitionKey;
    private VmInfo vmInfo;
    private HostInfo hostInfo;
    private DataStoreInfo dataStoreInfo;

}
