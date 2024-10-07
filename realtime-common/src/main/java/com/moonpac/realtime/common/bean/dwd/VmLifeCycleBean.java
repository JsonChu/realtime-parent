package com.moonpac.realtime.common.bean.dwd;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VmLifeCycleBean {

    @JSONField(alternateNames = {"region", "httpRegion"}, name = "region")
    private String httpRegion;
    @JSONField(alternateNames = {"vcenter", "httpVcenter"}, name = "vcenter")
    private String httpVcenter;
    @JSONField(alternateNames = {"vmMoid", "httpVmMoid"}, name = "vmMoid")
    private String httpVmMoid;
    @JSONField(alternateNames = {"vmName", "httpVmName"}, name = "vmName")
    private String httpVmName;
    @JSONField(alternateNames = {"eventDate", "httpEventDate"}, name = "eventDate")
    private Long httpEventDate;
    @JSONField(alternateNames = {"vmUserCode", "httpVmUserCode"}, name = "vmUserCode")
    private String httpVmUserCode;
    @JSONField(alternateNames = {"isVdi", "httpIsVdi"}, name = "isVdi")
    private Integer httpIsVdi;
    @JSONField(alternateNames = {"datacenterId", "httpDatacenterId"}, name = "datacenterId")
    private String httpDatacenterId;
    @JSONField(alternateNames = {"datacenterName", "httpDatacenterName"}, name = "datacenterName")
    private String httpDatacenterName;
    @JSONField(alternateNames = {"clusterId", "httpClusterId"}, name = "clusterId")
    private String httpClusterId;
    @JSONField(alternateNames = {"clusterName", "httpClusterName"}, name = "clusterName")
    private String httpClusterName;
    @JSONField(alternateNames = {"hostId", "httpHostId"}, name = "hostId")
    private String httpHostId;
    @JSONField(alternateNames = {"hostName", "httpHostName"}, name = "hostName")
    private String httpHostName;
    @JSONField(alternateNames = {"powerState", "pythonPowerState"}, name = "powerState")
    private String pythonPowerState;

    @JSONField(alternateNames = {"startTime"}, name = "startTime")
    private Long startTime;
    @JSONField(alternateNames = {"endTime"}, name = "endTime")
    private Long endTime;

}


