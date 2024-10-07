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
public class VmInfo {
    @JSONField(name = "objectType")
    private String objectType;
    @JSONField(name = "eventPartitionKey")
    private String eventPartitionKey;

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

    @JSONField(alternateNames = {"clusterDrsEnabled", "httpClusterDrsEnabled"}, name = "clusterDrsEnabled")
    private String httpClusterDrsEnabled;

    @JSONField(alternateNames = {"clusterHaEnabled", "httpClusterHaEnabled"}, name = "clusterHaEnabled")
    private String httpClusterHaEnabled;

    @JSONField(alternateNames = {"hostId", "httpHostId"}, name = "hostId")
    private String httpHostId;

    @JSONField(alternateNames = {"hostName", "httpHostName"}, name = "hostName")
    private String httpHostName;

    @JSONField(alternateNames = {"numCpu", "httpNumCpu"}, name = "numCpu")
    private Long httpNumCpu;

    @JSONField(alternateNames = {"memorySizeMb", "httpMemorySizeMb"}, name = "memorySizeMb")
    private Long httpMemorySizeMb;

    // python对应的属性
    @JSONField(alternateNames = {"guestId", "pythonGuestId"}, name = "guestId")
    private String pythonGuestId;

    @JSONField(alternateNames = {"guestFamily", "pythonGuestFamily"}, name = "guestFamily")
    private String pythonGuestFamily;

    @JSONField(alternateNames = {"guestFullName", "pythonGuestFullName"}, name = "guestFullName")
    private String pythonGuestFullName;

    @JSONField(alternateNames = {"guestHostName", "pythonGuestHostName"}, name = "guestHostName")
    private String pythonGuestHostName;

    @JSONField(alternateNames = {"guestIpAddress", "pythonGuestIpAddress"}, name = "guestIpAddress")
    private String pythonGuestIpAddress;

    @JSONField(alternateNames = {"guestDnsIpAddresses", "pythonGuestDnsIpAddresses"}, name = "guestDnsIpAddresses")
    private String pythonGuestDnsIpAddresses;

    @JSONField(alternateNames = {"storageCommittedB", "pythonStorageCommittedB"}, name = "storageCommittedB")
    private Long pythonStorageCommittedB;

    @JSONField(alternateNames = {"storageUncommittedB", "pythonStorageUncommittedB"}, name = "storageUncommittedB")
    private Long pythonStorageUncommittedB;

    @JSONField(alternateNames = {"storageUnsharedB", "pythonStorageUnsharedB"}, name = "storageUnsharedB")
    private Long pythonStorageUnsharedB;

    @JSONField(alternateNames = {"totalCapacityB", "pythonTotalCapacityB"}, name = "totalCapacityB")
    private Long pythonTotalCapacityB;

    @JSONField(alternateNames = {"totalFreeSpaceB", "pythonTotalFreeSpaceB"}, name = "totalFreeSpaceB")
    private Long pythonTotalFreeSpaceB;

    @JSONField(alternateNames = {"screenWidth", "pythonScreenWidth"}, name = "screenWidth")
    private Long pythonScreenWidth;

    @JSONField(alternateNames = {"screenHeight", "pythonScreenHeight"}, name = "screenHeight")
    private Long pythonScreenHeight;

    @JSONField(alternateNames = {"powerState", "pythonPowerState"}, name = "powerState")
    private String pythonPowerState;

    @JSONField(alternateNames = {"connectionState", "pythonConnectionState"}, name = "connectionState")
    private String pythonConnectionState;

    @JSONField(alternateNames = {"maxCpuUsage", "pythonMaxCpuUsage"}, name = "maxCpuUsage")
    private Long pythonMaxCpuUsage;

    @JSONField(alternateNames = {"maxMemoryUsage", "pythonMaxMemoryUsage"}, name = "maxMemoryUsage")
    private Long pythonMaxMemoryUsage;

    @JSONField(alternateNames = {"numMksConnections", "pythonNumMksConnections"}, name = "numMksConnections")
    private Long pythonNumMksConnections;

    @JSONField(alternateNames = {"changeVersionDate", "pythonChangeVersionDate"}, name = "changeVersionDate")
    private Long pythonChangeVersionDate;

    @JSONField(alternateNames = {"bootTime", "pythonBootTime"}, name = "bootTime")
    private Long pythonBootTime;

    @JSONField(alternateNames = {"createTime", "pythonCreateTime"}, name = "createTime")
    private Long pythonCreateTime;

    @JSONField(alternateNames = {"datastoreIds", "pythonDatastoreIds"}, name = "datastoreIds")
    private String pythonDatastoreIds;

    @JSONField(alternateNames = {"vmcdriveutilization", "pythonVmcdriveutilization"}, name = "vmcdriveutilization")
    private Double pythonVmcdriveutilization;

    @JSONField(alternateNames = {"uuid", "pythonUuid"}, name = "uuid")
    private String pythonUuid;

}
