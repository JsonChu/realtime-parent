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
public class DataStoreInfo {
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

    @JSONField(alternateNames = {"datastoreIds", "pythonDatastoreId"}, name = "datastoreId")
    private String pythonDatastoreId;

}
