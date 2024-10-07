package com.moonpac.realtime.dim.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.constant.EventNameConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.Map;


public class VmPythonProcessFunction extends ProcessFunction<String, VmInfo> {

    static  final DecimalFormat format = new DecimalFormat("#.0000");

    @Override
    public void processElement(String line, ProcessFunction<String, VmInfo>.Context context,
                               Collector<VmInfo> out) throws Exception {
        KafkaVcenterInputBean kafkaVcenterInputBean = JSON.parseObject(line, KafkaVcenterInputBean.class);
        String eventName = kafkaVcenterInputBean.getEventName();
        if(EventNameConstant.PYTHON_VM.equals(eventName)){ // 只需要python采集的维度数据
            VmInfo vmPythonInfo = getVmPythonInfo(kafkaVcenterInputBean);
            out.collect(vmPythonInfo);
        }
    }

    private static VmInfo getVmPythonInfo(KafkaVcenterInputBean kafkaVcenterInputBean) {

        VmInfo dimvmInfo = new VmInfo(); // 创建SinkDIMVMInfo对象并赋值
        Map<String, String> eventTags = kafkaVcenterInputBean.getEventTags();
        Map<String, String> eventFields = kafkaVcenterInputBean.getEventFields();

        // 复制维度属性
        dimvmInfo.setHttpRegion(eventTags.get("vcenter"));
        dimvmInfo.setHttpVmMoid(eventTags.get("moid"));
        dimvmInfo.setPythonCreateTime(Long.parseLong(eventFields.get("create_time")));
        dimvmInfo.setPythonGuestId(eventFields.get("guest_id"));
        dimvmInfo.setPythonGuestFamily(eventFields.get("guest_family"));
        dimvmInfo.setPythonGuestFullName(eventFields.get("guest_full_name"));
        dimvmInfo.setPythonGuestHostName(eventFields.get("guest_host_name"));
        dimvmInfo.setPythonGuestIpAddress(eventFields.get("guest_ip_address"));
        dimvmInfo.setPythonGuestDnsIpAddresses(eventFields.get("guest_dns_ip_addresses"));
        dimvmInfo.setPythonStorageCommittedB(Long.parseLong(eventFields.get("storage_committed_b")));
        dimvmInfo.setPythonStorageUncommittedB(Long.parseLong(eventFields.get("storage_uncommitted_b")));
        dimvmInfo.setPythonStorageUnsharedB(Long.parseLong(eventFields.get("storage_unshared_b")));
        dimvmInfo.setPythonTotalCapacityB(Long.parseLong(eventFields.get("total_capacity_b")));
        dimvmInfo.setPythonTotalFreeSpaceB(Long.parseLong(eventFields.get("total_free_space_b")));
        dimvmInfo.setPythonScreenWidth(Long.parseLong(eventFields.get("screen_width")));
        dimvmInfo.setPythonScreenHeight(Long.parseLong(eventFields.get("screen_height")));
        dimvmInfo.setPythonPowerState(eventFields.get("power_state"));
        dimvmInfo.setPythonConnectionState(eventFields.get("connection_state"));
        dimvmInfo.setPythonMaxCpuUsage(Long.parseLong(eventFields.get("max_cpu_usage")));
        dimvmInfo.setPythonMaxMemoryUsage(Long.parseLong(eventFields.get("max_memory_usage")));
        dimvmInfo.setPythonUuid(eventFields.get("uuid"));
        dimvmInfo.setHttpEventDate(kafkaVcenterInputBean.getEventTimestamp());

        double totalusedSpaceB = (double) (dimvmInfo.getPythonTotalCapacityB() - dimvmInfo.getPythonTotalFreeSpaceB()) * 100 ; // C盘
        double totalCapacityB = Double.valueOf(dimvmInfo.getPythonTotalCapacityB()) == 0?1:Double.valueOf(dimvmInfo.getPythonTotalCapacityB()); // c盘
        double cdriveutilization = Double.parseDouble(format.format(totalusedSpaceB / totalCapacityB));
        // 维度和事实表同时新增C盘使用率
        dimvmInfo.setPythonVmcdriveutilization(cdriveutilization);


        long bootTime = Long.parseLong(eventFields.get("boot_time"));
        if (bootTime < 343792892000L){
            dimvmInfo.setPythonBootTime(null);
        }else{
            dimvmInfo.setPythonBootTime(bootTime);
        }
        dimvmInfo.setPythonNumMksConnections(Long.parseLong(eventFields.get("num_mks_connections")));
        String changeVersionDate = eventFields.get("change_version_date");
        dimvmInfo.setPythonChangeVersionDate(StringUtils.isEmpty(changeVersionDate) ? null : Long.parseLong(changeVersionDate));
        //2023-11-30 新增
        dimvmInfo.setPythonDatastoreIds(eventFields.get("datastore_ids"));
        // 分区字段
        dimvmInfo.setEventPartitionKey(eventTags.get("vcenter")+"_"+eventTags.get("moid"));

        return dimvmInfo;
    }

}