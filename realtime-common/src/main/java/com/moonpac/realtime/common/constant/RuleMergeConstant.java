package com.moonpac.realtime.common.constant;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dws.meger.MergeAutoCompute;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileReader;

@Slf4j
public class RuleMergeConstant {

    public static void main(String[] args) throws Exception {

        File file = new File("D:\\bigdata\\IdeaProjects\\realtime-parent-new\\realtime-dws\\realtime-dws-merge\\src\\main\\resources\\widen2merge-rule.yml");
        Yaml yaml = new Yaml();
        MergeAutoCompute ruleMerge = yaml.loadAs(new FileReader(file), MergeAutoCompute.class);
        // 将日志对象，转成JSON
        String rule = JSON.toJSONString(ruleMerge);
        log.info("生成的规则：{}",rule);
    }
    public static final String widen2MergeRule = "{\n" +
            "\t\"objectAGGCompute\": [{\n" +
            "\t\t\"aggFieldCompute\": [{\n" +
            "\t\t\t\"aggType\": \"sum\",\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"hostdsnumberread\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('numberReadAveraged_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostdsnumberwrite\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('numberWriteAveraged_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \"'vsphere_host_datastore'.equals(eventName) && !'instance-total'.equals(eventTags.get('dsname')) \"\n" +
            "\t\t}],\n" +
            "\t\t\"combinationFields\": [{\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"hostcpuusage\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('usage_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostcpuusagemhz\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('usagemhz_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"region\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterRegion\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vcenter\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"eventdate\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTimestamp\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostmoid\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('moid')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostname\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('esxhostname')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_host_cpu'.equals(eventName) && 'instance-total'.equals(eventTags.get('cpu')) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"hostmemusage\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('usage_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostmemactive\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('active_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_host_mem'.equals(eventName) && !'DRAM'.equals(eventTags.get('instance')) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"hostdiskread\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('read_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostdiskwrite\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('write_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_host_disk'.equals(eventName) && 'instance-total'.equals(eventTags.get('disk')) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"hostnetbytesrx\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('bytesRx_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostnetbytestx\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('bytesTx_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_host_net'.equals(eventName) && 'instance-total'.equals(eventTags.get('interface')) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"ip\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.hostInfo.hostName\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"hostcpucoretotal\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.hostInfo.csCpuCoreCount\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"memorysizegb\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.hostInfo.csMemorySizemb\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_host_mem'.equals(eventName) \"\n" +
            "\t\t}],\n" +
            "\t\t\"groupKey\": [{\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_host_cpu'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('cpu')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_host_datastore'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('dsname')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_host_disk'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('disk')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_host_mem'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('instance')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_host_net'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('interface')\"\n" +
            "\t\t}],\n" +
            "\t\t\"mergeOgnlExpression\": \"vsphere_host_cpu,vsphere_host_datastore,vsphere_host_disk,vsphere_host_mem,vsphere_host_net\",\n" +
            "\t\t\"objectType\": \"host\"\n" +
            "\t}, {\n" +
            "\t\t\"aggFieldCompute\": [{\n" +
            "\t\t\t\"aggType\": \"sum\",\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"vmdsnumberread\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('numberReadAveraged_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmdsnumberwrite\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('numberWriteAveraged_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \"'vsphere_vm_datastore'.equals(eventName) && !'instance-total'.equals(eventTags.get('dsname')) \"\n" +
            "\t\t}],\n" +
            "\t\t\"combinationFields\": [{\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"region\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterRegion\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vcenter\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"eventdate\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTimestamp\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmmoid\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('moid')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmname\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vmname')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmmemusage\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('usage_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmmemactive\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('active_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"uptimeseconds\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"getUptimeSeconds()\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"ip\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.pythonGuestIpAddress\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"isvdi\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.httpIsVdi\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmcdriveutilization\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.pythonVmcdriveutilization\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmcpucoretotal\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.httpNumCpu\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmmemorysizemb\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.httpMemorySizeMb\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmpowerstate\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.pythonPowerState\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmtotalcapacityb\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterInfo.vmInfo.pythonTotalCapacityB\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_vm_mem'.equals(eventName) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"vmcpuusage\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('usage_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmcpuusagemhz\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('usagemhz_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_vm_cpu'.equals(eventName) && 'instance-total'.equals(eventTags.get('cpu')) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"vmdiskread\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('read_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmdiskwrite\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('write_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmdiskusage\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"doubleValue(eventFields.getOrDefault('read_average','0'))+doubleValue(eventFields.getOrDefault('write_average','0'))\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_vm_disk'.equals(eventName) && 'instance-total'.equals(eventTags.get('disk')) \"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"vmnetbytesrx\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('bytesRx_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vmnetbytestx\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('bytesTx_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_vm_net'.equals(eventName) && 'instance-total'.equals(eventTags.get('interface')) \"\n" +
            "\t\t}],\n" +
            "\t\t\"groupKey\": [{\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_cpu'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('cpu')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_datastore'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('dsname')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_disk'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('disk')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_mem'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_net'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('interface')\"\n" +
            "\t\t}, {\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_sys'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp\"\n" +
            "\t\t}],\n" +
            "\t\t\"mergeOgnlExpression\": \"vsphere_vm_cpu,vsphere_vm_datastore,vsphere_vm_disk,vsphere_vm_mem,vsphere_vm_net,vsphere_vm_sys\",\n" +
            "\t\t\"objectType\": \"vm\"\n" +
            "\t}, {\n" +
            "\t\t\"aggFieldCompute\": [{\n" +
            "\t\t\t\"aggType\": \"sum\",\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"bootedvmnum\",\n" +
            "\t\t\t\t\"getFieldExpression\": \" 'POWERED_ON'.equals(vcenterInfo.vmInfo.pythonPowerState) ? 1D : 0D \"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"shutdownvmnum\",\n" +
            "\t\t\t\t\"getFieldExpression\": \" 'POWERED_OFF'.equals(vcenterInfo.vmInfo.pythonPowerState) ? 1D : 0D \"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"totalvmnum\",\n" +
            "\t\t\t\t\"getFieldExpression\": \" 1D \"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_vm_mem'.equals(eventName) \"\n" +
            "\t\t}],\n" +
            "\t\t\"combinationFields\": [{\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"region\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterRegion\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vcenter\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"eventdate\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTimestamp\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vcmoid\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_vm_mem'.equals(eventName) \"\n" +
            "\t\t}],\n" +
            "\t\t\"groupKey\": [{\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_vm_mem'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+eventTimestamp\"\n" +
            "\t\t}],\n" +
            "\t\t\"mergeOgnlExpression\": \"vsphere_vm_mem\",\n" +
            "\t\t\"objectType\": \"vcenter\"\n" +
            "\t}, {\n" +
            "\t\t\"combinationFields\": [{\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"region\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterRegion\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vcenter\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"eventdate\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTimestamp\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsmoid\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('moid')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsname\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('dsname')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsusedcapacity\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('used_latest','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dstotalcapacity\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('capacity_latest','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsfreecapacity\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"doubleValue(eventFields.getOrDefault('capacity_latest','0'))-doubleValue(eventFields.getOrDefault('used_latest','0'))\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsprovisionedcapacity\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('provisioned_latest','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_datastore_disk'.equals(eventName) \"\n" +
            "\t\t}],\n" +
            "\t\t\"groupKey\": [{\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_datastore_disk'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"'vsphere_datastore_disk_'+eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('dsname')\"\n" +
            "\t\t}],\n" +
            "\t\t\"mergeOgnlExpression\": \"vsphere_datastore_disk\",\n" +
            "\t\t\"objectType\": \"datastore-disk\"\n" +
            "\t}, {\n" +
            "\t\t\"combinationFields\": [{\n" +
            "\t\t\t\"atomsFields\": [{\n" +
            "\t\t\t\t\"fieldName\": \"region\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"vcenterRegion\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"vcenter\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('vcenter')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"eventdate\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTimestamp\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsmoid\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('moid')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsname\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventTags.get('dsname')\",\n" +
            "\t\t\t\t\"isCoreField\": true\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsnumberreadaveraged\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('numberReadAveraged_average','0')\"\n" +
            "\t\t\t}, {\n" +
            "\t\t\t\t\"fieldName\": \"dsnumberwriteaeraged\",\n" +
            "\t\t\t\t\"getFieldExpression\": \"eventFields.getOrDefault('numberWriteAveraged_average','0')\"\n" +
            "\t\t\t}],\n" +
            "\t\t\t\"filterExpression\": \" 'vsphere_datastore_datastore'.equals(eventName) \"\n" +
            "\t\t}],\n" +
            "\t\t\"groupKey\": [{\n" +
            "\t\t\t\"eventNameExpression\": \"'vsphere_datastore_datastore'.equals(eventName)\",\n" +
            "\t\t\t\"groupExpression\": \"'vsphere_datastore_datastore_'+eventTags.get('vcenter')+'_'+eventTags.get('moid')\",\n" +
            "\t\t\t\"uniqueKeyExpression\": \"eventTags.get('vcenter')+'_'+eventTags.get('moid')+'_'+eventName+'_'+eventTimestamp+'_'+eventTags.get('dsname')\"\n" +
            "\t\t}],\n" +
            "\t\t\"mergeOgnlExpression\": \"vsphere_datastore_datastore\",\n" +
            "\t\t\"objectType\": \"datastore-datastore\"\n" +
            "\t}],\n" +
            "\t\"ruleCode\": \"rule_001\",\n" +
            "\t\"ruleStatus\": \"1\"\n" +
            "}";

}
