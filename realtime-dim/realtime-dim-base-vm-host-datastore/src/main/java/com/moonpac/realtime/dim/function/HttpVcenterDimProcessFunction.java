package com.moonpac.realtime.dim.function;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.constant.ObjectTypeConstant;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import com.moonpac.realtime.common.util.OKHttpUtil;
import com.moonpac.realtime.common.bean.vcenter.VcenterParam;


//K, I, O
@Slf4j
public class HttpVcenterDimProcessFunction extends KeyedProcessFunction<String, VcenterParam, String> {

    private OkHttpClient okHttpClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 okHttpClient
        okHttpClient = OKHttpUtil.getOKHttpClient();
    }

    @Override
    public void processElement(VcenterParam vcenterParam, KeyedProcessFunction<String, VcenterParam, String>.Context ctx,
                               Collector<String> out) throws Exception {

        try {
            //获取datacenter列表
            String vcToken = OKHttpUtil.getVCToken(okHttpClient, vcenterParam);

            String vcenterParamUrl = vcenterParam.getUrl();
            String vcenter = OKHttpUtil.getIP(vcenterParamUrl);
            String region = vcenterParam.getRegion();

            String rootUrl = vcenterParamUrl + "/api/vcenter";
            String datacenterUrl = rootUrl + "/datacenter";
            String datacenterListStr = OKHttpUtil.vcRequest(okHttpClient, datacenterUrl, vcToken, null);
            //封装结果集
            JsonNode datacenterJsonNode = new ObjectMapper().readTree(datacenterListStr);

            // 转换为秒级别时间戳
            Long eventDate = System.currentTimeMillis() / 1000;

            /*
                set的作用是为了 统一的获取所有的vm的维度数据 统一的发出去，为了保证eventDate是一样的 很重要
             */
            Set<VmInfo> dimVMSet = new HashSet<>();
            Set<HostInfo> dimHostSet = new HashSet<>();

            //遍历datacenter列表
            if (datacenterJsonNode.isArray() && datacenterJsonNode.size() > 0) {
                for (JsonNode datacenterNode : datacenterJsonNode) {
                    //获取datacenter_id
                    String datacenterId = datacenterNode.get("datacenter").asText();
                    String datacenterName = datacenterNode.get("name").asText();
                    Map<String, String> datacenterParam = new HashMap<>();
                    datacenterParam.put("datacenters", datacenterId);
                    //获取cluster列表
                    String clusterUrl = rootUrl + "/cluster";
                    String clusterListStr = OKHttpUtil.vcRequest(okHttpClient, clusterUrl, vcToken, datacenterParam);
                    //封装结果集
                    JsonNode clusterJsonNode = new ObjectMapper().readTree(clusterListStr);
                    if (clusterJsonNode.isArray() && clusterJsonNode.size() > 0) {
                        for (JsonNode clusterNode : clusterJsonNode) {
                            //获取cluster
                            String clusterId = clusterNode.get("cluster").asText();
                            String clusterName = clusterNode.get("name").asText();
                            String clusterDrsEnabled = clusterNode.get("drs_enabled").asText();
                            String clusterHaEnabled = clusterNode.get("ha_enabled").asText();

                            Map<String, String> clusterParam = new HashMap<>();
                            clusterParam.put("clusters", clusterId);
                            //获取host列表
                            String hostUrl = rootUrl + "/host";
                            String hostListStr = OKHttpUtil.vcRequest(okHttpClient, hostUrl, vcToken, clusterParam);
                            //封装结果集
                            JsonNode hostJsonNode = new ObjectMapper().readTree(hostListStr);

                            if (hostJsonNode.isArray() && hostJsonNode.size() > 0) {

                                for (JsonNode hostNode : hostJsonNode) {
                                    //获取host
                                    String hostId = hostNode.get("host").asText();
                                    String hostName = hostNode.get("name").asText();

                                    String host_connection_state = null;
                                    String host_power_state = null;
                                    JsonNode connectionStateNode = hostNode.get("connection_state");
                                    JsonNode powerStateNode = hostNode.get("power_state");
                                    if (connectionStateNode != null && powerStateNode != null) {
                                        host_connection_state = connectionStateNode.asText();
                                        host_power_state = powerStateNode.asText();
                                    }
                                    // 这里是 host维度数据
                                    HostInfo dimHostInfo = new HostInfo();
                                    dimHostInfo.setRegion(region);
                                    dimHostInfo.setEventDate(eventDate);
                                    dimHostInfo.setVcenter(vcenter);
                                    dimHostInfo.setDatacenterId(datacenterId);
                                    dimHostInfo.setDatacenterName(datacenterName);
                                    dimHostInfo.setClusterId(clusterId);
                                    dimHostInfo.setClusterName(clusterName);
                                    dimHostInfo.setClusterDrsEnabled(clusterDrsEnabled);
                                    dimHostInfo.setClusterHaEnabled(clusterHaEnabled);
                                    dimHostInfo.setHostId(hostId);
                                    dimHostInfo.setHostName(hostName);
                                    Map<String, String> hostParam = new HashMap<>();
                                    hostParam.put("hosts", hostId);
                                    //获取vm列表
                                    String vmUrl = rootUrl + "/vm";
                                    String vmListStr = OKHttpUtil.vcRequest(okHttpClient, vmUrl, vcToken, hostParam);
                                    //封装结果集
                                    JsonNode vmJsonNode = new ObjectMapper().readTree(vmListStr);
                                    dimHostInfo.setHostVMSize(vmJsonNode.size());
                                    dimHostInfo.setHostConnectionState(host_connection_state);
                                    dimHostInfo.setHostPowerState(host_power_state);
                                    dimHostInfo.setObjectType(ObjectTypeConstant.DIM_HOST);
                                    dimHostInfo.setEventPartitionKey(vcenter+"_"+hostId); // 和后面cs获取的数据进行关联
                                    dimHostSet.add(dimHostInfo);
                                    if (vmJsonNode.isArray() && vmJsonNode.size() > 0) {
                                        for (JsonNode vmNode : vmJsonNode) {
                                            //获取vm 维度
                                            String vmMoid = vmNode.get("vm").asText();
                                            String vmName = vmNode.get("name").asText();
                                            Long memory_size_MiB = vmNode.get("memory_size_MiB").asLong();
                                            Long cpu_count = vmNode.get("cpu_count").asLong();
                                            int isVdi = 0;
                                            String userCode = "";
                                            if (vmName.startsWith("VM") && vmName.length() > 4) {
                                                isVdi = 1;
                                                if (vmName.contains("-")) {
                                                    int i = vmName.indexOf("-");
                                                    if (i > 4) {
                                                        userCode = vmName.substring(4, i);
                                                    }
                                                }
                                            }
                                            VmInfo dimvmInfo = new VmInfo();
                                            dimvmInfo.setHttpRegion(region);
                                            dimvmInfo.setHttpVcenter(vcenter);
                                            dimvmInfo.setHttpVmMoid(vmMoid);
                                            dimvmInfo.setHttpVmName(vmName);
                                            dimvmInfo.setHttpIsVdi(isVdi);
                                            dimvmInfo.setHttpVmUserCode(userCode);
                                            dimvmInfo.setHttpDatacenterId(datacenterId);
                                            dimvmInfo.setHttpDatacenterName(datacenterName);
                                            dimvmInfo.setHttpClusterId(clusterId);
                                            dimvmInfo.setHttpClusterName(clusterName);
                                            dimvmInfo.setHttpClusterDrsEnabled(clusterDrsEnabled);
                                            dimvmInfo.setHttpClusterHaEnabled(clusterHaEnabled);
                                            dimvmInfo.setHttpHostId(hostId);
                                            dimvmInfo.setHttpHostName(hostName);
                                            dimvmInfo.setHttpMemorySizeMb(memory_size_MiB);
                                            dimvmInfo.setHttpNumCpu(cpu_count);
                                            dimvmInfo.setHttpEventDate(eventDate);
                                            dimvmInfo.setObjectType(ObjectTypeConstant.DIM_VM);
                                            dimvmInfo.setEventPartitionKey(vcenter+"_"+vmMoid);
                                            dimVMSet.add(dimvmInfo);
                                        }
                                    }
                                    int randomNumber = ThreadLocalRandom.current().nextInt(1, 20) * 100;
                                    TimeUnit.MILLISECONDS.sleep(1000+randomNumber);
                                }
                            }
                        }
                    }
                }
            }

            // 统一发送 vm 维度
            for (VmInfo dimvmInfo : dimVMSet) {
                out.collect(JSON.toJSONString(dimvmInfo));
            }
            // 统一发送 host 维度
            for ( HostInfo dimHostInfo : dimHostSet) {
                ctx.output(new OutputTag<>(ObjectTypeConstant.DIM_HOST, TypeInformation.of(String.class)), JSON.toJSONString(dimHostInfo));
            }

        }catch (Exception e){
            log.warn("请求vcenter http接口异常。");
            e.printStackTrace();
        }
     }
}
