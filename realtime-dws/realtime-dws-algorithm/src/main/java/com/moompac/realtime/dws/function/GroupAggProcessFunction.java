package com.moompac.realtime.dws.function;


import com.moonpac.realtime.common.bean.dim.DataStoreInfo;
import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import com.moonpac.realtime.common.bean.dws.AggBean;
import com.moonpac.realtime.common.bean.dws.MergeEventBean;
import com.moonpac.realtime.common.bean.dws.WidenResultBean;
import com.moonpac.realtime.common.util.NumUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;

@Slf4j
public class GroupAggProcessFunction extends ProcessFunction<WidenResultBean, AggBean> {

    private static DecimalFormat format = new DecimalFormat("#.0000");

    @Override
    public void processElement(WidenResultBean widenResultBean, ProcessFunction<WidenResultBean,
            AggBean>.Context context, Collector<AggBean> out) throws Exception {

        String objectType = widenResultBean.getObjectType();
        MergeEventBean mergeEventBean = widenResultBean.getMergeEventBean();
        VcenterInfo vcenterInfo = widenResultBean.getVcenterInfo();

        if ( "host".equals(objectType) ){

            HostInfo hostInfo = vcenterInfo.getHostInfo();
            String hostConnectionState = hostInfo.getHostConnectionState();
            String hostPowerState = hostInfo.getHostPowerState();
            Integer hostVMSize = hostInfo.getHostVMSize();

            if ("CONNECTED".equals(hostConnectionState) && "POWERED_ON".equals(hostPowerState)){
                AggBean aggBean = new AggBean();

                // 对脏数据做处理
                Double hostcpuusagerate = mergeEventBean.getHostcpuusage() != null ? mergeEventBean.getHostcpuusage() : 0.0;     //使用率
                Double hostcpuusagemhz = mergeEventBean.getHostcpuusagemhz() != null ? mergeEventBean.getHostcpuusagemhz() : 0.0; //使用量
                Double hostmemusagerate = mergeEventBean.getHostmemusage() != null ? mergeEventBean.getHostmemusage() : 0.0;
                Double memorysizetotalgb = mergeEventBean.getMemorysizegb() != null ? mergeEventBean.getMemorysizegb() : 0.0;
                hostcpuusagerate = hostcpuusagerate == 0.0 ? 1.0 : hostcpuusagerate;

                //把内存大小转换为GB
                memorysizetotalgb = NumUtils.doubleValue(format.format(memorysizetotalgb / 1024));

                // 度量值
                aggBean.setCpuusedmhz(hostcpuusagemhz);
                aggBean.setCputotalmhz(NumUtils.doubleValue(format.format(hostcpuusagemhz / hostcpuusagerate * 100)));
                aggBean.setMemusedgb(NumUtils.doubleValue(format.format(memorysizetotalgb * hostmemusagerate / 100)));
                aggBean.setMemtotalgb(memorysizetotalgb);
                aggBean.setNetused(mergeEventBean.getHostnetbytesrx() + mergeEventBean.getHostnetbytestx());
                aggBean.setDiskused(mergeEventBean.getHostdiskread() + mergeEventBean.getHostdiskwrite());
                aggBean.setVmsize(hostVMSize);// 主机对应的vm个数

                // 额外获取一下 datcentername clustername
                String datacenterName = hostInfo.getDatacenterName();
                String clusterName = hostInfo.getClusterName();

                String vcenter = hostInfo.getVcenter();
                String datacenterId = hostInfo.getDatacenterId();
                String clusterId = hostInfo.getClusterId();
                String region = vcenterInfo.getRegion();

                // 去重字段
                aggBean.setUniqueKey(region+"_"+vcenter+"_"+datacenterId+"_"+clusterId+"_"+mergeEventBean.getHostmoid());

                //维度值
                aggBean.setEventdate(widenResultBean.getEventDate());
                aggBean.setRegion(region);
                aggBean.setEventPartitionKey("cpumem-"+region);
                aggBean.setObjectType("cpumem-region");
                out.collect(aggBean);

                aggBean.setVcenter(vcenter);
                aggBean.setDatacenterId(datacenterId);
                aggBean.setDatacenterName(datacenterName);
                aggBean.setEventPartitionKey("cpumem-"+region+"_"+vcenter+"_"+datacenterId);
                aggBean.setObjectType("cpumem-datacenter");
                out.collect(aggBean);

                aggBean.setClusterId(clusterId);
                aggBean.setClusterName(clusterName);
                aggBean.setEventPartitionKey("cpumem-"+region+"_"+vcenter+"_"+datacenterId+"_"+clusterId);
                aggBean.setObjectType("cpumem-cluster");
                out.collect(aggBean);
            }

        }else if ( "datastore-disk".equals(objectType) ){ //  datastore 容量数据
            DataStoreInfo dataStoreInfo = vcenterInfo.getDataStoreInfo();
            String clusterId = dataStoreInfo.getHttpClusterId();
            String clusterName = dataStoreInfo.getHttpClusterName();
            String datacenterId = dataStoreInfo.getHttpDatacenterId();
            String region = dataStoreInfo.getHttpRegion();
            String vcenter = dataStoreInfo.getHttpVcenter();
            String datastoreId = dataStoreInfo.getPythonDatastoreId();
            String datacenterName = dataStoreInfo.getHttpDatacenterName();

            // 合并数据
            AggBean aggBean = new AggBean();
            // 去重字段
            aggBean.setUniqueKey(vcenter+"_"+datastoreId);

            // 度量值
            aggBean.setDatastoreusedgb(mergeEventBean.getDsusedcapacity());
            aggBean.setDatastoretotalgb(mergeEventBean.getDstotalcapacity());

            //维度值
            aggBean.setEventdate(widenResultBean.getEventDate());
            aggBean.setRegion(region);
            aggBean.setEventPartitionKey("datastore-disk-"+region);
            aggBean.setObjectType("datastore-disk-region");
            out.collect(aggBean);

            aggBean.setVcenter(vcenter);
            aggBean.setDatacenterId(datacenterId);
            aggBean.setDatacenterName(datacenterName);
            aggBean.setDatastoreId(datastoreId);
            aggBean.setEventPartitionKey("datastore-disk-"+region+"_"+vcenter+"_"+datacenterId);
            aggBean.setObjectType("datastore-disk-datacenter");
            out.collect(aggBean);

            aggBean.setClusterId(clusterId);
            aggBean.setClusterName(clusterName);
            aggBean.setEventPartitionKey("datastore-disk-"+region+"_"+vcenter+"_"+datacenterId+"_"+clusterId);
            aggBean.setObjectType("datastore-disk-cluster");
            out.collect(aggBean);

        }else if ( "datastore-datastore".equals(objectType) ){  //  datastore 读写性能
            DataStoreInfo dataStoreInfo = vcenterInfo.getDataStoreInfo();
            String clusterId = dataStoreInfo.getHttpClusterId();
            String clusterName = dataStoreInfo.getHttpClusterName();
            String datacenterId = dataStoreInfo.getHttpDatacenterId();
            String region = dataStoreInfo.getHttpRegion();
            String vcenter = dataStoreInfo.getHttpVcenter();
            String datastoreId = dataStoreInfo.getPythonDatastoreId();
            String datacenterName = dataStoreInfo.getHttpDatacenterName();

            // 合并数据
            AggBean aggBean = new AggBean();
            // 去重字段
            aggBean.setUniqueKey(vcenter+"_"+datastoreId);

            // 新增的度量值 datastore的读写 2024-4-12
            aggBean.setDsnumberreadaveraged(mergeEventBean.getDsnumberreadaveraged());
            aggBean.setDsnumberwriteaeraged(mergeEventBean.getDsnumberwriteaeraged());

            //维度值
            aggBean.setEventdate(widenResultBean.getEventDate());
            aggBean.setRegion(region);
            aggBean.setEventPartitionKey("datastore-datastore-"+region);
            aggBean.setObjectType("datastore-datastore-region");
            out.collect(aggBean);

            aggBean.setVcenter(vcenter);
            aggBean.setDatacenterId(datacenterId);
            aggBean.setDatacenterName(datacenterName);
            aggBean.setDatastoreId(datastoreId);
            aggBean.setEventPartitionKey("datastore-datastore-"+region+"_"+vcenter+"_"+datacenterId);
            aggBean.setObjectType("datastore-datastore-datacenter");
            out.collect(aggBean);

            aggBean.setClusterId(clusterId);
            aggBean.setClusterName(clusterName);
            aggBean.setEventPartitionKey("datastore-datastore-"+region+"_"+vcenter+"_"+datacenterId+"_"+clusterId);
            aggBean.setObjectType("datastore-datastore-cluster");
            out.collect(aggBean);

        }
    }
}
