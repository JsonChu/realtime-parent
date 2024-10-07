package com.moonpac.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.base.BaseAPP;
import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.bean.vcenter.VcenterParam;
import com.moonpac.realtime.common.constant.ObjectTypeConstant;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import com.moonpac.realtime.dim.function.*;
import com.moonpac.realtime.dim.source.DorisHostPythonSource;
import com.moonpac.realtime.dim.source.DorisVcenterConfigParamSource;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author zhanglingxing
 * @date 2024/4/15 14:17
 * 维度加工
 * 启动参数：
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092
--doris.username root
--doris.password 123456
--doris.url jdbc:mysql://172.24.100.150:9131/vcenter_db?characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true
--doris.driver com.mysql.cj.jdbc.Driver
--doris.query.interval 60
 */

public class DimBaseAPP extends BaseAPP {

    public static void main(String[] args) throws Exception{
        new DimBaseAPP().start(10001,  "dim_base_app_1", TopicConstant.TOPIC_ODS_TELEGRAF,args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

        // python采集到的telegraf的数据 过滤 分组
        KeyedStream<VmInfo, String> dimPythonVmDS = streamSource
                .process(new VmPythonProcessFunction())
                .keyBy(VmInfo::getEventPartitionKey);

        DataStreamSource<VcenterParam> vcenterParamSource =
                env.addSource(new DorisVcenterConfigParamSource());

        SingleOutputStreamOperator<String> dimDS =
                vcenterParamSource.name("from-doris-get-vcenter-param")
                        .keyBy(VcenterParam::getUrl)
                        .process(new HttpVcenterDimProcessFunction())
                        .name("get-restApi-vcenter-info");

        SingleOutputStreamOperator<VmInfo> dimHttpVmDS = dimDS
                .map(line -> JSON.parseObject(line, VmInfo.class));

        BroadcastStream<VmInfo> broadcast =
                dimHttpVmDS.broadcast(FlinkStateDescUtils.getHttpVMInfoBroadcastDesc);

        // vm 对应的 python 维度和 http 维度合并
        SingleOutputStreamOperator<String> dimVmDS = dimPythonVmDS
                .connect(broadcast)
                .process(new DimVmMergeProcessFunction());


        //*********************************************************vm_datastore维度的数据********************************
        // 测流 获取vm_dataStore维度数据
        DataStream<String> dimVmDataStore =
                dimVmDS.getSideOutput(new OutputTag<>(ObjectTypeConstant.DIM_DATASTORE, TypeInformation.of(String.class)));


        //*********************************************************host维度的数据*****************************************

        // 测流 获取host维度数据
        KeyedStream<HostInfo,String> dimVcenterHostDS =
                dimDS
                        .getSideOutput(new OutputTag<>("dim_host",TypeInformation.of(String.class)))
                        .map(line -> JSON.parseObject(line, HostInfo.class))
                        .keyBy(HostInfo::getEventPartitionKey);

        SingleOutputStreamOperator<HostInfo> hostPythonDS =
                env.addSource(new DorisHostPythonSource())
                .keyBy(new KeySelector<HostInfo, String>() {
                    @Override
                    public String getKey(HostInfo hostInfo) throws Exception {
                        return hostInfo.getVcenter() + "_" + hostInfo.getHostId();
                    }
                })
                .map(bean -> bean);


        BroadcastStream<HostInfo> hostPythonBroadcast =
                hostPythonDS.broadcast(FlinkStateDescUtils.getHttpCSHostInfoBroadcastDesc);

        // host 对应的 vcenter 维度和 Horizon 维度合并
        SingleOutputStreamOperator<String> dimHostDS = dimVcenterHostDS
                .connect(hostPythonBroadcast)
                .process(new DimHostMergeProcessFunction());

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            dimVmDS.print(">vm>");
            dimVmDataStore.print(">vmdatastore>");
            dimHostDS.print(">host>");
        }else{
            // 写入kafka vm维度数据
            dimVmDS.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DIM_VM)).name("sink_dim_vm_topic");
            // 写入kafka host维度数据
            dimVmDataStore.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DIM_DATASTORE)).name("sink_dim_vm_datastore_topic");
            // 写入kafka vmdatastore维度数据
            dimHostDS.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DIM_HOST)).name("sink_dim_host_topic");
        }

    }


}
