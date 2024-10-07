package com.moompac.dim.app;

import com.alibaba.fastjson.JSON;

import com.moompac.dim.function.VmLifeCycleKeyedProcessFunction;
import com.moonpac.realtime.common.base.BaseAPP;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wenss
 * @date 2024/04/09 10:33
 * 1.首次启动：从kafka dim_vm_topic获取vm power_state和开机时间，将数据发送到kafka dwd_vm_lifecycle_topic，并将状态保存到flink中
 * 2.消费 dim_vm_topic 与上次状态数据做对比
 * 启动参数：
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092
--doris.username root
--doris.password 123456
--doris.url jdbc:mysql://172.24.100.150:9131/vcenter_db?characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true
--doris.driver com.mysql.cj.jdbc.Driver

 */
@Slf4j
public class DimVmLifeCycleAPP extends BaseAPP {

    public static void main(String[] args) throws Exception{
        new DimVmLifeCycleAPP().start(10002,  "dim_vm_life_cycle", TopicConstant.TOPIC_DIM_VM,args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

        // 消费vm维度topic
        SingleOutputStreamOperator<String> result = streamSource
                .map(line -> JSON.parseObject(line, VmInfo.class))
                .keyBy(VmInfo::getEventPartitionKey)
                .process(new VmLifeCycleKeyedProcessFunction())
                .map(JSON::toJSONString);;

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DWD_VM_LIFECYCLE)).name("sink_life_cycle_topic");
        }

    }
}
