package com.moonpac.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.base.BaseAPP;
import com.moonpac.realtime.common.bean.dws.meger.MergeAutoCompute;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.constant.RuleMergeConstant;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import com.moonpac.realtime.dws.function.MergeAutoBroadcastProcessFunction;
import com.moonpac.realtime.dws.function.MergeAutoKeyedBroadcastProcessFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author zhanglingxing
 * @date 2024/4/15 15:17
 *
 * 动态聚合
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092

 */

public class DwsMergeAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new DwsMergeAPP().start(10021,"dws_merge_app", TopicConstant.TOPIC_DWD_WIDEN,args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

        SingleOutputStreamOperator<KafkaVcenterInputBean> odsDS = streamSource
                .map(line -> JSONObject.parseObject(line, KafkaVcenterInputBean.class)).name("kafka-ods-vcenter");

        // todo 临时方案  后期写入mysql cdc 的库
        MergeAutoCompute ruleMerge  = JSON.parseObject(RuleMergeConstant.widen2MergeRule,MergeAutoCompute.class);

        List<MergeAutoCompute> mergeAutoComputes = Collections.singletonList(ruleMerge);
        DataStreamSource<MergeAutoCompute> mergeAutoComputeDataStreamSource = env.fromCollection(mergeAutoComputes);

        // 动态合并拉宽的维度数据
        BroadcastStream<MergeAutoCompute> broadcast =
                mergeAutoComputeDataStreamSource.broadcast(FlinkStateDescUtils.mergeAutoComputeBroadcastDesc);

        // 动态过滤  动态分组 动态去重【新增去重字段】
        KeyedStream<KafkaVcenterInputBean, String> keyedStream = odsDS
                .connect(broadcast)
                .process(new MergeAutoBroadcastProcessFunction())
                .keyBy(KafkaVcenterInputBean::getEventPartitionKey);

        // 动态 合并
        SingleOutputStreamOperator<String> result =
                keyedStream
                        .connect(broadcast)
                        .process(new MergeAutoKeyedBroadcastProcessFunction())
                        .map(JSON::toJSONString);

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DWS_MERGE)).name("sink_merge_topic");
        }

    }
}
