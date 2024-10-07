package com.moompac.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.moompac.realtime.dws.function.AGGKeyedProcessFunction;
import com.moompac.realtime.dws.function.GroupAggProcessFunction;
import com.moonpac.realtime.common.base.BaseAPP;
import com.moonpac.realtime.common.bean.dws.AggBean;
import com.moonpac.realtime.common.bean.dws.WidenResultBean;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhanglingxing
 * @date 2024/4/16 9:49
 *启动
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092
 */

public class DwsAggAPP extends BaseAPP {
    public static void main(String[] args) throws Exception {
        new DwsAggAPP().start(10022,"dws_merge_app", TopicConstant.TOPIC_DWS_MERGE,args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

        KeyedStream<AggBean, String> keyedStream=  streamSource
                .map(line -> JSON.parseObject(line, WidenResultBean.class))
                //  .filter(bean ->  "datastore-disk".equals(bean.getObjectType()) || "datastore-datastore".equals(bean.getObjectType()) ) //  "datastore".equals(bean.getObjectType())
                .process(new GroupAggProcessFunction())
                .keyBy(AggBean::getEventPartitionKey);

        SingleOutputStreamOperator<String> result = keyedStream
                .process(new AGGKeyedProcessFunction())
                .map(JSON::toJSONString);
        //result.print(">>>");

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DWS_AGG)).name("sink_agg_topic");
        }

    }
}
