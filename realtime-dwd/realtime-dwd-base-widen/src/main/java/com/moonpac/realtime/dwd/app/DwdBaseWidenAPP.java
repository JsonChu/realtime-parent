package com.moonpac.realtime.dwd.app;

import com.moonpac.realtime.common.base.BaseAPP;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.constant.ObjectTypeConstant;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import com.moonpac.realtime.dwd.function.WidenMergeProcessFunction;
import com.moonpac.realtime.dwd.function.WidenProcessFunction;
import com.moonpac.realtime.dwd.source.DorisDimSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhanglingxing
 * @date 2024/4/15 14:22
 * 事实表添加维度写入widen
 *程序启动
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092
--doris.username root
--doris.password 123456
--doris.url jdbc:mysql://172.24.100.150:9131/vcenter_db?characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true
--doris.driver com.mysql.cj.jdbc.Driver
--doris.query.interval 60
 */

public class DwdBaseWidenAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new DwdBaseWidenAPP().start(10011,"dwd_base_widen_app", TopicConstant.TOPIC_ODS_TELEGRAF,args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

        // telegraf采集到的vcenter数据
        KeyedStream<KafkaVcenterInputBean, String> keyedStream = streamSource
                .name("kafka-ods-vcenter")
                .process(new WidenProcessFunction())
                .keyBy(KafkaVcenterInputBean::getEventPartitionKey);
        //         keyedStream.map(JSON::toJSONString).print("----->");

        DataStreamSource<VcenterInfo> dataStreamSource =
                env.addSource(new DorisDimSource(ObjectTypeConstant.DIM_VM,ObjectTypeConstant.DIM_HOST,ObjectTypeConstant.DIM_DATASTORE));
        //dataStreamSource.map(JSON::toJSONString).print("----->");

        BroadcastStream<VcenterInfo> broadcast =
                dataStreamSource.broadcast(FlinkStateDescUtils.getDIMVcenterVMInfoBroadcastDesc);

        SingleOutputStreamOperator<String> result = keyedStream
                .connect(broadcast)
                .process(new WidenMergeProcessFunction());

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DWD_WIDEN)).name("sink_widen_topic");
        }

    }
}
