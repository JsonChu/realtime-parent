package com.moonpac.realtime.dwd.app;

import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.base.BaseAPP;
import com.moonpac.realtime.common.bean.dwd.SqlServerAfterEventData;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import com.moonpac.realtime.dwd.function.VdiLatestStatusKeyedProcessFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhanglingxing
 * @date 2024/4/17 15:02
 *启动参数：
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092
--doris.username root
--doris.password 123456
--doris.url jdbc:mysql://172.24.100.150:9131/vcenter_db?characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true
--doris.driver com.mysql.cj.jdbc.Driver
 */

public class DwdVdiLatestStatusAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new DwdVdiLatestStatusAPP().start(10011,"dwd_vdi_latest_status", TopicConstant.TOPIC_CS_EVENT,args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

        SingleOutputStreamOperator<String> result = streamSource
                .map(line -> JSONObject.parseObject(line, SqlServerAfterEventData.class))
                .keyBy(new KeySelector<SqlServerAfterEventData, String>() {
                    @Override
                    public String getKey(SqlServerAfterEventData sqlServerAfterEventData) throws Exception {
                        String vcenter = sqlServerAfterEventData.getVcenter();
                        String csip = sqlServerAfterEventData.getCsip();
                        String vdi = sqlServerAfterEventData.getNode();
                        return vcenter + "_" + csip + "_" + vdi;
                    }
                })
                .process(new VdiLatestStatusKeyedProcessFunction());

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_VDI_LATEST_STATUS)).name("sink_vdi_latest_status_topic");
        }

    }
}
