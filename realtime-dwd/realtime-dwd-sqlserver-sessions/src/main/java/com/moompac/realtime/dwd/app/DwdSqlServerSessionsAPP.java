package com.moompac.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.moompac.realtime.dwd.function.DatabaseQueryKeyedProcessFunction;
import com.moompac.realtime.dwd.source.DorisDatabaseSource;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.moonpac.realtime.common.bean.dwd.DatabaseParam;

/**
 * @author zhanglingxing
 * @date 2024/4/15 14:22
 * 事实表添加维度写入widen
 *程序启动
--doris.username root
--doris.password 123456
--doris.url jdbc:mysql://172.24.100.150:9131/vcenter_db?characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true
--doris.driver com.mysql.cj.jdbc.Driver
--doris.query.interval 60
 */

public class DwdSqlServerSessionsAPP{

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameter);

        SingleOutputStreamOperator<String> result =
                env.addSource(new DorisDatabaseSource()).name("from-doris-get-database")
                        .keyBy(DatabaseParam::getUrl)
                        .process(new DatabaseQueryKeyedProcessFunction()).name("getRestApiVcenterInfo")
                        .map(JSON::toJSONString);
//        result.print(">>>>");

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_DWS_MERGE)).name("sink_dwd_merge_topic");
        }

        env.execute("DwdSqlServerSessionsAPP");

    }

}
