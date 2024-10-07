package com.moonpac.realtime.dwd.app;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dwd.SqlServerConnectionConfig;
import com.moonpac.realtime.common.bean.dwd.SqlServerEventData;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.FlinkSinkUtils;
import com.moonpac.realtime.common.util.FlinkUtils;
import com.moonpac.realtime.common.util.JdbcUtils;
import com.moonpac.realtime.dwd.function.CSEventMapFunction;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author zhanglingxing
 * @date 2023/11/7 10:51
 * 读取sqlServer的数据导入kafka中
 *消费kafka数据的命令
 * kafka-console-consumer --bootstrap-server 10.173.28.35:9092 --topic cs_event_topic
 * to_timestamp_ltz(`Time`,3) 将毫秒值转换为时间戳
 * to_timestamp_ltz(`Time`,0) 将秒值转换为时间戳
 *
 * 程序执行参数：
 * --sqlserver.ip 172.24.100.22 --sqlserver.port 1433 --sqlserver.database.list vdicsdb --sqlserver.table.list dbo.event --sqlserver.username sa --sqlserver.password Iop[]=-09*  --doris.ip 10.173.28.35:8036,10.173.28.36:8036,10.173.28.37:8036 --doris.username root --doris.password 123456 --sqlserver.time.offset -28800000

{
    "sqlServers": [{
    "name": "Server1",
    "ip": "172.24.100.22",
    "port": 1433,
    "username": "sa",
    "password": "Iop[]=-09*",
    "database": "vdicsdb",
    "table": "dbo.event"
    }]
}

 */


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

 */

@Slf4j
public class DwdSqlServerEventAPP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = FlinkUtils.setEnvParameters(env, args);

        // cs初始化的规则库
        String driver = parameters.get("doris.driver");
        String url = parameters.get("doris.url");
        String username = parameters.get("doris.username");
        String password = parameters.get("doris.password");
        List<SqlServerConnectionConfig> configs =
                JdbcUtils.getDorisDatabaseCSEventData(driver, url, username, password);
        for (SqlServerConnectionConfig config : configs) {
            log.info("获取到的cs参数:{}",config);
            SqlServerSourceBuilder.SqlServerIncrementalSource<String> sqlServerSource =
                    new SqlServerSourceBuilder()
                            .hostname(config.getIp())
                            .port(config.getPort())
                            .serverTimeZone("Asia/Shanghai") // 设置上海时区
                            .databaseList( config.getDatabase() )
                            .tableList( config.getTable() )
                            .username(config.getUsername())
                            .password(config.getPassword())
                            .deserializer(new JsonDebeziumDeserializationSchema())
                            .startupOptions(StartupOptions.initial())
                            .build();

            DataStreamSource<String> source = env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(),
                    "SqlServerIncrementalSource");

            SingleOutputStreamOperator<String> result = source
                    .map(line -> JSON.parseObject(line, SqlServerEventData.class))
                    .returns(Types.POJO(SqlServerEventData.class))
                    .filter(bean -> {
                        String op = bean.getOp();
                        return "c".equals(op) || "r".equals(op); // c:代表新增 r:代表全量读取的数据
                    }).map(new CSEventMapFunction(config));

            if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
                result.print(">result>");
            }else{
                // 写入kafka
                result.sinkTo(FlinkSinkUtils.getKafkaSink(parameters,TopicConstant.TOPIC_CS_EVENT)).name("sink_cs_event_topic");
            }
        }
        env.execute("Sqlserver2kafka");
    }
}
