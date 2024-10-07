package com.moompac.feature.metric.engine.app;

import com.alibaba.fastjson.JSON;
import com.moompac.feature.metric.engine.function.MetricRuleBroadcastProcessFunction;
import com.moonpac.realtime.common.bean.dws.MergeEventBean;
import com.moonpac.realtime.common.bean.dws.WidenResultBean;
import com.moonpac.realtime.common.bean.metric.MetricRuleInfo;
import com.moonpac.realtime.common.constant.TopicConstant;
import com.moonpac.realtime.common.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.util.List;

/**
 * @author zhanglingxing
 * @date 2024/4/16 10:42
程序启动
--kafka.broker 172.24.100.150:9092,172.24.100.151:9092,172.24.100.152:9092
--oracle.driver oracle.jdbc.driver.OracleDriver
--oracle.url jdbc:oracle:thin:@10.173.28.69:1521/TEST
--metric.oracle.username QyMjTVBfQUlPUFNfTUVUUklD
--metric.oracle.password bXBfYXV0b19vcHM=
--run.mode test
 */

@Slf4j
public class MetricEngineAPP {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 封装参数
        ParameterTool parameter = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameter);

        String kafkaServer = parameter.get("kafka.broker");
        String driver = parameter.get("oracle.driver");
        String url = parameter.get("oracle.url");
        String username = parameter.get("metric.oracle.username");
        String password = parameter.get("metric.oracle.password");
        String runMode = parameter.get("run.mode");

        log.info("参数打印： kafka.broker={}", kafkaServer);
        log.info("参数打印： metric.oracle.driver={}", driver);
        log.info("参数打印： metric.oracle.url={}", url);
        log.info("参数打印： metric.oracle.username={}", Base64Utils.decode(username));
        log.info("参数打印： metric.oracle.password={}",Base64Utils.decode(password));

        // 规则数据
        DataStream<MetricRuleInfo> metricRuleInfoStream;
        List<MetricRuleInfo> oracleMetric = JdbcUtils.getOracleMetricRule(driver, url, Base64Utils.decode(username), Base64Utils.decode(password));
        if (oracleMetric.size() > 0){
            // 从oracle库 初始化规则
            DataStreamSource<MetricRuleInfo> initMetricRuleStream =  env.fromCollection(oracleMetric);
            metricRuleInfoStream = FlinkSourceUtils
                    .createKafkaDataStream(env, kafkaServer, "feature_metric_engine",TopicConstant.TOPIC_METRIC_RULE)
                    .name("kafka-rule-source")
                    .map(line -> JSON.parseObject(line,MetricRuleInfo.class))
                    .union(initMetricRuleStream);// 将oracleMetric数据添加到ruleStream中
        }else {
            metricRuleInfoStream =  FlinkSourceUtils
                            .createKafkaDataStream(env, kafkaServer, "feature_metric_engine",TopicConstant.TOPIC_METRIC_RULE)
                            .name("kafka-rule-source")
                            .map(line -> JSON.parseObject(line,MetricRuleInfo.class));
            log.error("没有加载到oracle的指标规则");
        }

        // 将规则数据流广播出去  < objecttype,List<规则名称> >
        BroadcastStream<MetricRuleInfo> metricRuleBroadcast = metricRuleInfoStream.broadcast(FlinkStateDescUtils.metricRuleStateDesc);

        // 日志数据
        SingleOutputStreamOperator<MergeEventBean> eventStream = FlinkSourceUtils
                .createKafkaDataStream(env, kafkaServer, "feature_metric_engine",TopicConstant.TOPIC_DWS_MERGE)
                .name("kafka-dwd-merge-source")
                .process(new ProcessFunction<String, MergeEventBean>() {
                    @Override
                    public void processElement(String line, ProcessFunction<String, MergeEventBean>.Context context,
                                               Collector<MergeEventBean> out) throws Exception {
                        WidenResultBean widenResultBean = JSON.parseObject(line, WidenResultBean.class);
                        MergeEventBean mergeEventBean = widenResultBean.getMergeEventBean();
                        if (null != mergeEventBean){
                            out.collect(mergeEventBean);
                        }
                    }
                });

        // 完成关联 将规则进行 将规则数据放到广播变量中
        SingleOutputStreamOperator<String> result = eventStream
                .connect(metricRuleBroadcast)
                .process(new MetricRuleBroadcastProcessFunction(runMode));

        if (env instanceof LocalStreamEnvironment) {  // 在本地测试运行的逻辑
            result.print(">result>");
        }else{
            // 写入kafka
            result.sinkTo(FlinkSinkUtils.getKafkaSink(parameter, TopicConstant.TOPIC_METRIC_RESULT )).name("sink_metric_result_topic");
        }

        env.execute("MetricEngine");

    }

}
