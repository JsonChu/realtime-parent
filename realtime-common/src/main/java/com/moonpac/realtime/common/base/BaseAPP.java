package com.moonpac.realtime.common.base;

import com.moonpac.realtime.common.util.FlinkSourceUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import lombok.extern.slf4j.Slf4j;
/**
 * @author zhanglingxing
 * @date 2024/4/14 17:50
 *
 * BaseApp的设计初衷
 *  因为flink编程的都是 source - Transformation - sink 编程模式，所有使用抽象类进行封装
 */
@Slf4j
public abstract  class BaseAPP {

    public abstract void handle(StreamExecutionEnvironment env,
                 DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception;


    public void start(int port, String ckAndGroupId, String topic,String[] args) throws Exception {

        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 flink
        System.setProperty("HADOOP_USER_NAME", "flink");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //1.3  将ParameterTool的参数设置成全局的参数
        ParameterTool parameter = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameter);
        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 1.4.2 开启 checkpoint
        env.enableCheckpointing(300000);
        // 1.4.3 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.4 checkpoint 存储
        if (env instanceof LocalStreamEnvironment) {  // 在本地运行的逻辑
            HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
            env.setStateBackend(hashMapStateBackend); // 使用HashMapStateBackend  作为状态后端
            env.getCheckpointConfig().setCheckpointStorage("file:///D:\\tmp\\flink-checkpoints");
            log.info("flink提交作业模式：--本地");
        } else { // 在集群运行的逻辑
            EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
            env.setStateBackend(embeddedRocksDBStateBackend);  // 设置 EmbeddedRocksDBStateBackend 作为状态后端
            env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/flink-checkpoints");
            log.info("flink提交作业模式：--集群");
        }
        // 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.6 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 1.4.7 checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        // 1.4.8 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // 1.5 从 Kafka 目标主题读取数据，封装为流
        String kafkaServer = parameter.get("kafka.broker");
        KafkaSource<String> source = FlinkSourceUtils.getKafkaSource(kafkaServer,ckAndGroupId, topic);

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 2. 核心业务处理逻辑
        handle(env, stream,parameter);

        env.execute();

    }

}
