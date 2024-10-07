package com.moonpac.realtime.common.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FlinkSourceUtils {
    public static KafkaSource<String> getKafkaSource(String kafkaServers,String groupId,String topics) {

        List<String> topicList = Arrays.asList(topics.split(","));
        // 初始化一个kafka souce
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics(topicList) // 设置topic主题
                .setGroupId(groupId)//设置组ID
                .setBootstrapServers(kafkaServers)
                // setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)) 消费起始位置选择之前提交的偏移量（如果没有，则重置为latest）
                // .setStartingOffsets(OffsetsInitializer.latest()) 最新的
                // .setStartingOffsets(OffsetsInitializer.earliest()) 最早的
                // .setStartingOffsets(OffsetsInitializer.offsets(Map< TopicPartition,Long >)) 之前具体的偏移量进行消费 每个分区对应的偏移量
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {  // 修复value==null 的bug
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                // 默认是关闭的
                // 开启了kafka的自动提交偏移量机制 会把偏移量提交到 kafka的 consumer_offsets中
                // 就算算子开启了自动提交偏移量机制，kafkaSource依然不依赖自动提交的偏移量（优先从flink自动管理的状态中获取对应的偏移量 如果获取不到就会用自动提交的偏移量）
                //将本source算子设置为 Bounded属性（有界流），将来改source去读取数据的时候，读到指定的位置，就停止读取并退出程序
                .setProperty("auto.offset.commit", "ture")
                .build();

        return kafkaSource;

    }

    public static  DataStreamSource<String> createKafkaDataStream(StreamExecutionEnvironment env,String kafkaServers,
                                                                String groupId, String topics) throws Exception {
        KafkaSource<String> kafkaSource = getKafkaSource(kafkaServers, groupId, topics);
        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
    }
}