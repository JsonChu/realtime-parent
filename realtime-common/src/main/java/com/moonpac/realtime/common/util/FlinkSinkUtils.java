package com.moonpac.realtime.common.util;



import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;


public class FlinkSinkUtils {

    public static KafkaSink<String> getKafkaSink( ParameterTool parameter,
                                                 String topicName){
        return KafkaSink.<String>builder()
                .setBootstrapServers(parameter.get("kafka.broker"))
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.<String>builder()
                            .setTopic(topicName)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

    }

}
