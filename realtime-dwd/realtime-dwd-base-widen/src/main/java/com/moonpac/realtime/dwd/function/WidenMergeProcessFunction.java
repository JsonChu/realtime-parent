package com.moonpac.realtime.dwd.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class WidenMergeProcessFunction extends KeyedBroadcastProcessFunction<String, KafkaVcenterInputBean, VcenterInfo, String> {
    @Override
    public void processElement(KafkaVcenterInputBean kafkaVcenterInputBean, KeyedBroadcastProcessFunction<String,
            KafkaVcenterInputBean, VcenterInfo, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

        ReadOnlyBroadcastState<String, VcenterInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getDIMVcenterVMInfoBroadcastDesc);

        VcenterInfo vcenterInfo = broadcastState.get(kafkaVcenterInputBean.getEventPartitionKey());
        if ( null == vcenterInfo){
            log.warn("ODS topic 维度拉宽 写入Widen topic，【 事实数据已经到来，但是维度数据没有到来，下次合并。】，事实数据={}",
                    JSON.toJSONString(kafkaVcenterInputBean));
        }else {
            // 合并数据
            kafkaVcenterInputBean.setVcenterInfo(vcenterInfo);
            out.collect(JSON.toJSONString(kafkaVcenterInputBean));
        }

    }

    @Override
    public void processBroadcastElement(VcenterInfo vcenterDIMInfo, KeyedBroadcastProcessFunction<String,
            KafkaVcenterInputBean, VcenterInfo, String>.Context ctx, Collector<String> collector) throws Exception {

        // 从上下文中，获取广播状态对象（可读可写的状态对象）
        BroadcastState<String, VcenterInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getDIMVcenterVMInfoBroadcastDesc);
        broadcastState.put(vcenterDIMInfo.getEventPartitionKey(),vcenterDIMInfo);

    }
}
