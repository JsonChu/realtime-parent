package com.moonpac.realtime.dws.function;


import com.moonpac.realtime.common.bean.dws.meger.KeyAutoCompute;
import com.moonpac.realtime.common.bean.dws.meger.MergeAutoCompute;
import com.moonpac.realtime.common.bean.dws.meger.ObjectAGGCompute;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.util.AutoMergeOgnlUtils;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

@Slf4j
public class MergeAutoBroadcastProcessFunction extends BroadcastProcessFunction<KafkaVcenterInputBean,
        MergeAutoCompute, KafkaVcenterInputBean> {

    @Override
    public void processElement(KafkaVcenterInputBean kafkaVcenterInputBean, BroadcastProcessFunction<KafkaVcenterInputBean,
            MergeAutoCompute, KafkaVcenterInputBean>.ReadOnlyContext ctx, Collector<KafkaVcenterInputBean> out) throws Exception {

        // 动态合并的规则数据
        ReadOnlyBroadcastState<String, MergeAutoCompute> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.mergeAutoComputeBroadcastDesc);

        for (Map.Entry<String, MergeAutoCompute> next : broadcastState.immutableEntries()) {
            MergeAutoCompute mergeAutoCompute = next.getValue();
            List<ObjectAGGCompute> objectAGGCompute = mergeAutoCompute.getObjectAGGCompute();
            for (ObjectAGGCompute aggCompute : objectAGGCompute) {
                List<KeyAutoCompute> groupKeyList = aggCompute.getGroupKey();
                for (KeyAutoCompute keyAutoCompute : groupKeyList) {
                    String eventNameExpression = keyAutoCompute.getEventNameExpression();
                    String groupExpression = keyAutoCompute.getGroupExpression();
                    String uniqueKeyExpression = keyAutoCompute.getUniqueKeyExpression();
                    boolean filterResult = AutoMergeOgnlUtils.getBooleanValue(eventNameExpression,kafkaVcenterInputBean);
                    if (filterResult){
                        String eventPartitionKey = AutoMergeOgnlUtils.getStringValue(groupExpression,kafkaVcenterInputBean);
                        kafkaVcenterInputBean.setEventPartitionKey(eventPartitionKey);
                        String uniqueKey = AutoMergeOgnlUtils.getStringValue(uniqueKeyExpression,kafkaVcenterInputBean);
                        kafkaVcenterInputBean.setUniqueKey(uniqueKey);
                        kafkaVcenterInputBean.setObjectType(aggCompute.getObjectType());
                        out.collect(kafkaVcenterInputBean);
                    }
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(MergeAutoCompute mergeAutoCompute, BroadcastProcessFunction<KafkaVcenterInputBean,
            MergeAutoCompute, KafkaVcenterInputBean>.Context ctx, Collector<KafkaVcenterInputBean> collector) throws Exception {

        // 从上下文中，获取广播状态对象（可读可写的状态对象）
        BroadcastState<String, MergeAutoCompute> broadcastState = ctx.getBroadcastState(FlinkStateDescUtils.mergeAutoComputeBroadcastDesc);
        if ("1".equals(mergeAutoCompute.getRuleStatus())){ // 代表上线
            broadcastState.put(mergeAutoCompute.getRuleCode(),mergeAutoCompute);
        }else if ("0".equals(mergeAutoCompute.getRuleStatus())){
            broadcastState.remove(mergeAutoCompute.getRuleCode());
        }

    }
}
