package com.moonpac.realtime.dim.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dim.DataStoreInfo;
import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.constant.ObjectTypeConstant;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class DimHostMergeProcessFunction extends KeyedBroadcastProcessFunction<String, HostInfo, HostInfo, String> {

    private transient ValueState<HostInfo> dimVCHostInfoState;

    // key尝试合并的次数 如果 超过一定的次数 则取消合并，避免程序压力
    private transient ValueState<Integer> attemptsNumber;

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        dimVCHostInfoState = runtimeContext.getState(FlinkStateDescUtils.vcenterHostInfoStateDes);
        attemptsNumber = runtimeContext.getState(FlinkStateDescUtils.attemptsHostNumberStateDes);
    }


    @Override
    public void processElement(HostInfo dimVCHostInfo, KeyedBroadcastProcessFunction<String, HostInfo,
            HostInfo, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

        ReadOnlyBroadcastState<String, HostInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getHttpCSHostInfoBroadcastDesc);

        HostInfo dimHostPythonInfo = broadcastState.get(ctx.getCurrentKey());
        if ( null == dimHostPythonInfo ){
            if (null == dimVCHostInfoState.value()){
                long triggerTime = ctx.timerService().currentProcessingTime() + 60 * 1000; // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }
            // 将数据放入缓存中
            dimVCHostInfoState.update(dimVCHostInfo);
            log.warn("host维度触发延迟合并机制，【host对应的vcenter维度到来了，但是python采集的host维度还没有获取到，60秒后，再次尝试合并。】，key={}",ctx.getCurrentKey());
        }else {
            // 合并数据
            dimVCHostInfo.setCsCpuCoreCount(dimHostPythonInfo.getCsCpuCoreCount());
            dimVCHostInfo.setCsMemorySizemb(dimHostPythonInfo.getCsMemorySizemb());
            dimVCHostInfo.setCsCpuMhz(dimHostPythonInfo.getCsCpuMhz());
            out.collect(JSON.toJSONString(dimVCHostInfo));
            log.info("host维度合并成功，key={}",ctx.getCurrentKey());
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, HostInfo, HostInfo, String>.OnTimerContext ctx,
                        Collector<String> out) throws Exception {
        String currentKey = ctx.getCurrentKey();
        ReadOnlyBroadcastState<String, HostInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getHttpCSHostInfoBroadcastDesc);
        HostInfo dimCSHostInfo = broadcastState.get(ctx.getCurrentKey());
        if ( null == dimCSHostInfo){

            int count = attemptsNumber.value() == null ? 1 : attemptsNumber.value() ;
            log.warn("host维度延迟合并，尝试的次数：{},【host对应的vcenter维度到来了，但是python采集的host维度还没有获取到，60秒后，再次尝试合并。】，key={}",count,currentKey);
            attemptsNumber.update(count+1);
            if (count > 199 ){
                log.warn("host维度延迟合并取消，尝试的次数达到了系统最大值：{},【host对应的vcenter维度到来了，但是python采集的host维度还没有获取到】，key={}",count,currentKey);
            }else{
                // 将数据放入缓存中 再次注册定时器
                long triggerTime = ctx.timerService().currentProcessingTime() + 60 * 1000; // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }

        }else{
            // 合并数据
            HostInfo dimVCHostInfo = dimVCHostInfoState.value();
            dimVCHostInfo.setCsCpuCoreCount(dimCSHostInfo.getCsCpuCoreCount());
            dimVCHostInfo.setCsMemorySizemb(dimCSHostInfo.getCsMemorySizemb());
            dimVCHostInfo.setCsCpuMhz(dimCSHostInfo.getCsCpuMhz());
            out.collect(JSON.toJSONString(dimVCHostInfo));
            log.warn("host维度延迟合并成功，key={}",ctx.getCurrentKey());
        }

    }

    @Override
    public void processBroadcastElement(HostInfo dimHostPythonInfo, KeyedBroadcastProcessFunction<String, HostInfo,
            HostInfo, String>.Context ctx, Collector<String> collector) throws Exception {

        // 从上下文中，获取广播状态对象（可读可写的状态对象）
        BroadcastState<String, HostInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getHttpCSHostInfoBroadcastDesc);
        broadcastState.put(dimHostPythonInfo.getEventPartitionKey(),dimHostPythonInfo);
    }
}
