package com.moonpac.realtime.dim.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dim.DataStoreInfo;
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

/**
 * @author zhanglingxing
 * @date 2024/4/16 16:51
 * 优化功能：
 *  python数据到来的话 如果http的数据没有到来 放入state 等待
 */

@Slf4j
public class DimVmMergeProcessFunction extends KeyedBroadcastProcessFunction<String, VmInfo, VmInfo, String> {

    private transient ValueState<VmInfo> pythonVmInfoState;

    // key尝试合并的次数 如果 超过一定的次数 则取消合并，避免程序压力
    private transient ValueState<Integer> attemptsNumber;

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        pythonVmInfoState = runtimeContext.getState(FlinkStateDescUtils.pythonVmInfoStateDes);
        attemptsNumber = runtimeContext.getState(FlinkStateDescUtils.attemptsVmNumberStateDes);
    }

    @Override
    public void processElement(VmInfo dimPythonVmInfo, KeyedBroadcastProcessFunction<String,
            VmInfo, VmInfo, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

        String currentKey = ctx.getCurrentKey();
        ReadOnlyBroadcastState<String, VmInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getHttpVMInfoBroadcastDesc);
        VmInfo dimHttpVmInfo = broadcastState.get(currentKey);
        if ( null == dimHttpVmInfo ){

            // 将数据放入缓存中
            if (null == pythonVmInfoState.value()){
                long triggerTime = ctx.timerService().currentProcessingTime() + 60 * 1000; // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }
            pythonVmInfoState.update(dimPythonVmInfo);
            log.warn("vm维度触发延迟合并机制，【python维度数据已经到来，http维度数据没有到来，60秒后，再次尝试合并。】，key={}",ctx.getCurrentKey());
        }else {

            VmInfo vmInfo = mergeVmInfo(dimPythonVmInfo, dimHttpVmInfo);
            out.collect(JSON.toJSONString(vmInfo));

            // 同时输出 dim_vm_dataStore_info维度信息
            String pythonDatastoreIds = vmInfo.getPythonDatastoreIds();
            String[] split = pythonDatastoreIds.split(",");
            for (String datastoreId : split) {
                DataStoreInfo dimvmDataStoreInfo = new DataStoreInfo();
                BeanUtils.copyProperties(dimvmDataStoreInfo,vmInfo);
                dimvmDataStoreInfo.setPythonDatastoreId(datastoreId);
                dimvmDataStoreInfo.setObjectType(ObjectTypeConstant.DIM_DATASTORE);
                ctx.output(new OutputTag<>(ObjectTypeConstant.DIM_DATASTORE, TypeInformation.of(String.class)),
                        JSON.toJSONString(dimvmDataStoreInfo));
            }
            //log.info("vm维度合并成功，key={}",ctx.getCurrentKey());
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, VmInfo, VmInfo, String>.OnTimerContext ctx,
                        Collector<String> out) throws Exception {

        String currentKey = ctx.getCurrentKey();
        ReadOnlyBroadcastState<String, VmInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getHttpVMInfoBroadcastDesc);
        VmInfo dimHttpVmInfo = broadcastState.get(currentKey);
        if ( null == dimHttpVmInfo){
            int count = attemptsNumber.value() == null ? 1 : attemptsNumber.value() ;
            if (count > 199 ){
                log.warn("vm维度延迟合并取消，尝试的次数达到了系统最大值：{},【python维度数据已经到来，http维度数据没有到来】，key={}",count,currentKey);
            }else{
                log.warn("vm维度延迟合并，尝试的次数：{},【python维度数据已经到来，http维度数据没有到来，60秒后，再次尝试合并。】，key={}",count,currentKey);
                attemptsNumber.update(count+1);
                // 将数据放入缓存中 再次注册定时器
                long triggerTime = ctx.timerService().currentProcessingTime() + 60 * 1000; // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }

        }else{
            // 何婷

            VmInfo vmInfo = mergeVmInfo(pythonVmInfoState.value(), dimHttpVmInfo);
            out.collect(JSON.toJSONString(vmInfo));

            // 同时输出 dim_vm_dataStore_info维度信息
            String pythonDatastoreIds = vmInfo.getPythonDatastoreIds();
            String[] split = pythonDatastoreIds.split(",");
            for (String datastoreId : split) {
                DataStoreInfo dimvmDataStoreInfo = new DataStoreInfo();
                BeanUtils.copyProperties(dimvmDataStoreInfo,vmInfo);
                dimvmDataStoreInfo.setPythonDatastoreId(datastoreId);
                dimvmDataStoreInfo.setObjectType(ObjectTypeConstant.DIM_DATASTORE);
                ctx.output(new OutputTag<>(ObjectTypeConstant.DIM_DATASTORE, TypeInformation.of(String.class)),
                        JSON.toJSONString(dimvmDataStoreInfo));
            }
            log.warn("vm维度延迟合并成功，key={}",ctx.getCurrentKey());
        }
    }

    private static VmInfo mergeVmInfo(VmInfo dimPythonVmInfo, VmInfo dimHttpVmInfo) {
        // 合并数据
        dimPythonVmInfo.setObjectType(dimHttpVmInfo.getObjectType());
        dimPythonVmInfo.setHttpRegion(dimHttpVmInfo.getHttpRegion());
        dimPythonVmInfo.setHttpVcenter(dimHttpVmInfo.getHttpVcenter());
        dimPythonVmInfo.setHttpVmMoid(dimHttpVmInfo.getHttpVmMoid());
        dimPythonVmInfo.setHttpVmName(dimHttpVmInfo.getHttpVmName());
        dimPythonVmInfo.setHttpIsVdi(dimHttpVmInfo.getHttpIsVdi());
        dimPythonVmInfo.setHttpVmUserCode(dimHttpVmInfo.getHttpVmUserCode());
        dimPythonVmInfo.setHttpDatacenterId(dimHttpVmInfo.getHttpDatacenterId());
        dimPythonVmInfo.setHttpDatacenterName(dimHttpVmInfo.getHttpDatacenterName());
        dimPythonVmInfo.setHttpClusterId(dimHttpVmInfo.getHttpClusterId());
        dimPythonVmInfo.setHttpClusterName(dimHttpVmInfo.getHttpClusterName());
        dimPythonVmInfo.setHttpClusterDrsEnabled(dimHttpVmInfo.getHttpClusterDrsEnabled());
        dimPythonVmInfo.setHttpClusterHaEnabled(dimHttpVmInfo.getHttpClusterHaEnabled());
        dimPythonVmInfo.setHttpHostId(dimHttpVmInfo.getHttpHostId());
        dimPythonVmInfo.setHttpHostName(dimHttpVmInfo.getHttpHostName());
        dimPythonVmInfo.setHttpMemorySizeMb(dimHttpVmInfo.getHttpMemorySizeMb());
        dimPythonVmInfo.setHttpNumCpu(dimHttpVmInfo.getHttpNumCpu());
        dimPythonVmInfo.setEventPartitionKey(dimHttpVmInfo.getEventPartitionKey());
        return dimPythonVmInfo;
    }

    @Override
    public void processBroadcastElement(VmInfo dimHttpVmInfo, KeyedBroadcastProcessFunction<String,
            VmInfo, VmInfo, String>.Context ctx, Collector<String> collector) throws Exception {
        // 从上下文中，获取广播状态对象（可读可写的状态对象）
        BroadcastState<String, VmInfo> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.getHttpVMInfoBroadcastDesc);
        broadcastState.put(dimHttpVmInfo.getEventPartitionKey(),dimHttpVmInfo);
    }
}
