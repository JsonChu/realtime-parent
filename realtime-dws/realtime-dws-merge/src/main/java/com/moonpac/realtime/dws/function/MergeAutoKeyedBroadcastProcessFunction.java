package com.moonpac.realtime.dws.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.bean.dws.MergeEventBean;
import com.moonpac.realtime.common.bean.dws.WidenResultBean;
import com.moonpac.realtime.common.bean.dws.meger.*;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.util.AutoMergeCalculationUtils;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Slf4j
public class MergeAutoKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, KafkaVcenterInputBean,
        MergeAutoCompute, WidenResultBean> {
    private transient MapState<Long, Set<KafkaVcenterInputBean>> eventStateMap;
    private transient ValueState<TreeSet<Long>> sendEventDateState;
    private transient ValueState<Boolean> firstRegisterOnTimer; //用于标识processElement第一次注册定时器
    private transient MapState<Long,Integer> onTimerCheckResult; // 定时器检查结果
    static final DecimalFormat format = new DecimalFormat("#.0000");

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        eventStateMap = runtimeContext.getMapState(FlinkStateDescUtils.getWidenEventBeansStateSetDesc());
        sendEventDateState = runtimeContext.getState(FlinkStateDescUtils.mergeAutoSendEventDates);
        firstRegisterOnTimer = runtimeContext.getState(FlinkStateDescUtils.firstRegisterOnTimerValueStateDes);
        onTimerCheckResult = runtimeContext.getMapState(FlinkStateDescUtils.onTimeCheckResultMapStateDesc);
    }

    @Override
    public void processElement(KafkaVcenterInputBean kafkaVcenterInputBean, KeyedBroadcastProcessFunction<String,
            KafkaVcenterInputBean, MergeAutoCompute, WidenResultBean>.ReadOnlyContext ctx, Collector<WidenResultBean> out) throws Exception {
        long eventTimestamp = kafkaVcenterInputBean.getEventTimestamp();
        // 缓存明细数据
        Set<KafkaVcenterInputBean> valueSets = eventStateMap.get(eventTimestamp);
        if (null == valueSets) {
            valueSets = new HashSet<>();
        }
        valueSets.add(kafkaVcenterInputBean);
        eventStateMap.put(eventTimestamp, valueSets);
        if (null == firstRegisterOnTimer.value() || !firstRegisterOnTimer.value()) { // 第一条数据来的时候 注册一个定时器
            int randomNumber = ThreadLocalRandom.current().nextInt(1, 10) * 1000;
            long triggerTime = ctx.timerService().currentProcessingTime() + 20 * 1000+randomNumber; // 注册定时器
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
            firstRegisterOnTimer.update(true);
        }
    }

    /*
        定时器的设计理念
            1.定时的检查eventStateMap的key对应的数据
                1.1、如果有多个key，则立马发送 小的key的数据
                1.2、如果一个key，上次检查的数据条数和这次相等 则发送
     */
    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, KafkaVcenterInputBean, MergeAutoCompute,
            WidenResultBean>.OnTimerContext ctx, Collector<WidenResultBean> out) throws Exception {

        TreeSet<Long> sendEventDateTreeSet = sendEventDateState.value();
        if (null == sendEventDateTreeSet){
            sendEventDateTreeSet = new TreeSet<>();
        }
        trimTreeSet(sendEventDateTreeSet,10,5);

        Iterator<Map.Entry<Long, Set<KafkaVcenterInputBean>>> iterator = eventStateMap.iterator();
        while (iterator.hasNext()){
            Map.Entry<Long, Set<KafkaVcenterInputBean>> next = iterator.next();
            Long cacheEventDate = next.getKey();
            Set<KafkaVcenterInputBean> inputBeans = next.getValue();
            Integer cacheEventDateCount = next.getValue().size();

            if ( sendEventDateTreeSet.contains(cacheEventDate) ){ //删除已发送的数据
                log.warn("删除flinkstate过期数据：数据key：{},数据时间：{},已发送的数据时间集合：{}",ctx.getCurrentKey(),cacheEventDate,sendEventDateTreeSet);
                onTimerCheckResult.remove(cacheEventDate);
                iterator.remove();
            }else{
                Integer onTimerCheckDataCount = onTimerCheckResult.get(cacheEventDate);
                if (null == onTimerCheckDataCount){ // 表示第一次检查 或者 还在增长
                    onTimerCheckResult.put(cacheEventDate,cacheEventDateCount);
                    log.info("第一次检测 数据key：{},数据时间：{},数据条数：{}",ctx.getCurrentKey(),cacheEventDate,cacheEventDateCount);
                } else if (onTimerCheckDataCount < cacheEventDateCount){
                    onTimerCheckResult.put(cacheEventDate,cacheEventDateCount);
                    log.info("检测继续等待 数据key：{},数据时间：{},上次检测数据条数：{},本次检测数据条数：{}",ctx.getCurrentKey(),
                            cacheEventDate,onTimerCheckDataCount,cacheEventDateCount);
                }else if (onTimerCheckDataCount.equals(cacheEventDateCount)){ // 某个时间点的数据不在增长 则触发计算 inputBeans 合并发送到下游

                    // 根据规则动态的合并数据
                    ReadOnlyBroadcastState<String, MergeAutoCompute> broadcastState =
                            ctx.getBroadcastState(FlinkStateDescUtils.mergeAutoComputeBroadcastDesc);
                    // 获取开始时间
                    long startTime = System.currentTimeMillis();
                    // 动态合并 核心代码
                    WidenResultBean widenResultBean = mergeCalculation(broadcastState,inputBeans);
                    if (null != widenResultBean){
                        out.collect(widenResultBean);
                        // 获取结束时间
                        long endTime = System.currentTimeMillis();
                        // 计算耗时（以毫秒为单位）
                        long durationInMillis = endTime - startTime;
                        if (durationInMillis > 100){
                            log.warn("动态合并超过100ms 数据key：{},数据时间：{},数据条数：{},动态合并耗时：{}毫秒",ctx.getCurrentKey(),
                                    cacheEventDate,inputBeans.size(),durationInMillis);
                        }else{
                            int randomNumber = ThreadLocalRandom.current().nextInt(1, 100);
                            if (randomNumber == 6){
                                log.info("数据key：{},数据时间：{},数据条数：{},动态合并耗时：{}毫秒",ctx.getCurrentKey(),
                                        cacheEventDate,inputBeans.size(),durationInMillis);
                            }
                        }
                    }
                    // 删除缓存数据
                    iterator.remove();
                    // 删除onTimerCheckResult缓存数据
                    onTimerCheckResult.remove(cacheEventDate);
                    // 更新
                    sendEventDateTreeSet.add(cacheEventDate);
                    sendEventDateState.update(sendEventDateTreeSet);

                }
            }
        }
        // 再次注册定时器
        long triggerTime = ctx.timerService().currentProcessingTime() + 20 * 1000;
        ctx.timerService().registerProcessingTimeTimer(triggerTime);
    }

    private WidenResultBean mergeCalculation(ReadOnlyBroadcastState<String, MergeAutoCompute> broadcastState,
                                             Set<KafkaVcenterInputBean> inputBeans ) throws Exception {

        Set<String> eventNames = extractEventNames(inputBeans); // 获取日志里面所有的eventNames
        KafkaVcenterInputBean firstElement = getFirstElement(inputBeans);

        for (Map.Entry<String, MergeAutoCompute> next : broadcastState.immutableEntries()) {
            MergeAutoCompute mergeAutoCompute = next.getValue();
            List<ObjectAGGCompute> objectAGGCompute = mergeAutoCompute.getObjectAGGCompute();
            for (ObjectAGGCompute aggCompute : objectAGGCompute) {
                // 说明了 缓存的数据 就是规则需要计算的数据
                if (aggCompute.getObjectType().equals(firstElement.getObjectType())){
                    JSONObject jsonResult = new JSONObject();
                    jsonResult.put("datacount",inputBeans.size());
                    // 先通过了 eventName判断了 是否大概复合规则 待会可以删除掉 通过更加精细的判断
                    boolean eventNameContain = containsAllEventNames(eventNames, aggCompute.getMergeOgnlExpression());
                    if (eventNameContain){
                        // 普通字段计
                        List<CombinationFieldCompute> combinationFields = aggCompute.getCombinationFields();
                        if (combinationFields != null && !combinationFields.isEmpty()){
                            AutoMergeCalculationUtils.combinationFieldCalculation(jsonResult,combinationFields,inputBeans);
                        }
                        // 聚合字段计算
                        List<AGGFieldCompute> aggFieldCompute = aggCompute.getAggFieldCompute();
                        if (aggFieldCompute != null &&  !aggFieldCompute.isEmpty()){
                            AutoMergeCalculationUtils.aggFieldCalculation(jsonResult,aggFieldCompute,inputBeans);
                        }
                        // 设置objectType
                        jsonResult.put("objecttype",aggCompute.getObjectType());

                        // 统一检查是否 核心字段 是否存在 如果不存在，则打印错误日志，可以排查对应的错误
                        boolean coreFieldsResult = checkCoreFields(jsonResult, aggCompute, inputBeans);
                        if (coreFieldsResult){
                            MergeEventBean mergeEventBean = JSONObject.parseObject(jsonResult.toJSONString(), MergeEventBean.class);
                            WidenResultBean widenResultBean = new WidenResultBean();
                            widenResultBean.setObjectType(mergeEventBean.getObjecttype());
                            widenResultBean.setEventDate(mergeEventBean.getEventdate());
                            widenResultBean.setDataCount(mergeEventBean.getDatacount());

                            widenResultBean.setVcenterInfo(firstElement.getVcenterInfo());

                            // todo 这边先写死 对 mergeEventBean的结果进行二次聚合  后期可以扩展
                            Double bootedvmnum = mergeEventBean.getBootedvmnum(); // vcenter对应的开机数量
                            Double shutdownvmnum = mergeEventBean.getShutdownvmnum();
                            Double totalvmnum = mergeEventBean.getTotalvmnum();
                            if (bootedvmnum != null && totalvmnum != null && bootedvmnum > 0 && totalvmnum > 0){
                                mergeEventBean.setBootedrate(Double.parseDouble(format.format( bootedvmnum / totalvmnum * 100)));
                            }
                            if (shutdownvmnum != null && totalvmnum != null && shutdownvmnum > 0 && totalvmnum > 0){
                                mergeEventBean.setShutdownrate(Double.parseDouble(format.format( shutdownvmnum / totalvmnum * 100 )));
                            }
                            widenResultBean.setMergeEventBean(mergeEventBean);
                            return widenResultBean;
                        }else {
                            return null;
                        }
                    }
                }
            }
        }

        return null;
    }

    private boolean checkCoreFields(JSONObject jsonResult, ObjectAGGCompute aggCompute, Set<KafkaVcenterInputBean> inputBeans) {
        String objectType = aggCompute.getObjectType();
        List<AtomsField> allFields = extractAllFields(aggCompute);
        for (AtomsField atomsField : allFields) {
            Boolean isCoreField = atomsField.getIsCoreField();
            if (isCoreField != null && isCoreField) {
                String fieldName = atomsField.getFieldName();
                if (!jsonResult.containsKey(fieldName)) {
                    log.error("数据合并异常，objectType={},合并的结果jsonResult={},field={}没有获取到，这条数据丢弃。对应的数据明细={},打印完成",
                            objectType,JSON.toJSONString(jsonResult),fieldName, getJsonString(inputBeans));
                    return false;
                }
            }
        }
        return true;
    }

    private String getJsonString(Set<KafkaVcenterInputBean> inputBeans) {
        StringBuilder stringBuilder = new StringBuilder();
        for (KafkaVcenterInputBean inputBean : inputBeans) {
            stringBuilder.append(JSON.toJSONString(inputBean, true)).append(",\n");
        }
        return stringBuilder.toString();
    }

    private List<AtomsField> extractAllFields(ObjectAGGCompute aggCompute) {
        List<AtomsField> allFields = new ArrayList<>();
        if (aggCompute != null) {
            allFields.addAll(extractFieldsFromCombination(aggCompute.getCombinationFields()));
            allFields.addAll(extractFieldsFromAGG(aggCompute.getAggFieldCompute()));
        }
        return allFields;
    }

    private List<AtomsField> extractFieldsFromCombination(List<CombinationFieldCompute> combinationFields) {
        List<AtomsField> fields = new ArrayList<>();
        if (combinationFields != null) {
            for (CombinationFieldCompute combinationField : combinationFields) {
                fields.addAll(combinationField.getAtomsFields());
            }
        }
        return fields;
    }

    private List<AtomsField> extractFieldsFromAGG(List<AGGFieldCompute> aggFieldCompute) {
        List<AtomsField> fields = new ArrayList<>();
        if (aggFieldCompute != null) {
            for (AGGFieldCompute fieldCompute : aggFieldCompute) {
                fields.addAll(fieldCompute.getAtomsFields());
            }
        }
        return fields;
    }


    public KafkaVcenterInputBean getFirstElement(Set<KafkaVcenterInputBean> inputBeans) {
        if (inputBeans != null && !inputBeans.isEmpty()) {
            return inputBeans.iterator().next();
        } else {
            return null; // 或者抛出异常，视情况而定
        }
    }

    // 方法：从 KafkaVcenterInputBeanV2 集合中提取 eventName 并封装到 Set<String> 集合中
    private static Set<String> extractEventNames(Set<KafkaVcenterInputBean> inputBeans) {
        return inputBeans.stream()
                .map(KafkaVcenterInputBean::getEventName) // 提取 eventName
                .collect(Collectors.toSet()); // 封装到 Set<String> 集合中
    }
    // 辅助方法：判断一个字符串是否包含 split 数组的所有元素
    private static boolean containsAllEventNames(Set<String> eventNames,String mergeOgnlExpression) {
        String[] splitArray = mergeOgnlExpression.split(",");
        for (String split : splitArray) {
            if (!eventNames.contains(split)) {
                return false;
            }
        }
        return true;
    }


    @Override
    public void processBroadcastElement(MergeAutoCompute mergeAutoCompute, KeyedBroadcastProcessFunction<String,
            KafkaVcenterInputBean, MergeAutoCompute, WidenResultBean>.Context ctx, Collector<WidenResultBean> collector) throws Exception {
        // 从上下文中，获取广播状态对象（可读可写的状态对象）
        BroadcastState<String, MergeAutoCompute> broadcastState = ctx.getBroadcastState(FlinkStateDescUtils.mergeAutoComputeBroadcastDesc);
        if ("1".equals(mergeAutoCompute.getRuleStatus())){ // 代表上线
            broadcastState.put(mergeAutoCompute.getRuleCode(),mergeAutoCompute);
        }else if ("0".equals(mergeAutoCompute.getRuleStatus())){
            broadcastState.remove(mergeAutoCompute.getRuleCode());
        }
    }

    // 方法：判断 TreeSet 元素是否超过指定数量，若超过，则删除前面一部分元素
    private void trimTreeSet(TreeSet<Long> treeSet, int maxSize, int removeSize) {
        if (treeSet.size() > maxSize) {
            // 构建删除前后的日志消息
            //StringBuilder logMsg = new StringBuilder();
            // logMsg.append("删除前treeSet元素[维护已经发送的时间]：[").append(treeSet).append("], 删除后treeSet元素：[");
            // 获取要删除的元素数量
            int removeCount = Math.min(treeSet.size() - maxSize, removeSize);
            // 删除前面一部分元素
            for (int i = 0; i < removeCount; i++) {
                treeSet.pollFirst(); // 移除最小的元素
            }
            // 追加删除后的元素到日志消息中
            //  logMsg.append(treeSet).append("]");
            // 输出日志消息
            //log.info(logMsg.toString());
        }
    }
}
