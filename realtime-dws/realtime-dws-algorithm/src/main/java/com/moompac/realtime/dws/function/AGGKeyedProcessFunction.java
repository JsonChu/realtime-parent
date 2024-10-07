package com.moompac.realtime.dws.function;

import com.moonpac.realtime.common.bean.dws.AggBean;
import com.moonpac.realtime.common.bean.dws.AggBeanState;
import com.moonpac.realtime.common.util.DateUtils;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import com.moonpac.realtime.common.util.NumUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class AGGKeyedProcessFunction extends KeyedProcessFunction<String, AggBean, AggBean> {

    private transient MapState<Long, Set<AggBean> > eventStateMap; // key代表：小时
    private transient ValueState<Boolean> firstRegisterOnTimer; //用于标识processElement第一次注册定时器
    private transient MapState<Long,Integer> onTimerCheckResult; // 定时器检查结果

    // key对应的是 小时级别的时间戳
    private transient MapState<Long, AggBeanState> hourTJAggBeanStateMap;

    private static final DecimalFormat format = new DecimalFormat("#.0000");

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        eventStateMap = runtimeContext.getMapState(FlinkStateDescUtils.getKeyedStateSetDesc());
        firstRegisterOnTimer = runtimeContext.getState(FlinkStateDescUtils.firstRegisterOnTimerValueStateDes);
        onTimerCheckResult = runtimeContext.getMapState(FlinkStateDescUtils.onTimeCheckResultMapStateDesc);
        hourTJAggBeanStateMap = runtimeContext.getMapState(FlinkStateDescUtils.getAGGHourTJStateDesc);
    }

    @Override
    public void processElement(AggBean inputAggBean, KeyedProcessFunction<String, AggBean,
            AggBean>.Context ctx, Collector<AggBean> out) throws Exception {
        if (null != inputAggBean){
            long currentEventDate = inputAggBean.getEventdate();
            long currentHourEventDate = DateUtils.convertToHourLevel(currentEventDate);
            // 缓存明细数据
            Set<AggBean> valueSets = eventStateMap.get(currentHourEventDate);
            if (null == valueSets) {
                valueSets = new HashSet<>();
            }

            // 判断 key相等 且 度量值 不为空
            replaceIfExist(valueSets,inputAggBean);
            eventStateMap.put(currentHourEventDate, valueSets);
            if (null == firstRegisterOnTimer.value() || !firstRegisterOnTimer.value()) { // 第一条数据来的时候 注册一个定时器
                int randomNumber = ThreadLocalRandom.current().nextInt(1, 10) * 1000;
                long triggerTime = ctx.timerService().currentProcessingTime() + 30 * 1000+randomNumber; // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
                firstRegisterOnTimer.update(true);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, AggBean, AggBean>.OnTimerContext ctx,
                        Collector<AggBean> out) throws Exception {

        String currentKey = ctx.getCurrentKey();
        removeElement(currentKey,5,3); // 判断超过两个就删除最小的key对应的值

        Iterator<Map.Entry<Long, Set<AggBean>>> iterator = eventStateMap.iterator();
        while (iterator.hasNext()){

            Map.Entry<Long, Set<AggBean>> next = iterator.next();
            Long cacheHourEventDate = next.getKey();
            Set<AggBean> inputBeans = next.getValue();
            Integer cacheEventDateCount = next.getValue().size();
            Integer onTimerCheckDataCount = onTimerCheckResult.get(cacheHourEventDate);

            if (null == onTimerCheckDataCount){ // 表示第一次检查 或者 还在增长
                onTimerCheckResult.put(cacheHourEventDate,cacheEventDateCount);
                log.info("第一次检测 数据key：{},数据时间：{},数据条数：{}",currentKey,cacheHourEventDate,cacheEventDateCount);
            } else if (!onTimerCheckDataCount.equals(cacheEventDateCount)){
                onTimerCheckResult.put(cacheHourEventDate,cacheEventDateCount);
                log.info("检测继续等待 数据key：{},数据时间：{},上次检测数据条数：{},本次检测数据条数：{}",currentKey,
                        cacheHourEventDate,onTimerCheckDataCount,cacheEventDateCount);
            }else { // 某个时间点的数据不在增长 则触发计算 inputBeans 合并发送到下游
                AggBean outAggBean = null;
                if (currentKey.startsWith("datastore-disk")){
                    outAggBean = aggDataStoreDisk(inputBeans,currentKey,cacheHourEventDate);
                }else if(currentKey.startsWith("datastore-datastore")){
                    outAggBean = aggDataStoreDataStore(inputBeans,currentKey,cacheHourEventDate);
                }else if(currentKey.startsWith("cpumem-")){
                    outAggBean = aggCPUMEM(inputBeans,currentKey,cacheHourEventDate);
                }
                if (null != outAggBean){
                    out.collect(outAggBean);
                }
            }
        }
        // 再次注册定时器
        long triggerTime = ctx.timerService().currentProcessingTime() + 30 * 1000;
        ctx.timerService().registerProcessingTimeTimer(triggerTime);
    }

    private AggBean aggCPUMEM(Set<AggBean> inputBeans, String currentKey, Long cacheHourEventDate) throws Exception {

        AggBean outAggBean = new AggBean();
        AggBean firstAggBean = null;

        Double cpuusagemhz = 0.0D;
        Double cputotalmhz = 0.0D;
        Double memusagegb = 0.0D;
        Double memtotalgb = 0.0D;
        // 新增的net和disk
        Double netused = 0.0D;
        Double diskused = 0.0D;

        Integer hostVMSize = 0; // 主机对应的vm个数

        for (AggBean aggBean : inputBeans) {
            if (null == firstAggBean){
                firstAggBean = aggBean;
            }
            cpuusagemhz += aggBean.getCpuusedmhz();
            cputotalmhz += aggBean.getCputotalmhz();
            memusagegb += aggBean.getMemusedgb();
            memtotalgb += aggBean.getMemtotalgb();
            netused += aggBean.getNetused();
            diskused += aggBean.getDiskused();
            hostVMSize += aggBean.getVmsize();
        }
        if (null != firstAggBean){
            String vcenter = firstAggBean.getVcenter();
            String type = firstAggBean.getObjectType();
            outAggBean.setRegion(firstAggBean.getRegion());
            if ("cpumem-cluster".equals(type)){
                outAggBean.setVcenter(vcenter);
                outAggBean.setDatacenterId(firstAggBean.getDatacenterId());
                outAggBean.setDatacenterName(firstAggBean.getDatacenterName());
                outAggBean.setClusterId(firstAggBean.getClusterId());
                outAggBean.setClusterName(firstAggBean.getClusterName());
            }else if("cpumem-datacenter".equals(type)){
                outAggBean.setVcenter(vcenter);
                outAggBean.setDatacenterId(firstAggBean.getDatacenterId());
                outAggBean.setDatacenterName(firstAggBean.getDatacenterName());
            }
            outAggBean.setObjectType(type);
            outAggBean.setEventdate(cacheHourEventDate);

            outAggBean.setCpuusedmhz(cpuusagemhz);
            outAggBean.setCputotalmhz(cputotalmhz);
            outAggBean.setMemusedgb(memusagegb);
            outAggBean.setMemtotalgb(memtotalgb);
            outAggBean.setNetused(netused);
            outAggBean.setDiskused(diskused);
            outAggBean.setVmsize(hostVMSize);

            AggBeanState aggBeanState = setAggCPUMEMBeanState(hourTJAggBeanStateMap,outAggBean,cacheHourEventDate);
            // 计算统计值
            outAggBean.setMaxcpuusedmhz(calculateMax(aggBeanState.getCpuusedmhzList()));
            outAggBean.setAvgcpuusedmhz(calculateAverage(aggBeanState.getCpuusedmhzList()));

            outAggBean.setMaxmemusedgb(calculateMax(aggBeanState.getMemusedgbList()));
            outAggBean.setAvgmemusedgb(calculateAverage(aggBeanState.getMemusedgbList()));

            outAggBean.setMaxnetused(calculateMax(aggBeanState.getNetusedList()));
            outAggBean.setAvgnetused(calculateAverage(aggBeanState.getNetusedList()));

            outAggBean.setMaxdiskused(calculateMax(aggBeanState.getDiskusedList()));
            outAggBean.setAvgdiskused(calculateAverage(aggBeanState.getDiskusedList()));

           // log.info("合并数据成功，发送数据到下游：当前key={},数据时间={},数据条数={}",currentKey,DateUtils.formatTimestamp(cacheHourEventDate),inputBeans.size());

            return outAggBean;
        }
        return null;
    }

    private AggBean aggDataStoreDisk(Set<AggBean> inputBeans,String currentKey,
                                 Long cacheHourEventDate) throws Exception {
        AggBean outAggBean = new AggBean();
        AggBean firstAggBean = null;
        double diskusagegb = 0.0D;
        double disktotalgb = 0.0D;
        for (AggBean aggBean : inputBeans) {
            if (null == firstAggBean){
                firstAggBean = aggBean;
            }
            diskusagegb += aggBean.getDatastoreusedgb();
            disktotalgb += aggBean.getDatastoretotalgb();
        }
        if (null != firstAggBean){
            String vcenter = firstAggBean.getVcenter();
            String type = firstAggBean.getObjectType();
            outAggBean.setRegion(firstAggBean.getRegion());
            if ("datastore-disk-cluster".equals(type)){
                outAggBean.setVcenter(vcenter);
                outAggBean.setDatacenterId(firstAggBean.getDatacenterId());
                outAggBean.setDatacenterName(firstAggBean.getDatacenterName());
                outAggBean.setDatastoreId(firstAggBean.getDatastoreId());
                outAggBean.setClusterId(firstAggBean.getClusterId());
                outAggBean.setClusterName(firstAggBean.getClusterName());
            }else if("datastore-disk-datacenter".equals(type)){
                outAggBean.setVcenter(vcenter);
                outAggBean.setDatacenterId(firstAggBean.getDatacenterId());
                outAggBean.setDatacenterName(firstAggBean.getDatacenterName());
                outAggBean.setDatastoreId(firstAggBean.getDatastoreId());
            }
            outAggBean.setEventdate(cacheHourEventDate);
            outAggBean.setDatastoreusedgb(NumUtils.doubleValue(format.format(diskusagegb / 1024 / 1024 )));
            outAggBean.setDatastoretotalgb(NumUtils.doubleValue(format.format(disktotalgb / 1024 / 1024 )));
            // 增加objectType
            outAggBean.setObjectType(type);
            // 计算统计值
            AggBeanState aggBeanState = setAggDataStoreBeanState(hourTJAggBeanStateMap,outAggBean,cacheHourEventDate);
            outAggBean.setMaxdatastoreusedgb(calculateMax(aggBeanState.getDatastoreusedList()));
            outAggBean.setAvgdatastoreusedgb(calculateAverage(aggBeanState.getDatastoreusedList()));

            // log.info("合并数据成功，发送数据到下游：当前key={},数据时间={},数据条数={}",currentKey,DateUtils.formatTimestamp(cacheHourEventDate),inputBeans.size());
            return outAggBean;
        }
        return null;
    }


    private AggBean aggDataStoreDataStore(Set<AggBean> inputBeans,String currentKey,
                              Long cacheHourEventDate) throws Exception {
        AggBean outAggBean = new AggBean();
        AggBean firstAggBean = null;
        for (AggBean aggBean : inputBeans) {
            if (null == firstAggBean){
                firstAggBean = aggBean;
            }
        }

        if (null != firstAggBean){
            String vcenter = firstAggBean.getVcenter();
            String type = firstAggBean.getObjectType();
            outAggBean.setRegion(firstAggBean.getRegion());
            if ("datastore-datastore-cluster".equals(type)){
                outAggBean.setVcenter(vcenter);
                outAggBean.setDatacenterId(firstAggBean.getDatacenterId());
                outAggBean.setDatacenterName(firstAggBean.getDatacenterName());
                outAggBean.setDatastoreId(firstAggBean.getDatastoreId());
                outAggBean.setClusterId(firstAggBean.getClusterId());
                outAggBean.setClusterName(firstAggBean.getClusterName());
            }else if("datastore-datastore-datacenter".equals(type)){
                outAggBean.setVcenter(vcenter);
                outAggBean.setDatacenterId(firstAggBean.getDatacenterId());
                outAggBean.setDatacenterName(firstAggBean.getDatacenterName());
                outAggBean.setDatastoreId(firstAggBean.getDatastoreId());
            }
            outAggBean.setEventdate(cacheHourEventDate);
            // 增加objectType
            outAggBean.setObjectType(type);
            // 计算统计值
            AggBeanState aggBeanState = setAggDataStoreBeanState(hourTJAggBeanStateMap,outAggBean,cacheHourEventDate);
            // 新增聚合 2024-4-12
            outAggBean.setSumdsnumberreadaveraged(calculateSum(aggBeanState.getDsnumberreadaveragedList()));
            outAggBean.setSumdsnumberreadaveraged(calculateSum(aggBeanState.getDsnumberwriteaeragedList()));

            // log.info("合并数据成功，发送数据到下游：当前key={},数据时间={},数据条数={}",currentKey,DateUtils.formatTimestamp(cacheHourEventDate),inputBeans.size());
            return outAggBean;
        }
        return null;
    }


    private AggBeanState setAggCPUMEMBeanState(MapState<Long, AggBeanState> hourTJAggBeanStateMap,
                                         AggBean aggBean, Long cacheHourEventDate) throws Exception {

        AggBeanState cachedAggBeanState = hourTJAggBeanStateMap.get(cacheHourEventDate);
        if (cachedAggBeanState == null) {
            cachedAggBeanState = new AggBeanState();
        }
        Double memusedgb = aggBean.getMemusedgb();
        if (memusedgb != null && memusedgb != 0) {
            cachedAggBeanState.getMemusedgbList().add(memusedgb); // 添加内存使用量到集合
        }
        Double netused = aggBean.getNetused();
        if (netused != null && netused != 0) {
            cachedAggBeanState.getNetusedList().add(netused); // 添加网络使用量到集合
        }
        Double diskused = aggBean.getDiskused();
        if (diskused != null && diskused != 0) {
            cachedAggBeanState.getDiskusedList().add(diskused); // 添加磁盘使用量到集合
        }
        Double cpuusedmhz = aggBean.getCpuusedmhz();
        if (cpuusedmhz != null && cpuusedmhz != 0) {
            cachedAggBeanState.getCpuusedmhzList().add(cpuusedmhz); // 添加 CPU 使用量到集合
        }
        hourTJAggBeanStateMap.put(cacheHourEventDate,cachedAggBeanState);

        return cachedAggBeanState;

    }

    // 方法：将集合中每个元素的详细信息拼接到一行并打印
    private static String logDetails(Set<AggBean> inputBeans) {
        StringBuilder sb = new StringBuilder();
        for (AggBean bean : inputBeans) {
            sb.append(bean.toString()).append(", ");
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 2, sb.length()); // 移除末尾多余的逗号和空格
        }
        return sb.toString();
    }

    public static void replaceIfExist(Set<AggBean> set, AggBean inputAggBean) {
        for (AggBean existingBean : set) {
            if (existingBean.getUniqueKey().equals(inputAggBean.getUniqueKey())) {
                if (inputAggBean.getEventPartitionKey().startsWith("datastore-")) {
                    // 如果是以 "datastore-" 开头的 key
                    if (null == inputAggBean.getDatastoretotalgb() || inputAggBean.getDatastoretotalgb() == 0) {
                        inputAggBean.setDatastoretotalgb(existingBean.getDatastoretotalgb());
                    }
                    if (null == inputAggBean.getDatastoreusedgb() || inputAggBean.getDatastoreusedgb() == 0) {
                        inputAggBean.setDatastoreusedgb(existingBean.getDatastoreusedgb());
                    }
                    set.remove(existingBean);
                    break;
                } else if (inputAggBean.getEventPartitionKey().startsWith("cpumem-")) {
                    // 如果是以 "cpumem-" 开头的 key
                    if (null == inputAggBean.getCpuusedmhz() || inputAggBean.getCpuusedmhz() == 0) {
                        inputAggBean.setCpuusedmhz(existingBean.getCpuusedmhz());
                    }
                    if (null == inputAggBean.getCputotalmhz() || inputAggBean.getCputotalmhz() == 0) {
                        inputAggBean.setCputotalmhz(existingBean.getCputotalmhz());
                    }
                    if (null == inputAggBean.getMemusedgb() || inputAggBean.getMemusedgb() == 0) {
                        inputAggBean.setMemusedgb(existingBean.getMemusedgb());
                    }
                    if (null == inputAggBean.getMemtotalgb()|| inputAggBean.getMemtotalgb() == 0) {
                        inputAggBean.setMemtotalgb(existingBean.getMemtotalgb());
                    }
                    if (null == inputAggBean.getNetused()|| inputAggBean.getNetused() == 0) {
                        inputAggBean.setNetused(existingBean.getNetused());
                    }
                    if (null == inputAggBean.getDiskused()|| inputAggBean.getDiskused() == 0) {
                        inputAggBean.setDiskused(existingBean.getDiskused());
                    }
                    if (null == inputAggBean.getVmsize()|| inputAggBean.getVmsize() == 0) {
                        inputAggBean.setVmsize(existingBean.getVmsize());
                    }
                    set.remove(existingBean);
                    break;
                }
            }
        }
        set.add(inputAggBean); // 添加新元素
    }

    // 方法：移除集合中的最大元素并返回
    private  void removeElement(String currentKye,Integer maxSize,Integer removeSize ) throws Exception {
        TreeSet<Long> elementsTreeSet = new TreeSet<>();
        for (Long element : eventStateMap.keys()) {
            elementsTreeSet.add(element);
        }

        if (elementsTreeSet.size() > maxSize ) {
            // 构建删除前后的日志消息
            StringBuilder logMsg = new StringBuilder();
            logMsg.append("数据key："+currentKye+" 删除前treeSet元素[维护已经发送的时间]：[").append(elementsTreeSet).append("], 删除后treeSet元素：[");
            // 获取要删除的元素数量
            int removeCount = Math.min(elementsTreeSet.size() - maxSize, removeSize);
            // 删除前面一部分元素
            for (int i = 0; i < removeCount; i++) {
                Long minEventDate = elementsTreeSet.pollFirst();// 移除最小的元素
                hourTJAggBeanStateMap.remove(minEventDate);
                eventStateMap.remove(minEventDate);
                onTimerCheckResult.remove(minEventDate);
            }
            // 追加删除后的元素到日志消息中
            logMsg.append(elementsTreeSet).append("]");
            // 输出日志消息
            log.info(logMsg.toString());
        }
    }

    private static double calculateMax(List<Double> list) {
        if (list == null || list.isEmpty()) {
            return 0.0D;
        }
        return Double.parseDouble(format.format(Collections.max(list)));
    }

    private static double calculateMin(List<Double> list) {
        if (list == null || list.isEmpty()) {
            return 0.0D;
        }
        return Double.parseDouble(format.format(Collections.min(list)));
    }

    private static double calculateAverage(List<Double> list) {
        if (list == null || list.isEmpty()) {
            return 0.0D;
        }
        double sum = 0;
        for (double value : list) {
            sum += value;
        }
        return  Double.parseDouble(format.format(sum / list.size()));
    }

    private Double calculateSum(List<Double> list) {
        if (list == null || list.isEmpty()) {
            return 0.0D;
        }
        double sum = 0;
        for (double value : list) {
            sum += value;
        }
        return  Double.parseDouble(format.format(sum));
    }

    private AggBeanState setAggDataStoreBeanState(MapState<Long, AggBeanState> hourTJAggBeanStateMap,
                                         AggBean aggBean, Long sendHourEventDate) throws Exception {

        AggBeanState cachedAggBeanState = hourTJAggBeanStateMap.get(sendHourEventDate);
        if (cachedAggBeanState == null) {
            cachedAggBeanState = new AggBeanState();
        }

        Double datastoreusedgb = aggBean.getDatastoreusedgb();
        if (datastoreusedgb != null && datastoreusedgb != 0) {
            cachedAggBeanState.getDatastoreusedList().add(datastoreusedgb); // 添加 Datastoreused 使用量到集合
        }

        hourTJAggBeanStateMap.put(sendHourEventDate,cachedAggBeanState);
        return cachedAggBeanState;
    }



}
