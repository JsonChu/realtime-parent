package com.moompac.dim.function;


import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.bean.dwd.VmLifeCycleBean;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import com.moonpac.realtime.common.util.JdbcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

@Slf4j
public class VmLifeCycleKeyedProcessFunction extends KeyedProcessFunction<String, VmInfo, VmLifeCycleBean> {

    private transient ValueState<VmLifeCycleBean> valueState; //用于标识processElement第一次注册定时器

    Map<String, VmLifeCycleBean> iniVmLifeCycleMap ;

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        valueState = runtimeContext.getState(FlinkStateDescUtils.getVmLifeCycleValueStateDes);
        Map<String, String> dorisConf  = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        String driver = dorisConf.get("doris.driver");
        String url = dorisConf.get("doris.url");
        String username = dorisConf.get("doris.username");
        String password = dorisConf.get("doris.password");
        iniVmLifeCycleMap = JdbcUtils.getDorisVmLifeCycle(driver, url, username, password);
    }

    // kafka每来一条数据 会触发下面的方法
    @Override
    public void processElement(VmInfo dimvmInfo, Context context,
                               Collector<VmLifeCycleBean> out) throws Exception {
        //缓存数据
        VmLifeCycleBean cachedVMLifeCycleBean = null;
        String vmKey = dimvmInfo.getEventPartitionKey();
        if(valueState.value()==null){
            if(iniVmLifeCycleMap.containsKey(vmKey)){
                cachedVMLifeCycleBean = iniVmLifeCycleMap.get(vmKey);
            }
        }else{
            cachedVMLifeCycleBean = valueState.value();
        }

        //当前数据
        String newVmName = dimvmInfo.getHttpVmName();
        String newPowerState = dimvmInfo.getPythonPowerState();
        Long newBootTime = dimvmInfo.getPythonBootTime();
        Long newEventDate = dimvmInfo.getHttpEventDate() == null ? null : dimvmInfo.getHttpEventDate() * 1000;
        long nowTime = (new Date()).getTime();
        VmLifeCycleBean vMLifeCycleBean = new VmLifeCycleBean();
        BeanUtils.copyProperties(vMLifeCycleBean, dimvmInfo);

        if (newPowerState != null && newPowerState.startsWith("POWERED")) {
//            if ("10.173.28.241_vm-12046".equals(dimvmInfo.getEventPartitionKey())) {
                if (null == cachedVMLifeCycleBean) {
                    //新增数据
                    // 1.状态修改：
                    //  关机状态时，将startTime设置为当前时间，endTime设置为"9999-01-01 00:00:00";
                    //  开机状态时，bootTime为空时，将startTime设置为当前时间，不为空时设置为bootTime，endTime设置为"9999-01-01 00:00:00";
                    // 2.发送数据
                    // 3.更新缓存
                    if (newPowerState.equals("POWERED_OFF")) {
                        vMLifeCycleBean.setStartTime(nowTime);
                    } else {
                        if (newBootTime == null) {
                            vMLifeCycleBean.setStartTime(nowTime);
                        } else {
                            vMLifeCycleBean.setStartTime(newBootTime);
                        }
                    }
                    vMLifeCycleBean.setEndTime(253370736000000L);
                    log.info("新增一条数据====vm_name:{},powerstate:{},boottime:{},starttime:{},endtime:{},eventdate: {}",
                            newVmName, newPowerState, timestampToDate(newBootTime),
                            timestampToDate(vMLifeCycleBean.getStartTime()), timestampToDate(vMLifeCycleBean.getEndTime()), timestampToDate(newEventDate));
                    //发送数据
                    out.collect(vMLifeCycleBean);
                    valueState.update(vMLifeCycleBean);
                } else {
                    //修改数据
                    //1.状态修改：
                    //  powerState和上次一致时，不做任何修改
                    //  powerState和上次不一致时，修改缓存中endTime为当前时间，发送当前数据（bootTime为空时，将startTime设置为当前时间，不为空时设置为bootTime，）
                    //2.发送数据
                    //3.更新缓存
                    //缓存数据
                    String cachedPythonPowerState = cachedVMLifeCycleBean.getPythonPowerState();
                    //状态不一致
                    if (!newPowerState.equals(cachedPythonPowerState)) {
                        //当前状态为关机时，将上一条数据endTime修改为当前时间，将当前数据的startTime修改为当前时间
                        //当前状态为开机时，判断bootTime为空时，将startTime设置为当前时间，不为空时设置为bootTime，endTime设置为"9999-01-01 00:00:00";
                        if (newPowerState.equals("POWERED_OFF")) {
                            cachedVMLifeCycleBean.setEndTime(nowTime);
                            vMLifeCycleBean.setStartTime(nowTime);
                        } else {
                            if (newBootTime == null || newBootTime < cachedVMLifeCycleBean.getStartTime()) {
                                //将上一条数据结束时间和下一条数据开始时间设置为当前时间
                                cachedVMLifeCycleBean.setEndTime(nowTime);
                                vMLifeCycleBean.setStartTime(nowTime);
                            } else {
                                //将上一条数据结束时间和下一条数据开始时间设置为bootTime
                                cachedVMLifeCycleBean.setEndTime(newBootTime);
                                vMLifeCycleBean.setStartTime(newBootTime);
                            }
                        }
                        vMLifeCycleBean.setEndTime(253370736000000L);
                        log.info("产生一条新数据====vm_name:{},powerstate:{},boottime:{},starttime:{},endtime:{},eventdate: {}",
                                newVmName, newPowerState, timestampToDate(newBootTime),
                                timestampToDate(vMLifeCycleBean.getStartTime()), timestampToDate(vMLifeCycleBean.getEndTime()), timestampToDate(newEventDate));
                        log.info("修改上一条结束时间====vm_name:{},powerstate:{},starttime:{},endtime:{},eventdate: {}",
                                cachedVMLifeCycleBean.getHttpVmName(), cachedVMLifeCycleBean.getPythonPowerState(),
                                timestampToDate(cachedVMLifeCycleBean.getStartTime()), timestampToDate(cachedVMLifeCycleBean.getEndTime()),
                                timestampToDate(cachedVMLifeCycleBean.getHttpEventDate() == null ? null : cachedVMLifeCycleBean.getHttpEventDate() * 1000));
                        out.collect(cachedVMLifeCycleBean);
                        out.collect(vMLifeCycleBean);
                        valueState.update(vMLifeCycleBean);
                    }
//                }
            }
        } else {
            log.warn("当前VM电源状态异常====vm_name:{},powerstate:{}", newVmName, newPowerState);
        }
    }

    public String timestampToDate(Long timestamp) {
        String formattedDate = null;
        if (timestamp != null) {
            Timestamp ts = new Timestamp(timestamp);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            formattedDate = dateFormat.format(ts);
        }
        return formattedDate;
    }
}
