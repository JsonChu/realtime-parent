package com.moonpac.realtime.dwd.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dwd.SqlServerAfterEventData;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import com.moonpac.realtime.common.util.JdbcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class VdiLatestStatusKeyedProcessFunction extends KeyedProcessFunction<String, SqlServerAfterEventData, String> {

    private transient ValueState<SqlServerAfterEventData> valueState; //用于标识processElement第一次注册定时器

    Map<String, SqlServerAfterEventData> vdiLatestStatusMap ;

    private static  final List<String> eventTypes = Arrays.asList("AGENT_DISCONNECTED", "AGENT_ENDED", "AGENT_SHUTDOWN","AGENT_CONNECTED", "AGENT_RECONNECTED");

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        valueState = runtimeContext.getState(FlinkStateDescUtils.getVdiLatestStatusValueStateDes);
        Map<String, String> dorisConf  = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        String driver = dorisConf.get("doris.driver");
        String url = dorisConf.get("doris.url");
        String username = dorisConf.get("doris.username");
        String password = dorisConf.get("doris.password");
        vdiLatestStatusMap = JdbcUtils.getVdiLatestStatus(driver, url, username, password);
    }

    // kafka每来一条数据 会触发下面的方法
    @Override
    public void processElement(SqlServerAfterEventData sqlServerAfterEventData, Context ctx,
                               Collector<String> out) throws Exception {

        String eventType = sqlServerAfterEventData.getEventType();
        if (eventTypes.contains(eventType)){
            String currentKey = ctx.getCurrentKey();
            //更新状态
            SqlServerAfterEventData dorisSqlServerAfterEventData = vdiLatestStatusMap.get(currentKey);
            if( dorisSqlServerAfterEventData != null ){
                if (valueState.value() == null || valueState.value().getTime() < dorisSqlServerAfterEventData.getTime()){
                    valueState.update(dorisSqlServerAfterEventData);
                }
            }
            // 新来的数据比缓存的时间大
            if (valueState.value() == null || valueState.value().getTime() < sqlServerAfterEventData.getTime()){
                // 更新缓存
                valueState.update(sqlServerAfterEventData);
                //发送最新的数据即可
                out.collect(JSON.toJSONString(sqlServerAfterEventData));
                /*if (!valueState.value().getEventType().equals(eventType)){

                }*/
            }
        }
    }

}
