package com.moonpac.realtime.dwd.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.dwd.SqlServerAfterEventData;
import com.moonpac.realtime.common.bean.dwd.SqlServerConnectionConfig;
import com.moonpac.realtime.common.bean.dwd.SqlServerEventData;
import org.apache.flink.api.common.functions.MapFunction;

public class CSEventMapFunction implements MapFunction<SqlServerEventData, String> {

    private SqlServerConnectionConfig config;

    public CSEventMapFunction(SqlServerConnectionConfig config){
        this.config = config;
    }

    @Override
    public String map(SqlServerEventData sqlServerEventData) throws Exception {
        SqlServerAfterEventData after = sqlServerEventData.getAfter();
        after.setVcenter(config.getVcenter());
        after.setCsip(config.getIp());
        return JSON.toJSONString(after);
    }
}
