package com.moompac.realtime.dwd.function;


import com.moonpac.realtime.common.bean.dwd.DatabaseParam;
import com.moonpac.realtime.common.bean.dws.MergeEventBean;
import com.moonpac.realtime.common.bean.dws.WidenResultBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;

//K, I, O
@Slf4j
public class DatabaseQueryKeyedProcessFunction extends KeyedProcessFunction<String, DatabaseParam, WidenResultBean> {
    DecimalFormat df = new DecimalFormat("#.####");
    @Override
    public void processElement(DatabaseParam databaseParam, KeyedProcessFunction<String,
            DatabaseParam, WidenResultBean>.Context context, Collector<WidenResultBean> out) throws Exception {
        try {
        Connection conn = null;

        String dbType = databaseParam.getDbType();
        String url = databaseParam.getUrl();
        String username = databaseParam.getUsername();
        String password = databaseParam.getPassword();
        String region = databaseParam.getRegion();
        String vcenter = databaseParam.getVcenter();
        String dbMoid = databaseParam.getDbMoid();

        // 转换为秒级别时间戳
        Long eventDate = System.currentTimeMillis() / 1000;

        String queryCount = "";
        String queryMaxConnections = "";
        String driverName = "";
        double maxConnections = 0.0D;
        double currentConnections = 0.0D;

        WidenResultBean widenResultBean = new WidenResultBean();
        widenResultBean.setObjectType("db");
        widenResultBean.setEventDate(eventDate);

        MergeEventBean mergeEventBean = new MergeEventBean();
        mergeEventBean.setRegion(region);
        mergeEventBean.setKey(vcenter);
        mergeEventBean.setObjecttype("db");
        mergeEventBean.setKey(dbMoid);
        mergeEventBean.setEventdate(eventDate);
        mergeEventBean.setDbmoid(dbMoid);
        mergeEventBean.setDbtype(dbType);
        mergeEventBean.setIp(dbMoid);
        mergeEventBean.setVcenter(vcenter);

        if ("sqlserver".equals(dbType)){ // 去sqlserver查询数据
            queryCount = "SELECT COUNT(*) FROM sys.sysprocesses;";
            queryMaxConnections = "SELECT @@MAX_CONNECTIONS";
            driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }else if ("postgresql".equals(dbType)){ // 去PostgreSQL查询数据
            queryCount = "SELECT count(*)  FROM pg_stat_activity;";
            queryMaxConnections = "SHOW max_connections;";
            driverName = "org.postgresql.Driver";
        }

        Class.forName(driverName);
        conn = DriverManager.getConnection(url, username, password);

        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery(queryMaxConnections);
        while (result.next()) {
            maxConnections = result.getDouble(1);
        }
        result = statement.executeQuery(queryCount);
        while (result.next()) {
            currentConnections = result.getDouble(1);
        }

        if ("sqlserver".equals(dbType)){ // 去sqlserver查询数据
            mergeEventBean.setCsdbusermaxconnectnum(maxConnections);
            mergeEventBean.setCsdbusercurconnectnum(currentConnections);
            mergeEventBean.setCsdbuserrate(Double.valueOf(df.format((currentConnections / maxConnections) * 100)));
        }else if ("postgresql".equals(dbType)){ // 去PostgreSQL查询数据
            mergeEventBean.setVcdbusermaxconnectnum(maxConnections);
            mergeEventBean.setVcdbusercurconnectnum(currentConnections);
            mergeEventBean.setVcdbuserrate(Double.valueOf(df.format((currentConnections / maxConnections) * 100)));
        }
        widenResultBean.setMergeEventBean(mergeEventBean);
        out.collect(widenResultBean);

        result.close();
        statement.close();
        conn.close();
        } catch (Exception e) {
           log.warn("程序异常:{}",e.getMessage());
            e.printStackTrace();
        }
    }

}
