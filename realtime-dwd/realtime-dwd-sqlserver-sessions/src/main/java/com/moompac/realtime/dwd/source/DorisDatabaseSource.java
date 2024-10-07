package com.moompac.realtime.dwd.source;

import com.moonpac.realtime.common.bean.dwd.DatabaseParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhanglingxing
 * @date 2024/02/28 13:50
 * 1。从doris读取vc和cs对应的数据库配置
 */

@Slf4j
public class DorisDatabaseSource extends RichSourceFunction<DatabaseParam> {

    volatile boolean flag = true;

    private Connection dorisConn;

    private Integer intervalSecond;

    @Override
    public void open(Configuration parameters) throws Exception {

        // 获取doris配置
        Map<String, String> dorisConf  = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

        String username = dorisConf.get("doris.username");
        String password = dorisConf.get("doris.password");
        String url = dorisConf.get("doris.url");
        String driver = dorisConf.get("doris.driver");
        intervalSecond = Integer.parseInt(dorisConf.getOrDefault("doris.query.interval","600"));
        // 初始化 doris 连接
        Class.forName(driver);
        dorisConn = DriverManager.getConnection(url, username, password);
        log.debug("doris connection successfully created");

    }

    @Override
    public void run(SourceContext<DatabaseParam> ctx) throws Exception {
        while (flag) {
            String sql = "SELECT a.region ,b.* FROM vcenter_db.dim_vcenter_info  a\n" +
                    "inner join vcenter_db.dim_database_info b\n" +
                    "on a.vcenter_ip = b.vcenter";
            //执行sql
            Statement stmt = dorisConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            // 处理查询结果
            while (rs.next()) {

                String vcenter = rs.getString("vcenter");
                String region = rs.getString("region");
                String db_moid = rs.getString("db_moid");
                String db_type = rs.getString("db_type");
                String db_name = rs.getString("db_name");
                String url = rs.getString("url");
                String username = rs.getString("username");
                String password = rs.getString("pwd");

                DatabaseParam databaseParam = new DatabaseParam();
                databaseParam.setVcenter(vcenter);
                databaseParam.setDbType(db_type);
                databaseParam.setDbName(db_name);
                databaseParam.setDbMoid(db_moid);
                databaseParam.setUrl(url);
                databaseParam.setUsername(username);
                databaseParam.setPassword(password);
                databaseParam.setRegion(region);
                databaseParam.setIntervalSecond(intervalSecond);
                ctx.collect(databaseParam);
                TimeUnit.SECONDS.sleep(1);
            }

            rs.close();
            stmt.close();
            TimeUnit.SECONDS.sleep(intervalSecond);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

}
